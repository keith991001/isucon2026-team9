package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

var db *sql.DB

const (
	postsPerPage  = 20
	ISO8601Format = "2006-01-02T15:04:05-07:00"
	UploadLimit   = 10 * 1024 * 1024 // 10mb
)

type User struct {
	ID          int       `db:"id"`
	AccountName string    `db:"account_name"`
	Passhash    string    `db:"passhash"`
	Authority   int       `db:"authority"`
	DelFlg      int       `db:"del_flg"`
	CreatedAt   time.Time `db:"created_at"`
}

type Post struct {
	ID           int       `db:"id"`
	UserID       int       `db:"user_id"`
	Imgdata      []byte    `db:"imgdata"`
	Body         string    `db:"body"`
	Mime         string    `db:"mime"`
	CreatedAt    time.Time `db:"created_at"`
	CommentCount int       `db:"comment_count"`
	Comments     []Comment
	User         User
	CSRFToken    string
}

type Comment struct {
	ID        int       `db:"id"`
	PostID    int       `db:"post_id"`
	UserID    int       `db:"user_id"`
	Comment   string    `db:"comment"`
	CreatedAt time.Time `db:"created_at"`
	User      User
}

// テンプレートは起動時に一度だけパースして使い回す（リクエスト毎の再パースは重い）
var (
	tmplLogin        *template.Template
	tmplRegister     *template.Template
	tmplIndex        *template.Template
	tmplUser         *template.Template
	tmplPosts        *template.Template
	tmplPostID       *template.Template
	tmplPostFragment *template.Template
	tmplBanned       *template.Template
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func queryUsers(ctx context.Context, query string, args ...any) ([]User, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]User, 0, 1024)
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.AccountName, &u.Passhash, &u.Authority, &u.DelFlg, &u.CreatedAt); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

func queryUser(ctx context.Context, query string, args ...any) (User, bool) {
	var u User
	if err := db.QueryRowContext(ctx, query, args...).Scan(&u.ID, &u.AccountName, &u.Passhash, &u.Authority, &u.DelFlg, &u.CreatedAt); err != nil {
		return User{}, false
	}
	return u, true
}

// 全投稿・全コメントをメモリに載せ、読み取りは DB を一切叩かない。
// MySQL は書き込み（永続化）専用にする。これが最大のボトルネック削減。
var (
	memMu       sync.RWMutex
	memPosts    []Post            // initialize 時点の投稿。created_at DESC, id DESC でソート済み（新しい順）
	memNewPosts []Post            // initialize 後の投稿。append 順（古い -> 新しい）
	memPostByID map[int]Post      // id -> Post
	memComments map[int][]Comment // post_id -> コメント（created_at ASC, id ASC 古い順）
	// /@user の統計をメモリで維持し、DB の全表スキャン COUNT を排除する
	memPostCntByUser      map[int]int // user_id -> 投稿数
	memCommentCntByUser   map[int]int // user_id -> 投稿したコメント数
	memCommentedCntByUser map[int]int // user_id -> 自分の投稿が受けたコメント数
	memUserByName         map[string]User
	memUsers              []User         // id -> User。ID は小さく連続するので slice で直引きする
	memPostsByUserBase    map[int][]Post // initialize 時点の user_id -> 投稿（新しい順）
	memPostsByUserNew     map[int][]Post // initialize 後の user_id -> 投稿（古い -> 新しい）
)

// 起動・initialize 時に全投稿・全コメントをメモリへロードする。
func loadMemory(ctx context.Context) {
	var posts []Post
	postRows, err := db.QueryContext(ctx, "SELECT `id`, `user_id`, `body`, `mime`, `created_at` FROM `posts` ORDER BY `created_at` DESC, `id` DESC")
	if err != nil {
		log.Print(err)
		return
	}
	for postRows.Next() {
		var p Post
		if err := postRows.Scan(&p.ID, &p.UserID, &p.Body, &p.Mime, &p.CreatedAt); err != nil {
			postRows.Close()
			log.Print(err)
			return
		}
		posts = append(posts, p)
	}
	if err := postRows.Err(); err != nil {
		postRows.Close()
		log.Print(err)
		return
	}
	postRows.Close()

	byID := make(map[int]Post, len(posts))
	postsByUser := make(map[int][]Post)
	postCnt := map[int]int{}
	for _, p := range posts {
		byID[p.ID] = p
		postsByUser[p.UserID] = append(postsByUser[p.UserID], p)
		postCnt[p.UserID]++
	}

	var comments []Comment
	commentRows, err := db.QueryContext(ctx, "SELECT `id`, `post_id`, `user_id`, `comment`, `created_at` FROM `comments` ORDER BY `created_at` ASC, `id` ASC")
	if err != nil {
		log.Print(err)
		return
	}
	for commentRows.Next() {
		var c Comment
		if err := commentRows.Scan(&c.ID, &c.PostID, &c.UserID, &c.Comment, &c.CreatedAt); err != nil {
			commentRows.Close()
			log.Print(err)
			return
		}
		comments = append(comments, c)
	}
	if err := commentRows.Err(); err != nil {
		commentRows.Close()
		log.Print(err)
		return
	}
	commentRows.Close()

	cm := make(map[int][]Comment, len(byID))
	for _, c := range comments {
		cm[c.PostID] = append(cm[c.PostID], c)
	}

	// 統計カウンタを構築
	commentCnt := map[int]int{}
	commentedCnt := map[int]int{}
	for _, c := range comments {
		commentCnt[c.UserID]++
		if owner, ok := byID[c.PostID]; ok {
			commentedCnt[owner.UserID]++
		}
	}

	memMu.Lock()
	memPosts = posts
	memNewPosts = nil
	memPostByID = byID
	memComments = cm
	memPostCntByUser = postCnt
	memCommentCntByUser = commentCnt
	memCommentedCntByUser = commentedCnt
	memPostsByUserBase = postsByUser
	memPostsByUserNew = map[int][]Post{}
	memMu.Unlock()
}

// 以下、メモリから投稿を取り出すヘルパー（コピーを返すので呼び出し側の変更は安全）。
func memTopPosts(limit int) []Post {
	memMu.RLock()
	defer memMu.RUnlock()
	if limit <= 0 {
		return []Post{}
	}
	out := make([]Post, 0, limit)
	for i := len(memNewPosts) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, memNewPosts[i])
	}
	if len(out) < limit {
		n := limit - len(out)
		if n > len(memPosts) {
			n = len(memPosts)
		}
		out = append(out, memPosts[:n]...)
	}
	return out
}

func memPostsByUser(userID, limit int) []Post {
	memMu.RLock()
	defer memMu.RUnlock()
	if limit <= 0 {
		return []Post{}
	}
	out := make([]Post, 0, limit)
	if ns := memPostsByUserNew[userID]; len(ns) > 0 {
		for i := len(ns) - 1; i >= 0 && len(out) < limit; i-- {
			out = append(out, ns[i])
		}
	}
	if len(out) < limit {
		for _, p := range memPostsByUserBase[userID] {
			out = append(out, p)
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}

func memPostsBefore(t time.Time, limit int) []Post {
	memMu.RLock()
	defer memMu.RUnlock()
	if limit <= 0 {
		return []Post{}
	}
	out := make([]Post, 0, limit)
	for i := len(memNewPosts) - 1; i >= 0 && len(out) < limit; i-- {
		p := memNewPosts[i]
		if !p.CreatedAt.After(t) { // created_at <= t
			out = append(out, p)
		}
	}
	if len(out) < limit {
		start := sort.Search(len(memPosts), func(i int) bool {
			return !memPosts[i].CreatedAt.After(t)
		})
		for i := start; i < len(memPosts) && len(out) < limit; i++ {
			out = append(out, memPosts[i])
		}
	}
	return out
}

func memGetPost(id int) (Post, bool) {
	memMu.RLock()
	defer memMu.RUnlock()
	p, ok := memPostByID[id]
	return p, ok
}

// HTML 断片もプロセス内にキャッシュし、リクエスト毎の memcache システムコールを排除する。
type htmlEntry struct {
	html   string
	expiry time.Time
}

var htmlCache sync.Map // map[string]htmlEntry

func getHTMLCache(key string) (string, bool) {
	if v, ok := htmlCache.Load(key); ok {
		e := v.(htmlEntry)
		if time.Now().Before(e.expiry) {
			return e.html, true
		}
	}
	return "", false
}

func setHTMLCache(key, html string, ttl time.Duration) {
	htmlCache.Store(key, htmlEntry{html: html, expiry: time.Now().Add(ttl)})
}

func delHTMLCache(key string) {
	htmlCache.Delete(key)
}

func delHTMLCachePrefix(prefix string) {
	htmlCache.Range(func(k, v any) bool {
		if s, ok := k.(string); ok && strings.HasPrefix(s, prefix) {
			htmlCache.Delete(s)
		}
		return true
	})
}

// 投稿 HTML 内に埋め込む CSRF トークンのプレースホルダ（リクエスト毎に実トークンへ置換）。
const csrfPlaceholder = "----CSRF-PLACEHOLDER-9e3f7a21----"

// id 指定の User をプロセス内キャッシュ優先で取得する。
func getUserByID(ctx context.Context, id int) (User, bool) {
	memMu.RLock()
	if id > 0 && id < len(memUsers) {
		u := memUsers[id]
		memMu.RUnlock()
		if u.ID != 0 {
			return u, true
		}
	} else {
		memMu.RUnlock()
	}

	u, ok := queryUser(ctx, "SELECT `id`, `account_name`, `passhash`, `authority`, `del_flg`, `created_at` FROM `users` WHERE `id` = ?", id)
	if !ok {
		return User{}, false
	}
	memMu.Lock()
	if id >= len(memUsers) {
		ns := make([]User, id+1)
		copy(ns, memUsers)
		memUsers = ns
	}
	memUsers[id] = u
	if memUserByName == nil {
		memUserByName = map[string]User{}
	}
	memUserByName[u.AccountName] = u
	memMu.Unlock()
	return u, true
}

func invalidateIndexCache() {
	rebuildIndexSnap(context.Background())
}

func setupTemplates() {
	fmap := template.FuncMap{
		"imageURL": imageURL,
	}
	tmplLogin = template.Must(template.ParseFiles(
		getTemplPath("layout.html"), getTemplPath("login.html")))
	tmplRegister = template.Must(template.ParseFiles(
		getTemplPath("layout.html"), getTemplPath("register.html")))
	tmplBanned = template.Must(template.ParseFiles(
		getTemplPath("layout.html"), getTemplPath("banned.html")))
	tmplIndex = template.Must(template.New("layout.html").Funcs(fmap).ParseFiles(
		getTemplPath("layout.html"),
		getTemplPath("index.html"),
		getTemplPath("posts.html"),
		getTemplPath("post.html"),
	))
	tmplUser = template.Must(template.New("layout.html").Funcs(fmap).ParseFiles(
		getTemplPath("layout.html"),
		getTemplPath("user.html"),
		getTemplPath("posts.html"),
		getTemplPath("post.html"),
	))
	tmplPosts = template.Must(template.New("posts.html").Funcs(fmap).ParseFiles(
		getTemplPath("posts.html"),
		getTemplPath("post.html"),
	))
	tmplPostID = template.Must(template.New("layout.html").Funcs(fmap).ParseFiles(
		getTemplPath("layout.html"),
		getTemplPath("post_id.html"),
		getTemplPath("post.html"),
	))
	tmplPostFragment = template.Must(template.New("post.html").Funcs(fmap).ParseFiles(
		getTemplPath("post.html"),
	))
}

func postCacheKey(id int) string {
	return "post:html:" + strconv.Itoa(id)
}

// ベンチ開始直後のコールドスタート（キャッシュミスの嵐）を避けるための事前ウォームアップ。
func warmCaches(ctx context.Context) {
	// 全ユーザーをプロセス内キャッシュに載せる（約1000件）。
	// 以降 getSessionUser / makePosts は DB を叩かなくなる。
	if users, err := queryUsers(ctx, "SELECT `id`, `account_name`, `passhash`, `authority`, `del_flg`, `created_at` FROM `users`"); err == nil {
		maxID := 0
		for _, u := range users {
			if u.ID > maxID {
				maxID = u.ID
			}
		}
		byID := make([]User, maxID+1)
		byName := make(map[string]User, len(users))
		for _, u := range users {
			byID[u.ID] = u
			byName[u.AccountName] = u
		}
		memMu.Lock()
		memUsers = byID
		memUserByName = byName
		memMu.Unlock()
	}
	// 全投稿・全コメントをメモリへロード（読み取りは以降 DB を叩かない）
	loadMemory(ctx)
	// トップページの HTML キャッシュを温める
	rebuildIndexSnap(ctx)
}

func dbInitialize(ctx context.Context) {
	sqls := []string{
		"DELETE FROM users WHERE id > 1000",
		"DELETE FROM posts WHERE id > 10000",
		"DELETE FROM comments WHERE id > 100000",
		"UPDATE users SET del_flg = 0",
		"UPDATE users SET del_flg = 1 WHERE id % 50 = 0",
	}

	for _, sql := range sqls {
		db.ExecContext(ctx, sql)
	}
}

func tryLogin(ctx context.Context, accountName, password string) *User {
	memMu.RLock()
	u, ok := memUserByName[accountName]
	memMu.RUnlock()
	if !ok || u.DelFlg != 0 {
		return nil
	}

	if calculatePasshash(u.AccountName, password) == u.Passhash {
		return &u
	}
	return nil
}

func validateUser(accountName, password string) bool {
	if len(accountName) < 3 || len(password) < 6 {
		return false
	}
	for i := 0; i < len(accountName); i++ {
		c := accountName[i]
		if !('0' <= c && c <= '9') && !('a' <= c && c <= 'z') && !('A' <= c && c <= 'Z') && c != '_' {
			return false
		}
	}
	for i := 0; i < len(password); i++ {
		c := password[i]
		if !('0' <= c && c <= '9') && !('a' <= c && c <= 'z') && !('A' <= c && c <= 'Z') && c != '_' {
			return false
		}
	}
	return true
}

// 元実装は openssl コマンドを毎回起動して SHA-512 を計算していた。
// Go 標準ライブラリで同じ hex 文字列を計算する（出力は openssl と完全一致）。
func digest(src string) string {
	sum := sha512.Sum512([]byte(src))
	return hex.EncodeToString(sum[:])
}

func calculateSalt(accountName string) string {
	return digest(accountName)
}

func calculatePasshash(accountName, password string) string {
	return digest(password + ":" + calculateSalt(accountName))
}

const (
	sessionCookieName = "isu_session"
	flashCookieName   = "isu_notice"
)

type sessionData struct {
	uid  int
	csrf string
}

var sessionStore sync.Map // sid -> sessionData

func decodeSession(r *http.Request) (int, string, bool) {
	c, err := r.Cookie(sessionCookieName)
	if err != nil || c.Value == "" {
		return 0, "", false
	}
	v, ok := sessionStore.Load(c.Value)
	if !ok {
		return 0, "", false
	}
	s := v.(sessionData)
	return s.uid, s.csrf, true
}

func setCookie(w http.ResponseWriter, name, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
	})
}

func setLoginCookie(w http.ResponseWriter, uid int64, csrf string) {
	sid := secureRandomStr(16)
	sessionStore.Store(sid, sessionData{uid: int(uid), csrf: csrf})
	setCookie(w, sessionCookieName, sid, 86400)
}

func clearCookie(w http.ResponseWriter, name string) {
	setCookie(w, name, "", -1)
}

func clearSession(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookieName); err == nil && c.Value != "" {
		sessionStore.Delete(c.Value)
	}
	clearCookie(w, sessionCookieName)
}

func setFlash(w http.ResponseWriter, value string) {
	setCookie(w, flashCookieName, url.QueryEscape(value), 60)
}

func getSessionUser(r *http.Request) User {
	ctx := r.Context()
	id, _, ok := decodeSession(r)
	if !ok {
		return User{}
	}

	u, _ := getUserByID(ctx, id)
	return u
}

func getSessionUserAndCSRF(r *http.Request) (User, string) {
	id, csrf, ok := decodeSession(r)
	if !ok {
		return User{}, ""
	}
	u, _ := getUserByID(r.Context(), id)
	return u, csrf
}

func getFlash(w http.ResponseWriter, r *http.Request, key string) string {
	if key != "notice" {
		return ""
	}
	c, err := r.Cookie(flashCookieName)
	if err != nil || c.Value == "" {
		return ""
	}
	clearCookie(w, flashCookieName)
	value, err := url.QueryUnescape(c.Value)
	if err != nil {
		return c.Value
	}
	return value
}

// 元実装は投稿ごとに「コメント数・コメント・各ユーザー」を個別クエリしていた（N+1）。
// 対象投稿を先に絞り込み、コメント数/コメント/ユーザーをまとめて取得する。
func makePosts(ctx context.Context, results []Post, csrfToken string, allComments bool) ([]Post, error) {
	if len(results) == 0 {
		return []Post{}, nil
	}

	_ = ctx
	selected := make([]Post, 0, postsPerPage)
	memMu.RLock()
	for _, p := range results {
		if p.UserID <= 0 || p.UserID >= len(memUsers) {
			continue
		}
		user := memUsers[p.UserID]
		if user.ID == 0 || user.DelFlg != 0 {
			continue
		}
		p.User = user
		p.CSRFToken = csrfToken
		selected = append(selected, p)
		if len(selected) >= postsPerPage {
			break
		}
	}
	if len(selected) == 0 {
		memMu.RUnlock()
		return []Post{}, nil
	}

	// コメント数・コメント本体はすべてメモリから取得する（DB を叩かない）。
	// memComments は created_at 昇順（古い順）。表示も古い順。
	for i := range selected {
		p := &selected[i]
		cs := memComments[p.ID]
		p.CommentCount = len(cs)
		var disp []Comment
		if allComments || len(cs) <= 3 {
			disp = cs
		} else {
			disp = cs[len(cs)-3:] // 最新3件（古い順のまま）
		}
		out := make([]Comment, len(disp))
		copy(out, disp)
		for j := range out {
			uid := out[j].UserID
			if uid > 0 && uid < len(memUsers) && memUsers[uid].ID != 0 {
				u := memUsers[uid]
				out[j].User = u
			}
		}
		p.Comments = out
	}
	memMu.RUnlock()

	return selected, nil
}

func imageURL(p Post) string {
	ext := ""
	if p.Mime == "image/jpeg" {
		ext = ".jpg"
	} else if p.Mime == "image/png" {
		ext = ".png"
	} else if p.Mime == "image/gif" {
		ext = ".gif"
	}

	return "/image/" + strconv.Itoa(p.ID) + ext
}

func isLogin(u User) bool {
	return u.ID != 0
}

func getCSRFToken(r *http.Request) string {
	_, csrfToken, ok := decodeSession(r)
	if !ok {
		return ""
	}
	return csrfToken
}

func secureRandomStr(b int) string {
	k := make([]byte, b)
	if _, err := crand.Read(k); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", k)
}

func getTemplPath(filename string) string {
	return path.Join("templates", filename)
}

type appRouter struct {
	static http.Handler
}

func validAccountPath(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('0' <= c && c <= '9') && !('a' <= c && c <= 'z') && !('A' <= c && c <= 'Z') && c != '_' {
			return false
		}
	}
	return true
}

func (rt appRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch r.Method {
	case http.MethodGet:
		switch {
		case p == "/initialize":
			getInitialize(w, r)
			return
		case p == "/login":
			getLogin(w, r)
			return
		case p == "/register":
			getRegister(w, r)
			return
		case p == "/logout":
			getLogout(w, r)
			return
		case p == "/":
			getIndex(w, r)
			return
		case p == "/posts":
			getPosts(w, r)
			return
		case p == "/admin/banned":
			getAdminBanned(w, r)
			return
		case strings.HasPrefix(p, "/posts/"):
			id := p[len("/posts/"):]
			if id != "" && strings.IndexByte(id, '/') == -1 {
				r.SetPathValue("id", id)
				getPostsID(w, r)
				return
			}
		case strings.HasPrefix(p, "/image/"):
			name := p[len("/image/"):]
			if slash := strings.IndexByte(name, '/'); slash == -1 {
				if dot := strings.LastIndexByte(name, '.'); dot > 0 && dot < len(name)-1 {
					r.SetPathValue("id", name[:dot])
					r.SetPathValue("ext", name[dot+1:])
					getImage(w, r)
					return
				}
			}
		case strings.HasPrefix(p, "/@"):
			accountName := p[2:]
			if validAccountPath(accountName) {
				r.SetPathValue("accountName", accountName)
				getAccountName(w, r)
				return
			}
		}
	case http.MethodPost:
		switch p {
		case "/login":
			postLogin(w, r)
			return
		case "/register":
			postRegister(w, r)
			return
		case "/":
			postIndex(w, r)
			return
		case "/comment":
			postComment(w, r)
			return
		case "/admin/banned":
			postAdminBanned(w, r)
			return
		}
	}

	rt.static.ServeHTTP(w, r)
}

func getInitialize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbInitialize(ctx)
	// 前回ベンチのキャッシュ（特に del_flg）を持ち越さないよう全消去する
	htmlCache.Clear()
	sessionStore.Clear()
	// ベンチ開始時のコールドスタートを避けるためキャッシュを温めておく
	warmCaches(ctx)
	w.WriteHeader(http.StatusOK)
}

func getLogin(w http.ResponseWriter, r *http.Request) {
	me := getSessionUser(r)

	if isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	tmplLogin.Execute(w, struct {
		Me    User
		Flash string
	}{me, getFlash(w, r, "notice")})
}

func postLogin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isLogin(getSessionUser(r)) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	u := tryLogin(ctx, r.FormValue("account_name"), r.FormValue("password"))

	if u != nil {
		setLoginCookie(w, int64(u.ID), secureRandomStr(16))

		http.Redirect(w, r, "/", http.StatusFound)
	} else {
		setFlash(w, "アカウント名かパスワードが間違っています")

		http.Redirect(w, r, "/login", http.StatusFound)
	}
}

func getRegister(w http.ResponseWriter, r *http.Request) {
	if isLogin(getSessionUser(r)) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	tmplRegister.Execute(w, struct {
		Me    User
		Flash string
	}{User{}, getFlash(w, r, "notice")})
}

func postRegister(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if isLogin(getSessionUser(r)) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	accountName, password := r.FormValue("account_name"), r.FormValue("password")

	validated := validateUser(accountName, password)
	if !validated {
		setFlash(w, "アカウント名は3文字以上、パスワードは6文字以上である必要があります")

		http.Redirect(w, r, "/register", http.StatusFound)
		return
	}

	memMu.RLock()
	_, exists := memUserByName[accountName]
	memMu.RUnlock()
	if exists {
		setFlash(w, "アカウント名がすでに使われています")

		http.Redirect(w, r, "/register", http.StatusFound)
		return
	}

	passhash := calculatePasshash(accountName, password)
	query := "INSERT INTO `users` (`account_name`, `passhash`) VALUES (?,?)"
	result, err := db.ExecContext(ctx, query, accountName, passhash)
	if err != nil {
		log.Print(err)
		return
	}

	uid, err := result.LastInsertId()
	if err != nil {
		log.Print(err)
		return
	}
	// 新規ユーザーをメモリへ反映
	nu := User{ID: int(uid), AccountName: accountName, Passhash: passhash, Authority: 0, DelFlg: 0, CreatedAt: time.Now()}
	memMu.Lock()
	if nu.ID >= len(memUsers) {
		ns := make([]User, nu.ID+1)
		copy(ns, memUsers)
		memUsers = ns
	}
	memUsers[nu.ID] = nu
	if memUserByName == nil {
		memUserByName = map[string]User{}
	}
	memUserByName[accountName] = nu
	if memPostCntByUser == nil {
		memPostCntByUser = map[int]int{}
	}
	if memCommentCntByUser == nil {
		memCommentCntByUser = map[int]int{}
	}
	if memCommentedCntByUser == nil {
		memCommentedCntByUser = map[int]int{}
	}
	memMu.Unlock()
	setLoginCookie(w, uid, secureRandomStr(16))

	http.Redirect(w, r, "/", http.StatusFound)
}

func getLogout(w http.ResponseWriter, r *http.Request) {
	clearSession(w, r)
	clearCookie(w, flashCookieName)

	http.Redirect(w, r, "/", http.StatusFound)
}

// トップページの投稿一覧フラグメントをメモリから生成する（DB 不使用）。
func buildIndexFragment(ctx context.Context) string {
	results := memTopPosts(100)
	posts, err := makePosts(ctx, results, csrfPlaceholder, false)
	if err != nil {
		return ""
	}
	var buf strings.Builder
	if err := tmplPosts.Execute(&buf, posts); err != nil {
		return ""
	}
	return buf.String()
}

// レイアウトの静的部分（テンプレート実行を避けて文字列連結で組み立てる）
const layoutHead = `<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Iscogram</title>
    <link href="/css/style.css" media="screen" rel="stylesheet" type="text/css">
  </head>
  <body>
    <div class="container">
      <div class="header">
        <div class="isu-title">
          <h1><a href="/">Iscogram</a></h1>
        </div>
        <div class="isu-header-menu">
`

const layoutTail = `    </div>
    <script src="/js/timeago.min.js"></script>
    <script src="/js/main.js"></script>
  </body>
</html>
`

// ログイン状態に応じたヘッダーメニュー HTML を組み立てる。
// アカウント名は [0-9a-zA-Z_] のみなのでエスケープ不要。
func headerMenuHTML(me User) string {
	if me.ID == 0 {
		return `          <div><a href="/login">ログイン</a></div>` + "\n"
	}
	s := `          <div><a href="/@` + me.AccountName + `"><span class="isu-account-name">` + me.AccountName + `</span>さん</a></div>` + "\n"
	if me.Authority == 1 {
		s += `          <div><a href="/admin/banned">管理者用ページ</a></div>` + "\n"
	}
	s += `          <div><a href="/logout">ログアウト</a></div>` + "\n"
	return s
}

// 投稿一覧フラグメントを CSRF プレースホルダで分割した [][]byte を保持する。
type indexSnapshot struct {
	segs [][]byte
}

var indexSnap atomic.Pointer[indexSnapshot]

// スナップショットを再構築して atomic swap する。
func rebuildIndexSnap(ctx context.Context) {
	frag := buildIndexFragment(ctx)
	segs := bytes.Split([]byte(frag), []byte(csrfPlaceholder))
	indexSnap.Store(&indexSnapshot{segs: segs})
}

// 読み取りはロックフリー（atomic.Load）。キャッシュの更新は投稿・BAN・initialize で明示的に行う。
func getIndexSegments(ctx context.Context) [][]byte {
	if s := indexSnap.Load(); s != nil {
		return s.segs
	}
	rebuildIndexSnap(ctx)
	if s := indexSnap.Load(); s != nil {
		return s.segs
	}
	return nil
}

const idxMoreBtn = "\n<div id=\"isu-post-more\">\n  <button id=\"isu-post-more-btn\">もっと見る</button>\n  <img class=\"isu-loading-icon\" src=\"/img/ajax-loader.gif\">\n</div>\n"

func getIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me, csrf := getSessionUserAndCSRF(r)

	segs := getIndexSegments(ctx)
	csrfB := []byte(csrf)
	flash := getFlash(w, r, "notice")

	w.Header().Set("Content-Type", "text/html;charset=utf-8")
	io.WriteString(w, layoutHead)
	io.WriteString(w, headerMenuHTML(me))
	io.WriteString(w, "        </div>\n      </div>\n\n")
	// 投稿フォーム（小さいので都度組み立て）
	io.WriteString(w, `<div class="isu-submit">
  <form method="post" action="/" enctype="multipart/form-data">
    <div class="isu-form">
      <input type="file" name="file" value="file">
    </div>
    <div class="isu-form">
      <textarea name="body"></textarea>
    </div>
    <div class="form-submit">
      <input type="hidden" name="csrf_token" value="`)
	w.Write(csrfB)
	io.WriteString(w, "\">\n      <input type=\"submit\" name=\"submit\" value=\"submit\">\n    </div>\n")
	if flash != "" {
		io.WriteString(w, `    <div id="notice-message" class="alert alert-danger">`+"\n      "+flash+"\n    </div>\n")
	}
	io.WriteString(w, "  </form>\n</div>\n\n")
	// 投稿一覧：セグメントを CSRF を挟みながら直接 write（ReplaceAll なし）
	for i, s := range segs {
		if i > 0 {
			w.Write(csrfB)
		}
		w.Write(s)
	}
	io.WriteString(w, idxMoreBtn)
	io.WriteString(w, layoutTail)
}

func getAccountName(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	accountName := r.PathValue("accountName")
	me, csrf := getSessionUserAndCSRF(r)

	memMu.RLock()
	user, ok := memUserByName[accountName]
	memMu.RUnlock()
	if !ok || user.ID == 0 || user.DelFlg != 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// ユーザーの投稿一覧 HTML（全閲覧者共通）をキャッシュ優先で取得する
	postsFragment, err := getUserPostsHTML(ctx, user.ID, accountName)
	if err != nil {
		log.Print(err)
		return
	}
	postsHTML := template.HTML(strings.ReplaceAll(postsFragment, csrfPlaceholder, csrf))

	// 統計はメモリのカウンタから取得（DB の全表スキャン COUNT を排除）
	memMu.RLock()
	commentCount := memCommentCntByUser[user.ID]
	postCount := memPostCntByUser[user.ID]
	commentedCount := memCommentedCntByUser[user.ID]
	memMu.RUnlock()

	tmplUser.Execute(w, struct {
		PostsHTML      template.HTML
		User           User
		PostCount      int
		CommentCount   int
		CommentedCount int
		Me             User
	}{postsHTML, user, postCount, commentCount, commentedCount, me})
}

// ユーザーの投稿一覧 HTML（全閲覧者共通）を取得する。更新イベントで明示的に失効する。
func getUserPostsHTML(ctx context.Context, userID int, accountName string) (string, error) {
	key := "user_posts:" + accountName
	if html, ok := getHTMLCache(key); ok {
		return html, nil
	}

	results := memPostsByUser(userID, 100)

	posts, err := makePosts(ctx, results, csrfPlaceholder, false)
	if err != nil {
		return "", err
	}

	var buf strings.Builder
	if err := tmplPosts.Execute(&buf, posts); err != nil {
		return "", err
	}
	html := buf.String()
	setHTMLCache(key, html, 60*time.Second)

	return html, nil
}

// 「もっと見る」ページ（max_created_at 指定、全ユーザー共通）の HTML を取得する。
// 分頁の境界は安定していてキー数が限られるため、HTML 片段キャッシュがよく効く。
func getPostsListHTML(ctx context.Context, maxCreatedAt string) (string, bool, error) {
	key := "posts:" + maxCreatedAt
	if html, ok := getHTMLCache(key); ok {
		return html, true, nil
	}

	t, err := time.Parse(ISO8601Format, maxCreatedAt)
	if err != nil {
		return "", false, err
	}

	results := memPostsBefore(t, 100)

	posts, err := makePosts(ctx, results, csrfPlaceholder, false)
	if err != nil {
		return "", false, err
	}
	if len(posts) == 0 {
		return "", false, nil
	}

	var buf strings.Builder
	if err := tmplPosts.Execute(&buf, posts); err != nil {
		return "", false, err
	}
	html := buf.String()
	// 分頁の古い投稿は変化が少ないため、長めの TTL でキャッシュ命中率を上げる。
	setHTMLCache(key, html, 30*time.Second)

	return html, true, nil
}

func getPosts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	m, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Print(err)
		return
	}
	maxCreatedAt := m.Get("max_created_at")
	if maxCreatedAt == "" {
		return
	}

	fragment, found, err := getPostsListHTML(ctx, maxCreatedAt)
	if err != nil {
		log.Print(err)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Write([]byte(strings.ReplaceAll(fragment, csrfPlaceholder, getCSRFToken(r))))
}

// 単一投稿の HTML（全ユーザー共通）をキャッシュ優先で取得する。
// CSRF はプレースホルダで埋め込み、見つからない場合は found=false を返す。
func getPostHTML(ctx context.Context, pid int) (string, bool, error) {
	if html, ok := getHTMLCache(postCacheKey(pid)); ok {
		return html, true, nil
	}

	p, ok := memGetPost(pid)
	if !ok {
		return "", false, nil
	}
	results := []Post{p}

	posts, err := makePosts(ctx, results, csrfPlaceholder, true)
	if err != nil {
		return "", false, err
	}
	if len(posts) == 0 {
		return "", false, nil
	}

	var buf strings.Builder
	if err := tmplPostFragment.Execute(&buf, posts[0]); err != nil {
		return "", false, err
	}
	html := buf.String()
	setHTMLCache(postCacheKey(pid), html, 60*time.Second)

	return html, true, nil
}

func getPostsID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me, csrf := getSessionUserAndCSRF(r)
	pidStr := r.PathValue("id")
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	fragment, found, err := getPostHTML(ctx, pid)
	if err != nil {
		log.Print(err)
		return
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	postHTML := template.HTML(strings.ReplaceAll(fragment, csrfPlaceholder, csrf))

	tmplPostID.Execute(w, struct {
		PostHTML template.HTML
		Me       User
	}{postHTML, me})
}

func postIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me, csrf := getSessionUserAndCSRF(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if r.FormValue("csrf_token") != csrf {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		setFlash(w, "画像が必須です")

		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	defer file.Close()

	mime := ""
	ext := ""
	if file != nil {
		// 投稿のContent-Typeからファイルのタイプを決定する
		contentType := header.Header["Content-Type"][0]
		if strings.Contains(contentType, "jpeg") {
			mime = "image/jpeg"
			ext = "jpg"
		} else if strings.Contains(contentType, "png") {
			mime = "image/png"
			ext = "png"
		} else if strings.Contains(contentType, "gif") {
			mime = "image/gif"
			ext = "gif"
		} else {
			setFlash(w, "投稿できる画像形式はjpgとpngとgifだけです")

			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
	}

	if header.Size > UploadLimit {
		setFlash(w, "ファイルサイズが大きすぎます")

		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	// 画像はファイルに書き出して配信するため DB には blob を保存しない（巨大 blob の
	// MySQL 書き込みを省いて INSERT を高速化する）。配信はすべてファイル経由。
	query := "INSERT INTO `posts` (`user_id`, `mime`, `imgdata`, `body`) VALUES (?,?,?,?)"
	result, err := db.ExecContext(
		ctx,
		query,
		me.ID,
		mime,
		[]byte{},
		r.FormValue("body"),
	)
	if err != nil {
		log.Print(err)
		return
	}

	pid, err := result.LastInsertId()
	if err != nil {
		log.Print(err)
		return
	}

	// 画像をファイルにも書き出す。以降の GET /image/:id は Nginx が直接配信する。
	if ext != "" {
		dst, err := os.Create(fmt.Sprintf("../public/image/%d.%s", pid, ext))
		if err != nil {
			log.Print(err)
		} else {
			if _, err := io.Copy(dst, file); err != nil {
				log.Print(err)
			}
			if err := dst.Close(); err != nil {
				log.Print(err)
			}
		}
	}

	// メモリ状態へ新規投稿を反映。initialize 時点の大きな memPosts はコピーせず、
	// 増分スライスに append して投稿時の O(N) コピーを消す。
	np := Post{ID: int(pid), UserID: me.ID, Body: r.FormValue("body"), Mime: mime, CreatedAt: time.Now()}
	memMu.Lock()
	if memPostByID == nil {
		memPostByID = map[int]Post{}
	}
	if memPostCntByUser == nil {
		memPostCntByUser = map[int]int{}
	}
	if memPostsByUserNew == nil {
		memPostsByUserNew = map[int][]Post{}
	}
	memNewPosts = append(memNewPosts, np)
	memPostsByUserNew[me.ID] = append(memPostsByUserNew[me.ID], np)
	memPostByID[np.ID] = np
	memPostCntByUser[me.ID]++
	memMu.Unlock()

	// 新しい投稿はトップページに出るのでキャッシュを無効化する
	invalidateIndexCache()
	delHTMLCache("user_posts:" + me.AccountName)

	http.Redirect(w, r, "/posts/"+strconv.FormatInt(pid, 10), http.StatusFound)
}

func getImage(w http.ResponseWriter, r *http.Request) {
	// 通常は Nginx がファイルを直接配信する。ここはフォールバック。
	// 画像はすべて public/image/ 配下のファイルから配信する（DB の blob は使わない）。
	pidStr := r.PathValue("id")
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	ext := r.PathValue("ext")
	if ext != "jpg" && ext != "png" && ext != "gif" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	http.ServeFile(w, r, fmt.Sprintf("%s/%d.%s", image_dir(), pid, ext))
}

func image_dir() string {
	return "../public/image"
}

func postComment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me, csrf := getSessionUserAndCSRF(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if r.FormValue("csrf_token") != csrf {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	postID, err := strconv.Atoi(r.FormValue("post_id"))
	if err != nil {
		log.Print("post_idは整数のみです")
		return
	}

	body := r.FormValue("comment")

	query := "INSERT INTO `comments` (`post_id`, `user_id`, `comment`) VALUES (?,?,?)"
	if _, err := db.ExecContext(ctx, query, postID, me.ID, body); err != nil {
		log.Print(err)
		return
	}

	// DB 書き込み成功後にメモリ状態も同期更新する。読み取りは以降メモリだけを見る。
	nc := Comment{ID: 0, PostID: postID, UserID: me.ID, Comment: body, CreatedAt: time.Now()}

	ownerInIndex := false
	ownerAccountName := ""
	memMu.Lock()
	if memComments == nil {
		memComments = map[int][]Comment{}
	}
	if memCommentCntByUser == nil {
		memCommentCntByUser = map[int]int{}
	}
	if memCommentedCntByUser == nil {
		memCommentedCntByUser = map[int]int{}
	}
	memComments[postID] = append(memComments[postID], nc)
	memCommentCntByUser[me.ID]++
	if owner, ok := memPostByID[postID]; ok {
		memCommentedCntByUser[owner.UserID]++
		if owner.UserID > 0 && owner.UserID < len(memUsers) {
			ownerAccountName = memUsers[owner.UserID].AccountName
		}
	}
	for i := len(memNewPosts) - 1; i >= 0 && i >= len(memNewPosts)-postsPerPage; i-- {
		if memNewPosts[i].ID == postID {
			ownerInIndex = true
			break
		}
	}
	if !ownerInIndex {
		remaining := postsPerPage - len(memNewPosts)
		if remaining > 0 && remaining > len(memPosts) {
			remaining = len(memPosts)
		}
		for i := 0; i < remaining; i++ {
			if memPosts[i].ID == postID {
				ownerInIndex = true
				break
			}
		}
	}
	memMu.Unlock()

	delHTMLCache(postCacheKey(postID))
	if ownerAccountName != "" {
		delHTMLCache("user_posts:" + ownerAccountName)
	}
	delHTMLCachePrefix("posts:")
	if ownerInIndex {
		invalidateIndexCache()
	}

	http.Redirect(w, r, fmt.Sprintf("/posts/%d", postID), http.StatusFound)
}

func getAdminBanned(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me, csrf := getSessionUserAndCSRF(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if me.Authority == 0 {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	users, err := queryUsers(ctx, "SELECT `id`, `account_name`, `passhash`, `authority`, `del_flg`, `created_at` FROM `users` WHERE `authority` = 0 AND `del_flg` = 0 ORDER BY `created_at` DESC")
	if err != nil {
		log.Print(err)
		return
	}

	tmplBanned.Execute(w, struct {
		Users     []User
		Me        User
		CSRFToken string
	}{users, me, csrf})
}

func postAdminBanned(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me, csrf := getSessionUserAndCSRF(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if me.Authority == 0 {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if r.FormValue("csrf_token") != csrf {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	query := "UPDATE `users` SET `del_flg` = ? WHERE `id` = ?"

	err := r.ParseForm()
	if err != nil {
		log.Print(err)
		return
	}

	for _, id := range r.Form["uid[]"] {
		db.ExecContext(ctx, query, 1, id)
		if n, err := strconv.Atoi(id); err == nil {
			// メモリ上の User も del_flg=1 に更新（投稿が一覧から消える）
			if u, ok := getUserByID(ctx, n); ok {
				u.DelFlg = 1
				memMu.Lock()
				if n > 0 && n < len(memUsers) {
					memUsers[n] = u
				}
				memUserByName[u.AccountName] = u
				memMu.Unlock()
			}
		}
	}
	// 凍結されたユーザーの投稿はトップページから消える必要がある
	htmlCache.Clear()
	rebuildIndexSnap(ctx)

	http.Redirect(w, r, "/admin/banned", http.StatusFound)
}

func main() {
	host := os.Getenv("ISUCONP_DB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("ISUCONP_DB_PORT")
	if port == "" {
		port = "3306"
	}
	_, err := strconv.Atoi(port)
	if err != nil {
		log.Fatalf("Failed to read DB port number from an environment variable ISUCONP_DB_PORT.\nError: %s", err.Error())
	}
	user := os.Getenv("ISUCONP_DB_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("ISUCONP_DB_PASSWORD")
	dbname := os.Getenv("ISUCONP_DB_NAME")
	if dbname == "" {
		dbname = "isuconp"
	}

	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	cfg.DBName = dbname
	cfg.Params = map[string]string{
		"charset": "utf8mb4",
		// クライアント側でパラメータを展開し、サーバー側 prepared statement の往復を省く
		"interpolateParams": "true",
	}
	cfg.ParseTime = true
	cfg.Loc = time.Local
	dsn := cfg.FormatDSN()

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %s.", err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(40)
	db.SetMaxIdleConns(40)
	db.SetConnMaxLifetime(0)

	setupTemplates()

	// pprof（プロファイリング用、localhost のみ）
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.Fatal(http.ListenAndServe(":8080", appRouter{static: http.FileServer(http.Dir("../public"))}))
}
