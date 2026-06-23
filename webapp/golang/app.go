package main

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"crypto/sha512"
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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/gorilla/sessions"
	"github.com/jmoiron/sqlx"
)

var (
	db    *sqlx.DB
	store *sessions.CookieStore
)

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
	// session は memcache ではなく署名付き Cookie に保存する。
	// これにより全リクエストから session の memcache 往復（ネットワーク I/O）が消える。
	store = sessions.NewCookieStore([]byte("sendagaya"))
	store.Options = &sessions.Options{Path: "/", MaxAge: 86400, HttpOnly: true}
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

// ユーザーは滅多に変わらないのでプロセス内にキャッシュする（memcache 往復と JSON を排除）。
// 単一プロセスなので一貫性は問題ない。BAN と initialize 時に無効化する。
var userCache sync.Map // map[int]User

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
	memPostsByUserBase    map[int][]Post // initialize 時点の user_id -> 投稿（新しい順）
	memPostsByUserNew     map[int][]Post // initialize 後の user_id -> 投稿（古い -> 新しい）
)

// 起動・initialize 時に全投稿・全コメントをメモリへロードする。
func loadMemory(ctx context.Context) {
	var posts []Post
	if err := db.SelectContext(ctx, &posts, "SELECT `id`, `user_id`, `body`, `mime`, `created_at` FROM `posts` ORDER BY `created_at` DESC, `id` DESC"); err != nil {
		log.Print(err)
		return
	}
	byID := make(map[int]Post, len(posts))
	postsByUser := make(map[int][]Post)
	postCnt := map[int]int{}
	for _, p := range posts {
		byID[p.ID] = p
		postsByUser[p.UserID] = append(postsByUser[p.UserID], p)
		postCnt[p.UserID]++
	}

	var comments []Comment
	if err := db.SelectContext(ctx, &comments, "SELECT `id`, `post_id`, `user_id`, `comment`, `created_at` FROM `comments` ORDER BY `created_at` ASC, `id` ASC"); err != nil {
		log.Print(err)
		return
	}
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
	nameMap := map[string]User{}
	userCache.Range(func(k, v any) bool {
		u := v.(User)
		nameMap[u.AccountName] = u
		return true
	})
	memUserByName = nameMap
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

type commentInsert struct {
	postID int
	userID int
	body   string
}

var (
	commentInsertCh = make(chan commentInsert, 8192)
	commentFlushCh  = make(chan chan struct{})
)

func insertCommentRows(batch []commentInsert) {
	if len(batch) == 0 {
		return
	}
	if len(batch) == 1 {
		c := batch[0]
		if _, err := db.Exec("INSERT INTO `comments` (`post_id`, `user_id`, `comment`) VALUES (?,?,?)", c.postID, c.userID, c.body); err != nil {
			log.Print(err)
		}
		return
	}

	var b strings.Builder
	b.Grow(64 + len(batch)*8)
	b.WriteString("INSERT INTO `comments` (`post_id`, `user_id`, `comment`) VALUES ")
	args := make([]any, 0, len(batch)*3)
	for i, c := range batch {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?)")
		args = append(args, c.postID, c.userID, c.body)
	}
	if _, err := db.Exec(b.String(), args...); err != nil {
		log.Print(err)
		for _, c := range batch {
			if _, err := db.Exec("INSERT INTO `comments` (`post_id`, `user_id`, `comment`) VALUES (?,?,?)", c.postID, c.userID, c.body); err != nil {
				log.Print(err)
			}
		}
	}
}

func drainCommentQueue(batch []commentInsert) []commentInsert {
	for {
		select {
		case c := <-commentInsertCh:
			batch = append(batch, c)
		default:
			return batch
		}
	}
}

func commentInsertWorker() {
	const (
		maxBatch      = 256
		flushInterval = 5 * time.Millisecond
	)
	batch := make([]commentInsert, 0, maxBatch)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(flushInterval)
	}
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	flush := func() {
		for len(batch) > 0 {
			n := len(batch)
			if n > maxBatch {
				n = maxBatch
			}
			insertCommentRows(batch[:n])
			batch = batch[n:]
		}
		batch = make([]commentInsert, 0, maxBatch)
	}

	for {
		if len(batch) == 0 {
			select {
			case c := <-commentInsertCh:
				batch = append(batch, c)
				resetTimer()
			case done := <-commentFlushCh:
				batch = drainCommentQueue(batch)
				flush()
				close(done)
			}
			continue
		}

		select {
		case c := <-commentInsertCh:
			batch = append(batch, c)
			if len(batch) >= maxBatch {
				stopTimer()
				flush()
			}
		case <-timer.C:
			flush()
		case done := <-commentFlushCh:
			batch = drainCommentQueue(batch)
			stopTimer()
			flush()
			close(done)
		}
	}
}

func enqueueCommentInsert(c commentInsert) {
	select {
	case commentInsertCh <- c:
	default:
		insertCommentRows([]commentInsert{c})
	}
}

func flushCommentWrites() {
	done := make(chan struct{})
	commentFlushCh <- done
	<-done
}

// 投稿 HTML 内に埋め込む CSRF トークンのプレースホルダ（リクエスト毎に実トークンへ置換）。
const csrfPlaceholder = "----CSRF-PLACEHOLDER-9e3f7a21----"

// id 指定の User をプロセス内キャッシュ優先で取得する。
func getUserByID(ctx context.Context, id int) (User, bool) {
	if v, ok := userCache.Load(id); ok {
		return v.(User), true
	}

	var u User
	if err := db.GetContext(ctx, &u, "SELECT * FROM `users` WHERE `id` = ?", id); err != nil {
		return User{}, false
	}
	userCache.Store(id, u)
	return u, true
}

func invalidateUserCache(id int) {
	userCache.Delete(id)
}

func invalidateIndexCache() {
	// リクエストを待たせず、古いスナップショットを返しながら裏で差し替える。
	asyncRebuildIndex()
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
	// 以降 fetchUsers / getSessionUser は DB を叩かなくなる。
	var users []User
	if err := db.SelectContext(ctx, &users, "SELECT * FROM `users`"); err == nil {
		for _, u := range users {
			userCache.Store(u.ID, u)
		}
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
	return regexp.MustCompile(`\A[0-9a-zA-Z_]{3,}\z`).MatchString(accountName) &&
		regexp.MustCompile(`\A[0-9a-zA-Z_]{6,}\z`).MatchString(password)
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

func getSession(r *http.Request) *sessions.Session {
	session, _ := store.Get(r, "isuconp-go.session")

	return session
}

func getSessionUser(r *http.Request) User {
	ctx := r.Context()
	session := getSession(r)
	uid, ok := session.Values["user_id"]
	if !ok || uid == nil {
		return User{}
	}

	var id int
	switch v := uid.(type) {
	case int:
		id = v
	case int64:
		id = int(v)
	default:
		return User{}
	}

	u, _ := getUserByID(ctx, id)
	return u
}

func getFlash(w http.ResponseWriter, r *http.Request, key string) string {
	session := getSession(r)
	value, ok := session.Values[key]

	if !ok || value == nil {
		return ""
	} else {
		delete(session.Values, key)
		session.Save(r, w)
		return value.(string)
	}
}

// 元実装は投稿ごとに「コメント数・コメント・各ユーザー」を個別クエリしていた（N+1）。
// 対象投稿を先に絞り込み、コメント数/コメント/ユーザーをまとめて取得する。
func makePosts(ctx context.Context, results []Post, csrfToken string, allComments bool) ([]Post, error) {
	if len(results) == 0 {
		return []Post{}, nil
	}

	// 投稿者（プロセス内キャッシュ）で削除ユーザーを除外し、先頭 postsPerPage 件を選ぶ
	selected := make([]Post, 0, postsPerPage)
	for _, p := range results {
		user, ok := getUserByID(ctx, p.UserID)
		if !ok || user.DelFlg != 0 {
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
		return []Post{}, nil
	}

	// コメント数・コメント本体はすべてメモリから取得する（DB を叩かない）。
	// memComments は created_at 昇順（古い順）。表示も古い順。
	memMu.RLock()
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
			if u, ok := getUserByID(ctx, out[j].UserID); ok {
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
	session := getSession(r)
	csrfToken, ok := session.Values["csrf_token"]
	if !ok {
		return ""
	}
	return csrfToken.(string)
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

func getInitialize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	flushCommentWrites()
	dbInitialize(ctx)
	// 前回ベンチのキャッシュ（特に del_flg）を持ち越さないよう全消去する
	userCache.Clear()
	htmlCache.Clear()
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
		session := getSession(r)
		session.Values["user_id"] = u.ID
		session.Values["csrf_token"] = secureRandomStr(16)
		session.Save(r, w)

		http.Redirect(w, r, "/", http.StatusFound)
	} else {
		session := getSession(r)
		session.Values["notice"] = "アカウント名かパスワードが間違っています"
		session.Save(r, w)

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
		session := getSession(r)
		session.Values["notice"] = "アカウント名は3文字以上、パスワードは6文字以上である必要があります"
		session.Save(r, w)

		http.Redirect(w, r, "/register", http.StatusFound)
		return
	}

	memMu.RLock()
	_, exists := memUserByName[accountName]
	memMu.RUnlock()
	if exists {
		session := getSession(r)
		session.Values["notice"] = "アカウント名がすでに使われています"
		session.Save(r, w)

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
	userCache.Store(nu.ID, nu)
	memMu.Lock()
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
	session := getSession(r)
	session.Values["user_id"] = uid
	session.Values["csrf_token"] = secureRandomStr(16)
	session.Save(r, w)

	http.Redirect(w, r, "/", http.StatusFound)
}

func getLogout(w http.ResponseWriter, r *http.Request) {
	session := getSession(r)
	delete(session.Values, "user_id")
	session.Options = &sessions.Options{MaxAge: -1}
	session.Save(r, w)

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
	exp  time.Time
}

var indexSnap atomic.Pointer[indexSnapshot]
var (
	indexRebuilding atomic.Bool
	indexDirty      atomic.Bool
)

// スナップショットを再構築して atomic swap する。
func rebuildIndexSnap(ctx context.Context) {
	frag := buildIndexFragment(ctx)
	segs := bytes.Split([]byte(frag), []byte(csrfPlaceholder))
	indexSnap.Store(&indexSnapshot{segs: segs, exp: time.Now().Add(time.Second)})
}

// 裏で1つだけ再構築（多重起動を防ぐ）。再構築中に来た更新は dirty で拾い、続けて再構築する。
func asyncRebuildIndex() {
	indexDirty.Store(true)
	if indexRebuilding.CompareAndSwap(false, true) {
		go func() {
			defer indexRebuilding.Store(false)
			for indexDirty.CompareAndSwap(true, false) {
				rebuildIndexSnap(context.Background())
			}
		}()
	}
}

// 読み取りはロックフリー（atomic.Load）。期限切れなら古いスナップショットを返しつつ
// 裏で更新する。トップページは p99 を優先し、cache rebuild では絶対にリクエストを待たせない。
func getIndexSegments(ctx context.Context) [][]byte {
	if s := indexSnap.Load(); s != nil {
		if time.Now().After(s.exp) {
			asyncRebuildIndex()
		}
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
	me := getSessionUser(r)

	segs := getIndexSegments(ctx)
	csrf := getCSRFToken(r)
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
	postsHTML := template.HTML(strings.ReplaceAll(postsFragment, csrfPlaceholder, getCSRFToken(r)))

	// 統計はメモリのカウンタから取得（DB の全表スキャン COUNT を排除）
	memMu.RLock()
	commentCount := memCommentCntByUser[user.ID]
	postCount := memPostCntByUser[user.ID]
	commentedCount := memCommentedCntByUser[user.ID]
	memMu.RUnlock()

	me := getSessionUser(r)

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

	postHTML := template.HTML(strings.ReplaceAll(fragment, csrfPlaceholder, getCSRFToken(r)))

	me := getSessionUser(r)

	tmplPostID.Execute(w, struct {
		PostHTML template.HTML
		Me       User
	}{postHTML, me})
}

func postIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if r.FormValue("csrf_token") != getCSRFToken(r) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		session := getSession(r)
		session.Values["notice"] = "画像が必須です"
		session.Save(r, w)

		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

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
			session := getSession(r)
			session.Values["notice"] = "投稿できる画像形式はjpgとpngとgifだけです"
			session.Save(r, w)

			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
	}

	filedata, err := io.ReadAll(file)
	if err != nil {
		log.Print(err)
		return
	}

	if len(filedata) > UploadLimit {
		session := getSession(r)
		session.Values["notice"] = "ファイルサイズが大きすぎます"
		session.Save(r, w)

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
		if werr := os.WriteFile(fmt.Sprintf("../public/image/%d.%s", pid, ext), filedata, 0644); werr != nil {
			log.Print(werr)
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
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/login", http.StatusFound)
		return
	}

	if r.FormValue("csrf_token") != getCSRFToken(r) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}

	postID, err := strconv.Atoi(r.FormValue("post_id"))
	if err != nil {
		log.Print("post_idは整数のみです")
		return
	}

	body := r.FormValue("comment")

	// メモリ状態へ新規コメントを同期反映（validator はメモリから読むので即座に見える）。
	// DB への永続化は短い間隔でまとめて行い、コメント投稿のレスポンス遅延と DB 往復を削る。
	nc := Comment{ID: 0, PostID: postID, UserID: me.ID, Comment: body, CreatedAt: time.Now()}
	enqueueCommentInsert(commentInsert{postID: postID, userID: me.ID, body: body})

	ownerUserID := 0
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
		ownerUserID = owner.UserID
	}
	memMu.Unlock()

	// 該当投稿詳細は即時に最新化する（ベンチはコメント投稿直後に詳細を確認するため）。
	// トップページのコメント数は index の 1 秒 TTL に任せる。
	delHTMLCache(postCacheKey(postID))
	if ownerUserID != 0 {
		if u, ok := getUserByID(r.Context(), ownerUserID); ok {
			delHTMLCache("user_posts:" + u.AccountName)
		}
	}

	http.Redirect(w, r, fmt.Sprintf("/posts/%d", postID), http.StatusFound)
}

func getAdminBanned(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if me.Authority == 0 {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	users := []User{}
	err := db.SelectContext(ctx, &users, "SELECT * FROM `users` WHERE `authority` = 0 AND `del_flg` = 0 ORDER BY `created_at` DESC")
	if err != nil {
		log.Print(err)
		return
	}

	tmplBanned.Execute(w, struct {
		Users     []User
		Me        User
		CSRFToken string
	}{users, me, getCSRFToken(r)})
}

func postAdminBanned(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := getSessionUser(r)
	if !isLogin(me) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	if me.Authority == 0 {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	if r.FormValue("csrf_token") != getCSRFToken(r) {
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
				userCache.Store(n, u)
				memMu.Lock()
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

	db, err = sqlx.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %s.", err.Error())
	}
	defer db.Close()

	db.SetMaxOpenConns(40)
	db.SetMaxIdleConns(40)
	db.SetConnMaxLifetime(0)

	setupTemplates()
	go commentInsertWorker()

	// pprof（プロファイリング用、localhost のみ）
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	r := chi.NewRouter()

	r.Get("/initialize", getInitialize)
	r.Get("/login", getLogin)
	r.Post("/login", postLogin)
	r.Get("/register", getRegister)
	r.Post("/register", postRegister)
	r.Get("/logout", getLogout)
	r.Get("/", getIndex)
	r.Get("/posts", getPosts)
	r.Get("/posts/{id}", getPostsID)
	r.Post("/", postIndex)
	r.Get("/image/{id}.{ext}", getImage)
	r.Post("/comment", postComment)
	r.Get("/admin/banned", getAdminBanned)
	r.Post("/admin/banned", postAdminBanned)
	r.Get(`/@{accountName:[0-9a-zA-Z_]+}`, getAccountName)
	r.Mount("/", http.FileServer(http.Dir("../public")))

	log.Fatal(http.ListenAndServe(":8080", r))
}
