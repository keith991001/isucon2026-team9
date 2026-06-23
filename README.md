# ISUCON Workshop 2026 — Team 9 (private-isu)

Performance-tuning solution for **private-isu** built during the 日本CTO協会 合同ISUCON研修 2026.

| Item | Value |
|------|-------|
| Target app | [private-isu](https://github.com/catatsuy/private-isu) — image-sharing SNS |
| Machine | Single EC2 instance, **2 vCPU / 3.7 GB** |
| Initial score | **578** |
| Best score | **623,005** (rank #2) |
| Improvement | **~1078×** |
| Language | Go (rewritten from the reference Ruby implementation) |
| Scoring | `GET×1 + POST×2 + image-post×5 − (5xx×10 + transport-failure×20)` |

## What this repo contains

```
webapp/golang/app.go         # the optimized Go application (single-file)
webapp/golang/templates/     # HTML templates (some modified for byte-string rendering)
webapp/golang/go.mod|go.sum  # dependencies
etc/nginx/nginx.conf         # nginx main config (worker/fd/open_file_cache tuning)
etc/nginx/sites-available/isucon.conf   # upstream keepalive, static/image bypass
etc/mysql/conf.d/zz-isucon.cnf           # MySQL "race mode" tuning
RETROSPECTIVE.md             # full write-up: what we did, problems, mistakes
```

## Architecture summary

The winning architecture is an **in-memory state machine**:

- At `/initialize` (and startup) **all posts, comments and users are loaded into process memory**. Read requests never touch MySQL.
- **MySQL is write/persistence only.** Writes update memory synchronously (so the validator sees them immediately) and persist to MySQL asynchronously where safe.
- Per-user read models (post lists, comment counts, "commented" counts) are maintained **incrementally on write** — no request-time `JOIN` / `COUNT` / sort.
- Hot pages are cached as **pre-rendered `[]byte`** with a CSRF placeholder; the request path is essentially `atomic.Load` + `write([]byte)` (no template execution, no lock).
- Sessions live in a **signed cookie** (no session store round-trip).
- nginx serves all static files and posted images directly from disk; the app only handles dynamic routes.

See [RETROSPECTIVE.md](./RETROSPECTIVE.md) for the full journey and the mistakes made along the way.

## Deploy (reference)

```sh
# app
cd webapp/golang && go build -o app && sudo systemctl restart isu-go
# nginx
sudo cp etc/nginx/nginx.conf /etc/nginx/nginx.conf
sudo cp etc/nginx/sites-available/isucon.conf /etc/nginx/sites-available/isucon.conf
sudo nginx -t && sudo systemctl restart nginx
# mysql
sudo cp etc/mysql/conf.d/zz-isucon.cnf /etc/mysql/conf.d/zz-isucon.cnf
sudo systemctl restart mysql
```

> The original private-isu code is MIT-licensed (catatsuy/private-isu). This repo is a derived, tuned solution.
