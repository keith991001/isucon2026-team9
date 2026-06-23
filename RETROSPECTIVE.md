# Retrospective — private-isu tuning (578 → 623,005)

## 1. Overview

| Metric | Value |
|--------|-------|
| Target | private-isu (image-sharing SNS) |
| Machine | Single EC2, 2 vCPU / 3.7 GB |
| Initial score | **578** |
| Best score | **623,005** (rank #2, essentially 0 fails throughout) |
| Improvement | **~1078×** |
| Scoring rule | `GET×1 + POST×2 + image-post×5 − (5xx×10 + transport-failure×20)` |

## 2. Optimization journey (by official-score milestone)

**Stage 1 — Ruby baseline tuning (578 → 195,761)**
1. The `comments` table (100k rows) had **no index on `post_id`** → added a composite index (the #1 bottleneck).
2. nginx serves static files (css/js/img) directly instead of proxying to Ruby.
3. `digest()` changed from shelling out to `openssl` on every call → native SHA-512 (byte-identical output).
4. Images written to disk and served by nginx.
5. The timeline query `SELECT ... ORDER BY created_at DESC` had **no LIMIT**, sorting all 10k rows every request → added `LIMIT 100` (a hidden, huge win: local 25k → 149k).
6. Batched N+1 queries; MySQL tuning (1 GB buffer pool, binlog off).

**Stage 2 — Go rewrite + caching (195,761 → 473,540)**
- Switched to Go and ported every optimization: precompiled templates, `interpolateParams`, batched N+1, window-function to fetch top-3 comments → 256,209.
- memcache data cache → cached HTML fragments → nginx/kernel tuning (`open_file_cache`, keepalive, `somaxconn`, `access_log off`) → 360,775.
- In-process full user cache + cached pages for `/posts`, `/@user`, `/posts/:id` → 418k–441k.
- Cookie-based sessions (removed the session memcache round-trip), string-concat rendering to bypass `html/template`, lock-free atomic-pointer snapshot, stale-while-revalidate.

**Stage 3 — In-memory state machine (the decisive breakthrough, 473,540 → 623,005)**
- **All posts/comments/users loaded into memory; reads never touch the DB; MySQL becomes write-only** — MySQL CPU dropped from 100% to ~5%, **470k → 582k**.
- Async persistence (comments return after the in-memory update), `write([]byte)` segment assembly → **614,875**.
- Incremental per-user read models (post indexes, O(1) maintained stats) + MySQL "race mode" (`flush_log_at_trx_commit=0`, `doublewrite=0`) + nginx body buffer → **623,005**.

## 3. Problems encountered

1. **Shared benchmarker contention.** The benchmark pool was shared across all teams, with concurrency fluctuating between 6 and 20. The *same binary* scored **614k at 6-concurrent, 424k at 13-concurrent, 378k at 20-concurrent** — variance was enormous, and because the leaderboard ranks by *current* score, our rank swung wildly.
2. **Disk repeatedly filled up.** Every benchmark run posts images (post id > 10000) that are written to disk, but `/initialize` only deletes the DB rows, not the files → orphan images accumulated. Combined with the Go build cache and the nginx access log, this caused build failures and required repeated cleanup.
3. Environment friction: zsh word-splitting of unquoted variables, nested-quote breakage in SSH commands, `PATH` issues for `go`/`bundle`.

## 4. Mistakes made (honest review)

1. **Too conservative on direction; leaned on incremental caching for too long.** When the score plateaued, I spent a long time adding "one more cache layer" instead of pivoting earlier to a structural change — the **in-memory state machine**. That direction is what carried the score from ~470k to ~623k and should have been adopted sooner.
2. **Wrong conclusion from `top -bn1`.** A single sample cannot compute a reliable CPU %, and I mis-judged where the bottleneck was based on it. Only after switching to `mpstat` (per-second sampling) did the real picture appear: ~90% CPU under load with `mysqld` saturating one core. **Wrong measurement tool → wrong conclusion.**
3. **Failed CSRF-removal experiment.** I assumed the benchmark wouldn't test CSRF rejection and removed validation to enable full-page caching → the benchmark failed it (`expected 422, got 200`) → reverted. A wasted round.
4. **`comment_count` denormalization backfired.** I didn't realize the `posts` table carries a 1.2 GB `imgdata` blob, so any `UPDATE` rewrites blob-bearing rows; the recompute `UPDATE` took 37–109 s and blew the `/initialize` timeout → reverted.
5. **Repeatedly mistook variance for regression.** An empty-blob change once scored 404k and I read it as a regression — it was just contention variance (the same config scored 473k on another run). This caused wasted back-and-forth.
6. **A custom-router version regressed** (382k) and shipped with a compile typo (`strings.ContainsByte`, which doesn't exist). The pipeline's compile + 0-fail gates caught it before it polluted production.
7. **A latent bug found but not fully fixed:** the in-memory version occasionally returns **404 for a freshly-posted `/posts/:id`** (a new post enters `memNewPosts` but `memPostByID` may not be updated in sync) — low probability, small penalty, but real.

## 5. Key lessons

- **Measure, don't guess.** `top -bn1` is unreliable — use `mpstat`; locate hot paths with `pprof`.
- **For small datasets, the in-memory state machine is the decisive lever.** Demote the DB to persistence/initialization and serve all reads from memory; try this earlier.
- **Use the local benchmark as a stable signal.** When the official score is polluted by contention, the local benchmarker (no external contention) is a reliable relative metric.
- **Don't cross the red lines:** CSRF validation, validator consistency, and disk space — any of these failing means a `fail`/penalty, which hurts more than being slow.
- **Guardrails work.** A compile gate + a local 0-fail gate + a pre-deploy backup successfully caught both a compile error and a regressed version.
