# ChatPipeline
 
A silent Twitch chat scraper that extracts, sanitizes, and loads live chat messages into PostgreSQL via an ETL pipeline.
 
Like its namesake, ChatPipeline sits quietly in Twitch chat — watching everything, saying nothing, and recording it all.
 
## Overview
 
ChatPipeline runs as three independent processes. The first two share a local SQLite buffer:

- **`collector`** (`src/Worker.py`) connects to one or more Twitch channels via TwitchIO and listens to chat in real time. Each message passes through a sanitization pipeline before being flushed to the local buffer every 2 seconds. It never talks to Postgres on the hot path, so it keeps ingesting chat even if the database is down.
- **`sync`** (`src/Sync.py`) drains that local buffer into PostgreSQL every 5 seconds, with its own independent reconnect loop. It can start before Postgres is up and picks back up automatically if Postgres goes down mid-run.
- **`sanitizer`** (`src/Sanitizer.py`) repairs rows already in Postgres. It sweeps the whole `messages` table once in insert order (`created_at`, so disk access stays mostly sequential), in 2,000-row chunks (short transactions, saving its position after each so restarts resume), then checks only newly inserted rows every 5 minutes. Fixes: timestamps shifted by a timezone offset, message text clean-up (control characters, whitespace, 500-char cap), usernames, `is_bot`, `channel_name` (empty or out of date), and `stream_id` for messages sent while a stream was confirmed live. Only rows that actually change are written. Progress is in the `sanitizer_progress` table; `docker compose run --rm -e SANITIZER_DRY_RUN=1 sanitizer` reports what it would change without writing anything. To add a fix, add it to `ROW_FIXES` and bump `SWEEP_VERSION`.

All three are designed to run continuously in the background, independently of any frontend or API layer.
 
Built as the data ingestion component of a larger Twitch analytics platform, ChatPipeline is intentionally minimal — its only job is to collect clean, reliable data.
 
## Features

- Real-time chat scraping across multiple Twitch channels simultaneously
- ETL pipeline — extracts, sanitizes, and loads each message before it hits the database
- Collector and sync run as separate processes so a Postgres outage never blocks chat ingestion
- Duplicate-safe — `ON CONFLICT DO NOTHING` prevents double inserts on reconnect
- Automatic reconnection via TwitchIO, plus automatic Twitch access-token refresh before it expires
- Per-minute stats logging from both services — messages received, buffered/synced, skipped, errors, and local backlog
- Stream metadata tracking — polls Twitch API every 60 seconds to record active streams and peak viewer counts
- Skipped message logging — messages that can't be inserted are logged to a separate table for review
- Channel list driven by the database — add or remove channels by updating the `channels` table, no code changes required

## Daily reports

The `reporter` service builds a report of the previous day at 00:15 (`REPORT_TZ`, else `TZ`): messages, chatters, rows added (live vs recovered), streams, top chatters, skipped messages, sanitizer activity and storage, per channel and in total. Each report is stored in the `daily_reports` table (JSONB) and written to `reports/<date>.html` and `.md`; `reports/index.html` lists every day with its headline numbers. The `reports-web` service (nginx) serves that folder read-only at **http://&lt;host&gt;:8090**.

Rebuild a day (e.g. after backfilling it): `docker compose run --rm reporter python src/Reporter.py --date 2026-10-03`

**Where the files go:** `REPORTS_PATH` in `.env` sets the host folder (default `./reports`); both `reporter` and `reports-web` use it. To have reports backed up on a NAS, mount the share on the host first (e.g. in `/etc/fstab`), then point `REPORTS_PATH` at it, e.g. `REPORTS_PATH=/mnt/nas/chatpipeline/reports`, and run `docker compose up -d`. With a NAS, also create an empty `.chatpipeline-reports` file in that folder and set `REPORTS_REQUIRE_MARKER=true`: if the share is ever not mounted, the reporter won't silently write to the empty local mount point. Whenever `REPORTS_PATH` is unavailable (marker missing, or a write fails), reports go to a local fallback folder instead (`REPORTS_FALLBACK_PATH`, default `./reports-fallback`) with an error logged; once the share is back, they're moved there automatically within 30 minutes and removed from the fallback. Reports are always saved in `daily_reports` too. The containers write as root, so an NFS export with `root_squash` (or an SMB mount without write access for root) needs adjusting to allow writes.

## Sanitizer audit trail

Every repair the sanitizer makes is recorded in Postgres, in the same transaction as the change itself:

- **`sanitizer_runs`** — one row per sweep phase or 5-minute pass: `phase`, `status` (`running`, `completed`, `failed`, or `interrupted` if the process was killed), start/finish times, `rows_scanned`, `rows_changed`, `fixes` (changes per column) and any `error`.
- **`sanitizer_changes`** — one row per changed column of a message: `run_id`, `message_id`, `column_name`, `old_value`, `new_value`, `changed_at`.
- **`sanitizer_daily_report`** (view) — changes per day and column.

```sql
-- recent runs
SELECT id, phase, status, started_at, finished_at, rows_scanned, rows_changed, fixes
FROM sanitizer_runs ORDER BY id DESC LIMIT 20;

-- daily summary
SELECT * FROM sanitizer_daily_report ORDER BY day DESC, column_name;

-- full history of one message
SELECT * FROM sanitizer_changes WHERE message_id = '<id>' ORDER BY id;

-- undo one run's stream_id attributions (old values are kept for every column)
UPDATE messages m SET stream_id = c.old_value
FROM sanitizer_changes c
WHERE c.run_id = <run id> AND c.column_name = 'stream_id' AND m.message_id = c.message_id;
```

Old and new values are stored as text (timestamps in ISO 8601, booleans as `true`/`false`). Dry runs write nothing, including to these tables.

**Guards.** Independently of the fixes, every change must pass its column's guard before it's written, so the sanitizer can never alter what a message says, who sent it, or when it was sent: `message` may only lose whitespace/control characters (every visible character kept, in order) or be cut at 500 characters; `username` only trimmed/lowercased; `timestamp` only moved back by a whole number of hours (a timezone offset); `channel_name` only set to that channel's name; `stream_id` only filled when empty, with a stream of the same channel. A refused change is logged (`[Guard] Refused …`) and counted as `rejected_<column>` on the run. [db/checks/health_check.sql](db/checks/health_check.sql) re-verifies every recorded change against the same rules, and that no repaired value has been modified since.

## Gaps and backfill

Twitch has no chat history API, so the collector works to avoid gaps and to fill the ones it can't avoid:

- **Token refreshes don't disconnect.** Every ~3.4h the collector opens a second chat connection with the new token, waits for it to join every channel, keeps both open for 5 seconds, then closes the old one. Messages both connections receive are de-duplicated by Twitch message id.
- **Stopping is graceful.** On `docker stop` / redeploy, messages still held in memory are written to the local buffer before exit.
- **Restarts are backfilled.** On startup, the collector fetches each channel's recent messages from [recent-messages.robotty.de](https://recent-messages.robotty.de) (about the last 800 per channel, with Twitch's own ids and send times) and buffers any newer than the last message it saved before stopping. Backfilled rows are identical to live ones; their `created_at` is later than `timestamp`. On very busy channels 800 messages is only a couple of minutes of chat, so long outages are only partly recoverable.

- **Historical backfill (optional).** The `backfill` service imports past days from [logs.ivr.fi](https://logs.ivr.fi), which keeps raw daily chat logs for many large channels going back years. Set per channel in `.env`, e.g. `BACKFILL_CHANNELS=xqc:30,moonmoon:90,ludwig:2026-01-01,summit1g:all`; channels not listed (or an empty setting) aren't backfilled. It works newest day first, records each finished day so restarts resume, re-reads days already collected to fill outage gaps (duplicates are dropped), and pauses while the queue holds over 50,000 messages so live chat never waits long. Every 6 hours it picks up the newly finished day. Scale check first: one busy day can be 250,000+ messages; `name:all` on a big channel is hundreds of millions of rows.

Logs: `[Backfill] <channel>: N message(s) since <time>` per channel on startup, and `backfilled=` in the `[Stats]` line.

## Tech Stack

- **Language:** Python 3.12+
- **Chat:** TwitchIO 2.10.0
- **Database:** PostgreSQL via asyncpg
- **Hosting:** Designed to run continuously via Docker

---

## Prerequisites

- Docker + Docker Compose
- A Twitch Developer account
- A running PostgreSQL instance with the `chatpipeline` database and schema already set up

---

## Twitch Setup

1. Go to [Twitch Developer Console](https://dev.twitch.tv/console)
2. Click **Register Your Application**
3. Set OAuth Redirect URL to `https://localhost` (must be `https`, not `http` — Twitch requires an exact match on the scheme or the authorize step below fails with `redirect_mismatch`)
4. Copy your **Client ID** and generate a **Client Secret**
5. Get an access token + refresh token for the account you want chat to connect as (must include `chat:read` and `chat:edit` scopes to log into IRC — a bare client-credentials/app token cannot):
   1. Open in a browser, logged in as that account, and approve:
      ```
      https://id.twitch.tv/oauth2/authorize?client_id=YOUR_CLIENT_ID&redirect_uri=https://localhost&response_type=code&scope=chat:read+chat:edit
      ```
   2. The browser redirects to `https://localhost/?code=XXXX` — the page fails to load, that's expected. Copy the `code` value from the address bar.
   3. Exchange it for tokens:
      ```bash
      curl -X POST https://id.twitch.tv/oauth2/token \
        -d client_id=YOUR_CLIENT_ID \
        -d client_secret=YOUR_CLIENT_SECRET \
        -d code=THE_CODE_FROM_STEP_2 \
        -d grant_type=authorization_code \
        -d redirect_uri=https://localhost
      ```
   4. The response's `access_token` → `TWITCH_TOKEN`, `refresh_token` → `TWITCH_REFRESH_TOKEN`.
6. (Optional) Find the account's numeric Twitch user ID for `TWITCH_BOT_ID` — not read anywhere in the code, informational only:
   ```bash
   curl -H "Authorization: Bearer YOUR_ACCESS_TOKEN" -H "Client-Id: YOUR_CLIENT_ID" https://api.twitch.tv/helix/users
   ```

`TWITCH_TOKEN` expires in a few hours. `Worker.py` refreshes it automatically before it expires using `TWITCH_REFRESH_TOKEN` + `TWITCH_CLIENT_SECRET` (see `get_fresh_access_token()`), so you shouldn't need to repeat this flow unless Twitch revokes access entirely.

---

## Database Setup

### Local test database

`docker-compose.test.yml` runs a throwaway Postgres 17 with the full schema (`db/init/01_schema.sql`) and two seed channels (`db/init/02_seed.sql`) applied automatically:

```bash
docker compose -f docker-compose.test.yml up -d --wait   # start
docker compose -f docker-compose.test.yml down -v        # stop + wipe (re-runs db/init on next start)
docker exec -it chatpipeline-postgres-test psql -U postgres -d chatpipeline   # shell
```

It listens on host port **5433** (`postgres` / `postgres`, database `chatpipeline`). To point the pipeline at it, set in `.env`:

```dotenv
DB_HOST=localhost              # or host.docker.internal when running via docker compose
DB_PORT=5433
DB_NAME=chatpipeline
DB_USER=postgres
DB_PASSWORD=postgres
```

### Local testing against the production schema

`docker-compose.local.yml` runs the whole pipeline (collector, sync, sanitizer) built from your working tree, against a local Postgres created from a dump of production. Use it to check a change before pushing an image.

**1. Dump production into `db/prod/`** (from the project folder on your PC). [db/dump_prod.sh](db/dump_prod.sh) runs inside a throwaway `postgres` container, so nothing needs installing. Match the image's major version to production (`SELECT version();`). The password is prompted for rather than read from `.env`, which avoids the `$$` escaping:

```powershell
$p = Read-Host "Prod DB password" -AsSecureString
$env:PGPASSWORD = [Runtime.InteropServices.Marshal]::PtrToStringBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))
docker run --rm -e PGPASSWORD -e PGHOST=10.10.0.212 -e PGPORT=5432 -e PGUSER=postgres -e PGDATABASE=chatpipeline -v "${PWD}\db:/db" postgres:18-alpine sh /db/dump_prod.sh
Remove-Item Env:PGPASSWORD
```

That writes `01_schema.sql` (full schema), `02_data.sql` (`channels` + `streams` rows and sequence positions) and `03_messages_sample.sql` (the 10,000 most recent messages; change with `-e SAMPLE_MESSAGES=...`).

**2. Create `.env.local`** with a separate Twitch token pair (see below).

**3. Start the local stack:**

```powershell
docker compose -f docker-compose.local.yml up -d --build
docker compose -f docker-compose.local.yml logs -f
docker exec -it chatpipeline-postgres-local psql -U postgres -d chatpipeline   # inspect
```

Put a **separate Twitch token pair** in `.env.local` (git-ignored; it overrides `.env`). Sharing the server's `TWITCH_REFRESH_TOKEN` can log the server out. `.env.local` is also the place for a shorter `SEED_CHANNELS`, or `LOG_HOST=http://127.0.0.1:9` to keep local test logs out of LogStream.

To reload after refreshing `db/prod/`, wipe and recreate: `docker compose -f docker-compose.local.yml down -v`, then `up -d --build`.

### Channels

The simplest way is to list them in `.env`:

```bash
SEED_CHANNELS=xqc,summit1g,moonmoon
```

When `SEED_CHANNELS` is set, the collector watches exactly those channels and nothing else, and the sync service adds them to the `channels` table for you. After editing `.env`, run `docker compose up -d` (Compose recreates the containers when the env changes).

Alternatively, leave `SEED_CHANNELS` empty and manage the list in Postgres. Each row needs its numeric Twitch user id in `twitch_id` (Postgres assigns `id` itself):

```sql
INSERT INTO channels (name, twitch_id) VALUES
  ('xqc', '71092938'),
  ('summit1g', '26490481');
```

Then restart sync first (it copies the list from Postgres at startup), then the collector:
```bash
docker compose restart sync && docker compose restart collector
```

---

## Environment Variables

Copy the example file and fill in your values:

```bash
cp .env.example .env
```

```dotenv
# .env.example

# Twitch credentials — see "Twitch Setup" above for how to get each of these
TWITCH_TOKEN=
TWITCH_CLIENT_ID=
TWITCH_CLIENT_SECRET=
TWITCH_REFRESH_TOKEN=
TWITCH_BOT_ID=

# PostgreSQL connection (used by Sync.py and Sanitizer.py)
DB_HOST=
DB_PORT=5432
DB_NAME=chatpipeline
DB_USER=postgres
DB_PASSWORD=

# Optional manual bootstrap — see comments in .env.example
SEED_CHANNELS=
```

| Variable | Description |
|---|---|
| `TWITCH_TOKEN` | Access token for the account chat connects as. Short-lived; refreshed automatically at runtime. |
| `TWITCH_CLIENT_ID` | From Twitch Developer Console |
| `TWITCH_CLIENT_SECRET` | From Twitch Developer Console |
| `TWITCH_REFRESH_TOKEN` | Used to silently mint a new `TWITCH_TOKEN` before the current one expires |
| `TWITCH_BOT_ID` | Twitch user ID of the account used to connect — informational only, not read by the code |
| `DB_HOST` | IP or hostname of your PostgreSQL server |
| `DB_PORT` | PostgreSQL port, default `5432` |
| `DB_NAME` | Database name, default `chatpipeline` |
| `DB_USER` | PostgreSQL user |
| `DB_PASSWORD` | PostgreSQL password |
| `SEED_CHANNELS` | Optional one-time channel-list bootstrap if Postgres has never been reachable — see `.env.example` |

> **Note:** `docker-compose`'s `env_file` parsing treats `$` as its own escape character — a literal `$` in `DB_PASSWORD` must be written as `$$`, or Postgres auth fails silently with what looks like the right password. After editing it, verify what actually reached the container rather than assuming: `docker exec <container> printenv DB_PASSWORD`.

---

## Running with Docker

```bash
# Build and start
docker compose up -d

# Watch logs
docker compose logs -f

# Stop
docker compose down

# Restart after a code change
docker compose down && docker compose build && docker compose up -d
```

---

## Stats Output

The `collector` service (`Worker.py`) logs a stats line every 60 seconds:

| Field | Description |
|---|---|
| `received` | Total messages seen across all channels |
| `buffered` | Written to the local SQLite buffer |
| `skipped` | Filtered out — logged to `skipped_messages` table |
| `errors` | Local buffer write failures |
| `local_backlog` | Rows waiting in the local buffer for `sync` to drain |

The `sync` service (`Sync.py`) logs its own line whenever it drains anything, plus a rollup roughly once a minute — `[Sync] messages=… skipped=… streams=…` and `[Sync stats] totals=… local_backlog=…`.