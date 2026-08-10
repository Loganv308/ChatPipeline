# ChatPipeline
 
A silent Twitch chat scraper that extracts, sanitizes, and loads live chat messages into PostgreSQL via an ETL pipeline.
 
Like its namesake, ChatPipeline sits quietly in Twitch chat — watching everything, saying nothing, and recording it all.
 
## Overview
 
ChatPipeline runs as two independent processes sharing a local SQLite buffer:

- **`collector`** (`src/Worker.py`) connects to one or more Twitch channels via TwitchIO and listens to chat in real time. Each message passes through a sanitization pipeline before being flushed to the local buffer every 2 seconds. It never talks to Postgres on the hot path, so it keeps ingesting chat even if the database is down.
- **`sync`** (`src/Sync.py`) drains that local buffer into PostgreSQL every 5 seconds, with its own independent reconnect loop. It can start before Postgres is up and picks back up automatically if Postgres goes down mid-run.

Both are designed to run continuously in the background, independently of any frontend or API layer.
 
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

Add the channels you want to track:

```sql
INSERT INTO channels (name) VALUES
  ('xqc'),
  ('summit1g'),
  ('moonmoon');
```

To add a channel later without touching the code:
```sql
INSERT INTO channels (name) VALUES ('newchannel');
```

Then restart the pipeline to pick it up:
```bash
docker compose down && docker compose up -d
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

# PostgreSQL connection (used by Sync.py only)
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