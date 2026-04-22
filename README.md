# ChatPipeline
 
A silent Twitch chat scraper that extracts, sanitizes, and loads live chat messages into PostgreSQL via an ETL pipeline.
 
Like its namesake, ChatPipeline sits quietly in Twitch chat — watching everything, saying nothing, and recording it all.
 
## Overview
 
ChatPipeline connects to one or more Twitch channels via TwitchIO and listens to chat in real time. Each message passes through a sanitization pipeline before being batch-inserted into a PostgreSQL database every 2 seconds. It is designed to run continuously in the background, independently of any frontend or API layer.
 
Built as the data ingestion component of a larger Twitch analytics platform, ChatPipeline is intentionally minimal — its only job is to collect clean, reliable data.
 
## Features

- Real-time chat scraping across multiple Twitch channels simultaneously
- ETL pipeline — extracts, sanitizes, and loads each message before it hits the database
- Batch inserts every 2 seconds to minimize database load
- Duplicate-safe — `ON CONFLICT DO NOTHING` prevents double inserts on reconnect
- Automatic reconnection via TwitchIO
- Per-minute stats logging — messages received, inserted, skipped, and errors
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
3. Set OAuth Redirect URL to `http://localhost`
4. Copy your **Client ID** and generate a **Client Secret**
5. Generate an IRC OAuth token at [twitchapps.com/tmi](https://twitchapps.com/tmi)
6. Find your bot's Twitch user ID at [api.twitch.tv/helix/users?login=yourbotname](https://dev.twitch.tv/docs/api/reference/#get-users)

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

# Twitch credentials
TWITCH_TOKEN=oauth:your_irc_token
TWITCH_CLIENT_ID=your_client_id
TWITCH_CLIENT_SECRET=your_client_secret
TWITCH_BOT_ID=your_bot_twitch_user_id

# PostgreSQL connection
DB_HOST=your_db_host
DB_PORT=5432
DB_NAME=chatpipeline
DB_USER=your_db_user
DB_PASSWORD=your_db_password
```

| Variable | Description |
|---|---|
| `TWITCH_TOKEN` | IRC OAuth token from twitchapps.com/tmi — must include `oauth:` prefix |
| `TWITCH_CLIENT_ID` | From Twitch Developer Console |
| `TWITCH_CLIENT_SECRET` | From Twitch Developer Console |
| `TWITCH_BOT_ID` | Twitch user ID of the account used to connect |
| `DB_HOST` | IP or hostname of your PostgreSQL server |
| `DB_PORT` | PostgreSQL port, default `5432` |
| `DB_NAME` | Database name, default `chatpipeline` |
| `DB_USER` | PostgreSQL user |
| `DB_PASSWORD` | PostgreSQL password — supports special characters |

> **Note:** Do not use a URL-style `DB_DSN` if your password contains special characters (`@`, `$`, `#`). The individual `DB_*` variables handle any password safely.

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

Every 60 seconds the pipeline logs a stats line:

| Field | Description |
|---|---|
| `received` | Total messages seen across all channels |
| `inserted` | Successfully written to PostgreSQL |
| `skipped` | Filtered out — logged to `skipped_messages` table |
| `errors` | Failed DB inserts |
| `queue` | Messages waiting to be flushed |
| `db` | Current database size |