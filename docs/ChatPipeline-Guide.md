# ChatPipeline: How to Use It

Notes on running, configuring, deploying and checking ChatPipeline.

**Quick facts**

| | |
|---|---|
| Repo | `github.com/Loganv308/ChatPipeline` (on the PC: `X:\Forge-Codebase\Forge-projects\DataEngineer-Codebase\TwitchChatPipeline`) |
| Docker image | `loganvelier/chatpipeline:latest` (every service uses this one image) |
| Production server | `root@al-services`, folder `/opt/ChatPipeline` (a git clone) |
| Production database | PostgreSQL 18 at `10.10.0.212:5432`, database `chatpipeline` |
| Reports website | `http://al-services:8090` |
| Logs | `docker compose logs` (printed in `TZ`, i.e. Central) and LogStream (in UTC) |

---

## Contents

1. [How it works](#1-how-it-works)
2. [The services](#2-the-services)
3. [Configuration (.env)](#3-configuration-env)
4. [Everyday commands](#4-everyday-commands)
5. [Channels](#5-channels)
6. [Deploying a new version](#6-deploying-a-new-version)
7. [Testing locally first](#7-testing-locally-first)
8. [Sanitizer](#8-sanitizer)
9. [Backfill (filling gaps)](#9-backfill-filling-gaps)
10. [Daily reports](#10-daily-reports)
11. [Checking data health](#11-checking-data-health)
12. [Useful SQL](#12-useful-sql)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. How it works

```
 Twitch chat ──► collector ──► local buffer (SQLite) ──► sync ──► PostgreSQL (10.10.0.212)
                                    ▲                               │
 logs.ivr.fi ──► backfill ──────────┘                               ├──► sanitizer (repairs rows)
                                                                    └──► reporter ──► reports folder ──► reports-web (:8090)
```

- **Collecting and saving are separate.** The collector only writes to a local SQLite file (`/data/buffer.db` in the `chatbuffer` volume). Sync moves rows from there into Postgres. If Postgres goes down, chat keeps being collected and sync catches up when it's back.
- **No duplicates.** Each message is stored under Twitch's own message ID. Inserting the same message twice is silently ignored, so backfills and restarts can safely re-send messages.
- **Two identical messages are still both kept.** If someone sends the same text twice, each has a different Twitch ID, so both are stored.
- **Times are stored in UTC.** `messages.timestamp` is when the message was sent. `created_at` is when it was inserted into Postgres. For backfilled messages, `created_at` is later than `timestamp`.

---

## 2. The services

All six run from `docker-compose.yml` and restart automatically.

| Service | File | What it does | How often |
|---|---|---|---|
| `collector` | `src/Worker.py` | Joins the channels in `SEED_CHANNELS`, cleans each message, writes it to the local buffer. Also records streams (start time, title, peak viewers) and refreshes the Twitch token. | Live; buffer flush every 2 s, stream poll every 60 s |
| `sync` | `src/Sync.py` | Moves buffered messages, skipped messages and streams into Postgres. Keeps the `channels` table in step with `SEED_CHANNELS`. | Every 5 s |
| `sanitizer` | `src/Sanitizer.py` | Repairs rows already in Postgres, and records every change it makes. | One full sweep, then every 5 min for new rows |
| `backfill` | `src/Backfill.py` | Imports past chat from logs.ivr.fi for the channels you choose. Off unless `BACKFILL_CHANNELS` is set. | Rechecks every 6 h |
| `reporter` | `src/Reporter.py` | Builds a daily report of the previous day. | 00:15 Central; checks for late data every 30 min |
| `reports-web` | nginx | Serves the reports folder read-only. | Port 8090 |

---

## 3. Configuration (.env)

The server's settings live in `/opt/ChatPipeline/.env`, which is not in git. `.env.example` in the repo lists every setting with comments.

### Required

| Setting | What it is |
|---|---|
| `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET` | From your app at https://dev.twitch.tv/console |
| `TWITCH_TOKEN`, `TWITCH_REFRESH_TOKEN` | The chat account's tokens (how to get them: `.env.example` or README "Twitch Setup"). Only needed on first start; after that the collector keeps its own refreshed token in the buffer volume. |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | Postgres connection. **Every `$` in the password must be written as `$$`.** |
| `SEED_CHANNELS` | Comma-separated channel names to collect, e.g. `xqc,moonmoon,summit1g` |

### Optional

| Setting | Default | What it does |
|---|---|---|
| `LOG_HOST` | none | LogStream address, including `http://` and the port (LogStream is on 3005, not 3000) |
| `TZ` | UTC | Timezone for printed logs and for the reports' day boundaries. Use `America/Chicago`. |
| `BACKFILL_CHANNELS` | empty (off) | Which channels to backfill, and how far back. See [section 9](#9-backfill-filling-gaps). |
| `REPORTS_PATH` | `./reports` | Folder the reports are written to, e.g. a NAS mount |
| `REPORTS_REQUIRE_MARKER` | `false` | Only write to `REPORTS_PATH` if it contains a `.chatpipeline-reports` file |
| `REPORTS_FALLBACK_PATH` | `./reports-fallback` | Where reports go while `REPORTS_PATH` is unavailable |

After changing `.env`, run `docker compose up -d`. Compose recreates the containers whose settings changed.

---

## 4. Everyday commands

Run these on the server, in `/opt/ChatPipeline`.

```bash
docker compose ps                          # what's running
docker compose logs -f                     # follow all logs
docker compose logs -f collector sync      # follow specific services
docker compose logs --since 1h sanitizer   # last hour of one service
docker compose restart sync                # restart one service
docker compose up -d                       # start everything / apply .env changes
docker compose down                        # stop everything (the buffer volume is kept)
```

**What healthy logs look like:**

- collector, every minute: `[Stats] received=… buffered=… skipped=… errors=0 local_backlog=…`. `local_backlog` should stay small.
- sync: `[Sync] messages=… skipped=… streams=…`.
- sanitizer: `[Sweep:rows] ~N rows this run | fixed: …` during the sweep, then a short line every 5 minutes.

**Shutting down safely:** `docker compose down` or `docker stop` gives the collector time to write the messages it's holding in memory. Don't use `docker kill`.

---

## 5. Channels

`SEED_CHANNELS` is the complete list. The collector joins exactly those channels and nothing else.

**To add or remove a channel:**

1. Edit `SEED_CHANNELS` in `.env`, e.g. `SEED_CHANNELS=xqc,moonmoon,summit1g,newchannel`
2. Run `docker compose up -d`

Sync adds new channels to the `channels` table on its own: `id` is assigned by Postgres, `twitch_id` is looked up from Twitch. Removing a channel only stops collection; its existing messages stay in the database.

To skip the Twitch lookup, you can write `name:twitchid`, e.g. `xqc:71092938`.

---

## 6. Deploying a new version

Build on the PC, then pull on the server. The image contains the code. The server's git clone only provides `docker-compose.yml`.

### On the PC (VS Code terminal, project folder)

```powershell
docker compose build
docker compose push
git add -A; git commit -m "describe the change"; git push
```

### On the server

```bash
cd /opt/ChatPipeline
git pull
docker compose pull
docker compose up -d --no-build
docker image prune -f        # remove old, unused images
docker compose logs -f       # check it came up cleanly
```

If `git pull` refuses because `docker-compose.yml` was edited on the server, discard the server's edit and pull again (put server-specific settings in `.env`, not the compose file):

```bash
git checkout -- docker-compose.yml
git pull
```

### Backup before a risky deploy (optional)

The commands in `db/backup_prod.sh` (PowerShell, run on the PC) write a full `pg_dump` of production to `backups\chatpipeline-pre-deploy.dump`. It prompts for the password.

> `db/*.sh` is git-ignored, so `db/backup_prod.sh` and `db/dump_prod_testing.sh` exist only on the PC.

---

## 7. Testing locally first

`docker-compose.local.yml` runs the whole pipeline from your working copy against a **local Postgres 18 with production's schema**. It's a separate Compose project (`chatpipeline-local`), so it can't touch production or be pushed.

### One-time setup

1. **Copy production into `db/prod/`.** This copies the schema, all channels and streams, and the 10,000 most recent messages. It prompts for the production password:

   ```powershell
   $p = Read-Host "Prod DB password" -AsSecureString
   $env:PGPASSWORD = [Runtime.InteropServices.Marshal]::PtrToStringBSTR([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))
   docker run --rm -e PGPASSWORD -e PGHOST=10.10.0.212 -e PGPORT=5432 -e PGUSER=postgres -e PGDATABASE=chatpipeline -v "${PWD}\db:/db" postgres:18-alpine sh /db/dump_prod_testing.sh
   Remove-Item Env:PGPASSWORD
   ```

2. **(Recommended) Create `.env.local`.** This file is git-ignored and overrides `.env` for local runs. Use it for:
   - a separate Twitch token pair, so local and server refreshes don't log each other out
   - a shorter `SEED_CHANNELS`
   - `LOG_HOST=http://127.0.0.1:9`, to keep local logs out of LogStream

### Run it

```powershell
docker compose -f docker-compose.local.yml up -d --build     # start / rebuild
docker compose -f docker-compose.local.yml logs -f           # watch
docker compose -f docker-compose.local.yml stop              # pause (keeps the data)
docker compose -f docker-compose.local.yml down -v           # wipe; re-imports db/prod on next start
```

The local stack writes only to the local database, never to 10.10.0.212.

### Look at the local data

- From a terminal: `docker exec -it chatpipeline-postgres-local psql -U postgres -d chatpipeline`
- From a GUI tool (DBeaver, pgAdmin, …): host `localhost`, port **5435**, user `postgres`, password `postgres`, database `chatpipeline`

### Windows gotchas

- **Git Bash** rewrites paths like `/app/...` into Windows paths. Prefix the command with `MSYS_NO_PATHCONV=1`, or use PowerShell.
- **PowerShell** has no `<` redirection. Pipe the file in instead: `Get-Content file.sql | docker exec -i chatpipeline-postgres-local psql -U postgres -d chatpipeline`

---

## 8. Sanitizer

The sanitizer repairs rows already in the database. It's built so it **can't change what a message says, who sent it, or when it was sent**. It only fixes formatting and timezone errors, and fills in missing fields.

### What it fixes

| Column | Fix | Allowed change (enforced by a guard) |
|---|---|---|
| `channel_name` | Fills in the channel's name from `channel_id` | Only ever set to that channel's actual name |
| `timestamp` | Undoes timezone shifts (rows whose send time is more than 30 min after they were inserted) | Only moved back by a whole number of hours |
| `message` | Decodes HTML entities, removes control characters, collapses extra whitespace, caps at 500 characters | Every visible character kept, in order |
| `username` | Trims and lowercases | Nothing else |
| `is_bot` | Marks known bots (StreamElements, Nightbot, Fossabot, Moobot, Streamlabs) | Nothing else |
| `stream_id` | Links a message to the stream that was live when it was sent | Only fills empty values, only with a stream of the same channel |

If a fix ever produces a change its guard doesn't allow, the change is **refused**: it's logged as `[Guard] Refused …` and counted as `rejected_<column>`.

### How it runs

1. **Setup** (first real start): builds two indexes without locking the table. On a large table this takes a while.
2. **`rows` phase:** walks every message that existed when the sweep started, in the order they were inserted, 2,000 rows per chunk. It saves its position after every chunk, so a restart resumes where it stopped. Progress lines show how far it has got (`now at rows inserted <date>`).
3. **`streams` phase:** walks them again, linking messages that have no `stream_id` to streams.
4. **Incremental:** from then on, every 5 minutes it checks only newly inserted rows. The first pass also covers everything added during the sweep, e.g. by the backfill.

**"Pass failed (canceling statement due to statement timeout)"** means one chunk took longer than 60 seconds. Only that chunk is rolled back, and the sanitizer resumes from its last saved chunk a minute later. Nothing is skipped or applied twice. The "rows this run" counter starts again from zero after each retry.

### Dry run: see what it would change, without writing

```bash
docker compose run --rm --no-deps -e SANITIZER_DRY_RUN=1 sanitizer
```

The last line is `DRY RUN finished. Would fix: …`. On the full production table (~48.6M rows) a dry run takes about 5–6 hours. Run it inside `tmux`, or add `-d` and follow it with `docker logs -f <container>`, so an SSH disconnect doesn't kill it. To stop one early: `docker ps` to find the `…sanitizer-run-…` container, then `docker stop <name>`. Nothing is lost, because a dry run writes nothing.

### Audit trail

Every change is recorded in the same transaction as the change itself:

- **`sanitizer_runs`**: one row per phase or pass (status, rows scanned/changed, fixes per column, errors)
- **`sanitizer_changes`**: one row per changed value (`message_id`, `column_name`, `old_value`, `new_value`, `changed_at`)
- **`sanitizer_daily_report`** (a view): changes per day and column

Because old values are kept, any change can be undone (see [section 12](#12-useful-sql)).

---

## 9. Backfill (filling gaps)

Twitch doesn't keep chat history, so there are three layers of protection against missing messages:

| Layer | Covers | Automatic? |
|---|---|---|
| **Token swap** | The token refresh every ~3.4 h opens the new connection before closing the old one, so nothing is missed. | Yes |
| **Startup catch-up** | When the collector starts, it fetches about the last 800 messages per channel (from robotty) and saves anything newer than what it had. This covers restarts and short outages, but on busy channels 800 messages is only a few minutes of chat. | Yes |
| **Historical backfill** | Whole past days from logs.ivr.fi, per channel. | Set `BACKFILL_CHANNELS` |

### Historical backfill settings

```dotenv
BACKFILL_CHANNELS=xqc:30,moonmoon:90,ludwig:2026-01-01,summit1g:all
```

| Entry | Meaning |
|---|---|
| `name` | Last 30 days |
| `name:90` | Last 90 days |
| `name:2026-01-01` | Back to that date |
| `name:all` | Everything available |

- Channels not listed aren't backfilled. An empty value turns backfill off. Each channel must also be in `SEED_CHANNELS`.
- It works from yesterday backwards and remembers finished days, so restarts resume.
- Days that were already collected get re-read. Missing messages are added and existing ones are ignored, so this also fills outage gaps.
- It pauses while more than 50,000 messages are waiting in the buffer, so live chat never waits long.
- **Mind the size:** one busy day can be 250,000+ messages, and `:all` on a big channel can be hundreds of millions of rows.

**Which channels logs.ivr.fi has** (checked Oct 2026): xqc, moonmoon, summit1g, pokelawls, sodapoppin, ludwig. Not logged: emiru (opted out) and several others. Check any channel at `https://logs.ivr.fi/list?channel=<name>`.

---

## 10. Daily reports

At **00:15 Central** the reporter builds a report of the previous day:

- messages, chatters and first-time chatters
- rows added (live vs. recovered by backfill)
- streams, busiest hour, top chatters
- skipped messages
- sanitizer activity and storage

Everything is reported per channel and in total.

Each report is saved three ways:

- in the **`daily_reports`** table (JSONB)
- as **`<date>.html`** and **`<date>.md`** in the reports folder, plus **`index.html`** listing every day
- as a one-line summary in the logs

**View them** at `http://al-services:8090`.

**Automatic catch-up:**

- On startup, it builds any missing reports from the last 7 days.
- Every 30 minutes, it rebuilds any past day that received new rows (for example from a backfill), plus the day after it, whose comparisons change too.

**Rebuild one day by hand:**

```bash
docker compose run --rm reporter python src/Reporter.py --date 2026-10-03
```

### Saving reports to a NAS

1. Mount the NAS share on the server, e.g. in `/etc/fstab`.
2. Create an empty marker file in the share: `touch /mnt/nas/chatpipeline/reports/.chatpipeline-reports`
3. Set the following in `.env`, then run `docker compose up -d`:
   ```dotenv
   REPORTS_PATH=/mnt/nas/chatpipeline/reports
   REPORTS_REQUIRE_MARKER=true
   ```

If the share isn't mounted (marker missing) or a write fails, reports go to `REPORTS_FALLBACK_PATH` (default `./reports-fallback`) and an error is logged. When the share comes back, they're moved over automatically within 30 minutes. The containers write as root, so the NAS export must allow root writes (no `root_squash` on NFS).

---

## 11. Checking data health

`db/checks/health_check.sql` runs about 25 read-only checks and prints PASS / FAIL / INFO for each. They cover:

- ingestion is current
- no duplicates or orphaned rows
- timestamps are sane
- every sanitizer change is allowed by its guard
- nothing was modified after it was repaired

**Local:**

```powershell
Get-Content db/checks/health_check.sql | docker exec -i chatpipeline-postgres-local psql -U postgres -d chatpipeline
```

**Production** (from the server; several checks scan the whole table, so run it off-peak):

```bash
read -s -p "DB password: " PGPASSWORD; export PGPASSWORD; echo
docker run --rm -i -e PGPASSWORD postgres:18-alpine \
  psql -h 10.10.0.212 -U postgres -d chatpipeline < db/checks/health_check.sql
unset PGPASSWORD
```

---

## 12. Useful SQL

```sql
-- Messages per channel in the last 24 hours
SELECT channel_name, count(*) FROM messages
WHERE timestamp > now() - interval '24 hours'
GROUP BY 1 ORDER BY 2 DESC;

-- Latest messages, shown in Central time
SELECT timestamp AT TIME ZONE 'America/Chicago' AS sent, channel_name, username, message
FROM messages ORDER BY timestamp DESC LIMIT 50;

-- Is anything arriving? (should be > 0 while any channel is live)
SELECT count(*) FROM messages WHERE created_at > now() - interval '10 minutes';

-- Streams that are live right now
SELECT c.name, s.title, s.started_at FROM streams s JOIN channels c ON c.id = s.channel_id
WHERE s.is_live;

-- Sanitizer: recent runs
SELECT id, phase, status, started_at, finished_at, rows_scanned, rows_changed, fixes
FROM sanitizer_runs ORDER BY id DESC LIMIT 20;

-- Sanitizer: where the sweep is
SELECT * FROM sanitizer_progress;

-- Sanitizer: changes per day and column
SELECT * FROM sanitizer_daily_report ORDER BY day DESC, column_name;

-- Sanitizer: everything ever changed on one message
SELECT * FROM sanitizer_changes WHERE message_id = '<id>' ORDER BY id;

-- Sanitizer: undo one run's changes to one column (example: stream_id)
UPDATE messages m SET stream_id = c.old_value::int
FROM sanitizer_changes c
WHERE c.run_id = <run id> AND c.column_name = 'stream_id' AND m.message_id = c.message_id;

-- Reports stored in the database
SELECT report_date, generated_at FROM daily_reports ORDER BY report_date DESC;

-- Table and index sizes
SELECT pg_size_pretty(pg_table_size('messages'))   AS table_size,
       pg_size_pretty(pg_indexes_size('messages')) AS index_size;
```

> Times are stored in UTC. Tools show them in the session's timezone, so use `AT TIME ZONE 'America/Chicago'` (or set your SQL tool's timezone) to see Central time.

---

## 13. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Postgres login fails, but the password looks right | A `$` in `DB_PASSWORD` wasn't doubled | Write each `$` as `$$`; check with `docker exec <container> printenv DB_PASSWORD` |
| New channel not collected | `.env` changed but containers not recreated | `docker compose up -d` |
| Collector running but `received=0` | No channel is live, or it failed to join | Check the collector logs for join errors; check that the channel names are spelled correctly |
| `local_backlog` keeps growing | Sync can't reach Postgres | `docker compose logs sync`; nothing is lost, it catches up once Postgres is back |
| `AuthenticationError` / token errors | The refresh token was used elsewhere (e.g. a local run shared it) | Give local runs their own token pair in `.env.local`; on the server, put fresh tokens in `.env` and run `docker compose up -d` |
| Log times look wrong | `TZ` not set | Set `TZ=America/Chicago`. LogStream always shows UTC. |
| Timestamps hours in the future | Rows from before the timezone fix | The sanitizer repairs them (timestamp fix) |
| `can't open file '/app/src/…'` on the server | The server has an old image | Build and push from the PC, then `docker compose pull` on the server |
| `git pull` conflict on `docker-compose.yml` | The file was edited on the server | `git checkout -- docker-compose.yml`, then `git pull` |
| Reports missing for some days | No data for those days at the time, or older than the 7-day catch-up | Backfill them; the reporter rebuilds days that get new data. Or rebuild by hand with `--date`. |
| Reports in `reports-fallback` instead of the NAS | Share not mounted / marker missing | Remount; they move over automatically within 30 min |
| Local Postgres won't start | Postgres 18 uses a different data folder path | Already handled in `docker-compose.local.yml` (`/var/lib/postgresql`); if it persists, run `down -v` and start again |
| `pg_dump` "server version mismatch" | The dump tool is older than the server | Use the `postgres:18-alpine` image (as in the commands above) |
