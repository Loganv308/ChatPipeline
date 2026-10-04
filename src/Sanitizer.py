"""
ChatPipeline sanitizer.

Independent background process that repairs rows already in Postgres.
It never touches the local buffer or Twitch, so it can be stopped,
restarted or redeployed without affecting collection or sync.

It sweeps the messages table in three phases, saving its position after
every chunk (sanitizer_progress table) so a restart resumes rather than
starting over:

  rows         walk every message in primary-key order, applying ROW_FIXES
  streams      walk every message with no stream_id, attributing it to the
               stream that was live when it was sent. Runs after `rows`
               so it sees repaired timestamps.
  incremental  from then on, every SANITIZE_INTERVAL seconds, apply all
               fixes to rows inserted since the last pass

Every fix is idempotent and only writes rows whose values actually
change. Chunks are short transactions with a pause between them, so the
sweep never holds long locks on a table Sync is inserting into.

Adding a fix: write a function taking (row, ctx) and returning the
corrected value (or the current one), add it to ROW_FIXES, and bump
SWEEP_VERSION so existing rows are re-swept with it.

Dry run (read-only; reports what each fix would change, then exits):
  docker compose run --rm -e SANITIZER_DRY_RUN=1 sanitizer
"""
import asyncio
import json
import os
import re
from datetime import datetime, timedelta, timezone

import asyncpg
from dotenv import load_dotenv
from logstream import LogStream

from pgdb import connect_pg_with_retry
from sanitize import BOT_NAMES, MAX_MESSAGE_LEN, MAX_USERNAME_LEN, normalize_text, normalize_username

load_dotenv()

log = LogStream(service="ChatPipeline-Sanitizer-Prod", host=os.getenv("LOG_HOST"))

SWEEP_VERSION     = 1       # bump when adding/changing a fix, to re-sweep every row
SANITIZE_INTERVAL = 300     # seconds between incremental passes
CHUNK_SIZE        = 5000    # rows read (and at most written) per transaction
CHUNK_PAUSE       = 0.5     # seconds between chunks, to leave room for Sync
STATEMENT_TIMEOUT = "60s"   # per chunk transaction
PROGRESS_EVERY    = 50      # chunks between progress log lines
SHIFT_THRESHOLD   = timedelta(minutes=30)
INCREMENTAL_OVERLAP = timedelta(minutes=2)  # re-checked at the start of each incremental pass
DRY_RUN = os.getenv("SANITIZER_DRY_RUN", "").lower() in ("1", "true", "yes")

JOB = "messages_sweep"
COLUMNS = "message_id, channel_id, channel_name, stream_id, username, message, timestamp, created_at, is_bot"


# ─── Fixes ─────────────────────────────────────────────────────────────────
#
# Each takes the row (dict, with earlier fixes in this list already applied)
# and the pass context, and returns the column's corrected value.

def fix_timestamp(row, ctx):
    """A message can't be sent after it was inserted: a timestamp more
    than 30 min past created_at was shifted by a whole-hour UTC offset
    (the TZ bug), which is recovered from the gap itself."""
    ts, created = row["timestamp"], row["created_at"]
    if ts and created and ts > created + SHIFT_THRESHOLD:
        hours = round((ts - created).total_seconds() / 3600)
        return ts - timedelta(hours=hours)
    return ts


def fix_message(row, ctx):
    """Re-applies the collector's idempotent text clean-up (control
    characters, whitespace, 500-char cap). HTML decoding is deliberately
    not re-applied -- see sanitize.sanitize_message."""
    return normalize_text(row["message"]) if row["message"] is not None else None


def fix_username(row, ctx):
    return normalize_username(row["username"]) if row["username"] else row["username"]


def fix_is_bot(row, ctx):
    return row["username"] in BOT_NAMES


def fix_channel_name(row, ctx):
    """Fills empty names and corrects old ones after a channel rename."""
    return ctx["channel_names"].get(row["channel_id"], row["channel_name"])


def fix_stream_id(row, ctx):
    """Only fills an empty stream_id, and only when the message's send
    time falls inside a window where that stream is known to have been
    live: from its start to its last live-tagged message (or now, if
    still live). Messages outside every known window stay unattributed
    rather than guessed."""
    if row["stream_id"] is not None or row["timestamp"] is None:
        return row["stream_id"]
    for start, end, stream_id in ctx["stream_windows"].get(row["channel_id"], ()):
        if start <= row["timestamp"] <= end:
            return stream_id
    return None


ROW_FIXES = [
    ("timestamp",    fix_timestamp),
    ("message",      fix_message),
    ("username",     fix_username),
    ("is_bot",       fix_is_bot),
    ("channel_name", fix_channel_name),
]
STREAM_FIXES = [("stream_id", fix_stream_id)]
ALL_FIXES = ROW_FIXES + STREAM_FIXES


# ─── Guards ────────────────────────────────────────────────────────────────
#
# Independent of the fixes above: each states the only kind of change its
# column may ever receive, so a bug in a fix can't alter what a message
# says, who sent it, or when. A change that fails its guard is not
# written; it's logged and counted as rejected_<column> on the run.

_INVISIBLE = re.compile(r"[\s\x00-\x1f\x7f]")


def _visible(text: str) -> str:
    return _INVISIBLE.sub("", text)


def guard_message(old, new, row, ctx) -> bool:
    """Every visible character kept, in order -- only whitespace and
    control characters may differ, or the end cut off by the length cap."""
    if old is None or new is None:
        return False
    if _visible(new) == _visible(old):
        return True
    return len(new) == MAX_MESSAGE_LEN and _visible(old).startswith(_visible(new))


def guard_username(old, new, row, ctx) -> bool:
    """Same characters; only surrounding spaces and letter case may change."""
    return old is not None and new == old.strip().lower()[:MAX_USERNAME_LEN]


def guard_timestamp(old, new, row, ctx) -> bool:
    """Only moved back by a whole number of hours (a UTC offset)."""
    if old is None or new is None:
        return False
    shift = (old - new).total_seconds()
    return shift % 3600 == 0 and 1 <= shift / 3600 <= 14


def guard_channel_name(old, new, row, ctx) -> bool:
    return new is not None and new == ctx["channel_names"].get(row["channel_id"])


def guard_stream_id(old, new, row, ctx) -> bool:
    """Only fills an empty stream_id, with a stream of the same channel."""
    return old is None and new is not None and ctx["stream_channels"].get(new) == row["channel_id"]


def guard_is_bot(old, new, row, ctx) -> bool:
    return isinstance(new, bool)


GUARDS = {
    "message":      guard_message,
    "username":     guard_username,
    "timestamp":    guard_timestamp,
    "channel_name": guard_channel_name,
    "stream_id":    guard_stream_id,
    "is_bot":       guard_is_bot,
}


# ─── Setup ─────────────────────────────────────────────────────────────────

async def ensure_index(pg: asyncpg.Pool, name: str, definition: str) -> None:
    """CREATE INDEX CONCURRENTLY, so building it never blocks Sync's
    inserts. A concurrent build that was interrupted leaves an INVALID
    index that IF NOT EXISTS would skip forever, so drop and rebuild it."""
    valid = await pg.fetchval("""
        SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = $1
    """, name)
    if valid is False:
        log.info(f"Rebuilding invalid index {name}.")
        await pg.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
    if valid is not True:
        log.info(f"Building index {name} (concurrently; can take a while on a large table).")
        await pg.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {definition}")


async def setup(pg: asyncpg.Pool) -> None:
    await pg.execute("""
        CREATE TABLE IF NOT EXISTS sanitizer_progress (
            job            TEXT        PRIMARY KEY,
            version        INTEGER     NOT NULL,
            phase          TEXT        NOT NULL,
            cursor_id      TEXT,
            cursor_created TIMESTAMPTZ,
            sweep_started  TIMESTAMPTZ NOT NULL,
            rows_scanned   BIGINT      NOT NULL DEFAULT 0,
            fixed          JSONB       NOT NULL DEFAULT '{}',
            updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    # Audit trail. One row per sweep phase / incremental pass, and one per
    # changed column of a message (with its old value, so any run can be
    # reviewed or reversed). Both are written in the same transaction as
    # the change itself, so they always match what actually happened.
    await pg.execute("""
        CREATE TABLE IF NOT EXISTS sanitizer_runs (
            id             BIGSERIAL   PRIMARY KEY,
            sweep_version  INTEGER     NOT NULL,
            phase          TEXT        NOT NULL,   -- rows | streams | incremental
            status         TEXT        NOT NULL,   -- running | completed | failed | interrupted
            started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at    TIMESTAMPTZ,
            updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),   -- last chunk written
            rows_scanned   BIGINT      NOT NULL DEFAULT 0,
            rows_changed   BIGINT      NOT NULL DEFAULT 0,
            fixes          JSONB       NOT NULL DEFAULT '{}',  -- changes per column
            error          TEXT
        );
        CREATE TABLE IF NOT EXISTS sanitizer_changes (
            id           BIGSERIAL   PRIMARY KEY,
            run_id       BIGINT      NOT NULL REFERENCES sanitizer_runs (id),
            message_id   TEXT        NOT NULL,
            column_name  TEXT        NOT NULL,
            old_value    TEXT,
            new_value    TEXT,
            changed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS idx_sanitizer_changes_message ON sanitizer_changes (message_id);
        CREATE INDEX IF NOT EXISTS idx_sanitizer_changes_run     ON sanitizer_changes (run_id);
        CREATE OR REPLACE VIEW sanitizer_daily_report AS
            SELECT changed_at::date AS day, column_name,
                   count(*) AS changes, count(DISTINCT message_id) AS messages, count(DISTINCT run_id) AS runs
            FROM sanitizer_changes
            GROUP BY 1, 2;
    """)
    # A run still 'running' at startup belongs to a process that was killed;
    # it ended at its last written chunk.
    await pg.execute("""
        UPDATE sanitizer_runs SET status = 'interrupted', finished_at = updated_at
        WHERE status = 'running'
    """)
    # keyset paging for incremental passes
    await ensure_index(pg, "idx_messages_created_at_id", "messages (created_at, message_id)")
    # "last live-tagged message per stream" for stream windows
    await ensure_index(pg, "idx_messages_stream_timestamp", "messages (stream_id, timestamp)")


# ─── Pass context ──────────────────────────────────────────────────────────

async def load_context(pg: asyncpg.Pool) -> dict:
    channel_names = {r["id"]: r["name"] for r in await pg.fetch("SELECT id, name FROM channels")}

    # Per stream: [start, end] where it's known to have been live. end is
    # the send time of its last live-tagged message; now() if still live.
    # With idx_messages_stream_timestamp (built by setup()) each stream's
    # last message is one index probe. Without it (e.g. a dry run, which
    # builds nothing), per-stream lookups read every tagged message at
    # random, so one sequential pass over the table is far faster.
    has_index = await pg.fetchval("""
        SELECT coalesce(bool_and(i.indisvalid), false) FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'idx_messages_stream_timestamp'
    """)
    if has_index:
        rows = await pg.fetch("""
            SELECT s.id, s.channel_id, s.started_at, s.is_live,
                   (SELECT max(m.timestamp) FROM messages m WHERE m.stream_id = s.id) AS last_tagged
            FROM streams s
            WHERE s.started_at IS NOT NULL
        """)
    else:
        log.info("[Sweep] Finding each stream's last message with one pass over messages "
                 "(no stream/timestamp index yet); this can take a few minutes on a large table.")
        rows = await pg.fetch("""
            SELECT s.id, s.channel_id, s.started_at, s.is_live, t.last_tagged
            FROM streams s
            LEFT JOIN (SELECT stream_id, max(timestamp) AS last_tagged
                       FROM messages WHERE stream_id IS NOT NULL GROUP BY stream_id) t
                   ON t.stream_id = s.id
            WHERE s.started_at IS NOT NULL
        """)
    now = datetime.now(timezone.utc)
    windows: dict[int, list] = {}
    for r in rows:
        end = now if r["is_live"] else r["last_tagged"]
        if end is not None and end >= r["started_at"]:
            windows.setdefault(r["channel_id"], []).append((r["started_at"], end, r["id"]))
    for w in windows.values():
        w.sort(reverse=True)  # latest stream first
    stream_channels = {r["id"]: r["channel_id"] for r in await pg.fetch("SELECT id, channel_id FROM streams")}
    return {"channel_names": channel_names, "stream_windows": windows, "stream_channels": stream_channels}


# ─── Progress ──────────────────────────────────────────────────────────────

def new_progress() -> dict:
    return {"version": SWEEP_VERSION, "phase": "rows", "cursor_id": None, "cursor_created": None,
            "sweep_started": datetime.now(timezone.utc), "rows_scanned": 0, "fixed": {}}


async def load_progress(pg: asyncpg.Pool) -> dict:
    row = None if DRY_RUN else await pg.fetchrow("SELECT * FROM sanitizer_progress WHERE job = $1", JOB)
    if row is None or row["version"] != SWEEP_VERSION:
        if row is not None:
            log.info(f"Fixes changed (sweep v{row['version']} -> v{SWEEP_VERSION}); re-sweeping every row.")
        return new_progress()
    p = dict(row)
    p["fixed"] = json.loads(p["fixed"]) if isinstance(p["fixed"], str) else dict(p["fixed"])
    return p


async def save_progress(conn, p: dict) -> None:
    if DRY_RUN:
        return
    await conn.execute("""
        INSERT INTO sanitizer_progress
            (job, version, phase, cursor_id, cursor_created, sweep_started, rows_scanned, fixed, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now())
        ON CONFLICT (job) DO UPDATE SET
            version = EXCLUDED.version, phase = EXCLUDED.phase, cursor_id = EXCLUDED.cursor_id,
            cursor_created = EXCLUDED.cursor_created, sweep_started = EXCLUDED.sweep_started,
            rows_scanned = EXCLUDED.rows_scanned, fixed = EXCLUDED.fixed, updated_at = now()
    """, JOB, p["version"], p["phase"], p["cursor_id"], p["cursor_created"], p["sweep_started"],
        p["rows_scanned"], json.dumps(p["fixed"]))


# ─── Chunk processing ──────────────────────────────────────────────────────

async def fetch_chunk(conn, p: dict) -> list:
    if p["phase"] == "rows":
        return await conn.fetch(f"""
            SELECT {COLUMNS} FROM messages
            WHERE $1::text IS NULL OR message_id > $1
            ORDER BY message_id LIMIT {CHUNK_SIZE}
        """, p["cursor_id"])
    if p["phase"] == "streams":
        # same key order, but only rows that could need a stream_id
        return await conn.fetch(f"""
            SELECT {COLUMNS} FROM messages
            WHERE stream_id IS NULL AND ($1::text IS NULL OR message_id > $1)
            ORDER BY message_id LIMIT {CHUNK_SIZE}
        """, p["cursor_id"])
    return await conn.fetch(f"""
        SELECT {COLUMNS} FROM messages
        WHERE (created_at, message_id) > ($1, $2)
        ORDER BY created_at, message_id LIMIT {CHUNK_SIZE}
    """, p["cursor_created"], p["cursor_id"] or "")


def apply_fixes(rows: list, fixes: list, ctx: dict) -> tuple[list[dict], list[tuple], list[tuple]]:
    """Returns (rows that changed, with corrected values), the audit
    entries for them, and the changes refused by their column's guard.
    Entries are (message_id, column, old value, new value)."""
    changed, audit, rejected = [], [], []
    for record in rows:
        row = dict(record)
        dirty = False
        for column, fn in fixes:
            new = fn(row, ctx)
            if new == row[column]:
                continue
            entry = (row["message_id"], column, as_text(row[column]), as_text(new))
            if not GUARDS[column](row[column], new, row, ctx):
                rejected.append(entry)
                continue
            audit.append(entry)
            row[column] = new
            dirty = True
        if dirty:
            changed.append(row)
    return changed, audit, rejected


def as_text(value) -> str | None:
    """How a value is recorded in sanitizer_changes."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def count_by_column(audit: list[tuple]) -> dict:
    counts: dict[str, int] = {}
    for _, column, _, _ in audit:
        counts[column] = counts.get(column, 0) + 1
    return counts


def add_counts(a: dict, b: dict) -> dict:
    return {k: a.get(k, 0) + b.get(k, 0) for k in a.keys() | b.keys()}


async def write_changes(conn, changed: list[dict]) -> None:
    """One UPDATE for the whole chunk. Sync never modifies existing rows
    (ON CONFLICT DO NOTHING), so writing back every column of a row we
    just read can't overwrite anyone else's change."""
    await conn.execute("""
        UPDATE messages m SET
            timestamp    = u.timestamp,
            message      = u.message,
            username     = u.username,
            is_bot       = u.is_bot,
            channel_name = u.channel_name,
            stream_id    = u.stream_id
        FROM unnest($1::text[], $2::timestamptz[], $3::text[], $4::text[], $5::bool[], $6::text[], $7::text[])
             AS u(message_id, timestamp, message, username, is_bot, channel_name, stream_id)
        WHERE m.message_id = u.message_id
    """,
        [r["message_id"] for r in changed], [r["timestamp"] for r in changed],
        [r["message"] for r in changed], [r["username"] for r in changed],
        [r["is_bot"] for r in changed], [r["channel_name"] for r in changed],
        [r["stream_id"] for r in changed])


async def write_audit(conn, run: dict, audit: list[tuple]) -> None:
    await conn.execute("""
        INSERT INTO sanitizer_changes (run_id, message_id, column_name, old_value, new_value)
        SELECT $1, * FROM unnest($2::text[], $3::text[], $4::text[], $5::text[])
    """, run["id"], [a[0] for a in audit], [a[1] for a in audit], [a[2] for a in audit], [a[3] for a in audit])


async def run_chunk(pg: asyncpg.Pool, p: dict, ctx: dict, run: dict) -> int:
    """Processes one chunk: the repairs, their audit entries, the run's
    running totals and the saved position all commit together, so the
    audit trail always matches what was actually written. In-memory
    state is only advanced after that commit. Returns how many rows
    were read (0 = phase finished)."""
    fixes = {"rows": ROW_FIXES, "streams": STREAM_FIXES, "incremental": ALL_FIXES}[p["phase"]]
    async with pg.acquire() as conn, conn.transaction():
        await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
        rows = await fetch_chunk(conn, p)
        if not rows:
            return 0
        changed, audit, rejected = apply_fixes(rows, fixes, ctx)
        counts = count_by_column(audit)
        for message_id, column, old, new in rejected:
            counts[f"rejected_{column}"] = counts.get(f"rejected_{column}", 0) + 1
            log.error(f"[Guard] Refused {column} change on {message_id}: {old!r} -> {new!r}")

        new_p = dict(p, rows_scanned=p["rows_scanned"] + len(rows),
                     cursor_id=rows[-1]["message_id"], fixed=add_counts(p["fixed"], counts))
        if p["phase"] == "incremental":
            new_p["cursor_created"] = rows[-1]["created_at"]
        new_run = dict(run, rows_scanned=run["rows_scanned"] + len(rows),
                       rows_changed=run["rows_changed"] + len(changed),
                       fixes=add_counts(run["fixes"], counts))

        if not DRY_RUN:
            if changed:
                await write_changes(conn, changed)
                await write_audit(conn, run, audit)
            await conn.execute("""
                UPDATE sanitizer_runs
                SET rows_scanned = $2, rows_changed = $3, fixes = $4::jsonb, updated_at = now()
                WHERE id = $1
            """, run["id"], new_run["rows_scanned"], new_run["rows_changed"], json.dumps(new_run["fixes"]))
            await save_progress(conn, new_p)
    p.update(new_p)
    run.update(new_run)
    return len(rows)


# ─── Runs (audit) ──────────────────────────────────────────────────────────

async def start_run(pg: asyncpg.Pool, phase: str) -> dict:
    run = {"id": None, "phase": phase, "rows_scanned": 0, "rows_changed": 0, "fixes": {}}
    if not DRY_RUN:
        # Anything still 'running' is from a pass that died without
        # finishing (e.g. a lost connection) -- close it out first.
        await pg.execute("""
            UPDATE sanitizer_runs SET status = 'interrupted', finished_at = updated_at
            WHERE status = 'running'
        """)
        run["id"] = await pg.fetchval("""
            INSERT INTO sanitizer_runs (sweep_version, phase, status) VALUES ($1, $2, 'running')
            RETURNING id
        """, SWEEP_VERSION, phase)
    return run


async def finish_run(pg: asyncpg.Pool, run: dict, status: str, error: str | None = None) -> None:
    if DRY_RUN or run["id"] is None:
        return
    try:
        await pg.execute("""
            UPDATE sanitizer_runs SET status = $2, error = $3, finished_at = now(), updated_at = now()
            WHERE id = $1
        """, run["id"], status, error)
    except Exception as e:
        # e.g. the connection is gone; the next start_run marks it interrupted
        log.error(f"Couldn't record the end of sanitizer run {run['id']}: {e}")


# ─── Phases ────────────────────────────────────────────────────────────────

def fixed_summary(fixed: dict) -> str:
    return ", ".join(f"{k}={v:,}" for k, v in sorted(fixed.items())) or "nothing"


async def walk_phase(pg: asyncpg.Pool, p: dict) -> None:
    """Runs the current full-table phase ('rows' or 'streams') to the end,
    as one audited run (or a new one per resume)."""
    estimate = await pg.fetchval("SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE relname = 'messages'")
    ctx = await load_context(pg)
    run = await start_run(pg, p["phase"])
    suffix = " (DRY RUN: nothing will be written)" if DRY_RUN else f" (run {run['id']})"
    log.info(f"[Sweep] Phase '{p['phase']}' {'resuming' if p['cursor_id'] else 'starting'} "
             f"over ~{estimate:,} rows{suffix}.")
    chunks = 0
    try:
        while await run_chunk(pg, p, ctx, run):
            chunks += 1
            if chunks % PROGRESS_EVERY == 0:
                log.info(f"[Sweep:{p['phase']}] ~{run['rows_scanned']:,} rows this run | "
                         f"fixed: {fixed_summary(run['fixes'])}")
            await asyncio.sleep(0 if DRY_RUN else CHUNK_PAUSE)
    except Exception as e:
        await finish_run(pg, run, "failed", str(e))
        raise
    await finish_run(pg, run, "completed")
    log.info(f"[Sweep] Phase '{p['phase']}' complete | this run fixed: {fixed_summary(run['fixes'])} "
             f"| sweep total: {fixed_summary(p['fixed'])}")


async def advance(pg: asyncpg.Pool, p: dict) -> None:
    if p["phase"] == "rows":
        p.update(phase="streams", cursor_id=None)
    elif p["phase"] == "streams":
        # Rows inserted during the sweep may have been passed already;
        # starting incremental from the sweep's start re-checks them.
        p.update(phase="incremental", cursor_id=None, cursor_created=p["sweep_started"])
    async with pg.acquire() as conn:
        await save_progress(conn, p)


async def incremental_pass(pg: asyncpg.Pool, p: dict) -> None:
    # created_at is set when Sync's insert transaction *starts*, so rows
    # committed just after the previous pass can carry a created_at older
    # than where it stopped. Re-checking a short overlap catches them;
    # rows already correct aren't rewritten.
    if p["cursor_created"] is not None:
        p.update(cursor_created=p["cursor_created"] - INCREMENTAL_OVERLAP, cursor_id=None)
    ctx = await load_context(pg)
    run = await start_run(pg, "incremental")
    try:
        while await run_chunk(pg, p, ctx, run):
            await asyncio.sleep(CHUNK_PAUSE)
    except Exception as e:
        await finish_run(pg, run, "failed", str(e))
        raise
    await finish_run(pg, run, "completed")
    if run["rows_changed"]:
        log.info(f"[Sweep:incremental] run {run['id']}: checked {run['rows_scanned']:,} new row(s), "
                 f"fixed {fixed_summary(run['fixes'])}")


# ─── Main ──────────────────────────────────────────────────────────────────

async def main() -> None:
    pg = await connect_pg_with_retry(log)
    if not DRY_RUN:
        await setup(pg)

    p = await load_progress(pg)
    log.info(f"Sanitizer started: sweep v{SWEEP_VERSION}, phase '{p['phase']}'.")
    try:
        while True:
            try:
                if p["phase"] in ("rows", "streams"):
                    await walk_phase(pg, p)
                    if DRY_RUN and p["phase"] == "streams":
                        log.info(f"[Sweep] DRY RUN finished. Would fix: {fixed_summary(p['fixed'])}")
                        return
                    await advance(pg, p)
                    if p["phase"] == "incremental":
                        log.info(f"[Sweep] Full sweep complete. Total fixed: {fixed_summary(p['fixed'])}")
                    continue
                await incremental_pass(pg, p)
                await asyncio.sleep(SANITIZE_INTERVAL)
            except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as e:
                log.error(f"Lost PostgreSQL connection ({e}); reconnecting and resuming.")
                await pg.close()
                pg = await connect_pg_with_retry(log)
                p = await load_progress(pg) if not DRY_RUN else p
            except Exception as e:
                log.error(f"[Sweep] Pass failed ({e}); retrying from the saved position in 60s.")
                await asyncio.sleep(60)
                p = await load_progress(pg) if not DRY_RUN else p
    finally:
        await log.flush()
        await pg.close()

if __name__ == "__main__":
    asyncio.run(main())
