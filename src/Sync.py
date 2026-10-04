"""
ChatPipeline sync.

Independent process from worker.py. Its only job is draining the local
SQLite buffer into Postgres. It runs its own reconnect loop so it can
be started before Postgres is up, survive Postgres going down mid-run,
and pick back up automatically when it comes back — none of that has
any effect on the worker, which keeps ingesting chat the whole time.

Run this as a separate process/container/service from worker.py.
"""
import asyncio
import os
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv
from logstream import LogStream

import store
from pgdb import connect_pg_with_retry

load_dotenv()

log = LogStream(service="ChatPipeline-Sync-Prod", host=os.getenv("LOG_HOST"))

BATCH_SIZE          = 500
DRAIN_BATCH_SIZE    = 2000  # messages per insert while working through a backlog
DRAIN_BUDGET        = 4.0   # seconds per pass spent draining a backlog of messages
SYNC_INTERVAL        = 5    # seconds between sync passes
STATS_EVERY_N_CYCLES = 12   # ~ once a minute at a 5s interval


def parse_utc(iso: str) -> datetime:
    """Parses a buffered ISO timestamp, treating one without an offset as
    UTC. asyncpg would otherwise read a naive datetime as the container's
    local time (TZ) and shift it by that offset on insert."""
    dt = datetime.fromisoformat(iso)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


async def drain_messages(db_conn, pg: asyncpg.Pool) -> int:
    """Inserts batches of buffered messages until the buffer is empty or
    DRAIN_BUDGET runs out, so a backlog (e.g. from Backfill.py, or after
    a Postgres outage) drains at thousands of messages per pass instead
    of one batch."""
    total = 0
    deadline = asyncio.get_running_loop().time() + DRAIN_BUDGET
    while True:
        n = await sync_messages(db_conn, pg, DRAIN_BATCH_SIZE)
        total += n
        if n < DRAIN_BATCH_SIZE or asyncio.get_running_loop().time() > deadline:
            return total


async def sync_messages(db_conn, pg: asyncpg.Pool, limit: int = BATCH_SIZE) -> int:
    rows = await store.fetch_pending_messages(db_conn, limit)
    if not rows:
        return 0
    # Translate to Postgres's channels.id by name (every buffered message
    # carries its channel name, whatever id scheme it was written with).
    mapped, unmapped = [], []
    for r in rows:
        pg_id = pg_ids_by_name.get(r["channel"])
        (mapped if pg_id is not None else unmapped).append((r, pg_id))
    if unmapped:
        names = sorted({r["channel"] for r, _ in unmapped})
        log.error(f"Dropping {len(unmapped)} message(s) for channels missing from Postgres: {names}")
    await pg.executemany("""
        INSERT INTO messages
            (message_id, channel_id, channel_name, stream_id, user_id, username, message, timestamp, subscriber, is_bot)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (message_id) DO NOTHING
    """, [
        (r["message_id"], pg_id, r["channel"], r["stream_id"], r["user_id"], r["username"],
         r["message"], parse_utc(r["timestamp"]), bool(r["subscriber"]), bool(r["is_bot"]))
        for r, pg_id in mapped
    ])
    await store.delete_messages(db_conn, [r["id"] for r in rows])
    return len(mapped)


async def sync_skipped(db_conn, pg: asyncpg.Pool) -> int:
    rows = await store.fetch_pending_skipped(db_conn, BATCH_SIZE)
    if not rows:
        return 0
    await pg.executemany("""
        INSERT INTO skipped_messages
            (reason, message_id, channel_name, username, content, raw_tags, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    """, [
        (r["reason"], r["message_id"], r["channel_name"], r["username"],
         r["content"], r["raw_tags"], parse_utc(r["timestamp"]))
        for r in rows
    ])
    await store.delete_skipped(db_conn, [r["id"] for r in rows])
    return len(rows)


# Postgres's own channels.id, keyed by Twitch user id and by name. The
# local buffer only knows Twitch ids; these are rebuilt by
# push_channel_map at the start of every sync pass and used to translate.
pg_ids_by_twitch: dict[int, int] = {}
pg_ids_by_name:   dict[str, int] = {}


async def pull_channel_map(db_conn, pg: asyncpg.Pool) -> int:
    """Pushes Postgres's channels table into the local cache that the
    collector (worker.py) reads at startup when SEED_CHANNELS is empty.
    Only channels with a known twitch_id can be cached. This is the only
    place that list flows in this direction — keeps the collector's
    Postgres dependency at exactly zero."""
    rows = await pg.fetch("SELECT name, twitch_id FROM channels WHERE twitch_id IS NOT NULL")
    channel_map = {
        r["name"].lower(): int(r["twitch_id"])
        for r in rows if r["twitch_id"].strip().isdigit()
    }
    if channel_map:
        await store.cache_channel_map(db_conn, channel_map)
    return len(channel_map)


async def push_channel_map(db_conn, pg: asyncpg.Pool) -> int:
    """The reverse of pull_channel_map: makes sure every channel the
    collector is watching (e.g. ones added via SEED_CHANNELS) has a row
    in Postgres with its twitch_id filled in, since streams and messages
    carry a foreign key to it. Rows are matched by name; Postgres assigns
    channels.id itself. Then rebuilds the id translation maps."""
    channel_map = await store.load_cached_channel_map(db_conn)
    if channel_map:
        args = [(name, str(twitch_id)) for name, twitch_id in channel_map.items()]
        async with pg.acquire() as conn, conn.transaction():
            await conn.executemany("""
                UPDATE channels SET twitch_id = $2::text
                WHERE name = $1::text AND twitch_id IS NULL
            """, args)
            await conn.executemany("""
                INSERT INTO channels (name, twitch_id)
                SELECT $1::text, $2::text
                WHERE NOT EXISTS (SELECT 1 FROM channels WHERE name = $1::text)
            """, args)

    rows = await pg.fetch("SELECT id, name, twitch_id FROM channels")
    pg_ids_by_name.clear()
    pg_ids_by_twitch.clear()
    for r in rows:
        pg_ids_by_name[r["name"].lower()] = r["id"]
        if r["twitch_id"] and r["twitch_id"].strip().isdigit():
            pg_ids_by_twitch[int(r["twitch_id"])] = r["id"]
    return len(channel_map)


async def sync_streams(db_conn, pg: asyncpg.Pool) -> int:
    rows = await store.fetch_all_streams(db_conn)
    if not rows:
        return 0
    # Translate Twitch user id -> channels.id. Rows buffered before the
    # switch to Twitch ids may already hold a channels.id; keep those.
    known_pg_ids = set(pg_ids_by_name.values())
    mapped = []
    for r in rows:
        pg_id = pg_ids_by_twitch.get(r["channel_id"])
        if pg_id is None and r["channel_id"] in known_pg_ids:
            pg_id = r["channel_id"]
        if pg_id is not None:
            mapped.append((r, pg_id))
    if not mapped:
        return 0
    await pg.executemany("""
        INSERT INTO streams (id, channel_id, title, game_name, started_at, peak_viewers, is_live)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET
            title        = EXCLUDED.title,
            game_name    = EXCLUDED.game_name,
            peak_viewers = GREATEST(streams.peak_viewers, EXCLUDED.peak_viewers),
            is_live      = EXCLUDED.is_live
    """, [
        (r["id"], pg_id, r["title"], r["game_name"],
         parse_utc(r["started_at"]) if r["started_at"] else None,
         r["peak_viewers"], bool(r["is_live"]))
        for r, pg_id in mapped
    ])
    return len(mapped)


async def run_sync_step(label: str, fn, db_conn, pg: asyncpg.Pool) -> tuple[int, asyncpg.Pool]:
    """Runs one sync step in isolation so a failure in one (e.g. a bad
    row) can't block the others from running, and so a lost connection
    is reconnected without derailing the rest of the pass."""
    try:
        return await fn(db_conn, pg), pg
    except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as e:
        log.error(f"Lost PostgreSQL connection ({e}); reconnecting.")
        await pg.close()
        pg = await connect_pg_with_retry(log)
        return 0, pg
    except Exception as e:
        log.error(f"Sync pass failed ({label}): {e}")
        return 0, pg


async def main() -> None:
    await store.init_db()
    db_conn = await store.get_connection()
    pg = await connect_pg_with_retry(log)

    try:
        n_channels = await pull_channel_map(db_conn, pg)
        log.info(f"Cached {n_channels} channel(s) locally for the collector.")
    except Exception as e:
        log.error(f"Initial channel map pull failed ({e}); collector will wait and retry.")

    log.info("Sync service started.")
    totals = {"messages": 0, "skipped": 0, "streams": 0}
    cycles = 0

    try:
        while True:
            await asyncio.sleep(SYNC_INTERVAL)

            # channels, then streams, then messages: each carries a
            # foreign key to the one before, so the referenced row has to
            # exist in Postgres first. Each step is isolated (see
            # run_sync_step) so one failing step — e.g. a stray FK
            # violation — can't block the others from running.
            _,         pg = await run_sync_step("channels", push_channel_map, db_conn, pg)
            n_streams, pg = await run_sync_step("streams",  sync_streams,  db_conn, pg)
            n_msg,     pg = await run_sync_step("messages", drain_messages, db_conn, pg)
            n_skip,    pg = await run_sync_step("skipped",  sync_skipped,  db_conn, pg)

            totals["messages"] += n_msg
            totals["skipped"]  += n_skip
            totals["streams"]  += n_streams
            cycles += 1

            if cycles % STATS_EVERY_N_CYCLES == 0:
                try:
                    await pull_channel_map(db_conn, pg)
                except Exception as e:
                    log.error(f"Channel map refresh failed: {e}")

            if n_msg or n_skip or n_streams:
                log.info(f"[Sync] messages={n_msg} skipped={n_skip} streams={n_streams}")

            if cycles % STATS_EVERY_N_CYCLES == 0:
                pending = await store.count_pending(db_conn)
                log.info(f"[Sync stats] totals={totals} local_backlog={pending}")
    finally:
        await db_conn.close()
        await pg.close()

if __name__ == "__main__":
    asyncio.run(main())