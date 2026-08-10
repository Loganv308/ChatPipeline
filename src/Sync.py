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
from datetime import datetime

import asyncpg
from dotenv import load_dotenv
from logstream import LogStream

import store

load_dotenv()

log = LogStream(service="ChatPipeline-Sync", host="http://10.10.0.97:3000")

BATCH_SIZE          = 500
SYNC_INTERVAL        = 5    # seconds between sync passes
PG_RETRY_INTERVAL    = 15   # seconds between reconnect attempts while PG is down
STATS_EVERY_N_CYCLES = 12   # ~ once a minute at a 5s interval


async def connect_pg_with_retry() -> asyncpg.Pool:
    while True:
        try:
            pool = await asyncpg.create_pool(
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", "5432")),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                min_size=1,
                max_size=5,
                statement_cache_size=0,
            )
            # create_pool doesn't itself guarantee the server is reachable
            # until a query runs, so probe it once here.
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
            log.info("Connected to PostgreSQL.")
            return pool
        except Exception as e:
            log.error(f"PostgreSQL unreachable ({e}); retrying in {PG_RETRY_INTERVAL}s.")
            await asyncio.sleep(PG_RETRY_INTERVAL)


async def sync_messages(db_conn, pg: asyncpg.Pool) -> int:
    rows = await store.fetch_pending_messages(db_conn, BATCH_SIZE)
    if not rows:
        return 0
    await pg.executemany("""
        INSERT INTO messages
            (message_id, channel_id, stream_id, user_id, username, message, timestamp, subscriber, is_bot)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (message_id) DO NOTHING
    """, [
        (r["message_id"], r["channel_id"], r["stream_id"], r["user_id"], r["username"],
         r["message"], datetime.fromisoformat(r["timestamp"]), bool(r["subscriber"]), bool(r["is_bot"]))
        for r in rows
    ])
    await store.delete_messages(db_conn, [r["id"] for r in rows])
    return len(rows)


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
         r["content"], r["raw_tags"], datetime.fromisoformat(r["timestamp"]))
        for r in rows
    ])
    await store.delete_skipped(db_conn, [r["id"] for r in rows])
    return len(rows)


async def pull_channel_map(db_conn, pg: asyncpg.Pool) -> int:
    """Pushes Postgres's channels table into the local cache that the
    collector (worker.py) reads at startup. This is the only place that
    list flows in this direction — keeps the collector's Postgres
    dependency at exactly zero."""
    rows = await pg.fetch("SELECT id, name FROM channels")
    channel_id_map = {r["name"]: r["id"] for r in rows}
    if channel_id_map:
        await store.cache_channel_map(db_conn, channel_id_map)
    return len(channel_id_map)


async def sync_streams(db_conn, pg: asyncpg.Pool) -> int:
    rows = await store.fetch_all_streams(db_conn)
    if not rows:
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
        (r["id"], r["channel_id"], r["title"], r["game_name"],
         datetime.fromisoformat(r["started_at"]) if r["started_at"] else None,
         r["peak_viewers"], bool(r["is_live"]))
        for r in rows
    ])
    return len(rows)


async def run_sync_step(label: str, fn, db_conn, pg: asyncpg.Pool) -> tuple[int, asyncpg.Pool]:
    """Runs one sync step in isolation so a failure in one (e.g. a bad
    row) can't block the others from running, and so a lost connection
    is reconnected without derailing the rest of the pass."""
    try:
        return await fn(db_conn, pg), pg
    except (asyncpg.PostgresConnectionError, ConnectionError, OSError) as e:
        log.error(f"Lost PostgreSQL connection ({e}); reconnecting.")
        await pg.close()
        pg = await connect_pg_with_retry()
        return 0, pg
    except Exception as e:
        log.error(f"Sync pass failed ({label}): {e}")
        return 0, pg


async def main() -> None:
    await store.init_db()
    db_conn = await store.get_connection()
    pg = await connect_pg_with_retry()

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

            # streams first: messages carry a foreign key to streams, so
            # the stream row has to exist in Postgres before any message
            # referencing it can insert. Each step is isolated (see
            # run_sync_step) so one failing step — e.g. a stray FK
            # violation — can't block the others from running.
            n_streams, pg = await run_sync_step("streams",  sync_streams,  db_conn, pg)
            n_msg,     pg = await run_sync_step("messages", sync_messages, db_conn, pg)
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