"""
Shared Postgres helpers for the services that talk to Postgres
(Sync.py and Sanitizer.py). Worker.py never imports this.
"""
import asyncio
import os

import asyncpg

PG_RETRY_INTERVAL = 15  # seconds between reconnect attempts while PG is down


async def ensure_schema(pg: asyncpg.Pool) -> None:
    """Additive, idempotent schema changes the code depends on. Checks
    information_schema first rather than relying on ADD COLUMN IF NOT
    EXISTS alone, since ALTER TABLE takes an exclusive lock on messages
    even when the column is already there."""
    has_channel_name = await pg.fetchval("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = current_schema()
            AND table_name = 'messages' AND column_name = 'channel_name'
        )
    """)
    if not has_channel_name:
        await pg.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS channel_name TEXT")


async def connect_pg_with_retry(log) -> asyncpg.Pool:
    while True:
        pool = None
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
            await ensure_schema(pool)
            log.info("Connected to PostgreSQL.")
            return pool
        except Exception as e:
            if pool is not None:
                pool.terminate()
            log.error(f"PostgreSQL unreachable ({e}); retrying in {PG_RETRY_INTERVAL}s.")
            await asyncio.sleep(PG_RETRY_INTERVAL)
