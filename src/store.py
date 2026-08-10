"""
Local durable buffer shared by worker.py (writer) and sync.py (reader).

SQLite runs in WAL mode so the worker can keep appending rows while the
sync process reads and deletes them concurrently, without either one
blocking the other. This file has no dependency on Postgres or Twitch —
it's just the queue.
"""
import os
from pathlib import Path

import aiosqlite

DB_PATH = Path(os.getenv("LOCAL_BUFFER_PATH", "./buffer.db"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS pending_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id  TEXT NOT NULL,
    channel     TEXT NOT NULL,
    channel_id  INTEGER NOT NULL,
    stream_id   TEXT,
    user_id     TEXT,
    username    TEXT,
    message     TEXT,
    timestamp   TEXT,
    subscriber  INTEGER,
    is_bot      INTEGER,
    created_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pending_skipped_messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    reason        TEXT,
    message_id    TEXT,
    channel_name  TEXT,
    username      TEXT,
    content       TEXT,
    raw_tags      TEXT,
    timestamp     TEXT,
    created_at    TEXT DEFAULT (datetime('now'))
);

-- Streams are current *state*, not an append log: one row per stream id,
-- carrying the latest known values plus whether it's still live. The
-- worker upserts here; the sync program re-upserts the whole table into
-- Postgres each pass rather than draining/deleting rows.
CREATE TABLE IF NOT EXISTS pending_streams (
    id            TEXT PRIMARY KEY,
    channel_id    INTEGER NOT NULL,
    title         TEXT,
    game_name     TEXT,
    started_at    TEXT,
    peak_viewers  INTEGER,
    is_live       INTEGER DEFAULT 1,
    updated_at    TEXT DEFAULT (datetime('now'))
);

-- Cached channel name -> id map so the worker can boot even if Postgres
-- is unreachable at startup.
CREATE TABLE IF NOT EXISTS channel_cache (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL UNIQUE
);

-- Small durable key/value store. Currently holds the Twitch OAuth
-- refresh token: Twitch rotates it on every use, so the value in .env
-- only ever works for the first refresh -- after that, the current one
-- has to survive container restarts, and this volume-backed table is
-- what does that.
CREATE TABLE IF NOT EXISTS kv_state (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pending_messages_id ON pending_messages(id);
CREATE INDEX IF NOT EXISTS idx_pending_skipped_id ON pending_skipped_messages(id);
"""


async def get_connection() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(DB_PATH, timeout=30)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    await conn.execute("PRAGMA busy_timeout=5000;")
    return conn


async def init_db() -> None:
    conn = await get_connection()
    try:
        await conn.executescript(SCHEMA)
        await conn.commit()
    finally:
        await conn.close()


# ─── Writes (worker side) ───────────────────────────────────────────────

async def insert_messages(conn: aiosqlite.Connection, rows: list[tuple]) -> None:
    if not rows:
        return
    await conn.executemany("""
        INSERT INTO pending_messages
            (message_id, channel, channel_id, stream_id, user_id, username, message, timestamp, subscriber, is_bot)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    await conn.commit()


async def insert_skipped(conn: aiosqlite.Connection, rows: list[tuple]) -> None:
    if not rows:
        return
    await conn.executemany("""
        INSERT INTO pending_skipped_messages
            (reason, message_id, channel_name, username, content, raw_tags, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)
    await conn.commit()


async def upsert_streams(conn: aiosqlite.Connection, rows: list[tuple]) -> None:
    """rows: (id, channel_id, title, game_name, started_at, peak_viewers)"""
    if not rows:
        return
    await conn.executemany("""
        INSERT INTO pending_streams (id, channel_id, title, game_name, started_at, peak_viewers, is_live)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(id) DO UPDATE SET
            title        = excluded.title,
            game_name    = excluded.game_name,
            peak_viewers = MAX(peak_viewers, excluded.peak_viewers),
            is_live      = 1,
            updated_at   = datetime('now')
    """, rows)
    await conn.commit()


async def mark_channels_offline(conn: aiosqlite.Connection, live_channel_ids: list[int]) -> None:
    """Flip is_live=0 for any locally-known stream whose channel isn't live anymore."""
    if live_channel_ids:
        placeholders = ",".join("?" for _ in live_channel_ids)
        await conn.execute(f"""
            UPDATE pending_streams
            SET is_live = 0, updated_at = datetime('now')
            WHERE is_live = 1 AND channel_id NOT IN ({placeholders})
        """, live_channel_ids)
    else:
        await conn.execute("""
            UPDATE pending_streams SET is_live = 0, updated_at = datetime('now')
            WHERE is_live = 1
        """)
    await conn.commit()


async def cache_channel_map(conn: aiosqlite.Connection, channel_id_map: dict[str, int]) -> None:
    await conn.executemany("""
        INSERT INTO channel_cache (id, name) VALUES (?, ?)
        ON CONFLICT(id) DO UPDATE SET name = excluded.name
    """, [(v, k) for k, v in channel_id_map.items()])
    await conn.commit()


async def load_cached_channel_map(conn: aiosqlite.Connection) -> dict[str, int]:
    cursor = await conn.execute("SELECT id, name FROM channel_cache")
    rows = await cursor.fetchall()
    return {name: id_ for id_, name in rows}


async def save_kv(conn: aiosqlite.Connection, key: str, value: str) -> None:
    await conn.execute("""
        INSERT INTO kv_state (key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (key, value))
    await conn.commit()


async def load_kv(conn: aiosqlite.Connection, key: str) -> str | None:
    cursor = await conn.execute("SELECT value FROM kv_state WHERE key = ?", (key,))
    row = await cursor.fetchone()
    return row[0] if row else None


# ─── Reads / deletes (sync side) ────────────────────────────────────────

async def _fetch_rows(conn: aiosqlite.Connection, query: str, params=()) -> list[dict]:
    cursor = await conn.execute(query, params)
    rows = await cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, r)) for r in rows]


async def fetch_pending_messages(conn: aiosqlite.Connection, limit: int = 500) -> list[dict]:
    return await _fetch_rows(conn, "SELECT * FROM pending_messages ORDER BY id LIMIT ?", (limit,))


async def fetch_pending_skipped(conn: aiosqlite.Connection, limit: int = 500) -> list[dict]:
    return await _fetch_rows(conn, "SELECT * FROM pending_skipped_messages ORDER BY id LIMIT ?", (limit,))


async def fetch_all_streams(conn: aiosqlite.Connection) -> list[dict]:
    return await _fetch_rows(conn, "SELECT * FROM pending_streams")


async def delete_messages(conn: aiosqlite.Connection, ids: list[int]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    await conn.execute(f"DELETE FROM pending_messages WHERE id IN ({placeholders})", ids)
    await conn.commit()


async def delete_skipped(conn: aiosqlite.Connection, ids: list[int]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    await conn.execute(f"DELETE FROM pending_skipped_messages WHERE id IN ({placeholders})", ids)
    await conn.commit()


async def count_pending(conn: aiosqlite.Connection) -> dict:
    msgs = await conn.execute_fetchall("SELECT COUNT(*) FROM pending_messages")
    skipped = await conn.execute_fetchall("SELECT COUNT(*) FROM pending_skipped_messages")
    return {"messages": msgs[0][0], "skipped": skipped[0][0]}