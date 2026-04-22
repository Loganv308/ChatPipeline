import asyncio
import asyncpg
import re
import html
from datetime import datetime, timezone
from twitchio.ext import commands
from twitchio import Message
from dotenv import load_dotenv
import os

load_dotenv()

TOKEN     = os.getenv("TWITCH_TOKEN")
CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")

# Known bots to filter out
BOT_NAMES = {"streamelements", "nightbot", "fossabot", "moobot", "streamlabs"}

# ─── ETL: Transform / Sanitize ─────────────────────────────────────────────

def sanitize_message(text: str) -> str:
    if not text:
        return "[EMPTY MESSAGE]"
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'[\x00-\x08\x0b-\x1f\x7f]', '', text)
    return text[:500]

def sanitize_username(username: str) -> str:
    if not username:
        return "anonymous"
    return username.strip().lower()[:25]

def extract_message(msg: Message, channel_id_map: dict, active_streams: dict) -> tuple[dict | None, str | None]:

    if not msg.author:
        return None, "no_author"

    username     = sanitize_username(msg.author.name)
    channel_name = msg.channel.name.lower()
    channel_id   = channel_id_map.get(channel_name)

    if channel_id is None:
        return None, "unknown_channel"

    message_id = msg.id or f"{username}_{datetime.now().timestamp()}"

    return {
        "message_id": message_id,
        "channel":    channel_name,
        "channel_id": channel_id,
        "stream_id":  active_streams.get(channel_name),
        "user_id":    str(msg.author.id) if msg.author.id else None,
        "username":   username,
        "message":    sanitize_message(msg.content),
        "timestamp":  msg.timestamp or datetime.now(timezone.utc),
        "subscriber": msg.author.is_subscriber,
        "is_bot":     username in BOT_NAMES,
    }, None

# ─── Bot ───────────────────────────────────────────────────────────────────

class ScraperBot(commands.Bot):
    def __init__(self, db_pool: asyncpg.Pool, channel_id_map: dict[str, int], channels: list[str]):
        self.db             = db_pool
        self.channel_id_map = channel_id_map
        self.queue: list[dict] = []
        self.stats = {
            "received": 0,
            "inserted": 0,
            "skipped":  0,
            "errors":   0,
        }
        self.skip_queue: list[dict] = []
        self.active_streams: dict[str, str] = {}
        self.channels = channels

        super().__init__(
            token=TOKEN,
            prefix="!",
            initial_channels=channels,      # ← from DB, not hardcoded
            client_id=CLIENT_ID,
            client_secret=os.getenv("TWITCH_CLIENT_SECRET"),
            bot_id=os.getenv("TWITCH_BOT_ID"),
        )

    async def flush_skip_queue(self) -> None:
        while True:
            await asyncio.sleep(2)

            if not self.skip_queue:
                continue

            batch = self.skip_queue.copy()
            self.skip_queue.clear()

            try:
                await self.db.executemany("""
                    INSERT INTO skipped_messages
                        (reason, message_id, channel_name, username, content, raw_tags, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """, [
                    (
                        m["reason"],
                        m["message_id"],
                        m["channel_name"],
                        m["username"],
                        m["content"],
                        m["raw_tags"],
                        m["timestamp"],
                    )
                    for m in batch
                ])

            except Exception as e:
                print(f"Skip queue flush error: {e}")
                self.skip_queue = batch + self.skip_queue

    async def event_ready(self) -> None:
        print(f"Scraper ready | Watching: {', '.join(self.channels)}")

    async def event_message(self, message) -> None:
        if message.echo:
            return

        self.stats["received"] += 1

        record, skip_reason = extract_message(message, self.channel_id_map, self.active_streams)

        if skip_reason:
            self.stats["skipped"] += 1
            self.skip_queue.append({
                "reason":       skip_reason,
                "message_id":   message.id,
                "channel_name": message.channel.name.lower() if message.channel else None,
                "username":     message.author.name.lower() if message.author else None,
                "content":      sanitize_message(message.content) if message.content else None,
                "raw_tags":     str(message.tags) if message.tags else None,
                "timestamp":    datetime.now(timezone.utc),
            })
            return

        self.queue.append(record)
        await self.handle_commands(message)

    async def event_command_error(self, ctx, error) -> None:
        if isinstance(error, commands.CommandNotFound):
            return
        raise error

    async def flush_queue(self) -> None:
        while True:
            await asyncio.sleep(2)

            if not self.queue:
                continue

            batch = self.queue.copy()
            self.queue.clear()

            try:
                await self.db.executemany("""
                    INSERT INTO messages
                        (message_id, channel_id, stream_id, user_id, username, message, timestamp, subscriber, is_bot)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (message_id) DO NOTHING
                """, [
                    (
                        m["message_id"],
                        m["channel_id"],
                        m["stream_id"],
                        m["user_id"],
                        m["username"],
                        m["message"],
                        m["timestamp"],
                        m["subscriber"],
                        m["is_bot"],
                    )
                    for m in batch
                ])

                self.stats["inserted"] += len(batch)

            except Exception as e:
                self.stats["errors"] += 1
                print(f"DB flush error: {e}")
                self.queue = batch + self.queue

    async def log_stats(self) -> None:
        while True:
            await asyncio.sleep(60)
            row = await self.db.fetchrow(
                "SELECT pg_database_size(current_database()) AS size_bytes"
            )
            size_mb = row["size_bytes"] / (1024 * 1024)
            print(
                f"[Stats] received={self.stats['received']} "
                f"inserted={self.stats['inserted']} "
                f"skipped={self.stats['skipped']} "
                f"errors={self.stats['errors']} "
                f"queue={len(self.queue)} "
                f"db={size_mb:.1f}MB"
            )

    async def poll_streams(self) -> None:
        while True:
            try:
                streams = await self.fetch_streams(user_logins=self.channels)

                self.active_streams = {}

                if not streams:
                    await asyncio.sleep(60)
                    continue

                rows = []
                for stream in streams:
                    channel_name = stream.user.name.lower()
                    channel_id   = self.channel_id_map.get(channel_name)
                    if channel_id is None:
                        continue

                    self.active_streams[channel_name] = stream.id

                    rows.append((
                        stream.id,
                        channel_id,
                        stream.title,
                        stream.game_name,
                        stream.started_at,
                        stream.viewer_count,
                    ))

                await self.db.executemany("""
                    INSERT INTO streams (id, channel_id, title, game_name, started_at, peak_viewers)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (id) DO UPDATE SET
                        title        = EXCLUDED.title,
                        game_name    = EXCLUDED.game_name,
                        peak_viewers = GREATEST(streams.peak_viewers, EXCLUDED.peak_viewers)
                """, rows)

                print(f"[Streams] Updated {len(rows)} stream(s)")

            except Exception as e:
                print(f"[Streams] Poll error: {e}")

            await asyncio.sleep(60)

# ─── Main ──────────────────────────────────────────────────────────────────

async def main() -> None:
    print("Connecting to PostgreSQL...")
    db = await asyncpg.create_pool(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        min_size=2,
        max_size=5,
        statement_cache_size=0,
    )
    print("Connected to PostgreSQL.")

    rows = await db.fetch("SELECT id, name FROM channels")
    channel_id_map = {row["name"]: row["id"] for row in rows}
    channels       = list(channel_id_map.keys())
    print(f"Loaded channels: {channel_id_map}")

    if not channel_id_map:
        print("No channels found in DB. Add rows to the channels table first.")
        return

    bot = ScraperBot(db, channel_id_map, channels)

    await asyncio.gather(
        bot.start(),
        bot.flush_queue(),
        bot.flush_skip_queue(),
        bot.log_stats(),
        bot.poll_streams(),
    )

if __name__ == "__main__":
    asyncio.run(main())