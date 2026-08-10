"""
ChatPipeline worker.

Connects to Twitch chat and writes everything to a local SQLite buffer
(store.py). It never talks to Postgres on the hot path — the only place
Postgres is touched is an optional, best-effort bootstrap read of the
channel list at startup, which falls back to a local cache if Postgres
is unreachable. sync.py is the process responsible for draining the
buffer into Postgres.
"""
import asyncio
import re
import html
import json
from datetime import datetime, timezone

import aiohttp
from logstream import LogStream
from twitchio.ext import commands
from twitchio import Message
from dotenv import load_dotenv

import os
import store

load_dotenv()

TOKEN         = os.getenv("TWITCH_TOKEN")
CLIENT_ID     = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("TWITCH_REFRESH_TOKEN")

BOT_NAMES = {"streamelements", "nightbot", "fossabot", "moobot", "streamlabs"}

log = LogStream(service="ChatPipeline-Worker", host=os.getenv("LOG_HOST"))

LOCAL_FLUSH_INTERVAL = 2      # seconds between local buffer writes
STREAM_POLL_INTERVAL = 60     # seconds between Twitch stream-status polls
TOKEN_REFRESH_FRACTION = 0.8  # refresh once this much of the token's lifetime has elapsed


class TokenRefreshRestart(Exception):
    """Raised to unwind asyncio.gather() in main() for a clean restart
    after proactively refreshing the Twitch token -- twitchio's IRC
    connection can't hot-swap its token, so applying a new one means
    exiting so restart:always brings up a fresh process."""


# ─── OAuth token refresh ────────────────────────────────────────────────────
#
# Twitch chat access tokens are short-lived (observed ~4.3h for this app).
# twitchio 2.x's IRC connection doesn't refresh mid-session, so a token
# that goes stale crashes the bot with AuthenticationError. To avoid that:
#   - at startup, always exchange the refresh token for a brand-new access
#     token rather than trusting TWITCH_TOKEN's remaining lifetime
#   - Twitch rotates the refresh token on every use, so the new one is
#     persisted to the local buffer DB (survives container restarts) --
#     the .env value only ever works for the very first exchange
#   - a background task re-exchanges it before the access token expires,
#     then exits the process; restart:always (docker-compose.yml) brings
#     a fresh process up, which reads the now-persisted refresh token and
#     gets a fresh access token at boot

async def _exchange_refresh_token(refresh_token: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.post("https://id.twitch.tv/oauth2/token", data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }) as resp:
            body = await resp.json()
            if resp.status != 200:
                raise RuntimeError(f"Token refresh failed ({resp.status}): {body}")
            return body


async def get_fresh_access_token(db_conn) -> tuple[str, int | None]:
    """Returns (access_token, seconds_until_expiry). expiry is None when
    no refresh token is configured, meaning TOKEN_TOKEN is used as-is and
    no proactive refresh will be scheduled."""
    refresh_token = await store.load_kv(db_conn, "twitch_refresh_token") or REFRESH_TOKEN
    if not refresh_token or not CLIENT_SECRET:
        log.info(
            "TWITCH_REFRESH_TOKEN/TWITCH_CLIENT_SECRET not configured; "
            "using TWITCH_TOKEN as-is with no auto-refresh."
        )
        return TOKEN, None

    data = await _exchange_refresh_token(refresh_token)
    await store.save_kv(db_conn, "twitch_refresh_token", data["refresh_token"])
    log.info(f"Refreshed Twitch access token (expires in {data['expires_in']}s).")
    return data["access_token"], data["expires_in"]

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
        "timestamp":  (msg.timestamp or datetime.now(timezone.utc)).isoformat(),
        "subscriber": int(bool(msg.author.is_subscriber)),
        "is_bot":     int(username in BOT_NAMES),
    }, None


# ─── Channel bootstrap (local cache only — no Postgres dependency) ────────
#
# The collector never talks to Postgres, full stop — not even at startup.
# Normally sync.py pulls the channels table from Postgres and writes it
# into store's channel_cache, and this just waits for that to happen.
#
# But if Postgres has never been reachable even once (not a transient
# outage -- e.g. it's genuinely unreachable from this network), there's
# nothing for sync.py to pull, and the collector would wait forever. For
# that case, set SEED_CHANNELS in .env as a one-time manual bootstrap:
#   SEED_CHANNELS=somechannel,otherchannel
# (channel login name : numeric Twitch user id, comma-separated). This
# gets written into the same local cache sync.py would have populated,
# so it only needs to be read once -- after that the normal cache applies,
# and sync.py will overwrite it with the real Postgres data once it can
# finally connect.

CHANNEL_CACHE_POLL_INTERVAL = 10  # seconds between checks while waiting

def _parse_seed_channels() -> tuple[dict[str, int], list[str]]:
    """Returns (explicit id->name pairs already resolved, plain usernames
    still needing a Twitch API lookup). SEED_CHANNELS accepts a
    comma-separated list or a JSON array, entries mixed freely between
    bare names and name:id pairs:
        SEED_CHANNELS=paymoneywubby,somechannel,otherstreamer
        SEED_CHANNELS=["paymoneywubby", "somechannel"]
    A bare name resolves its numeric Twitch id automatically at startup;
    `name:id` skips that lookup if you already know the id."""
    raw = os.getenv("SEED_CHANNELS", "").strip()
    if not raw:
        return {}, []

    if raw.startswith("["):
        try:
            entries = json.loads(raw)
            if not isinstance(entries, list):
                raise ValueError("SEED_CHANNELS JSON must be an array")
        except Exception as e:
            log.error(f"Failed to parse SEED_CHANNELS as JSON ({e}); falling back to comma-split.")
            entries = raw.split(",")
    else:
        entries = raw.split(",")

    resolved: dict[str, int] = {}
    unresolved: list[str] = []
    for entry in entries:
        entry = str(entry).strip()
        if not entry:
            continue
        if ":" in entry:
            name, _, channel_id = entry.partition(":")
            name = name.strip().lower()
            channel_id = channel_id.strip()
            if not name or not channel_id.isdigit():
                log.error(f"Skipping malformed SEED_CHANNELS entry: {entry!r} (expected name:id)")
                continue
            resolved[name] = int(channel_id)
        else:
            name = entry.strip().lower()
            if name:
                unresolved.append(name)
    return resolved, unresolved


async def _resolve_channel_ids(usernames: list[str]) -> dict[str, int]:
    """Looks up numeric Twitch user ids for plain usernames via the Twitch
    API. Uses an app access token generated from client_id/client_secret --
    twitchio 2.x's plain Client doesn't accept token=/client_id= directly,
    so from_client_credentials() is the documented way to get an API-only
    client without a full user login."""
    if not usernames:
        return {}
    client_secret = os.getenv("TWITCH_TOKEN")
    if not client_secret:
        log.error(
            "Cannot resolve SEED_CHANNELS usernames: TWITCH_TOKEN "
            "is required for the API lookup but isn't set in .env."
        )
        return {}
    import twitchio
    try:
        client = await twitchio.Client.from_client_credentials(
            client_id=CLIENT_ID, client_secret=client_secret
        )
    except Exception as e:
        log.error(f"Failed to obtain an app access token for SEED_CHANNELS lookup: {e}")
        return {}
    try:
        users = await client.fetch_users(names=usernames)
    except Exception as e:
        log.error(f"Failed to resolve SEED_CHANNELS usernames via Twitch API: {e}")
        return {}
    finally:
        await client.close()

    found = {u.name.lower(): int(u.id) for u in users}
    missing = set(usernames) - set(found)
    if missing:
        log.error(f"SEED_CHANNELS: could not resolve these usernames on Twitch: {sorted(missing)}")
    return found

async def wait_for_channel_map(db_conn) -> dict[str, int]:
    channel_id_map = await store.load_cached_channel_map(db_conn)
    if channel_id_map:
        log.info(f"Loaded {len(channel_id_map)} channel(s) from local cache.")
        return channel_id_map

    explicit, plain_names = _parse_seed_channels()
    seeded = dict(explicit)
    if plain_names:
        seeded.update(await _resolve_channel_ids(plain_names))

    if seeded:
        await store.cache_channel_map(db_conn, seeded)
        log.info(
            f"Local cache was empty; bootstrapped {len(seeded)} channel(s) "
            f"from SEED_CHANNELS. sync.py will overwrite this with real "
            f"Postgres data once it can connect."
        )
        return seeded

    log.info(
        "Local channel cache is empty — waiting for sync.py to populate it "
        "from Postgres. (Make sure sync.py is running, or set SEED_CHANNELS "
        "in .env to bootstrap without Postgres.)"
    )
    while not channel_id_map:
        await asyncio.sleep(CHANNEL_CACHE_POLL_INTERVAL)
        channel_id_map = await store.load_cached_channel_map(db_conn)

    log.info(f"Loaded {len(channel_id_map)} channel(s) from local cache.")
    return channel_id_map


# ─── Bot ───────────────────────────────────────────────────────────────────

class ScraperBot(commands.Bot):
    def __init__(
        self, db_conn, channel_id_map: dict[str, int], channels: list[str],
        access_token: str, token_ttl: int | None,
    ):
        self.db              = db_conn
        self.channel_id_map  = channel_id_map
        self.channels        = channels
        self.token_ttl       = token_ttl
        self.msg_queue: list[dict] = []
        self.skip_queue: list[dict] = []
        self.active_streams: dict[str, str] = {}
        self.stats = {"received": 0, "buffered": 0, "skipped": 0, "errors": 0}

        super().__init__(
            token=access_token,
            prefix="!",
            initial_channels=channels,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

    async def event_ready(self) -> None:
        log.info(f"Worker ready | Watching: {', '.join(self.channels)}")

    async def event_message(self, message) -> None:
        if message.echo:
            return

        self.stats["received"] += 1

        record, skip_reason = extract_message(message, self.channel_id_map, self.active_streams)

        if skip_reason:
            self.stats["skipped"] += 1
            self.skip_queue.append((
                skip_reason,
                message.id,
                message.channel.name.lower() if message.channel else None,
                message.author.name.lower() if message.author else None,
                sanitize_message(message.content) if message.content else None,
                str(message.tags) if message.tags else None,
                datetime.now(timezone.utc).isoformat(),
            ))
            return

        self.msg_queue.append(record)
        await self.handle_commands(message)

    async def event_command_error(self, ctx, error) -> None:
        if isinstance(error, commands.CommandNotFound):
            return
        raise error

    async def flush_to_local_buffer(self) -> None:
        """Writes everything to SQLite. This never depends on Postgres,
        so it never blocks or fails because the database server is down."""
        while True:
            await asyncio.sleep(LOCAL_FLUSH_INTERVAL)

            if self.msg_queue:
                batch, self.msg_queue = self.msg_queue, []
                try:
                    rows = [(
                        m["message_id"], m["channel"], m["channel_id"], m["stream_id"],
                        m["user_id"], m["username"], m["message"], m["timestamp"],
                        m["subscriber"], m["is_bot"],
                    ) for m in batch]
                    await store.insert_messages(self.db, rows)
                    self.stats["buffered"] += len(batch)
                except Exception as e:
                    self.stats["errors"] += 1
                    log.error(f"Local buffer write error (messages): {e}")
                    self.msg_queue = batch + self.msg_queue

            if self.skip_queue:
                batch, self.skip_queue = self.skip_queue, []
                try:
                    await store.insert_skipped(self.db, batch)
                except Exception as e:
                    self.stats["errors"] += 1
                    log.error(f"Local buffer write error (skipped): {e}")
                    self.skip_queue = batch + self.skip_queue

    async def refresh_token_periodically(self) -> None:
        """No-op if no refresh token is configured (self.token_ttl is
        None). Otherwise, proactively re-exchanges the refresh token
        before the current access token expires, then exits the process
        cleanly so restart:always brings up a fresh one -- twitchio's
        IRC connection can't hot-swap its token, so a restart is the
        reliable way to apply a new one."""
        if self.token_ttl is None:
            return
        wait = self.token_ttl * TOKEN_REFRESH_FRACTION
        while True:
            await asyncio.sleep(wait)
            try:
                await get_fresh_access_token(self.db)
            except Exception as e:
                # Current token is still valid for now -- retry sooner
                # than waiting out the full TTL again rather than crash
                # on a transient refresh failure.
                log.error(f"Proactive token refresh failed: {e}")
                wait = 300
                continue
            log.info("Refreshed Twitch token proactively; restarting to apply it.")
            raise TokenRefreshRestart()

    async def log_stats(self) -> None:
        while True:
            await asyncio.sleep(60)
            pending = await store.count_pending(self.db)
            log.info(
                f"[Stats] received={self.stats['received']} "
                f"buffered={self.stats['buffered']} "
                f"skipped={self.stats['skipped']} "
                f"errors={self.stats['errors']} "
                f"local_backlog={pending}"
            )

    async def poll_streams(self) -> None:
        while True:
            try:
                streams = await self.fetch_streams(user_logins=self.channels)

                live_channel_ids = []
                rows = []

                for stream in streams:
                    channel_name = stream.user.name.lower()
                    channel_id   = self.channel_id_map.get(channel_name)
                    if channel_id is None:
                        continue

                    live_channel_ids.append(channel_id)
                    self.active_streams[channel_name] = stream.id

                    rows.append((
                        stream.id,
                        channel_id,
                        stream.title,
                        stream.game_name,
                        stream.started_at.isoformat() if stream.started_at else None,
                        stream.viewer_count,
                    ))

                # Channels that dropped off the live list get their local
                # 'active_streams' entry cleared too, so new messages stop
                # getting tagged with a stale stream_id.
                now_live_names = {s.user.name.lower() for s in streams}
                for name in list(self.active_streams):
                    if name not in now_live_names:
                        del self.active_streams[name]

                if rows:
                    await store.upsert_streams(self.db, rows)
                await store.mark_channels_offline(self.db, live_channel_ids)

                log.info(f"[Streams] Buffered {len(rows)} live stream(s) | Live: {list(now_live_names) or 'none'}")

            except Exception as e:
                log.error(f"[Streams] Poll error: {e}")

            await asyncio.sleep(STREAM_POLL_INTERVAL)


# ─── Main ──────────────────────────────────────────────────────────────────

async def main() -> None:
    await store.init_db()
    db_conn = await store.get_connection()

    channel_id_map = await wait_for_channel_map(db_conn)
    channels = list(channel_id_map.keys())

    access_token, token_ttl = await get_fresh_access_token(db_conn)
    bot = ScraperBot(db_conn, channel_id_map, channels, access_token, token_ttl)

    try:
        await asyncio.gather(
            bot.start(),
            bot.flush_to_local_buffer(),
            bot.log_stats(),
            bot.poll_streams(),
            bot.refresh_token_periodically(),
        )
    except TokenRefreshRestart:
        pass
    finally:
        await db_conn.close()

if __name__ == "__main__":
    asyncio.run(main())