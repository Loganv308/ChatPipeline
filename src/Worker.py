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
import json
import signal
from collections import deque
from datetime import datetime, timezone

import aiohttp
from logstream import LogStream
from twitchio.ext import commands
from twitchio import Message
from dotenv import load_dotenv

import os
import store
from sanitize import BOT_NAMES, sanitize_message, sanitize_username
from irc import parse_privmsg, record_from_privmsg

load_dotenv()

TOKEN         = os.getenv("TWITCH_TOKEN")
CLIENT_ID     = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("TWITCH_REFRESH_TOKEN")

log = LogStream(service="ChatPipeline-Worker-Prod", host=os.getenv("LOG_HOST"))

LOCAL_FLUSH_INTERVAL = 2      # seconds between local buffer writes
STREAM_POLL_INTERVAL = 60     # seconds between Twitch stream-status polls
TOKEN_REFRESH_FRACTION = 0.8  # refresh once this much of the token's lifetime has elapsed
JOIN_TIMEOUT = 30             # seconds to wait for a new connection to join every channel
SWAP_OVERLAP = 5              # seconds both connections stay open during a token-refresh swap
SEEN_IDS_MAX = 100_000        # recent message ids remembered for de-duplication


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
#   - a background task re-exchanges it before the access token expires
#     and swaps in a second chat connection using the new token, closing
#     the old one only once the new one has joined every channel, so no
#     chat is missed (messages both connections see are de-duplicated by
#     id) -- twitchio's IRC connection can't hot-swap its token in place

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

# sanitize_message / sanitize_username / BOT_NAMES live in sanitize.py,
# shared with Sanitizer.py so stored rows are repaired to the same rules.

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
        # twitchio returns a naive datetime holding UTC; tag it as UTC so
        # nothing downstream reads it as the container's local time (TZ).
        "timestamp":  (msg.timestamp.replace(tzinfo=timezone.utc) if msg.timestamp
                       else datetime.now(timezone.utc)).isoformat(),
        "subscriber": int(bool(msg.author.is_subscriber)),
        "is_bot":     int(username in BOT_NAMES),
    }, None


# ─── Channel list (local cache only — no Postgres dependency) ────────
#
# The collector never talks to Postgres, full stop — not even at startup.
#
# If SEED_CHANNELS is set in .env, it is the channel list: the collector
# joins exactly those channels and nothing else, on every start. Their
# Twitch user ids are written into store's twitch_channels cache, and
# sync.py pushes them from there into Postgres's channels table (matched
# by name, filling twitch_id) so their messages can insert.
#   SEED_CHANNELS=somechannel,otherchannel
#
# If SEED_CHANNELS is empty, the channel list comes from Postgres instead:
# sync.py pulls the channels table into twitch_channels, and this just
# waits for that to happen.

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
    API. Uses an app access token generated from client_id/client_secret,
    calling Helix directly -- twitchio 2.x's from_client_credentials()
    returns a half-initialized Client that isn't safe to await or close."""
    if not usernames:
        return {}
    if not CLIENT_SECRET:
        log.error(
            "Cannot resolve SEED_CHANNELS usernames: TWITCH_CLIENT_SECRET "
            "is required for the API lookup but isn't set in .env."
        )
        return {}

    found: dict[str, int] = {}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post("https://id.twitch.tv/oauth2/token", data={
                "grant_type":    "client_credentials",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }) as resp:
                body = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"({resp.status}): {body}")
                app_token = body["access_token"]
        except Exception as e:
            log.error(f"Failed to obtain an app access token for SEED_CHANNELS lookup: {e}")
            return {}

        headers = {"Client-Id": CLIENT_ID, "Authorization": f"Bearer {app_token}"}
        try:
            # Helix accepts at most 100 logins per request.
            for i in range(0, len(usernames), 100):
                params = [("login", name) for name in usernames[i:i + 100]]
                async with session.get(
                    "https://api.twitch.tv/helix/users", params=params, headers=headers
                ) as resp:
                    body = await resp.json()
                    if resp.status != 200:
                        raise RuntimeError(f"({resp.status}): {body}")
                    for user in body["data"]:
                        found[user["login"].lower()] = int(user["id"])
        except Exception as e:
            log.error(f"Failed to resolve SEED_CHANNELS usernames via Twitch API: {e}")
            return found

    missing = set(usernames) - set(found)
    if missing:
        log.error(f"SEED_CHANNELS: could not resolve these usernames on Twitch: {sorted(missing)}")
    return found

async def wait_for_channel_map(db_conn) -> dict[str, int]:
    cached = await store.load_cached_channel_map(db_conn)

    explicit, plain_names = _parse_seed_channels()
    if explicit or plain_names:
        # Reuse ids already in the cache (from a previous run or from
        # Postgres) so only genuinely new names hit the Twitch API.
        seeded = dict(explicit)
        to_lookup = []
        for name in plain_names:
            if name in cached:
                seeded[name] = cached[name]
            else:
                to_lookup.append(name)
        seeded.update(await _resolve_channel_ids(to_lookup))

        if seeded:
            await store.cache_channel_map(db_conn, seeded)
            log.info(f"Using {len(seeded)} channel(s) from SEED_CHANNELS.")
            return seeded
        log.error("SEED_CHANNELS is set but none of its channels resolved; falling back to the local cache.")

    channel_id_map = cached
    if channel_id_map:
        log.info(f"Loaded {len(channel_id_map)} channel(s) from local cache.")
        return channel_id_map

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


# ─── Recent-messages backfill ─────────────────────────────────────────────
#
# Twitch has no chat history API: anything sent while the collector isn't
# connected never arrives over IRC. recent-messages.robotty.de (the
# service the Chatterino client uses) keeps roughly the last 800 messages
# per channel, with Twitch's own message ids and send times. On startup
# the collector buffers any of those newer than the last message it saved
# for that channel. Overlap with live chat is harmless: duplicates are
# dropped in memory by message id, and again by Sync.py's
# ON CONFLICT (message_id).

RECENT_MESSAGES_URL = "https://recent-messages.robotty.de/api/v2/recent-messages/{}"
LAST_SEEN_KEY = "last_seen_ts:{}"  # kv_state: send time of the newest saved message per channel

# parse_privmsg / record_from_privmsg live in irc.py, shared with Backfill.py.


# ─── Chat connection ───────────────────────────────────────────────────────

class ChatConnection(commands.Bot):
    """One authenticated IRC connection. Holds no state of its own beyond
    join tracking -- everything it receives goes to the Collector, so a
    connection can be replaced (token refresh) without losing anything."""

    def __init__(self, collector: "Collector", access_token: str):
        self.collector  = collector
        self.ready      = asyncio.Event()
        self.joined:    set[str] = set()
        self.all_joined = asyncio.Event()
        super().__init__(
            token=access_token,
            prefix="!",
            initial_channels=collector.channels,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

    async def event_ready(self) -> None:
        self.ready.set()

    async def event_channel_joined(self, channel) -> None:
        self.joined.add(channel.name.lower())
        if self.joined >= set(self.collector.channels):
            self.all_joined.set()

    async def event_message(self, message) -> None:
        if message.echo:
            return
        self.collector.handle_message(message)

    async def wait_until_joined(self) -> bool:
        """True once connected and every channel is joined, or connected
        with some joins still outstanding after JOIN_TIMEOUT (logged).
        False if it never connected at all."""
        try:
            await asyncio.wait_for(self.all_joined.wait(), JOIN_TIMEOUT)
        except asyncio.TimeoutError:
            if not self.ready.is_set():
                return False
            missing = sorted(set(self.collector.channels) - self.joined)
            log.error(f"Chat connected but these channels didn't join within {JOIN_TIMEOUT}s: {missing}")
        return True


# ─── Collector ─────────────────────────────────────────────────────────────

class Collector:
    def __init__(self, db_conn, channel_id_map: dict[str, int], token_ttl: int | None):
        self.db              = db_conn
        self.channel_id_map  = channel_id_map
        self.channels        = list(channel_id_map.keys())
        self.token_ttl       = token_ttl
        self.msg_queue: list[dict] = []
        self.skip_queue: list[tuple] = []
        self.active_streams: dict[str, str] = {}               # channel -> live stream id
        self.active_stream_started: dict[str, datetime] = {}   # channel -> that stream's start
        self.stats = {"received": 0, "buffered": 0, "skipped": 0, "errors": 0, "backfilled": 0}
        self._seen_order: deque[str] = deque()
        self._seen_ids:   set[str] = set()
        self.chat: ChatConnection | None = None
        self.chat_task: asyncio.Task | None = None

    # ── Ingest ──

    def _first_sighting(self, message_id: str | None) -> bool:
        """False if this id was already queued, e.g. by both connections
        during a token-refresh overlap, or by backfill and live chat."""
        if not message_id:
            return True
        if message_id in self._seen_ids:
            return False
        self._seen_ids.add(message_id)
        self._seen_order.append(message_id)
        if len(self._seen_order) > SEEN_IDS_MAX:
            self._seen_ids.discard(self._seen_order.popleft())
        return True

    def handle_message(self, message) -> None:
        if not self._first_sighting(message.id):
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

    # ── Local buffer ──

    async def flush_once(self) -> None:
        """Writes everything queued to SQLite. Never depends on Postgres,
        so it never blocks or fails because the database server is down."""
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
            else:
                await self._record_last_seen(batch)

        if self.skip_queue:
            batch, self.skip_queue = self.skip_queue, []
            try:
                await store.insert_skipped(self.db, batch)
            except Exception as e:
                self.stats["errors"] += 1
                log.error(f"Local buffer write error (skipped): {e}")
                self.skip_queue = batch + self.skip_queue

    async def _record_last_seen(self, batch: list[dict]) -> None:
        """Remembers the newest send time saved per channel, which is where
        the next startup's backfill picks up from."""
        try:
            newest: dict[str, datetime] = {}
            for m in batch:
                ts = datetime.fromisoformat(m["timestamp"])
                if m["channel"] not in newest or ts > newest[m["channel"]]:
                    newest[m["channel"]] = ts
            updates = {}
            for channel, ts in newest.items():
                current = await store.load_kv(self.db, LAST_SEEN_KEY.format(channel))
                if not current or ts > datetime.fromisoformat(current):
                    updates[LAST_SEEN_KEY.format(channel)] = ts.isoformat()
            await store.save_kv_many(self.db, updates)
        except Exception as e:
            log.error(f"Couldn't record last-seen message times: {e}")

    async def flush_to_local_buffer(self) -> None:
        while True:
            await asyncio.sleep(LOCAL_FLUSH_INTERVAL)
            await self.flush_once()

    # ── Backfill ──

    async def backfill_recent_messages(self) -> None:
        """Buffers each channel's recent messages (see the section comment
        above) that are newer than the last one saved before this start."""
        total = 0
        timeout = aiohttp.ClientTimeout(total=20)
        headers = {"User-Agent": "ChatPipeline (Twitch chat archiver)"}
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            for channel in self.channels:
                try:
                    async with session.get(RECENT_MESSAGES_URL.format(channel)) as resp:
                        body = await resp.json()
                    if body.get("error"):
                        raise RuntimeError(f"{body.get('error_code')}: {body['error']}")
                except Exception as e:
                    log.error(f"[Backfill] {channel}: recent-messages unavailable ({e})")
                    continue

                last_seen_raw = await store.load_kv(self.db, LAST_SEEN_KEY.format(channel))
                last_seen = datetime.fromisoformat(last_seen_raw) if last_seen_raw else None
                started   = self.active_stream_started.get(channel)

                added = 0
                for line in body.get("messages", []):
                    parsed = parse_privmsg(line)
                    if not parsed or parsed["channel"] != channel:
                        continue
                    record = record_from_privmsg(parsed, self.channel_id_map[channel], None)
                    if record is None:
                        continue
                    sent = datetime.fromisoformat(record["timestamp"])
                    if last_seen and sent <= last_seen:
                        continue
                    # Only attribute it to the current stream if it was sent
                    # during that stream; earlier messages stay unattributed.
                    if started and sent >= started:
                        record["stream_id"] = self.active_streams.get(channel)
                    if self._first_sighting(record["message_id"]):
                        self.msg_queue.append(record)
                        added += 1
                if added:
                    since = f"since {last_seen_raw}" if last_seen_raw else "(no previous save)"
                    log.info(f"[Backfill] {channel}: {added} message(s) {since}")
                total += added

        self.stats["backfilled"] += total
        log.info(f"[Backfill] Recovered {total} message(s) across {len(self.channels)} channel(s).")

    # ── Chat connection lifecycle ──

    async def connect_chat(self, access_token: str) -> bool:
        """Starts a new connection and makes it the active one once it has
        joined. The previous connection (if any) is closed only after
        that, so the two overlap rather than leaving a gap."""
        new = ChatConnection(self, access_token)
        if self.chat is None:
            # First connection: learn which channels are live before chat
            # starts flowing, so the first messages get their stream_id.
            # (Uses only the API side of the connection, not IRC.)
            await self.poll_streams_once(new)
        task = asyncio.create_task(new.start())
        if not await new.wait_until_joined():
            task.cancel()
            return False
        old = self.chat
        self.chat, self.chat_task = new, task
        log.info(f"Worker ready | Watching: {', '.join(self.channels)}")
        if old is not None:
            # Keep the old connection briefly so nothing in flight to it is
            # lost; messages both receive are dropped by _first_sighting.
            await asyncio.sleep(SWAP_OVERLAP)
            await old.close()
        return True

    async def watch_chat(self) -> None:
        """Ends the process if the active connection dies on its own
        (restart:always then brings a fresh one up). A connection closed
        because it was replaced is expected and ignored."""
        while True:
            task = self.chat_task
            await task
            if task is self.chat_task:
                raise RuntimeError("Chat connection closed unexpectedly.")

    async def refresh_token_periodically(self) -> None:
        """No-op if no refresh token is configured. Otherwise exchanges the
        refresh token before the access token expires and swaps in a new
        connection with it (see connect_chat)."""
        if self.token_ttl is None:
            return
        wait = self.token_ttl * TOKEN_REFRESH_FRACTION
        while True:
            await asyncio.sleep(wait)
            try:
                access_token, ttl = await get_fresh_access_token(self.db)
                if not await self.connect_chat(access_token):
                    raise RuntimeError("new chat connection didn't connect")
            except Exception as e:
                # The current connection keeps running on its still-valid
                # token -- retry well before it expires.
                log.error(f"Proactive token refresh failed: {e}")
                wait = 300
                continue
            log.info("Swapped chat connection onto refreshed token with no gap.")
            wait = ttl * TOKEN_REFRESH_FRACTION

    # ── Stats / streams ──

    async def log_stats(self) -> None:
        while True:
            await asyncio.sleep(60)
            pending = await store.count_pending(self.db)
            log.info(
                f"[Stats] received={self.stats['received']} "
                f"buffered={self.stats['buffered']} "
                f"backfilled={self.stats['backfilled']} "
                f"skipped={self.stats['skipped']} "
                f"errors={self.stats['errors']} "
                f"local_backlog={pending}"
            )

    async def poll_streams_once(self, client: ChatConnection | None = None) -> None:
        try:
            streams = await (client or self.chat).fetch_streams(user_logins=self.channels)

            live_channel_ids = []
            rows = []

            for stream in streams:
                channel_name = stream.user.name.lower()
                channel_id   = self.channel_id_map.get(channel_name)
                if channel_id is None:
                    continue

                live_channel_ids.append(channel_id)
                self.active_streams[channel_name] = stream.id
                if stream.started_at:
                    self.active_stream_started[channel_name] = stream.started_at

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
                    self.active_stream_started.pop(name, None)

            if rows:
                await store.upsert_streams(self.db, rows)
            await store.mark_channels_offline(self.db, live_channel_ids)

            log.info(f"[Streams] Buffered {len(rows)} live stream(s) | Live: {list(now_live_names) or 'none'}")

        except Exception as e:
            log.error(f"[Streams] Poll error: {e}")

    async def poll_streams(self) -> None:
        while True:
            await asyncio.sleep(STREAM_POLL_INTERVAL)
            await self.poll_streams_once()


# ─── Main ──────────────────────────────────────────────────────────────────

async def main() -> None:
    # docker stop sends SIGTERM; turn it into a cancellation so the finally
    # block below gets to write queued messages before the process exits.
    main_task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, main_task.cancel)
        except (NotImplementedError, AttributeError):
            pass  # Windows: Ctrl+C still raises KeyboardInterrupt

    await store.init_db()
    db_conn = await store.get_connection()
    collector = None

    try:
        channel_id_map = await wait_for_channel_map(db_conn)
        access_token, token_ttl = await get_fresh_access_token(db_conn)
        collector = Collector(db_conn, channel_id_map, token_ttl)

        if not await collector.connect_chat(access_token):
            raise RuntimeError("Couldn't connect to Twitch chat.")

        # Live chat is already flowing; now fill in what was missed while
        # down. (connect_chat polled streams first, so backfilled messages
        # sent during the current stream get its stream_id.)
        await collector.backfill_recent_messages()

        await asyncio.gather(
            collector.watch_chat(),
            collector.flush_to_local_buffer(),
            collector.log_stats(),
            collector.poll_streams(),
            collector.refresh_token_periodically(),
        )
    except asyncio.CancelledError:
        log.info("Shutting down; writing queued messages first.")
    finally:
        if collector is not None:
            await collector.flush_once()
            if collector.chat is not None:
                await collector.chat.close()
        await log.flush()
        await db_conn.close()

if __name__ == "__main__":
    asyncio.run(main())
