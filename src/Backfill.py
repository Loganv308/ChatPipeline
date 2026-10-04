"""
ChatPipeline historical backfill.

Imports past chat from logs.ivr.fi's daily logs -- Twitch's raw IRC lines,
with the original message ids and send times -- into the same local
buffer the collector writes to, so Sync.py appends it to Postgres like
any other chat. Messages are built by irc.record_from_privmsg, exactly
as the collector's catch-up does, and anything already stored is
dropped by Sync's ON CONFLICT (message_id), so re-reading days that
were already collected fills gaps without duplicating anything.

Per-channel, from .env:
    BACKFILL_CHANNELS=xqc:30,moonmoon:90,ludwig:2026-01-01,summit1g:all
      name:30          the last 30 days
      name:2026-01-01  back to that date
      name:all         everything the log service has
      name             same as name:30
Channels not listed aren't backfilled; empty turns the backfill off.
A channel must also be one the collector watches (SEED_CHANNELS).

Works newest day first (yesterday backwards; today is the collector's),
records each finished day so a restart resumes, and pauses whenever the
buffer holds more than MAX_BACKLOG messages so live chat is never stuck
behind a backlog for long. Every RECHECK_INTERVAL it looks again, which
picks up each newly finished day.
"""
import asyncio
import json
import os
from datetime import date, datetime, timedelta, timezone

import aiohttp
from dotenv import load_dotenv
from logstream import LogStream

import store
from irc import parse_privmsg, record_from_privmsg

load_dotenv()

log = LogStream(service="ChatPipeline-Backfill-Prod", host=os.getenv("LOG_HOST"))

LOGS_BASE        = "https://logs.ivr.fi"
DEFAULT_DAYS     = 30
BATCH_SIZE       = 5000      # messages per buffer insert
MAX_BACKLOG      = 50_000    # pause while the buffer holds more than this
RECHECK_INTERVAL = 6 * 3600  # seconds between looks for newly finished days
DAY_PAUSE        = 1.0       # seconds between day downloads (politeness)
DONE_KEY         = "backfill_done:{}:{}"  # kv_state, per channel and day


# ─── Config ────────────────────────────────────────────────────────────────

def parse_config(raw: str) -> dict[str, date | None]:
    """{channel: earliest day to backfill}, None meaning everything."""
    today = datetime.now(timezone.utc).date()
    config: dict[str, date | None] = {}
    for entry in raw.split(","):
        name, _, depth = entry.strip().partition(":")
        name, depth = name.strip().lower(), depth.strip().lower()
        if not name:
            continue
        try:
            if depth == "all":
                config[name] = None
            elif not depth:
                config[name] = today - timedelta(days=DEFAULT_DAYS)
            elif depth.isdigit():
                config[name] = today - timedelta(days=int(depth))
            else:
                config[name] = date.fromisoformat(depth)
        except ValueError:
            log.error(f"BACKFILL_CHANNELS: can't read {entry.strip()!r} (expected name, name:30, name:2026-01-01 or name:all)")
    return config


# ─── Log service ───────────────────────────────────────────────────────────

async def available_days(session: aiohttp.ClientSession, channel: str) -> list[date]:
    async with session.get(f"{LOGS_BASE}/list", params={"channel": channel}) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {(await resp.text())[:100]}")
        body = await resp.json()
    return [date(int(d["year"]), int(d["month"]), int(d["day"])) for d in body.get("availableLogs", [])]


async def wait_for_room(db) -> None:
    while (await store.count_pending(db))["messages"] > MAX_BACKLOG:
        await asyncio.sleep(5)


async def backfill_day(session, db, channel: str, channel_id: int, day: date) -> dict:
    """Streams one day's raw log into the buffer. Returns its counts."""
    url = f"{LOGS_BASE}/channel/{channel}/{day.year}/{day.month}/{day.day}"
    counts = {"lines": 0, "messages": 0, "skipped": 0, "undecodable": 0}
    seen: set[str] = set()
    batch: list[tuple] = []

    async def flush():
        await wait_for_room(db)
        await store.insert_messages(db, batch)
        batch.clear()

    async with session.get(url, params={"raw": ""}) as resp:
        if resp.status == 404:
            return counts
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        async for raw_line in resp.content:
            counts["lines"] += 1
            try:
                line = raw_line.decode("utf-8").rstrip("\r\n")
            except UnicodeDecodeError:
                # never guess at text: skip it rather than store it altered
                counts["undecodable"] += 1
                continue
            parsed = parse_privmsg(line)
            record = record_from_privmsg(parsed, channel_id, None) if parsed and parsed["channel"] == channel else None
            if record is None or record["message_id"] in seen:
                counts["skipped"] += 1
                continue
            seen.add(record["message_id"])
            batch.append((
                record["message_id"], record["channel"], record["channel_id"], record["stream_id"],
                record["user_id"], record["username"], record["message"], record["timestamp"],
                record["subscriber"], record["is_bot"],
            ))
            counts["messages"] += 1
            if len(batch) >= BATCH_SIZE:
                await flush()
    if batch:
        await flush()
    return counts


# ─── Main ──────────────────────────────────────────────────────────────────

async def backfill_channel(session, db, channel: str, channel_id: int, since: date | None) -> int:
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    days = sorted((d for d in await available_days(session, channel)
                   if d <= yesterday and (since is None or d >= since)), reverse=True)
    todo = [d for d in days if not await store.load_kv(db, DONE_KEY.format(channel, d.isoformat()))]
    if not todo:
        return 0
    log.info(f"[Backfill] {channel}: {len(todo)} day(s) to import "
             f"({todo[-1].isoformat()} to {todo[0].isoformat()}; {len(days) - len(todo)} already done).")
    total = 0
    for day in todo:
        counts = await backfill_day(session, db, channel, channel_id, day)
        await store.save_kv(db, DONE_KEY.format(channel, day.isoformat()), json.dumps(
            {**counts, "finished_at": datetime.now(timezone.utc).isoformat()}))
        total += counts["messages"]
        extra = f", {counts['undecodable']} undecodable line(s) skipped" if counts["undecodable"] else ""
        log.info(f"[Backfill] {channel} {day.isoformat()}: queued {counts['messages']:,} message(s){extra}")
        await asyncio.sleep(DAY_PAUSE)
    log.info(f"[Backfill] {channel}: done, {total:,} message(s) queued.")
    return total


async def main() -> None:
    config = parse_config(os.getenv("BACKFILL_CHANNELS", ""))
    if not config:
        log.info("BACKFILL_CHANNELS is empty; historical backfill is off.")
        await asyncio.Event().wait()  # stay up quietly so restart:always doesn't loop

    await store.init_db()
    db = await store.get_connection()
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)
    headers = {"User-Agent": "ChatPipeline (Twitch chat archiver; historical backfill)"}
    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            while True:
                channel_map = await store.load_cached_channel_map(db)
                for channel, since in config.items():
                    if channel not in channel_map:
                        log.error(f"[Backfill] {channel}: not in the collector's channel list (SEED_CHANNELS); skipping.")
                        continue
                    try:
                        await backfill_channel(session, db, channel, channel_map[channel], since)
                    except Exception as e:
                        # the day in progress isn't marked done, so it's retried
                        log.error(f"[Backfill] {channel}: stopped ({e}); will retry.")
                await asyncio.sleep(RECHECK_INTERVAL)
    finally:
        await log.flush()
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
