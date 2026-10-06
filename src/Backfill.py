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

Each day is downloaded completely to a temporary file before anything is
queued, so those pauses never hold a download open (the log service
drops idle connections). A failed download is retried after each of
RETRY_DELAYS; a day that still fails is skipped until the next check,
and the channel carries on with older days.
"""
import asyncio
import json
import os
import tempfile
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
RETRY_DELAYS     = (30, 120, 300)  # seconds before each retry of a failed download
DOWNLOAD_CHUNK   = 1 << 16
DONE_KEY         = "backfill_done:{}:{}"  # kv_state, per channel and day


class LogsUnavailable(Exception):
    """The log service doesn't keep this channel (not logged, or opted out)."""


class DownloadFailed(Exception):
    """A day's log couldn't be downloaded completely, even after retries."""


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
        if resp.status in (403, 404):
            raise LogsUnavailable(f"HTTP {resp.status}: {(await resp.text())[:100]}")
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {(await resp.text())[:100]}")
        body = await resp.json()
    return [date(int(d["year"]), int(d["month"]), int(d["day"])) for d in body.get("availableLogs", [])]


async def download_day(session, channel: str, day: date, out) -> bool:
    """Writes one day's raw log to `out`. False if the service has no log
    for that day. Raises if the download fails or is cut short (aiohttp
    raises ClientPayloadError on a truncated body)."""
    url = f"{LOGS_BASE}/channel/{channel}/{day.year}/{day.month}/{day.day}"
    async with session.get(url, params={"raw": ""}) as resp:
        if resp.status == 404:
            return False
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        async for chunk in resp.content.iter_chunked(DOWNLOAD_CHUNK):
            out.write(chunk)
    return True


async def download_day_with_retries(session, channel: str, day: date, out) -> bool:
    for attempt, delay in enumerate((*RETRY_DELAYS, None), start=1):
        out.seek(0)
        out.truncate()
        try:
            return await download_day(session, channel, day, out)
        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionError, RuntimeError) as e:
            if delay is None:
                raise DownloadFailed(f"{attempt} attempts; last error: {e}") from e
            log.warn(f"[Backfill] {channel} {day.isoformat()}: download failed ({e}); "
                     f"retrying in {delay}s (attempt {attempt + 1} of {len(RETRY_DELAYS) + 1}).")
            await asyncio.sleep(delay)


async def wait_for_room(db) -> None:
    while (await store.count_pending(db))["messages"] > MAX_BACKLOG:
        await asyncio.sleep(5)


async def backfill_day(session, db, channel: str, channel_id: int, day: date) -> dict:
    """Downloads one day's raw log in full, then queues it into the buffer.
    Returns its counts."""
    counts = {"lines": 0, "messages": 0, "skipped": 0, "undecodable": 0}
    seen: set[str] = set()
    batch: list[tuple] = []

    async def flush():
        await wait_for_room(db)
        await store.insert_messages(db, batch)
        batch.clear()

    with tempfile.TemporaryFile() as raw_log:
        if not await download_day_with_retries(session, channel, day, raw_log):
            return counts
        raw_log.seek(0)
        for raw_line in raw_log:
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
    total, failed = 0, []
    for day in todo:
        try:
            counts = await backfill_day(session, db, channel, channel_id, day)
        except DownloadFailed as e:
            # not marked done, so the next check tries it again
            log.error(f"[Backfill] {channel} {day.isoformat()}: couldn't download ({e}); "
                      f"skipping it until the next check.")
            failed.append(day)
            continue
        await store.save_kv(db, DONE_KEY.format(channel, day.isoformat()), json.dumps(
            {**counts, "finished_at": datetime.now(timezone.utc).isoformat()}))
        total += counts["messages"]
        extra = f", {counts['undecodable']} undecodable line(s) skipped" if counts["undecodable"] else ""
        log.info(f"[Backfill] {channel} {day.isoformat()}: queued {counts['messages']:,} message(s){extra}")
        await asyncio.sleep(DAY_PAUSE)
    left = (f"; {len(failed)} day(s) failed and will be retried at the next check "
            f"({', '.join(d.isoformat() for d in failed)})") if failed else ""
    log.info(f"[Backfill] {channel}: done, {total:,} message(s) queued{left}.")
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
        unavailable: set[str] = set()  # no logs at the service: reported once, then left alone
        unlisted: set[str] = set()     # not (yet) in the channel map: reported once, rechecked every pass
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            while True:
                channel_map = await store.load_cached_channel_map(db)
                for channel, since in config.items():
                    if channel in unavailable:
                        continue
                    if channel not in channel_map:
                        if channel not in unlisted:
                            log.warn(f"[Backfill] {channel}: not in the collector's channel list (SEED_CHANNELS); "
                                     f"skipping. Check the spelling in BACKFILL_CHANNELS.")
                            unlisted.add(channel)
                        continue
                    try:
                        await backfill_channel(session, db, channel, channel_map[channel], since)
                    except LogsUnavailable as e:
                        log.warn(f"[Backfill] {channel}: logs.ivr.fi has no logs for this channel ({e}); "
                                 f"skipping it. Remove it from BACKFILL_CHANNELS.")
                        unavailable.add(channel)
                    except Exception as e:
                        # the day in progress isn't marked done, so it's retried
                        log.error(f"[Backfill] {channel}: stopped ({e}); will retry.")
                await asyncio.sleep(RECHECK_INTERVAL)
    finally:
        await log.flush()
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
