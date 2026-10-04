"""
Parsing raw Twitch IRC chat lines ('@tags :nick!nick@host PRIVMSG #chan
:text') into the same record the collector builds from live chat. Used
for every message that doesn't arrive through twitchio: the collector's
startup catch-up (recent-messages) and the historical backfill
(Backfill.py, logs.ivr.fi), so all of them are imported identically.
"""
import re
from datetime import datetime, timezone

from sanitize import BOT_NAMES, sanitize_message, sanitize_username

_TAG_ESCAPES = {r"\s": " ", r"\:": ";", "\\\\": "\\", r"\r": "\r", r"\n": "\n"}


def _unescape_tag(value: str) -> str:
    return re.sub(r"\\[s:\\rn]", lambda m: _TAG_ESCAPES[m.group()], value)


def parse_privmsg(line: str) -> dict | None:
    """Parses one raw IRC line into its tags, login, channel and text.
    Returns None for anything that isn't a chat message (notices,
    clears, sub announcements, etc.)."""
    tags: dict[str, str] = {}
    if line.startswith("@"):
        raw_tags, _, line = line[1:].partition(" ")
        for pair in raw_tags.split(";"):
            key, _, value = pair.partition("=")
            tags[key] = _unescape_tag(value)
    if not line.startswith(":"):
        return None
    prefix, _, rest = line[1:].partition(" ")
    command, _, rest = rest.partition(" ")
    if command != "PRIVMSG":
        return None
    channel, _, text = rest.partition(" ")
    return {
        "tags":    tags,
        "login":   prefix.partition("!")[0],
        "channel": channel.lstrip("#").lower(),
        "text":    text[1:] if text.startswith(":") else text,
    }


def _badge_names(badges: str) -> set[str]:
    """'subscriber/12,founder/0' -> {'subscriber', 'founder'}"""
    return {b.partition("/")[0] for b in badges.split(",") if b}


def record_from_privmsg(parsed: dict, channel_id: int, stream_id: str | None) -> dict | None:
    """Builds the same record Worker.extract_message() does for live chat,
    so these rows are indistinguishable from live ones apart from
    created_at."""
    tags = parsed["tags"]
    if not tags.get("id") or not tags.get("tmi-sent-ts", "").isdigit():
        return None
    username = sanitize_username(parsed["login"])
    sent = datetime.fromtimestamp(int(tags["tmi-sent-ts"]) / 1000, timezone.utc)
    return {
        "message_id": tags["id"],
        "channel":    parsed["channel"],
        "channel_id": channel_id,
        "stream_id":  stream_id,
        "user_id":    tags.get("user-id") or None,
        "username":   username,
        "message":    sanitize_message(parsed["text"]),
        "timestamp":  sent.isoformat(),
        # exactly twitchio's Chatter.is_subscriber: the subscriber tag, or a
        # badge named "founder" (not just any badge containing the word)
        "subscriber": int(tags.get("subscriber") == "1" or "founder" in _badge_names(tags.get("badges", ""))),
        "is_bot":     int(username in BOT_NAMES),
    }
