"""
Clean-up rules shared by the collector (Worker.py, live chat) and the
sanitizer (Sanitizer.py, rows already stored), so both produce
identical results.
"""
import html
import re

BOT_NAMES = {"streamelements", "nightbot", "fossabot", "moobot", "streamlabs"}

MAX_MESSAGE_LEN  = 500
MAX_USERNAME_LEN = 25

_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
_WHITESPACE    = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    """Removes control characters, collapses whitespace, trims and caps
    the length. Idempotent, so it's safe to re-apply to stored rows.
    Control characters go first so their removal can't leave behind
    whitespace that then needs collapsing on a second pass."""
    text = _CONTROL_CHARS.sub("", text)
    text = _WHITESPACE.sub(" ", text).strip()
    return text[:MAX_MESSAGE_LEN]


def sanitize_message(text: str) -> str:
    """For live chat: decodes HTML entities, then normalize_text. The
    sanitizer re-applies only normalize_text -- decoding a second time
    would alter messages that legitimately contain text like '&lt;'."""
    if not text:
        return "[EMPTY MESSAGE]"
    return normalize_text(html.unescape(text))


def normalize_username(username: str) -> str:
    """Idempotent part of sanitize_username, safe to re-apply."""
    return username.strip().lower()[:MAX_USERNAME_LEN]


def sanitize_username(username: str) -> str:
    if not username:
        return "anonymous"
    return normalize_username(username)
