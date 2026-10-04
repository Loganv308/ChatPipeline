"""
ChatPipeline daily reports.

Shortly after midnight (REPORT_TZ), builds a report of the previous day
from Postgres and delivers it three ways:
  - the daily_reports table (the whole report as JSONB; history and
    day-over-day comparisons come from here)
  - REPORT_DIR/<date>.md and <date>.html
  - a one-line summary to LogStream
On startup it also builds any report missing from the last CATCHUP_DAYS,
and every REFRESH_EVERY it rebuilds any earlier day that received new
rows since (e.g. from the historical backfill).

Read-only against the data: it never modifies messages, streams or
channels.

  ReportCollector  runs the queries and returns the report as a dict
  ReportFormatter  turns that dict into Markdown, HTML or JSON
  IndexFormatter   the reports/index.html page listing every day
  ReportWriter     stores and delivers it, and refreshes the index

On demand (rebuilds and overwrites that day's report, then exits):
  docker compose run --rm reporter python src/Reporter.py --date 2026-10-03
"""
import argparse
import asyncio
import html
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg
from dotenv import load_dotenv
from logstream import LogStream

from pgdb import connect_pg_with_retry

load_dotenv()

log = LogStream(service="ChatPipeline-Reporter-Prod", host=os.getenv("LOG_HOST"))

REPORT_TZ     = os.getenv("REPORT_TZ") or os.getenv("TZ") or "UTC"
REPORT_DIR    = Path(os.getenv("REPORT_DIR", "/reports"))
# With REPORTS_REQUIRE_MARKER=true, files are only written where this marker
# file exists. A NAS share that isn't mounted leaves an empty local folder
# at the mount point; reports written there would silently miss the backup.
REPORTS_MARKER = ".chatpipeline-reports"
REPORT_FALLBACK_DIR = Path(os.getenv("REPORT_FALLBACK_DIR", "/reports-fallback"))  # used when REPORT_DIR is unavailable
REQUIRE_MARKER = os.getenv("REPORTS_REQUIRE_MARKER", "").lower() in ("1", "true", "yes")
REPORT_AT     = time(0, 15)   # local time each day's report is built (Sync has caught up by then)
CATCHUP_DAYS  = 7
REFRESH_EVERY = 1800          # seconds between checks for late data (backfill) in reported days
SETTLE_TIME   = timedelta(minutes=10)  # rows newer than this are left for the next check
LATE_COMMIT   = timedelta(minutes=2)   # created_at is set at insert *start*; re-check this much
TOP_CHATTERS  = 5
RECOVERED_LAG = timedelta(minutes=1)  # inserted this long after being sent = recovered, not live


# ─── Collect ───────────────────────────────────────────────────────────────

class ReportCollector:
    """Builds one day's report. Every query is a read."""

    def __init__(self, pg: asyncpg.Pool, tz: ZoneInfo):
        self.pg = pg
        self.tz = tz

    async def collect(self, day: date) -> dict:
        start = datetime.combine(day, time.min, self.tz)
        end = datetime.combine(day + timedelta(days=1), time.min, self.tz)  # DST-safe
        w = (start, end)

        channels: dict[str, dict] = {}

        def ch(name: str) -> dict:
            return channels.setdefault(name, {
                "channel": name, "messages": 0, "chatters": 0, "first_time_chatters": 0,
                "subscriber_messages": 0, "bot_messages": 0, "busiest_hour": None,
                "added": 0, "recovered": 0, "added_for_earlier_days": 0,
                "top_chatters": [], "streams": [],
            })

        # messages sent during the day
        for r in await self.pg.fetch("""
            SELECT c.name, count(*) AS messages, count(DISTINCT m.username) AS chatters,
                   count(*) FILTER (WHERE m.subscriber) AS subs, count(*) FILTER (WHERE m.is_bot) AS bots
            FROM messages m JOIN channels c ON c.id = m.channel_id
            WHERE m.timestamp >= $1 AND m.timestamp < $2
            GROUP BY 1""", *w):
            ch(r["name"]).update(messages=r["messages"], chatters=r["chatters"],
                                 subscriber_messages=r["subs"], bot_messages=r["bots"])

        for r in await self.pg.fetch("""
            SELECT DISTINCT ON (name) name, hour, n FROM (
                SELECT c.name, extract(hour FROM m.timestamp AT TIME ZONE $3)::int AS hour, count(*) AS n
                FROM messages m JOIN channels c ON c.id = m.channel_id
                WHERE m.timestamp >= $1 AND m.timestamp < $2
                GROUP BY 1, 2) x
            ORDER BY name, n DESC""", *w, self.tz.key):
            ch(r["name"])["busiest_hour"] = {"hour": r["hour"], "messages": r["n"]}

        # "first-time" = no earlier message in that channel in this database
        for r in await self.pg.fetch("""
            SELECT c.name, count(*) AS n
            FROM (SELECT DISTINCT channel_id, username FROM messages
                  WHERE timestamp >= $1 AND timestamp < $2) d
            JOIN channels c ON c.id = d.channel_id
            WHERE NOT EXISTS (SELECT 1 FROM messages p
                              WHERE p.channel_id = d.channel_id AND p.username = d.username AND p.timestamp < $1)
            GROUP BY 1""", *w):
            ch(r["name"])["first_time_chatters"] = r["n"]

        for r in await self.pg.fetch("""
            SELECT name, username, n FROM (
                SELECT c.name, m.username, count(*) AS n,
                       row_number() OVER (PARTITION BY c.name ORDER BY count(*) DESC, m.username) AS rn
                FROM messages m JOIN channels c ON c.id = m.channel_id
                WHERE m.timestamp >= $1 AND m.timestamp < $2 AND NOT coalesce(m.is_bot, false)
                GROUP BY 1, 2) x
            WHERE rn <= $3 ORDER BY name, n DESC""", *w, TOP_CHATTERS):
            ch(r["name"])["top_chatters"].append({"username": r["username"], "messages": r["n"]})

        # rows inserted during the day (live, or recovered by catch-up/backfill)
        for r in await self.pg.fetch("""
            SELECT c.name, count(*) AS added,
                   count(*) FILTER (WHERE m.created_at - m.timestamp > $3) AS recovered,
                   count(*) FILTER (WHERE m.timestamp < $1) AS earlier
            FROM messages m JOIN channels c ON c.id = m.channel_id
            WHERE m.created_at >= $1 AND m.created_at < $2
            GROUP BY 1""", *w, RECOVERED_LAG):
            ch(r["name"]).update(added=r["added"], recovered=r["recovered"], added_for_earlier_days=r["earlier"])

        for r in await self.pg.fetch("""
            SELECT c.name, s.id, s.title, s.game_name, s.started_at, s.peak_viewers, s.is_live
            FROM streams s JOIN channels c ON c.id = s.channel_id
            WHERE s.started_at >= $1 AND s.started_at < $2
            ORDER BY s.started_at""", *w):
            ch(r["name"])["streams"].append({
                "id": r["id"], "title": r["title"], "game": r["game_name"],
                "started_at": r["started_at"].isoformat(), "peak_viewers": r["peak_viewers"], "live": r["is_live"],
            })

        unique_chatters = await self.pg.fetchval(
            "SELECT count(DISTINCT username) FROM messages WHERE timestamp >= $1 AND timestamp < $2", *w)
        skipped = {r["reason"]: r["n"] for r in await self.pg.fetch(
            "SELECT reason, count(*) AS n FROM skipped_messages WHERE timestamp >= $1 AND timestamp < $2 GROUP BY 1", *w)}

        rows = sorted(channels.values(), key=lambda c: c["messages"], reverse=True)
        totals = {k: sum(c[k] for c in rows) for k in (
            "messages", "first_time_chatters", "subscriber_messages", "bot_messages",
            "added", "recovered", "added_for_earlier_days")}
        totals.update(unique_chatters=unique_chatters, streams_started=sum(len(c["streams"]) for c in rows),
                      skipped=sum(skipped.values()))

        previous = await self._previous(day)
        return {
            "date": day.isoformat(),
            "timezone": self.tz.key,
            "window": {"start": start.isoformat(), "end": end.isoformat()},
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "totals": totals,
            "channels": rows,
            "skipped": skipped,
            "sanitizer": await self._sanitizer(w),
            "storage": await self._storage(),
            "previous": previous,
        }

    async def _sanitizer(self, w) -> dict | None:
        if not await self.pg.fetchval("SELECT to_regclass('sanitizer_runs') IS NOT NULL"):
            return None
        runs = {r["status"]: r["n"] for r in await self.pg.fetch(
            "SELECT status, count(*) AS n FROM sanitizer_runs WHERE started_at >= $1 AND started_at < $2 GROUP BY 1", *w)}
        counts = {r["key"]: int(r["n"]) for r in await self.pg.fetch("""
            SELECT f.key, sum(f.value::bigint) AS n
            FROM sanitizer_runs r, jsonb_each_text(r.fixes) f
            WHERE r.started_at >= $1 AND r.started_at < $2
            GROUP BY 1""", *w)}
        return {
            "runs": runs,
            "changes":  {k: v for k, v in counts.items() if not k.startswith("rejected_")},
            "rejected": {k.removeprefix("rejected_"): v for k, v in counts.items() if k.startswith("rejected_")},
        }

    async def _storage(self) -> dict:
        r = await self.pg.fetchrow("""
            SELECT pg_total_relation_size('messages') AS bytes,
                   (SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE relname = 'messages') AS rows""")
        return {"messages_table_bytes": r["bytes"], "messages_rows_estimate": r["rows"]}

    async def _previous(self, day: date) -> dict | None:
        """The day before's totals and per-channel message counts, for deltas."""
        raw = await self.pg.fetchval(
            "SELECT report FROM daily_reports WHERE report_date = $1", day - timedelta(days=1))
        if raw is None:
            return None
        prev = json.loads(raw) if isinstance(raw, str) else raw
        return {
            "totals": prev["totals"],
            "channels": {c["channel"]: c["messages"] for c in prev["channels"]},
            "messages_table_bytes": prev["storage"]["messages_table_bytes"],
        }


# ─── Format ────────────────────────────────────────────────────────────────

PAGE_CSS = """
:root { --bg:#f7f7f8; --panel:#fff; --text:#1d1d1f; --muted:#6b6b72; --line:#e3e3e8; --accent:#6441a5; --warn:#b3261e; }
@media (prefers-color-scheme: dark) { :root { --bg:#141416; --panel:#1e1e22; --text:#ececf0; --muted:#9a9aa3; --line:#2e2e34; --accent:#a98bf5; --warn:#f2b8b5; } }
* { box-sizing:border-box }
body { margin:0; background:var(--bg); color:var(--text); font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif; }
main { max-width:1100px; margin:0 auto; padding:24px 16px 48px; }
h1 { font-size:1.5rem; margin:0 0 4px } h2 { font-size:1.1rem; margin:32px 0 12px }
.meta, .sub { color:var(--muted); font-size:.85rem }
.cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px; margin-top:20px }
.card { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:14px }
.label { color:var(--muted); font-size:.8rem } .value { font-size:1.6rem; font-weight:600; color:var(--accent) }
.scroll { overflow-x:auto; background:var(--panel); border:1px solid var(--line); border-radius:10px }
table { border-collapse:collapse; width:100%; font-size:.9rem }
th, td { padding:8px 12px; border-bottom:1px solid var(--line); text-align:left; white-space:nowrap }
th { color:var(--muted); font-weight:500 } td.n, th.n { text-align:right; font-variant-numeric:tabular-nums }
td .sub { display:block } tr:last-child td { border-bottom:none }
ul { padding-left:20px } li { margin:4px 0 } .warn { color:var(--warn) }
a { color:var(--accent); text-decoration:none } a:hover { text-decoration:underline }
.bar { height:6px; border-radius:3px; background:var(--accent); opacity:.55; margin-top:4px; min-width:2px }
"""


def _n(v) -> str:
    return f"{v:,}"


def _delta(now: int, before: int | None) -> str:
    if before is None:
        return ""
    if before == 0:
        return " (new)" if now else ""
    pct = (now - before) / before * 100
    return f" ({'+' if pct >= 0 else ''}{pct:.0f}% vs prior day)"


def _bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024 or unit == "TB":
            return f"{b:,.1f} {unit}" if unit != "B" else f"{b} B"
        b /= 1024


def _hour(h: dict | None) -> str:
    if not h:
        return "—"
    start = time(h["hour"]).strftime("%I %p").lstrip("0")
    return f"{start} ({_n(h['messages'])})"


def _pct(part: int, whole: int) -> str:
    return f"{part / whole * 100:.0f}%" if whole else "—"


class ReportFormatter:
    """Renders a report dict (from ReportCollector) in different formats.
    Every method reads the same dict, so all formats always agree."""

    def __init__(self, report: dict):
        self.r = report

    @property
    def title(self) -> str:
        d = date.fromisoformat(self.r["date"])
        return f"ChatPipeline daily report — {d.strftime('%A, %B')} {d.day}, {d.year}"

    def summary_line(self) -> str:
        t = self.r["totals"]
        prev = (self.r["previous"] or {}).get("totals", {})
        return (f"{self.r['date']}: {_n(t['messages'])} messages{_delta(t['messages'], prev.get('messages'))} "
                f"from {_n(t['unique_chatters'])} chatters across {len(self.r['channels'])} channel(s); "
                f"{_n(t['added'])} rows added ({_n(t['recovered'])} recovered), "
                f"{t['streams_started']} stream(s) started, {_n(t['skipped'])} skipped.")

    def to_json(self) -> str:
        return json.dumps(self.r, indent=2)

    # ── Markdown ──

    def to_markdown(self) -> str:
        r, t = self.r, self.r["totals"]
        prev = r["previous"] or {}
        prev_t, prev_ch = prev.get("totals", {}), prev.get("channels", {})
        md = lambda s: str(s).replace("|", "\\|").replace("\n", " ")
        out = [f"# {self.title}", "",
               f"Times in {r['timezone']}. Generated {r['generated_at'][:16].replace('T', ' ')} UTC.", "",
               "## Summary", "",
               f"- **Messages sent:** {_n(t['messages'])}{_delta(t['messages'], prev_t.get('messages'))}",
               f"- **Unique chatters:** {_n(t['unique_chatters'])} ({_n(t['first_time_chatters'])} first-time)",
               f"- **Rows added to the database:** {_n(t['added'])} — {_n(t['recovered'])} recovered by catch-up/backfill"
               + (f", {_n(t['added_for_earlier_days'])} of them for earlier days" if t["added_for_earlier_days"] else ""),
               f"- **Streams started:** {t['streams_started']}",
               f"- **Skipped messages:** {_n(t['skipped'])}", "",
               "## Channels", "",
               "| Channel | Messages | Chatters | First-time | Subscriber msgs | Bot msgs | Busiest hour | Added (recovered) |",
               "|---|---:|---:|---:|---:|---:|---|---:|"]
        for c in r["channels"]:
            out.append(f"| {md(c['channel'])} | {_n(c['messages'])}{_delta(c['messages'], prev_ch.get(c['channel']))} "
                       f"| {_n(c['chatters'])} | {_n(c['first_time_chatters'])} | {_pct(c['subscriber_messages'], c['messages'])} "
                       f"| {_n(c['bot_messages'])} | {_hour(c['busiest_hour'])} | {_n(c['added'])} ({_n(c['recovered'])}) |")

        streams = [(c["channel"], s) for c in r["channels"] for s in c["streams"]]
        out += ["", "## Streams started", ""]
        if streams:
            out += ["| Channel | Started | Title | Game | Peak viewers |", "|---|---|---|---|---:|"]
            for name, s in streams:
                started = datetime.fromisoformat(s["started_at"]).astimezone(ZoneInfo(r["timezone"])).strftime("%I:%M %p").lstrip("0")
                out.append(f"| {md(name)} | {started}{' (live)' if s['live'] else ''} | {md(s['title'] or '')} "
                           f"| {md(s['game'] or '')} | {_n(s['peak_viewers'] or 0)} |")
        else:
            out.append("None.")

        out += ["", "## Top chatters", ""]
        for c in r["channels"]:
            if c["top_chatters"]:
                people = ", ".join(f"{md(u['username'])} ({_n(u['messages'])})" for u in c["top_chatters"])
                out.append(f"- **{md(c['channel'])}:** {people}")

        out += ["", "## Data quality", ""]
        out.append("- **Skipped:** " + (", ".join(f"{md(k)} {_n(v)}" for k, v in r["skipped"].items()) or "none"))
        s = r["sanitizer"]
        if s is not None:
            runs = ", ".join(f"{_n(v)} {k}" for k, v in sorted(s["runs"].items())) or "none"
            changes = ", ".join(f"{k} {_n(v)}" for k, v in sorted(s["changes"].items())) or "none"
            out.append(f"- **Sanitizer:** runs: {runs}; changes: {changes}")
            if s["rejected"]:
                out.append("- **Sanitizer refused (guards):** "
                           + ", ".join(f"{k} {_n(v)}" for k, v in sorted(s["rejected"].items()))
                           + " — investigate: see `[Guard]` lines in the sanitizer log")
        st = r["storage"]
        growth = ""
        if prev.get("messages_table_bytes"):
            growth = f" ({'+' if st['messages_table_bytes'] >= prev['messages_table_bytes'] else '-'}"\
                     f"{_bytes(abs(st['messages_table_bytes'] - prev['messages_table_bytes']))} since the prior report was generated)"
        out.append(f"- **Storage (when this report was generated):** messages table {_bytes(st['messages_table_bytes'])}, "
                   f"~{_n(st['messages_rows_estimate'])} rows{growth}")
        return "\n".join(out) + "\n"

    # ── HTML ──

    def to_html(self) -> str:
        """A standalone page (no external assets), light and dark theme.
        Every value from chat or Twitch (usernames, titles) is escaped."""
        r, t = self.r, self.r["totals"]
        prev = r["previous"] or {}
        prev_t, prev_ch = prev.get("totals", {}), prev.get("channels", {})
        e = lambda s: html.escape(str(s))
        tz = ZoneInfo(r["timezone"])

        cards = [
            ("Messages sent", _n(t["messages"]), _delta(t["messages"], prev_t.get("messages")).strip(" ()")),
            ("Unique chatters", _n(t["unique_chatters"]), f"{_n(t['first_time_chatters'])} first-time"),
            ("Rows added", _n(t["added"]), f"{_n(t['recovered'])} recovered"),
            ("Streams started", str(t["streams_started"]), ""),
            ("Skipped", _n(t["skipped"]), ""),
        ]
        card_html = "".join(f'<div class="card"><div class="label">{e(a)}</div><div class="value">{e(b)}</div>'
                            f'<div class="sub">{e(c)}</div></div>' for a, b, c in cards)

        ch_rows = "".join(
            f"<tr><td>{e(c['channel'])}</td><td class=n>{_n(c['messages'])}"
            f"<span class=sub>{e(_delta(c['messages'], prev_ch.get(c['channel'])))}</span></td>"
            f"<td class=n>{_n(c['chatters'])}</td><td class=n>{_n(c['first_time_chatters'])}</td>"
            f"<td class=n>{_pct(c['subscriber_messages'], c['messages'])}</td><td class=n>{_n(c['bot_messages'])}</td>"
            f"<td>{e(_hour(c['busiest_hour']))}</td><td class=n>{_n(c['added'])} ({_n(c['recovered'])})</td></tr>"
            for c in r["channels"])

        st_rows = "".join(
            f"<tr><td>{e(c['channel'])}</td>"
            f"<td>{datetime.fromisoformat(s['started_at']).astimezone(tz).strftime('%I:%M %p').lstrip('0')}"
            f"{' (live)' if s['live'] else ''}</td><td>{e(s['title'] or '')}</td><td>{e(s['game'] or '')}</td>"
            f"<td class=n>{_n(s['peak_viewers'] or 0)}</td></tr>"
            for c in r["channels"] for s in c["streams"]) or '<tr><td colspan=5>None.</td></tr>'

        top = "".join(
            f"<li><b>{e(c['channel'])}:</b> "
            + ", ".join(f"{e(u['username'])} ({_n(u['messages'])})" for u in c["top_chatters"]) + "</li>"
            for c in r["channels"] if c["top_chatters"])

        quality = [f"<li><b>Skipped:</b> {e(', '.join(f'{k} {_n(v)}' for k, v in r['skipped'].items()) or 'none')}</li>"]
        s = r["sanitizer"]
        if s is not None:
            runs = ", ".join(f"{_n(v)} {k}" for k, v in sorted(s["runs"].items())) or "none"
            changes = ", ".join(f"{k} {_n(v)}" for k, v in sorted(s["changes"].items())) or "none"
            quality.append(f"<li><b>Sanitizer:</b> runs: {e(runs)}; changes: {e(changes)}</li>")
            if s["rejected"]:
                quality.append('<li class="warn"><b>Sanitizer refused (guards):</b> '
                               + e(", ".join(f"{k} {_n(v)}" for k, v in sorted(s["rejected"].items()))) + "</li>")
        stg = r["storage"]
        quality.append(f"<li><b>Storage (when this report was generated):</b> messages table {e(_bytes(stg['messages_table_bytes']))}, "
                       f"~{_n(stg['messages_rows_estimate'])} rows</li>")

        return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{e(self.title)}</title>
<style>{PAGE_CSS}</style></head>
<body><main>
<div class="meta"><a href="index.html">&larr; All reports</a></div>
<h1>{e(self.title)}</h1>
<div class="meta">Times in {e(r['timezone'])} · generated {e(r['generated_at'][:16].replace('T', ' '))} UTC</div>
<div class="cards">{card_html}</div>
<h2>Channels</h2>
<div class="scroll"><table><thead><tr><th>Channel</th><th class=n>Messages</th><th class=n>Chatters</th><th class=n>First-time</th>
<th class=n>Subscriber msgs</th><th class=n>Bot msgs</th><th>Busiest hour</th><th class=n>Added (recovered)</th></tr></thead>
<tbody>{ch_rows}</tbody></table></div>
<h2>Streams started</h2>
<div class="scroll"><table><thead><tr><th>Channel</th><th>Started</th><th>Title</th><th>Game</th><th class=n>Peak viewers</th></tr></thead>
<tbody>{st_rows}</tbody></table></div>
<h2>Top chatters</h2><ul>{top}</ul>
<h2>Data quality</h2><ul>{''.join(quality)}</ul>
</main></body></html>
"""


class IndexFormatter:
    """The reports/index.html page: every stored report, newest first,
    with its headline numbers and links to the day's pages."""

    def __init__(self, rows: list[dict], report_dir: Path):
        self.rows = rows      # from daily_reports, newest first
        self.dir = report_dir

    def to_html(self) -> str:
        e = lambda s: html.escape(str(s))
        peak = max((r["messages"] for r in self.rows), default=0) or 1
        by_date = {r["date"]: r for r in self.rows}
        body = []
        for r in self.rows:
            d = r["date"]
            prev = by_date.get(d - timedelta(days=1))
            delta = _delta(r["messages"], prev["messages"] if prev else None).strip(" ()").replace(" vs prior day", "")
            page = f"{d.isoformat()}.html"
            date_cell = (f'<a href="{page}">{d.strftime("%a %b")} {d.day}, {d.year}</a>'
                         if (self.dir / page).exists() else f'{d.strftime("%a %b")} {d.day}, {d.year} <span class=sub>(page not on disk)</span>')
            md_link = f'<a href="{d.isoformat()}.md">md</a>' if (self.dir / f"{d.isoformat()}.md").exists() else ""
            notes = []
            if r["rejected"]:
                notes.append(f'<span class="warn">sanitizer refused {_n(r["rejected"])}</span>')
            if r["skipped"]:
                notes.append(f'{_n(r["skipped"])} skipped')
            body.append(
                f"<tr><td>{date_cell}</td>"
                f'<td class=n>{_n(r["messages"])}<div class="bar" style="width:{max(r["messages"] / peak * 100, 0):.1f}%"></div></td>'
                f"<td class=n>{e(delta)}</td><td class=n>{_n(r['chatters'])}</td><td class=n>{r['active_channels']}</td>"
                f"<td class=n>{_n(r['added'])} ({_n(r['recovered'])})</td><td class=n>{r['streams']}</td>"
                f"<td>{' · '.join(notes)}</td><td>{md_link}</td></tr>")
        rows_html = "".join(body) or '<tr><td colspan=9>No reports yet.</td></tr>'
        tz = self.rows[0]["timezone"] if self.rows else REPORT_TZ
        return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>ChatPipeline reports</title>
<style>{PAGE_CSS}</style></head>
<body><main>
<h1>ChatPipeline reports</h1>
<div class="meta">{len(self.rows)} daily report(s) · days in {e(tz)} · updated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC</div>
<h2>Days</h2>
<div class="scroll"><table><thead><tr><th>Day</th><th class=n>Messages</th><th class=n>vs prior day</th><th class=n>Chatters</th>
<th class=n>Active channels</th><th class=n>Rows added (recovered)</th><th class=n>Streams</th><th>Notes</th><th></th></tr></thead>
<tbody>{rows_html}</tbody></table></div>
</main></body></html>
"""


# ─── Store / deliver ───────────────────────────────────────────────────────

class ReportWriter:
    """Saves each report to daily_reports, then writes its files to the
    reports folder -- or, if that folder is unavailable (marker missing,
    or a write fails), to the local fallback folder. Fallback copies are
    moved to the reports folder once it's back (recover_fallback)."""

    def __init__(self, pg: asyncpg.Pool, report_dir: Path, fallback_dir: Path):
        self.pg = pg
        self.dir = report_dir
        self.fallback = fallback_dir

    async def setup(self) -> None:
        await self.pg.execute("""
            CREATE TABLE IF NOT EXISTS daily_reports (
                report_date   DATE        PRIMARY KEY,
                timezone      TEXT        NOT NULL,
                generated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                report        JSONB       NOT NULL
            )""")
        await self.pg.execute("""
            CREATE TABLE IF NOT EXISTS reporter_state (
                key    TEXT PRIMARY KEY,
                value  TEXT NOT NULL
            )""")

    async def exists(self, day: date) -> bool:
        return await self.pg.fetchval("SELECT EXISTS (SELECT 1 FROM daily_reports WHERE report_date = $1)", day)

    def primary_ok(self) -> bool:
        """See REQUIRE_MARKER."""
        return not REQUIRE_MARKER or (self.dir / REPORTS_MARKER).exists()

    @staticmethod
    def _write_files(folder: Path, files: dict[str, str]) -> list[Path]:
        folder.mkdir(parents=True, exist_ok=True)
        written = []
        for name, content in files.items():
            path = folder / name
            tmp = path.with_name(name + ".tmp")
            tmp.write_text(content, encoding="utf-8")
            tmp.replace(path)  # never leave a half-written file
            written.append(path)
        return written

    def _deliver(self, files: dict[str, str], what: str) -> list[Path]:
        """Writes to the reports folder, or the fallback folder if that's
        unavailable. Returns the paths written (empty if both failed --
        the report is still in daily_reports either way)."""
        if self.primary_ok():
            try:
                return self._write_files(self.dir, files)
            except OSError as e:
                reason = f"couldn't write to {self.dir} ({e})"
        else:
            reason = f"{self.dir / REPORTS_MARKER} not found, so the reports folder looks unmounted (e.g. the NAS share is down)"
        try:
            written = self._write_files(self.fallback, files)
        except OSError as e:
            log.error(f"[Report] {what}: {reason}, and the fallback {self.fallback} failed too ({e}). "
                      f"Saved in daily_reports only; rebuild the files later with --date.")
            return []
        log.error(f"[Report] {what}: {reason}; wrote to the local fallback {self.fallback} instead. "
                  f"It'll be moved to {self.dir} once that's available.")
        return written

    @staticmethod
    def _report_files(report: dict) -> dict[str, str]:
        fmt = ReportFormatter(report)
        return {f"{report['date']}.md": fmt.to_markdown(), f"{report['date']}.html": fmt.to_html()}

    async def write(self, report: dict) -> list[Path]:
        fmt = ReportFormatter(report)
        await self.pg.execute("""
            INSERT INTO daily_reports (report_date, timezone, report) VALUES ($1, $2, $3::jsonb)
            ON CONFLICT (report_date) DO UPDATE
                SET timezone = EXCLUDED.timezone, report = EXCLUDED.report, generated_at = now()
        """, date.fromisoformat(report["date"]), report["timezone"], fmt.to_json())
        written = self._deliver(self._report_files(report), report["date"])
        log.info(f"[Report] {fmt.summary_line()}")
        return written

    async def recover_fallback(self) -> None:
        """Once the reports folder is available again, re-renders every day
        that was written to the fallback into it (from daily_reports, so
        it's the latest version), then removes the fallback copies. A
        fallback file is only deleted after its replacement is written."""
        if not self.fallback.is_dir() or not self.primary_ok():
            return
        days = sorted({p.stem for p in self.fallback.glob("????-??-??.*")
                       if p.suffix in (".html", ".md")})
        if not days:
            return
        moved = 0
        for d in days:
            raw = await self.pg.fetchval("SELECT report FROM daily_reports WHERE report_date = $1", date.fromisoformat(d))
            try:
                if raw is not None:
                    self._write_files(self.dir, self._report_files(json.loads(raw) if isinstance(raw, str) else raw))
            except OSError as e:
                log.error(f"[Report] Couldn't move fallback report {d} to {self.dir} ({e}); will retry.")
                return
            for ext in ("html", "md"):
                (self.fallback / f"{d}.{ext}").unlink(missing_ok=True)
            moved += 1
        (self.fallback / "index.html").unlink(missing_ok=True)
        log.info(f"[Report] {self.dir} is available again: moved {moved} report(s) back from the fallback {self.fallback}.")

    async def write_index(self) -> None:
        rows = [{
            "date": r["report_date"], "timezone": r["timezone"],
            "messages": r["messages"], "chatters": r["chatters"], "active_channels": r["active_channels"],
            "added": r["added"], "recovered": r["recovered"], "streams": r["streams"],
            "skipped": r["skipped"], "rejected": r["rejected"],
        } for r in await self.pg.fetch("""
            SELECT report_date, timezone,
                   (report->'totals'->>'messages')::bigint        AS messages,
                   (report->'totals'->>'unique_chatters')::bigint AS chatters,
                   (report->'totals'->>'added')::bigint           AS added,
                   (report->'totals'->>'recovered')::bigint       AS recovered,
                   (report->'totals'->>'streams_started')::int    AS streams,
                   (report->'totals'->>'skipped')::bigint         AS skipped,
                   (SELECT count(*) FROM jsonb_array_elements(report->'channels') c
                     WHERE (c->>'messages')::bigint > 0)          AS active_channels,
                   coalesce((SELECT sum(v::bigint) FROM jsonb_each_text(report->'sanitizer'->'rejected') AS x(k, v)), 0) AS rejected
            FROM daily_reports ORDER BY report_date DESC""")]
        # the index links only pages present in the folder it's written to
        target = self.dir if self.primary_ok() else self.fallback
        self._deliver({"index.html": IndexFormatter(rows, target).to_html()}, "index")


# ─── Main ──────────────────────────────────────────────────────────────────

async def build(pg, tz, day: date) -> None:
    report = await ReportCollector(pg, tz).collect(day)
    files = await ReportWriter(pg, REPORT_DIR, REPORT_FALLBACK_DIR).write(report)
    if files:
        log.info(f"[Report] {day.isoformat()} written: {', '.join(str(f) for f in files)}")


async def days_with_late_data(pg, tz: ZoneInfo) -> list[date]:
    """Days (before today) that received rows since the last check, e.g.
    from the historical backfill. Only rows that have had SETTLE_TIME to
    stop arriving are considered, so a day still being backfilled is
    rebuilt once things settle rather than on every check. The position
    is kept in reporter_state, so nothing is missed across restarts."""
    cutoff = datetime.now(timezone.utc) - SETTLE_TIME
    raw = await pg.fetchval("SELECT value FROM reporter_state WHERE key = 'late_data_checked_to'")
    if raw is None:
        # first run: earlier days are covered by the catch-up builds
        await pg.execute("INSERT INTO reporter_state (key, value) VALUES ('late_data_checked_to', $1)", cutoff.isoformat())
        return []
    since = datetime.fromisoformat(raw) - LATE_COMMIT
    today = datetime.now(tz).date()
    days = [r["day"] for r in await pg.fetch("""
        SELECT DISTINCT (timestamp AT TIME ZONE $3)::date AS day
        FROM messages WHERE created_at > $1 AND created_at <= $2""", since, cutoff, tz.key)
            if r["day"] < today]
    await pg.execute("UPDATE reporter_state SET value = $1 WHERE key = 'late_data_checked_to'", cutoff.isoformat())
    return sorted(days)


async def refresh_late_days(pg, tz: ZoneInfo, writer: "ReportWriter") -> None:
    """Rebuilds every reported day that got new data, plus the day after
    it (its 'vs prior day' figures depend on it). Days with no report
    yet (e.g. older than CATCHUP_DAYS, now backfilled) get one."""
    late = await days_with_late_data(pg, tz)
    if not late:
        return
    today = datetime.now(tz).date()
    rebuild = sorted({d for d in late} | {d + timedelta(days=1) for d in late
                                           if d + timedelta(days=1) < today and await writer.exists(d + timedelta(days=1))})
    log.info(f"[Report] New data arrived for {len(late)} earlier day(s); rebuilding: {', '.join(d.isoformat() for d in rebuild)}")
    for day in rebuild:  # oldest first, so each compares against an up-to-date prior day
        try:
            await build(pg, tz, day)
        except Exception as e:
            log.error(f"[Report] {day.isoformat()} rebuild failed: {e}")


def seconds_until(at: time, tz: ZoneInfo) -> float:
    now = datetime.now(tz)
    target = datetime.combine(now.date(), at, tz)
    if target <= now:
        target = datetime.combine(now.date() + timedelta(days=1), at, tz)
    return (target - now).total_seconds()


async def main() -> None:
    parser = argparse.ArgumentParser(description="ChatPipeline daily reports")
    parser.add_argument("--date", type=date.fromisoformat, help="build (or rebuild) one day's report and exit")
    args = parser.parse_args()

    tz = ZoneInfo(REPORT_TZ)
    pg = await connect_pg_with_retry(log)
    writer = ReportWriter(pg, REPORT_DIR, REPORT_FALLBACK_DIR)
    try:
        await writer.setup()
        if args.date:
            await writer.recover_fallback()
            await build(pg, tz, args.date)
            await writer.write_index()
            return

        log.info(f"Reporter started: daily reports at {REPORT_AT:%H:%M} {tz.key}, into daily_reports and {REPORT_DIR}; "
                 f"checking every {REFRESH_EVERY // 60} min for late data in reported days.")
        while True:
            today = datetime.now(tz).date()
            # oldest first, so each report can compare against the day before
            for back in range(CATCHUP_DAYS, 0, -1):
                day = today - timedelta(days=back)
                if not await writer.exists(day):
                    try:
                        await build(pg, tz, day)
                    except Exception as e:
                        log.error(f"[Report] {day.isoformat()} failed: {e}")
            try:
                await refresh_late_days(pg, tz, writer)
            except Exception as e:
                log.error(f"[Report] Late-data check failed: {e}")
            try:
                await writer.recover_fallback()
            except Exception as e:
                log.error(f"[Report] Moving fallback reports failed: {e}")
            await writer.write_index()
            # wake for the nightly report, or sooner to check for late data
            await asyncio.sleep(min(seconds_until(REPORT_AT, tz), REFRESH_EVERY))
    finally:
        await log.flush()
        await pg.close()

if __name__ == "__main__":
    asyncio.run(main())
