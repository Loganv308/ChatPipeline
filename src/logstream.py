"""
LogStream Client — Python
Usage:
    from logstream import LogStream

    log = LogStream(service="my-python-app", host="${LOG_HOST}")
    log.info("Server started")
    log.warn("High memory usage", metadata={"memory_mb": 1024})
    log.error("Database connection failed", metadata={"host": "db:5432"})
"""

import asyncio
import json
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo


def _local_tz():
    """Timezone for the stdout copy of each log line, from the TZ env var
    (e.g. TZ=America/Chicago). Falls back to UTC if unset or unknown.
    Entries sent to the LogStream server always stay in UTC."""
    try:
        return ZoneInfo(os.getenv("TZ") or "UTC")
    except Exception:
        return timezone.utc

LOCAL_TZ = _local_tz()

class LogStream:
    def __init__(
        self,
        service: str,
        host: str = os.getenv("LOG_HOST"),
        batch_size: int = 10,
        fallback_path: str = "logstream_fallback.log",
    ):
        self.service       = service
        self.url           = f"{host.rstrip('/')}/api/ingest/batch"
        self.batch_size    = batch_size
        self.fallback_path = fallback_path
        self._queue:  list[dict] = []
        self._task:   asyncio.Task | None = None

    def _ensure_task(self) -> None:
        if self._task is None or self._task.done():
            try:
                loop = asyncio.get_running_loop()
                self._task = loop.create_task(self._flush_loop())
            except RuntimeError:
                pass

    def _enqueue(self, level: str, message: str, metadata: Optional[dict]) -> None:
        entry = {
            "level":   level,
            "service": self.service,
            "message": message,
            "ts":      datetime.now(timezone.utc).isoformat(),
            **({"metadata": metadata} if metadata else {}),
        }
        self._print_local(entry)
        self._queue.append(entry)
        self._ensure_task()
        if len(self._queue) >= self.batch_size:
            asyncio.ensure_future(self._flush())

    @staticmethod
    def _print_local(entry: dict) -> None:
        """Mirrors every entry to stdout so it also shows up in
        `docker compose logs`, independent of whether the LogStream
        server is reachable. Shown in LOCAL_TZ, e.g.
        2026-10-02 09:03:31.334 CDT."""
        local = datetime.fromisoformat(entry["ts"]).astimezone(LOCAL_TZ)
        ts = f"{local:%Y-%m-%d %H:%M:%S}.{local.microsecond // 1000:03d} {local:%Z}"
        line = f"{ts} {entry['level']:<5} [{entry['service']}] {entry['message']}"
        if "metadata" in entry:
            line += f" {json.dumps(entry['metadata'])}"
        print(line, flush=True)

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(2)
            await self._flush()

    async def _flush(self) -> None:
        if not self._queue:
            return
        batch = self._queue.copy()
        self._queue.clear()
        try:
            data = json.dumps(batch).encode("utf-8")
            req  = urllib.request.Request(
                self.url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: urllib.request.urlopen(req, timeout=3))
        except Exception:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._write_fallback, batch)

    def _write_fallback(self, batch: list[dict]) -> None:
        """Appends each log entry as a JSON line to the local fallback
        file. Runs in a thread executor so a slow/full disk doesn't
        block the event loop. Swallows its own errors -- if even the
        fallback write fails, there's nowhere left for the log to go."""
        try:
            os.makedirs(os.path.dirname(self.fallback_path) or ".", exist_ok=True)
            with open(self.fallback_path, "a", encoding="utf-8") as f:
                for entry in batch:
                    f.write(json.dumps(entry) + "\n")
        except Exception:
            pass

    async def flush(self) -> None:
        await self._flush()

    def debug(self, message: str, metadata: Optional[dict] = None) -> None:
        self._enqueue("DEBUG", message, metadata)

    def info(self, message: str, metadata: Optional[dict] = None) -> None:
        self._enqueue("INFO", message, metadata)

    def warn(self, message: str, metadata: Optional[dict] = None) -> None:
        self._enqueue("WARN", message, metadata)

    def error(self, message: str, metadata: Optional[dict] = None) -> None:
        self._enqueue("ERROR", message, metadata)

    def fatal(self, message: str, metadata: Optional[dict] = None) -> None:
        self._enqueue("FATAL", message, metadata)