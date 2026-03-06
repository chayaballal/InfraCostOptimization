"""
cache.py — Postgres-backed analysis response cache.

Provides a TTL-based cache backed by the `analysis_cache` table,
with a background cleanup loop for expired entries.
"""

import asyncio
import hashlib
import json
import logging

from sqlalchemy import text

log = logging.getLogger(__name__)


class AnalysisCache:
    """Postgres-backed cache for LLM analysis responses."""

    def __init__(self, db, ttl_hours: int = 24) -> None:
        self._db = db
        self.ttl_hours = ttl_hours

    # ── Key generation ────────────────────────────────────────────

    @staticmethod
    def build_key(
        instance_ids: list[str],
        window_days: int,
        focus: list[str],
        question: str | None,
    ) -> str:
        """SHA-256 hash of the request parameters."""
        payload = json.dumps({
            "ids": sorted(instance_ids),
            "w": window_days,
            "focus": sorted(focus),
            "q": (question or "").strip().lower(),
        }, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    # ── Read / Write ──────────────────────────────────────────────

    async def get(self, cache_key: str) -> str | None:
        """Return cached response text if exists and is fresh, else None."""
        async with self._db.session_factory() as session:
            result = await session.execute(text(
                f"SELECT response_text FROM analysis_cache "
                f"WHERE cache_key = :k AND created_at > NOW() - INTERVAL '{self.ttl_hours} hours'"
            ), {"k": cache_key})
            return result.scalar_one_or_none()

    async def save(self, cache_key: str, response_text: str) -> None:
        """Upsert the response into the cache."""
        async with self._db.session_factory() as session:
            await session.execute(text("""
                INSERT INTO analysis_cache (cache_key, response_text, created_at)
                VALUES (:k, :t, NOW())
                ON CONFLICT (cache_key) DO UPDATE
                    SET response_text = :t, created_at = NOW()
            """), {"k": cache_key, "t": response_text})
            await session.commit()

    # ── Background cleanup ────────────────────────────────────────

    async def start_cleanup_loop(self) -> None:
        """Purge expired cache entries every hour. Run as asyncio.create_task."""
        while True:
            await asyncio.sleep(3600)
            try:
                async with self._db.session_factory() as session:
                    result = await session.execute(text(
                        f"DELETE FROM analysis_cache "
                        f"WHERE created_at < NOW() - INTERVAL '{self.ttl_hours} hours'"
                    ))
                    await session.commit()
                    log.info(f"Cache cleanup: purged {result.rowcount} expired entries.")
            except Exception as e:
                log.warning(f"Cache cleanup error: {e}")
