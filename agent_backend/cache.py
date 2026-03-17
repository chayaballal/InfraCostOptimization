"""
Postgres-backed Cache for Analysis Responses

This module provides the `AnalysisCache` class, which manages a TTL-based cache
stored in a PostgreSQL table (`analysis_cache`). This prevents redundant
calls to the LLM for identical requests.
"""

import asyncio
import hashlib
import json
import logging

from sqlalchemy import text

log = logging.getLogger(__name__)


class AnalysisCache:
    """
    Manages persistence and retrieval of LLM analysis responses using PostgreSQL.
    
    Attributes:
        _db: The Database object used to interact with PostgreSQL.
        ttl_hours: The Time-To-Live for cache entries in hours.
    """

    def __init__(self, db, ttl_hours: int = 24) -> None:
        """
        Initializes the cache service with a database connection and TTL.
        """
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
        """
        Generates a unique SHA-256 hash for a specific analysis request.
        
        Args:
            instance_ids: IDs of the instances being analyzed.
            window_days: The time window for the analysis.
            focus: The specific focus areas (e.g., rightsizing, risks).
            question: Any custom user prompt modification.
            
        Returns:
            A deterministic hash string representing these parameters.
        """
        payload = json.dumps({
            "ids": sorted(instance_ids),
            "w": window_days,
            "focus": sorted(focus),
            "q": (question or "").strip().lower(),
        }, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    # ── Read / Write ──────────────────────────────────────────────

    async def get(self, cache_key: str) -> str | None:
        """
        Retrieves a cached analysis response from the database if it hasn't expired.
        
        Args:
            cache_key: The unique hash generated for the request.
            
        Returns:
            The cached response text if found and fresh; otherwise, None.
        """
        async with self._db.session_factory() as session:
            result = await session.execute(text(
                f"SELECT response_text FROM analysis_cache "
                f"WHERE cache_key = :k AND created_at > NOW() - INTERVAL '{self.ttl_hours} hours'"
            ), {"k": cache_key})
            return result.scalar_one_or_none()

    async def save(self, cache_key: str, response_text: str) -> None:
        """
        Saves or updates an analysis response in the cache.
        
        Args:
            cache_key: The unique hash for the request.
            response_text: The full response text from the LLM.
        """
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
        """
        Starts an infinite loop that periodically purges expired cache entries.
        Usually executed as a background asyncio task during application startup.
        """
        while True:
            await asyncio.sleep(3600)  # Check once per hour
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
