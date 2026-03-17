"""
Local File-backed Cache for Analysis Responses

This module provides the `AnalysisCache` class, which manages a TTL-based cache
stored in a local JSON file. This prevents redundant calls to the LLM for 
identical requests without requiring a database.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

class AnalysisCache:
    """
    Manages persistence and retrieval of LLM analysis responses using a local JSON file.
    """

    def __init__(self, db=None, ttl_hours: int = 24) -> None:
        """
        Initializes the cache service with a local file path and TTL.
        `db` argument is kept for backward compatibility but ignored.
        """
        self.ttl_hours = ttl_hours
        self.file_path = Path(__file__).parent / "data" / "analysis_cache.json"
        self._ensure_file()

    def _ensure_file(self):
        if not self.file_path.exists():
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, "w") as f:
                json.dump({}, f)

    def _read_cache(self) -> dict:
        try:
            if not self.file_path.exists(): return {}
            with open(self.file_path, "r") as f:
                return json.load(f)
        except Exception as e:
            log.error(f"Failed to read analysis cache: {e}")
            return {}

    def _write_cache(self, data: dict):
        try:
            with open(self.file_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.error(f"Failed to write analysis cache: {e}")

    @staticmethod
    def build_key(
        instance_ids: list[str],
        window_days: int,
        focus: list[str],
        question: str | None,
    ) -> str:
        payload = json.dumps({
            "ids": sorted(instance_ids),
            "w": window_days,
            "focus": sorted(focus),
            "q": (question or "").strip().lower(),
        }, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    async def get(self, cache_key: str) -> str | None:
        cache = self._read_cache()
        entry = cache.get(cache_key)
        if not entry:
            return None
        
        created_at = datetime.fromisoformat(entry["created_at"])
        if datetime.now(timezone.utc) - created_at > timedelta(hours=self.ttl_hours):
            return None
            
        return entry["response_text"]

    async def save(self, cache_key: str, response_text: str) -> None:
        cache = self._read_cache()
        cache[cache_key] = {
            "response_text": response_text,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        self._write_cache(cache)

    async def start_cleanup_loop(self) -> None:
        """Periodically purges expired cache entries from the local file."""
        import asyncio
        while True:
            try:
                cache = self._read_cache()
                now = datetime.now(timezone.utc)
                new_cache = {}
                purged = 0
                for k, v in cache.items():
                    created_at = datetime.fromisoformat(v["created_at"])
                    if now - created_at <= timedelta(hours=self.ttl_hours):
                        new_cache[k] = v
                    else:
                        purged += 1
                
                if purged > 0:
                    self._write_cache(new_cache)
                    log.info(f"Cache cleanup: purged {purged} expired entries from local file.")
            except Exception as e:
                log.warning(f"Cache cleanup error: {e}")
            
            await asyncio.sleep(3600)  # Check once per hour
