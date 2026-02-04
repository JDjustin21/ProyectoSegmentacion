# backend/modules/segmentacion/app_cache_service.py
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import json
import time


class AppCacheService:
    def __init__(self, repo, table_name: str = "app_cache"):
        self._repo = repo
        self._table = table_name

    def get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        row = self._repo.fetch_one(
            f"SELECT payload_json, updated_at FROM {self._table} WHERE cache_key=%(k)s",
            {"k": cache_key}
        )
        if not row:
            return None

        try:
            payload = json.loads(row["payload_json"])
        except Exception:
            payload = None

        return {"payload": payload, "updated_at": row["updated_at"]}

    def set(self, cache_key: str, payload: Any) -> None:
        now = datetime.now(timezone.utc)
        payload_json = json.dumps(payload, ensure_ascii=False)

        self._repo.execute(f"""
            INSERT INTO {self._table} (cache_key, payload_json, updated_at)
            VALUES (%(k)s, %(p)s, %(u)s)
            ON CONFLICT (cache_key)
            DO UPDATE SET payload_json=EXCLUDED.payload_json, updated_at=EXCLUDED.updated_at;
        """, {"k": cache_key, "p": payload_json, "u": now})

    def is_fresh(self, cached: Optional[Dict[str, Any]], ttl_seconds: int) -> bool:
        if not cached or cached.get("payload") is None:
            return False

        updated_at = cached.get("updated_at")
        if not updated_at:
            return False

        try:
            age = (datetime.now(timezone.utc) - updated_at.astimezone(timezone.utc)).total_seconds()
        except Exception:
            return False

        return age < ttl_seconds

    # ---------- Lock helpers (Postgres advisory lock) ----------
    def try_lock(self, lock_key: int) -> bool:
        row = self._repo.fetch_one("SELECT pg_try_advisory_lock(%(k)s) AS ok;", {"k": int(lock_key)})
        return bool(row and row.get("ok") is True)

    def unlock(self, lock_key: int) -> None:
        # liberamos aunque haya error
        try:
            self._repo.fetch_one("SELECT pg_advisory_unlock(%(k)s) AS ok;", {"k": int(lock_key)})
        except Exception:
            pass

    def wait_for_refresh(self, cache_key: str, ttl_seconds: int, max_wait_seconds: float = 6.0) -> Optional[Dict[str, Any]]:
        """
        Espera a que otro proceso refresque el cache.
        Chequea cada 250ms hasta max_wait_seconds.
        """
        start = time.time()
        while (time.time() - start) < max_wait_seconds:
            cached = self.get(cache_key)
            if self.is_fresh(cached, ttl_seconds):
                return cached
            time.sleep(0.25)
        return self.get(cache_key)
