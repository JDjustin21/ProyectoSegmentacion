# backend/repositories/postgres_repository.py
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Any, Dict, List, Optional


class PostgresRepository:
    """
    Repositorio mínimo para Postgres.
    Responsabilidad: ejecutar queries y devolver dicts.
    No contiene lógica de negocio (SOLID).
    """

    def __init__(self, dsn: str):
        if not dsn:
            raise ValueError("POSTGRES_DSN está vacío. Configura la variable de entorno POSTGRES_DSN.")
        self._dsn = dsn

    @contextmanager
    def _conn(self):
        conn = psycopg2.connect(self._dsn)
        try:
            yield conn
        finally:
            conn.close()

    def fetch_all(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params or {})
                rows = cur.fetchall()
                return [dict(r) for r in rows]

    def fetch_one(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params or {})
                row = cur.fetchone()
                return dict(row) if row else None

    def execute_returning_id(self, sql: str, params: Dict[str, Any], id_field: str) -> int:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                conn.commit()
                if not row or id_field not in row:
                    raise RuntimeError(f"No se retornó {id_field} en execute_returning_id.")
                return int(row[id_field])

    def execute_many(self, sql: str, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(sql, r)
                conn.commit()
