# backend/repositories/postgres_repository.py
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Callable
import re


_VIEW_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

def _safe_view_name(name: str) -> str:
    n = (name or "").strip()
    if not n or not _VIEW_NAME_RE.match(n):
        raise ValueError(f"Nombre de vista inválido: {name!r}")
    return n


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
        sql_clean = (sql or "").lstrip().lower()
        is_select = sql_clean.startswith("select") or sql_clean.startswith("with")

        with self._conn() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql, params or {})
                    row = cur.fetchone()

                # Si no es SELECT (por ejemplo INSERT/UPDATE/DELETE con RETURNING), confirmar la transacción
                if not is_select:
                    conn.commit()

                return dict(row) if row else None

            except Exception:
                # Si falló algo en un statement que pudo modificar datos, revertir
                # (en SELECT también es seguro)
                conn.rollback()
                raise
    
    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                conn.commit()

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
                
    def run_in_transaction(self, fn: Callable[[Any], Any]) -> Any:
        """
        Ejecuta varias operaciones en una sola transacción.
        - Si fn termina OK: commit
        - Si fn falla: rollback
        fn recibe el cursor (RealDictCursor) para ejecutar SQL.
        """
        with self._conn() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    result = fn(cur)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def obtener_metricas_por_referencia(self, referencia_sku: str, llave_naval: str | None,
                                    view_cpd_talla: str, view_cpd_tienda: str,
                                    view_prom_talla: str, view_prom_tienda: str):
        """
        Trae métricas desde vistas (Postgres) para una referencia_sku.
        Devuelve:
        - resumen_por_tienda: lista de filas por llave_naval
        - detalle_por_talla: lista de filas por llave_naval+talla+ean
        """
        v_cpd_talla = _safe_view_name(view_cpd_talla)
        v_cpd_tienda = _safe_view_name(view_cpd_tienda)
        v_prom_talla = _safe_view_name(view_prom_talla)
        v_prom_tienda = _safe_view_name(view_prom_tienda)

        ref = (referencia_sku or "").strip()
        if not ref:
            return [], []

        params = {"ref": ref, "llave": (llave_naval or "").strip()}

        where_llave = ""
        if params["llave"]:
            where_llave = " AND llave_naval = %(llave)s "

        sql_resumen = f"""
            SELECT
                llave_naval,
                referencia_sku,
                cpd_total,
                venta_promedio_mensual_total
            FROM (
                SELECT
                    t.llave_naval,
                    t.referencia_sku,
                    t.cpd_total,
                    p.venta_promedio_mensual_total
                FROM public.{v_cpd_tienda} t
                LEFT JOIN public.{v_prom_tienda} p
                    ON p.llave_naval = t.llave_naval
                AND p.referencia_sku = t.referencia_sku
                WHERE t.referencia_sku = %(ref)s
                {where_llave}
            ) x
            ORDER BY llave_naval;
        """

        sql_detalle = f"""
            SELECT
                t.llave_naval,
                t.referencia_sku,
                t.talla,
                t.ean,
                t.cpd,
                p.venta_promedio_mensual
            FROM public.{v_cpd_talla} t
            LEFT JOIN public.{v_prom_talla} p
                ON p.llave_naval = t.llave_naval
            AND p.referencia_sku = t.referencia_sku
            AND p.talla = t.talla
            AND COALESCE(p.ean,'') = COALESCE(t.ean,'')
            WHERE t.referencia_sku = %(ref)s
            {where_llave}
            ORDER BY llave_naval, talla;
        """

        resumen = self.fetch_all(sql_resumen, params)
        detalle = self.fetch_all(sql_detalle, params)
        return resumen, detalle
