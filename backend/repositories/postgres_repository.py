# backend/repositories/postgres_repository.py
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager as ctxmanager
from typing import Any, Dict, List, Optional, Callable
from decimal import Decimal
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

    @ctxmanager
    def _conn(self):
        conn = psycopg2.connect(self._dsn)
        try:
            yield conn
        finally:
            conn.close()

    def _json_safe_value(self, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, dict):
            return {k: self._json_safe_value(val) for k, val in v.items()}
        if isinstance(v, (list, tuple)):
            return [self._json_safe_value(x) for x in v]
        return v

    def _json_safe_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {k: self._json_safe_value(v) for k, v in (row or {}).items()}

    def fetch_all(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params or {})
                rows = cur.fetchall()
                return [self._json_safe_row(dict(r)) for r in rows]

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

                return self._json_safe_row(dict(row)) if row else None

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
                                    view_prom_talla: str, view_prom_tienda: str,
                                    view_rotacion_talla: str, view_rotacion_tienda: str):
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
        v_rotacion_talla = _safe_view_name(view_rotacion_talla)
        v_rotacion_tienda = _safe_view_name(view_rotacion_tienda)

        ref = (referencia_sku or "").strip()
        if not ref:
            return [], []

        params = {"ref": ref, "llave": (llave_naval or "").strip()}

        where_llave = ""
        if params["llave"]:
            where_llave = " AND t.llave_naval = %(llave)s "

        sql_resumen = f"""
            SELECT
                llave_naval,
                referencia_sku,
                cpd_total,
                venta_promedio_mensual_total,
                rotacion_tienda
            FROM (
                SELECT
                    t.llave_naval,
                    t.referencia_sku,
                    t.cpd_total,
                    p.venta_promedio_mensual_total,
                    r.rotacion_tienda
                FROM public.{v_cpd_tienda} t
                LEFT JOIN public.{v_prom_tienda} p
                    ON p.llave_naval = t.llave_naval
                AND p.referencia_sku = t.referencia_sku
                LEFT JOIN public.{v_rotacion_tienda} r
                    ON r.llave_naval = t.llave_naval
                AND r.referencia_sku = t.referencia_sku
                WHERE t.referencia_sku = %(ref)s
                {where_llave}
            ) x
            ORDER BY llave_naval;
        """

        where_llave_det = ""
        if params["llave"]:
            where_llave_det = " AND COALESCE(t.llave_naval, r.llave_naval) = %(llave)s "

        sql_detalle = f"""
            SELECT
                COALESCE(t.llave_naval, r.llave_naval)        AS llave_naval,
                COALESCE(t.referencia_sku, r.referencia_sku)  AS referencia_sku,
                COALESCE(t.talla, r.talla)                    AS talla,
                COALESCE(t.ean, r.ean)                        AS ean,
                t.cpd                                         AS cpd,
                p.venta_promedio_mensual                      AS venta_promedio_mensual,
                r.rotacion_talla                              AS rotacion_talla
            FROM public.{v_cpd_talla} t
            FULL OUTER JOIN public.{v_rotacion_talla} r
                ON r.llave_naval = t.llave_naval
                AND r.referencia_sku = t.referencia_sku
                AND r.talla = t.talla
                AND COALESCE(r.ean,'') = COALESCE(t.ean,'')
            LEFT JOIN public.{v_prom_talla} p
                ON p.llave_naval = COALESCE(t.llave_naval, r.llave_naval)
                AND p.referencia_sku = COALESCE(t.referencia_sku, r.referencia_sku)
                AND p.talla = COALESCE(t.talla, r.talla)
                AND COALESCE(p.ean,'') = COALESCE(COALESCE(t.ean, r.ean),'')
            WHERE COALESCE(t.referencia_sku, r.referencia_sku) = %(ref)s
            {where_llave_det}
            ORDER BY COALESCE(t.llave_naval, r.llave_naval), COALESCE(t.talla, r.talla);
        """

        resumen = self.fetch_all(sql_resumen, params)
        detalle = self.fetch_all(sql_detalle, params)
        return resumen, detalle
    
    def obtener_existencia_por_talla(
        self,
        referencia_sku: str,
        llave_naval: str | None,
        view_existencia_talla: str,
    ) -> List[Dict[str, Any]]:
        """
        Trae existencia actual por talla desde una vista Postgres.
        NO depende de ventas. Sirve incluso si la referencia nunca vendió.

        Retorna filas:
        llave_naval, referencia_sku, talla, ean, existencia_talla, disponible_talla, fecha_ultima_actualizacion
        """
        v_ex = _safe_view_name(view_existencia_talla)

        ref = (referencia_sku or "").strip()
        if not ref:
            return []

        params = {"ref": ref, "llave": (llave_naval or "").strip()}
        where_llave = ""
        if params["llave"]:
            where_llave = " AND e.llave_naval = %(llave)s "

        sql = f"""
            SELECT
                e.llave_naval,
                e.referencia_sku,
                e.talla,
                e.ean,
                e.existencia_talla,
                e.disponible_talla,
                e.fecha_ultima_actualizacion
            FROM public.{v_ex} e
            WHERE e.referencia_sku = %(ref)s
            {where_llave}
            ORDER BY e.llave_naval, e.talla;
        """
        return self.fetch_all(sql, params)
    
    def obtener_participacion_linea_por_tiendas(
        self,
        linea: str,
        dependencia: str | None,
        view_part_linea: str,
    ) -> List[Dict[str, Any]]:
        """
        Trae participación por línea (3 meses) por tienda, desde una vista Postgres.

        - linea: puede venir como "13 - Hombre Deportivo" o "Hombre Deportivo"
        - dependencia: opcional (si se filtra, mejor performance)
        - view_part_linea: nombre de vista (configurable, validado)

        Retorna filas de la vista:
        dependencia, linea, llave_naval, cod_bodega, desc_dependencia,
        venta_promedio_mensual_linea_tienda, venta_promedio_mensual_linea_cliente, participacion_venta_linea
        """
        v_part = _safe_view_name(view_part_linea)

        linea_in = (linea or "").strip()
        if not linea_in:
            return []

        dep_in = (dependencia or "").strip() if dependencia else ""

        params = {"linea": linea_in, "dep": dep_in}

        where_dep = ""
        if dep_in:
            where_dep = " AND dependencia = %(dep)s "

        # Normaliza en WHERE para que "13 - X" matchee con "X"
        sql = f"""
            SELECT
                dependencia,
                linea,
                llave_naval,
                cod_bodega,
                desc_dependencia,
                venta_promedio_mensual_linea_tienda,
                venta_promedio_mensual_linea_cliente,
                participacion_venta_linea
            FROM public.{v_part}
            WHERE
                lower(trim(regexp_replace(linea, '^[0-9]+\\s*-\\s*', ''))) =
                lower(trim(regexp_replace(%(linea)s, '^[0-9]+\\s*-\\s*', '')))
                {where_dep}
            ORDER BY dependencia, linea, desc_dependencia;
        """
        return self.fetch_all(sql, params)