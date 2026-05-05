# backend/repositories/postgres_repository.py
import re
from contextlib import contextmanager as ctxmanager
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor


# Expresión regular permitida para nombres de vistas/tablas configurables.
# Solo acepta identificadores simples, por ejemplo:
#   vw_metricas_cpd_30_dias_por_talla
# No acepta espacios, comillas, puntos, guiones ni sentencias SQL.
_VIEW_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _safe_view_name(name: str) -> str:
    """
    Valida nombres de vistas usados en SQL dinámico.

    Los nombres de vistas no pueden enviarse como parámetros normales de psycopg2,
    por eso se validan manualmente antes de interpolarlos en las consultas.

    Esta función reduce el riesgo de inyección SQL cuando el nombre de la vista
    viene desde configuración o variables de entorno.
    """
    n = (name or "").strip()

    if not n or not _VIEW_NAME_RE.match(n):
        raise ValueError(f"Nombre de vista inválido: {name!r}")

    return n


class PostgresRepository:
    """
    Repositorio base para ejecutar consultas contra PostgreSQL.

    Responsabilidades principales:
    - Abrir y cerrar conexiones.
    - Ejecutar consultas parametrizadas.
    - Retornar resultados como diccionarios.
    - Convertir valores no serializables a JSON, como Decimal.

    Además contiene algunas consultas especializadas de métricas usadas por
    Segmentación, Inventario y Analíticas. Si el módulo crece, esas consultas
    pueden moverse a un repositorio específico de métricas.
    """

    def __init__(self, dsn: str):
        """
        Inicializa el repositorio con la cadena de conexión de PostgreSQL.
        """
        if not dsn:
            raise ValueError(
                "POSTGRES_DSN está vacío. Configura la variable de entorno POSTGRES_DSN."
            )

        self._dsn = dsn

    @ctxmanager
    def _conn(self):
        """
        Crea una conexión temporal a PostgreSQL y garantiza su cierre.

        Cada operación abre su propia conexión. Esto simplifica el manejo de recursos
        y evita conexiones abiertas accidentalmente.
        """
        conn = psycopg2.connect(self._dsn)

        try:
            yield conn
        finally:
            conn.close()

    def _json_safe_value(self, value: Any) -> Any:
        """
        Convierte valores devueltos por PostgreSQL a tipos compatibles con JSON.
        """
        if value is None:
            return None

        if isinstance(value, Decimal):
            return float(value)

        if isinstance(value, dict):
            return {
                key: self._json_safe_value(item)
                for key, item in value.items()
            }

        if isinstance(value, (list, tuple)):
            return [
                self._json_safe_value(item)
                for item in value
            ]

        return value

    def _json_safe_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convierte una fila completa a valores compatibles con JSON.
        """
        return {
            key: self._json_safe_value(value)
            for key, value in (row or {}).items()
        }

    def fetch_all(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Ejecuta una consulta y retorna todas las filas como lista de diccionarios.
        """
        with self._conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params or {})
                rows = cur.fetchall()

        return [
            self._json_safe_row(dict(row))
            for row in rows
        ]

    def fetch_one(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Ejecuta una consulta y retorna una sola fila.

        También soporta sentencias INSERT/UPDATE/DELETE con RETURNING.
        En esos casos confirma la transacción automáticamente.
        """
        sql_clean = (sql or "").lstrip().lower()
        is_select = sql_clean.startswith("select") or sql_clean.startswith("with")

        with self._conn() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql, params or {})
                    row = cur.fetchone()

                if not is_select:
                    conn.commit()

                return self._json_safe_row(dict(row)) if row else None

            except Exception:
                conn.rollback()
                raise

    def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Ejecuta una sentencia que modifica datos y confirma la transacción.
        """
        with self._conn() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params or {})

                conn.commit()

            except Exception:
                conn.rollback()
                raise

    def execute_returning_id(
        self,
        sql: str,
        params: Dict[str, Any],
        id_field: str
    ) -> int:
        """
        Ejecuta una sentencia con RETURNING y devuelve el identificador indicado.
        """
        with self._conn() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql, params)
                    row = cur.fetchone()

                conn.commit()

                if not row or id_field not in row:
                    raise RuntimeError(
                        f"No se retornó {id_field} en execute_returning_id."
                    )

                return int(row[id_field])

            except Exception:
                conn.rollback()
                raise

    def execute_many(
        self,
        sql: str,
        rows: List[Dict[str, Any]]
    ) -> None:
        """
        Ejecuta la misma sentencia para múltiples filas dentro de una transacción.
        """
        if not rows:
            return

        with self._conn() as conn:
            try:
                with conn.cursor() as cur:
                    for row in rows:
                        cur.execute(sql, row)

                conn.commit()

            except Exception:
                conn.rollback()
                raise

    def run_in_transaction(self, fn: Callable[[Any], Any]) -> Any:
        """
        Ejecuta varias operaciones dentro de una sola transacción.

        La función recibida obtiene un cursor RealDictCursor.
        Si la función termina correctamente, se confirma la transacción.
        Si ocurre un error, se revierte todo.
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

    def obtener_metricas_por_referencia(
        self,
        referencia_sku: str,
        llave_naval: str | None,
        view_cpd_talla: str,
        view_cpd_tienda: str,
        view_prom_talla: str,
        view_prom_tienda: str,
        view_rotacion_talla: str,
        view_rotacion_tienda: str,
        llaves: Optional[list[str]] = None
    ):
        """
        Consulta métricas comerciales de una referencia SKU desde vistas de PostgreSQL.

        Retorna dos estructuras:
        - resumen_por_tienda: métricas agregadas por tienda.
        - detalle_por_talla: métricas por tienda, talla y EAN.

        El parámetro llaves permite limitar la consulta a un conjunto de tiendas.
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

        params = {
            "ref": ref,
            "llave": (llave_naval or "").strip(),
            "llaves": llaves or []
        }

        sql_resumen = f"""
            WITH
            cpd AS (
                SELECT llave_naval, referencia_sku, cpd_total
                FROM public.{v_cpd_tienda}
                WHERE referencia_sku = %(ref)s
                {"AND llave_naval = ANY(%(llaves)s)" if params["llaves"] else ""}
            ),
            prom AS (
                SELECT llave_naval, referencia_sku, venta_promedio_mensual_total
                FROM public.{v_prom_tienda}
                WHERE referencia_sku = %(ref)s
                {"AND llave_naval = ANY(%(llaves)s)" if params["llaves"] else ""}
            ),
            rot AS (
                SELECT llave_naval, referencia_sku, rotacion_tienda
                FROM public.{v_rotacion_tienda}
                WHERE referencia_sku = %(ref)s
                {"AND llave_naval = ANY(%(llaves)s)" if params["llaves"] else ""}
            )
            SELECT
                c.llave_naval,
                c.referencia_sku,
                c.cpd_total,
                p.venta_promedio_mensual_total,
                r.rotacion_tienda
            FROM cpd c
            LEFT JOIN prom p ON p.llave_naval = c.llave_naval
                            AND p.referencia_sku = c.referencia_sku
            LEFT JOIN rot r ON r.llave_naval = c.llave_naval
                           AND r.referencia_sku = c.referencia_sku
            ORDER BY c.llave_naval;
        """

        sql_detalle = f"""
            WITH
            cpd AS (
                SELECT llave_naval, referencia_sku, talla, ean, cpd
                FROM public.{v_cpd_talla}
                WHERE referencia_sku = %(ref)s
                {"AND llave_naval = ANY(%(llaves)s)" if params["llaves"] else ""}
            ),
            rot AS (
                SELECT llave_naval, referencia_sku, talla, ean, rotacion_talla
                FROM public.{v_rotacion_talla}
                WHERE referencia_sku = %(ref)s
                {"AND llave_naval = ANY(%(llaves)s)" if params["llaves"] else ""}
            ),
            prom AS (
                SELECT llave_naval, referencia_sku, talla, ean, venta_promedio_mensual
                FROM public.{v_prom_talla}
                WHERE referencia_sku = %(ref)s
                {"AND llave_naval = ANY(%(llaves)s)" if params["llaves"] else ""}
            ),
            base AS (
                SELECT llave_naval, referencia_sku, talla, ean FROM cpd
                UNION
                SELECT llave_naval, referencia_sku, talla, ean FROM rot
            )
            SELECT
                b.llave_naval,
                b.referencia_sku,
                b.talla,
                b.ean,
                c.cpd,
                p.venta_promedio_mensual,
                r.rotacion_talla
            FROM base b
            LEFT JOIN cpd c ON c.llave_naval = b.llave_naval
                            AND c.referencia_sku = b.referencia_sku
                            AND c.talla = b.talla
                            AND COALESCE(c.ean, '') = COALESCE(b.ean, '')
            LEFT JOIN rot r ON r.llave_naval = b.llave_naval
                            AND r.referencia_sku = b.referencia_sku
                            AND r.talla = b.talla
                            AND COALESCE(r.ean, '') = COALESCE(b.ean, '')
            LEFT JOIN prom p ON p.llave_naval = b.llave_naval
                             AND p.referencia_sku = b.referencia_sku
                             AND p.talla = b.talla
                             AND COALESCE(p.ean, '') = COALESCE(b.ean, '')
            ORDER BY b.llave_naval, b.talla;
        """

        resumen = self.fetch_all(sql_resumen, params)
        detalle = self.fetch_all(sql_detalle, params)

        return resumen, detalle

    def obtener_existencia_por_talla(
        self,
        referencia_sku: str,
        llave_naval: str | None,
        view_existencia_talla: str
    ) -> List[Dict[str, Any]]:
        """
        Consulta existencia actual por talla desde una vista de PostgreSQL.

        Esta consulta no depende de ventas. Por eso puede devolver inventario
        incluso para referencias que nunca han vendido.
        """
        v_ex = _safe_view_name(view_existencia_talla)

        ref = (referencia_sku or "").strip()
        if not ref:
            return []

        params = {
            "ref": ref,
            "llave": (llave_naval or "").strip()
        }

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
        view_part_linea: str
    ) -> List[Dict[str, Any]]:
        """
        Consulta la participación de venta de una línea por tienda.

        La línea puede recibirse con código, por ejemplo:
            13 - Hombre Deportivo

        La consulta normaliza ese valor para compararlo también contra:
            Hombre Deportivo
        """
        v_part = _safe_view_name(view_part_linea)

        linea_in = (linea or "").strip()
        if not linea_in:
            return []

        dep_in = (dependencia or "").strip() if dependencia else ""

        params = {
            "linea": linea_in,
            "dep": dep_in
        }

        where_dep = ""
        if dep_in:
            where_dep = " AND dependencia = %(dep)s "

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