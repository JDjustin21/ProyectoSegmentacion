# backend/modules/mapa/mapa_db_service.py

from __future__ import annotations

import re
from typing import Any, Dict, List

from backend.repositories.postgres_repository import PostgresRepository


_VIEW_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _safe_view_name(name: str) -> str:
    """
    Valida nombres de vistas/tablas usados en SQL dinámico.

    Los nombres de relaciones no pueden parametrizarse con psycopg2.
    Por eso se validan antes de interpolarlos en SQL.
    """
    n = (name or "").strip()

    if not n or not _VIEW_NAME_RE.match(n):
        raise ValueError(f"Nombre de vista inválido: {name!r}")

    return n


class MapaDbService:
    """
    Servicio de datos para el módulo Mapa.

    El mapa trabaja a nivel ciudad:
    - Un punto representa una ciudad.
    - El valor del punto es una métrica agregada.
    - La tabla muestra el detalle analítico según el tipo seleccionado.
    """

    TIPOS_PERMITIDOS = {"ventas", "inventario", "segmentacion"}

    def __init__(
        self,
        repo: PostgresRepository,
        view_ciudades_geo: str,
        view_tiendas: str,
        view_ventas_mapa: str,
        view_inventario_tienda: str,
        view_inventario_referencia: str,
    ):
        self._repo = repo
        self._view_ciudades_geo = _safe_view_name(view_ciudades_geo)
        self._view_tiendas = _safe_view_name(view_tiendas)
        self._view_ventas_mapa = _safe_view_name(view_ventas_mapa)
        self._view_inventario_tienda = _safe_view_name(view_inventario_tienda)
        self._view_inventario_referencia = _safe_view_name(view_inventario_referencia)

    def obtener_datos_mapa(
        self,
        tipo: str,
        filtros: Dict[str, Any],
        anio: int | None = None,
        mes: int | None = None,
    ) -> Dict[str, Any]:
        """
        Obtiene puntos y tabla del mapa según el tipo solicitado.
        """
        tipo = (tipo or "").strip().lower()
        filtros_limpios = self._normalizar_filtros(filtros)

        if tipo not in self.TIPOS_PERMITIDOS:
            raise ValueError("Tipo de mapa no permitido.")

        if tipo == "ventas":
            periodo = self._normalizar_periodo(anio, mes)

            puntos = self._obtener_puntos_ventas(
                filtros=filtros_limpios,
                anio=periodo["anio"],
                mes=periodo["mes"],
            )
            tabla = self._obtener_tabla_ventas(
                filtros=filtros_limpios,
                anio=periodo["anio"],
                mes=periodo["mes"],
            )

            titulo = f"Mapa de calor por ventas {periodo['mes']:02d}/{periodo['anio']}"
            unidad = "unidades vendidas"

        elif tipo == "inventario":
            puntos = self._obtener_puntos_inventario(filtros_limpios)

            # Inventario solo se muestra como mapa.
            # No se calcula tabla porque el módulo de inventario ya existe
            # y la tabla genera muchas filas innecesarias.
            tabla = []

            titulo = "Mapa de calor por inventario"
            unidad = "unidades en existencia"

        else:
            periodo = self._normalizar_periodo(anio, mes)

            puntos = self._obtener_puntos_segmentacion(
                filtros=filtros_limpios,
                anio=periodo["anio"],
                mes=periodo["mes"],
            )
            tabla = self._obtener_tabla_segmentacion(
                filtros=filtros_limpios,
                anio=periodo["anio"],
                mes=periodo["mes"],
            )

            titulo = f"Mapa de calor por segmentación {periodo['mes']:02d}/{periodo['anio']}"
            unidad = "unidades segmentadas"

        puntos_normalizados = [self._normalizar_punto_ciudad(row) for row in puntos]
        puntos_normalizados = [
            p for p in puntos_normalizados
            if p["latitud"] is not None
            and p["longitud"] is not None
        ]

        total_valor = sum(p["valor"] for p in puntos_normalizados)

        return {
            "data": {
                "tipo": tipo,
                "titulo": titulo,
                "unidad": unidad,
                "puntos": puntos_normalizados,
                "tabla": tabla,
            },
            "meta": {
                "total_ciudades": len(puntos_normalizados),
                "total_valor": total_valor,
                "total_filas_tabla": len(tabla),
            },
        }

    def obtener_filtros(self) -> Dict[str, Any]:
        """
        Retorna valores disponibles para filtros del mapa.

        Los catálogos se consultan desde una materialized view para evitar
        recalcular ventas, inventario y segmentación cada vez que se abre el mapa.
        """
        sql = """
            SELECT
                COALESCE(lineas, ARRAY[]::text[]) AS lineas,
                COALESCE(clientes, ARRAY[]::text[]) AS clientes,
                COALESCE(ciudades, ARRAY[]::text[]) AS ciudades,
                COALESCE(tipos_portafolio, ARRAY[]::text[]) AS tipos_portafolio,
                COALESCE(estados, ARRAY[]::text[]) AS estados
            FROM public.mv_mapa_filtros
            LIMIT 1;
        """

        rows = self._repo.fetch_all(sql, {})

        if not rows:
            return {
                "lineas": [],
                "clientes": [],
                "ciudades": [],
                "tipos_portafolio": [],
                "estados": [],
            }

        return rows[0]

    # =========================================================
    # VENTAS
    # =========================================================

    def _obtener_puntos_ventas(
        self,
        filtros: Dict[str, List[str]],
        anio: int,
        mes: int,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "anio": anio,
            "anio_anterior": anio - 1,
            "mes": mes,
        }
        where_extra = self._where_ventas(filtros, params)

        sql = f"""
            WITH base AS (
                SELECT
                    ciudad_norm,
                    ciudad,
                    linea,
                    dependencia,
                    tipo_portafolio,
                    estado_sku,
                    llave_naval,
                    anio,
                    mes,
                    unidades,
                    venta_pvp_lista,
                    venta_pvp_hist_lista
                FROM public.{self._view_ventas_mapa}
                WHERE mes = %(mes)s
                AND anio IN (%(anio)s, %(anio_anterior)s)
                {where_extra}
            ),
            agg_ciudad AS (
                SELECT
                    ciudad_norm,
                    SUM(unidades) FILTER (WHERE anio = %(anio)s) AS venta_unds,
                    SUM(unidades) FILTER (WHERE anio = %(anio_anterior)s) AS venta_unds_anterior,
                    COUNT(DISTINCT llave_naval) FILTER (
                        WHERE anio = %(anio)s AND unidades > 0
                    ) AS tiendas
                FROM base
                GROUP BY ciudad_norm
            )
            SELECT
                g.ciudad,
                g.departamento,
                g.latitud,
                g.longitud,
                COALESCE(a.venta_unds, 0) AS valor,
                COALESCE(a.venta_unds_anterior, 0) AS venta_unds_anterior,
                COALESCE(a.tiendas, 0) AS tiendas
            FROM agg_ciudad a
            JOIN public.{self._view_ciudades_geo} g
                ON g.ciudad_norm = a.ciudad_norm
            WHERE COALESCE(a.venta_unds, 0) > 0
            ORDER BY valor DESC;
        """

        return self._repo.fetch_all(sql, params)

    def _obtener_tabla_ventas(
        self,
        filtros: Dict[str, List[str]],
        anio: int,
        mes: int,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "anio": anio,
            "anio_anterior": anio - 1,
            "mes": mes,
        }
        where_extra = self._where_ventas(filtros, params)

        sql = f"""
            WITH base AS (
                SELECT
                    ciudad_norm,
                    ciudad,
                    linea,
                    dependencia,
                    tipo_portafolio,
                    estado_sku,
                    llave_naval,
                    anio,
                    mes,
                    unidades,
                    venta_pvp_lista,
                    venta_pvp_hist_lista
                FROM public.{self._view_ventas_mapa}
                WHERE mes = %(mes)s
                AND anio IN (%(anio)s, %(anio_anterior)s)
                {where_extra}
            ),
            agg AS (
                SELECT
                    linea,
                    SUM(unidades) FILTER (WHERE anio = %(anio_anterior)s)
                        AS venta_unds_anterior,
                    SUM(unidades) FILTER (WHERE anio = %(anio)s)
                        AS venta_unds,
                    COUNT(DISTINCT llave_naval) FILTER (
                        WHERE anio = %(anio)s AND unidades > 0
                    ) AS tiendas,
                    SUM(venta_pvp_lista) FILTER (WHERE anio = %(anio)s)
                        AS venta_pvp_lista,
                    SUM(venta_pvp_hist_lista) FILTER (WHERE anio = %(anio)s)
                        AS venta_pvp_hist_lista
                FROM base
                GROUP BY linea
            ),
            total AS (
                SELECT
                    SUM(COALESCE(venta_unds, 0)) AS total_venta_unds,
                    SUM(COALESCE(venta_unds_anterior, 0)) AS total_venta_unds_anterior,
                    SUM(COALESCE(venta_pvp_lista, 0)) AS total_venta_pvp_lista,
                    SUM(COALESCE(venta_pvp_hist_lista, 0)) AS total_venta_pvp_hist_lista,
                    COUNT(*) AS total_lineas
                FROM agg
            ),
            detalle AS (
                SELECT
                    a.linea,
                    COALESCE(a.venta_unds_anterior, 0) AS venta_unds_anterior,
                    COALESCE(a.venta_unds, 0) AS venta_unds,
                    CASE
                        WHEN COALESCE(t.total_venta_unds, 0) = 0 THEN 0
                        ELSE COALESCE(a.venta_unds, 0) / t.total_venta_unds
                    END AS participacion,
                    CASE
                        WHEN COALESCE(a.venta_unds_anterior, 0) = 0
                            AND COALESCE(a.venta_unds, 0) > 0
                        THEN 1
                        WHEN COALESCE(a.venta_unds_anterior, 0) = 0
                        THEN 0
                        ELSE (
                            COALESCE(a.venta_unds, 0)
                            - COALESCE(a.venta_unds_anterior, 0)
                        ) / COALESCE(a.venta_unds_anterior, 0)
                    END AS variacion_anterior,
                    COALESCE(a.tiendas, 0) AS tiendas,
                    CASE
                        WHEN COALESCE(a.tiendas, 0) = 0 THEN 0
                        ELSE COALESCE(a.venta_unds, 0) / a.tiendas
                    END AS promedio_unds_tienda,
                    COALESCE(a.venta_pvp_lista, 0) AS venta_pvp_lista,
                    COALESCE(a.venta_pvp_hist_lista, 0) AS venta_pvp_hist_lista,
                    0 AS orden
                FROM agg a
                CROSS JOIN total t
            ),
            fila_total AS (
                SELECT
                    'Total'::text AS linea,
                    COALESCE(t.total_venta_unds_anterior, 0) AS venta_unds_anterior,
                    COALESCE(t.total_venta_unds, 0) AS venta_unds,
                    1::numeric AS participacion,
                    CASE
                        WHEN COALESCE(t.total_venta_unds_anterior, 0) = 0
                            AND COALESCE(t.total_venta_unds, 0) > 0
                        THEN 1
                        WHEN COALESCE(t.total_venta_unds_anterior, 0) = 0
                        THEN 0
                        ELSE (
                            COALESCE(t.total_venta_unds, 0)
                            - COALESCE(t.total_venta_unds_anterior, 0)
                        ) / COALESCE(t.total_venta_unds_anterior, 0)
                    END AS variacion_anterior,
                    (
                        SELECT COUNT(DISTINCT llave_naval)
                        FROM base
                        WHERE anio = %(anio)s
                        AND unidades > 0
                    ) AS tiendas,
                    CASE
                        WHEN (
                            SELECT COUNT(DISTINCT llave_naval)
                            FROM base
                            WHERE anio = %(anio)s
                            AND unidades > 0
                        ) = 0 THEN 0
                        ELSE COALESCE(t.total_venta_unds, 0) / (
                            SELECT COUNT(DISTINCT llave_naval)
                            FROM base
                            WHERE anio = %(anio)s
                            AND unidades > 0
                        )
                    END AS promedio_unds_tienda,
                    COALESCE(t.total_venta_pvp_lista, 0) AS venta_pvp_lista,
                    COALESCE(t.total_venta_pvp_hist_lista, 0) AS venta_pvp_hist_lista,
                    1 AS orden
                FROM total t
            )
            SELECT *
            FROM detalle

            UNION ALL

            SELECT *
            FROM fila_total

            ORDER BY orden, venta_unds DESC;
        """

        return self._repo.fetch_all(sql, params)
    
    def _normalizar_periodo(
        self,
        anio: int | None,
        mes: int | None,
    ) -> Dict[str, int]:
        """
        Normaliza el periodo usado para ventas.

        Si no llega año o mes desde el frontend, usa el año y mes actual.
        """
        from datetime import date

        hoy = date.today()

        try:
            anio_int = int(anio) if anio is not None else hoy.year
        except (TypeError, ValueError):
            anio_int = hoy.year

        try:
            mes_int = int(mes) if mes is not None else hoy.month
        except (TypeError, ValueError):
            mes_int = hoy.month

        if mes_int < 1 or mes_int > 12:
            mes_int = hoy.month

        return {
            "anio": anio_int,
            "mes": mes_int,
            "anio_anterior": anio_int - 1,
        }

    # =========================================================
    # INVENTARIO
    # =========================================================

    def _obtener_puntos_inventario(
        self,
        filtros: Dict[str, List[str]],
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        where_extra = self._where_inventario(filtros, params)

        sql = f"""
            WITH base AS (
                SELECT
                    lower(trim(t.ciudad)) AS ciudad_norm,
                    public.fn_linea_mapa(r.linea) AS linea,
                    t.dependencia,
                    r.tipo_portafolio,
                    r.estado AS estado_sku,
                    i.llave_naval,
                    i.referencia_sku,
                    COALESCE(i.existencia, 0) AS existencia
                FROM public.{self._view_inventario_tienda} i
                JOIN public.{self._view_tiendas} t
                    ON t.llave_naval = i.llave_naval
                LEFT JOIN public.{self._view_inventario_referencia} r
                    ON r.referencia_sku = i.referencia_sku
                WHERE t.ciudad IS NOT NULL
                 AND public.fn_linea_mapa(r.linea) IS NOT NULL
                {where_extra}
            ),
            agg AS (
                SELECT
                    ciudad_norm,
                    SUM(existencia) AS valor,
                    COUNT(DISTINCT llave_naval) AS tiendas,
                    COUNT(DISTINCT referencia_sku) AS referencias
                FROM base
                GROUP BY ciudad_norm
            )
            SELECT
                g.ciudad,
                g.departamento,
                g.latitud,
                g.longitud,
                a.valor,
                a.tiendas,
                a.referencias
            FROM agg a
            JOIN public.{self._view_ciudades_geo} g
                ON g.ciudad_norm = a.ciudad_norm
            WHERE COALESCE(a.valor, 0) > 0
            ORDER BY a.valor DESC;
        """

        return self._repo.fetch_all(sql, params)

    # =========================================================
    # SEGMENTACIÓN
    # =========================================================

    def _obtener_puntos_segmentacion(
        self,
        filtros: Dict[str, List[str]],
        anio: int,
        mes: int,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "anio": anio,
            "mes": mes,
        }
        where_extra = self._where_segmentacion(filtros, params)

        sql = f"""
            WITH base AS (
                SELECT
                    lower(trim(t.ciudad)) AS ciudad_norm,
                    s.linea,
                    t.dependencia,
                    s.tipo_portafolio,
                    s.estado_sku,
                    d.llave_naval,
                    COALESCE(d.cantidad, 0) AS cantidad
                FROM public.segmentacion s
                JOIN public.segmentacion_detalle d
                    ON d.id_segmentacion = s.id_segmentacion
                JOIN public.{self._view_tiendas} t
                    ON t.llave_naval = d.llave_naval
                WHERE t.ciudad IS NOT NULL
                  AND s.estado_segmentacion = 'Activa'
                  AND d.estado_detalle = 'Activo'
                  AND EXTRACT(YEAR FROM s.fecha_creacion)::integer = %(anio)s
                  AND EXTRACT(MONTH FROM s.fecha_creacion)::integer = %(mes)s
                  AND public.fn_linea_mapa(s.linea) IS NOT NULL
                {where_extra}
            ),
            agg AS (
                SELECT
                    ciudad_norm,
                    SUM(cantidad) AS valor,
                    COUNT(DISTINCT llave_naval) AS tiendas
                FROM base
                GROUP BY ciudad_norm
            )
            SELECT
                g.ciudad,
                g.departamento,
                g.latitud,
                g.longitud,
                a.valor,
                a.tiendas
            FROM agg a
            JOIN public.{self._view_ciudades_geo} g
                ON g.ciudad_norm = a.ciudad_norm
            WHERE COALESCE(a.valor, 0) > 0
            ORDER BY a.valor DESC;
        """

        return self._repo.fetch_all(sql, params)

    def _obtener_tabla_segmentacion(
        self,
        filtros: Dict[str, List[str]],
        anio: int,
        mes: int,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "anio": anio,
            "mes": mes,
        }
        where_extra = self._where_segmentacion(filtros, params)

        sql = f"""
            WITH base AS (
                SELECT
                    lower(trim(t.ciudad)) AS ciudad_norm,
                    s.linea,
                    t.dependencia,
                    s.tipo_portafolio,
                    s.estado_sku,
                    s.referencia,
                    d.llave_naval,
                    COALESCE(d.cantidad, 0) AS cantidad
                FROM public.segmentacion s
                JOIN public.segmentacion_detalle d
                    ON d.id_segmentacion = s.id_segmentacion
                JOIN public.{self._view_tiendas} t
                    ON t.llave_naval = d.llave_naval
                WHERE t.ciudad IS NOT NULL
                  AND s.estado_segmentacion = 'Activa'
                  AND d.estado_detalle = 'Activo'
                  AND EXTRACT(YEAR FROM s.fecha_creacion)::integer = %(anio)s
                  AND EXTRACT(MONTH FROM s.fecha_creacion)::integer = %(mes)s
                {where_extra}
            )
            SELECT
                g.ciudad,
                g.departamento,
                b.linea,
                b.dependencia AS cliente,
                b.tipo_portafolio,
                b.estado_sku,
                SUM(b.cantidad) AS cantidad_segmentada,
                COUNT(DISTINCT b.llave_naval) AS tiendas,
                COUNT(DISTINCT b.referencia) AS referencias_segmentadas,
                SUM(b.cantidad) / NULLIF(COUNT(DISTINCT b.llave_naval), 0)
                    AS promedio_segmentado_tienda
            FROM base b
            JOIN public.{self._view_ciudades_geo} g
                ON g.ciudad_norm = b.ciudad_norm
            GROUP BY
                g.ciudad,
                g.departamento,
                b.linea,
                b.dependencia,
                b.tipo_portafolio,
                b.estado_sku
            HAVING SUM(b.cantidad) > 0
            ORDER BY cantidad_segmentada DESC;
        """

        return self._repo.fetch_all(sql, params)

    # =========================================================
    # FILTROS
    # =========================================================

    def _where_ventas(
        self,
        filtros: Dict[str, List[str]],
        params: Dict[str, Any],
    ) -> str:
        columnas = {
            "lineas": "linea",
            "clientes": "dependencia",
            "ciudades": "ciudad",
            "tipos_portafolio": "tipo_portafolio",
            "estados": "estado_sku",
        }

        return self._construir_where_multi(filtros, params, columnas)

    def _where_segmentacion(
        self,
        filtros: Dict[str, List[str]],
        params: Dict[str, Any],
    ) -> str:
        columnas = {
            "lineas": "public.fn_linea_mapa(s.linea)",
            "clientes": "t.dependencia",
            "ciudades": "t.ciudad",
            "tipos_portafolio": "s.tipo_portafolio",
            "estados": "s.estado_sku",
        }

        return self._construir_where_multi(filtros, params, columnas)

    def _where_inventario(
        self,
        filtros: Dict[str, List[str]],
        params: Dict[str, Any],
    ) -> str:
        """
        Filtros para inventario.

        La existencia viene de vw_inventario_tienda_rotacion.
        Las dimensiones de producto vienen de vw_inventario_resumen_referencia.
        """
        columnas = {
            "lineas": "r.linea",
            "clientes": "t.dependencia",
            "ciudades": "t.ciudad",
            "tipos_portafolio": "r.tipo_portafolio",
            "estados": "r.estado",
        }

        return self._construir_where_multi(filtros, params, columnas)

    def _construir_where_multi(
        self,
        filtros: Dict[str, List[str]],
        params: Dict[str, Any],
        columnas: Dict[str, str],
    ) -> str:
        condiciones: List[str] = []

        for nombre_filtro, columna_sql in columnas.items():
            valores = filtros.get(nombre_filtro) or []

            if not valores:
                continue

            params[nombre_filtro] = valores
            condiciones.append(f"AND {columna_sql} = ANY(%({nombre_filtro})s)")

        return "\n".join(condiciones)

    def _normalizar_filtros(self, filtros: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Normaliza filtros recibidos desde frontend.

        Acepta listas o valores individuales. Devuelve siempre listas limpias.
        """
        if not isinstance(filtros, dict):
            return {}

        campos = {
            "lineas",
            "clientes",
            "ciudades",
            "tipos_portafolio",
            "estados",
        }

        salida: Dict[str, List[str]] = {}

        for campo in campos:
            valor = filtros.get(campo)

            if valor is None or valor == "":
                salida[campo] = []
                continue

            if isinstance(valor, list):
                salida[campo] = [
                    str(item).strip()
                    for item in valor
                    if str(item).strip()
                ]
            else:
                salida[campo] = [str(valor).strip()]

        return salida

    # =========================================================
    # NORMALIZACIÓN DE RESPUESTA
    # =========================================================

    def _normalizar_punto_ciudad(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convierte una fila SQL en un punto de ciudad para el frontend.
        """
        return {
            "ciudad": row.get("ciudad"),
            "departamento": row.get("departamento"),
            "latitud": self._to_float(row.get("latitud")),
            "longitud": self._to_float(row.get("longitud")),
            "valor": self._to_float(row.get("valor")) or 0,
            "tiendas": self._to_int(row.get("tiendas")),
            "participacion_promedio": self._to_float(
                row.get("participacion_promedio")
            ),
        }

    def _to_float(self, value: Any) -> float | None:
        if value is None:
            return None

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _to_int(self, value: Any) -> int:
        if value is None:
            return 0

        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0