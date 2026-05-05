#backend/modules/inventario/inventario_db_service.py

from __future__ import annotations

from typing import Any, Dict, List

from backend.repositories.postgres_repository import PostgresRepository


class InventarioDbService:
    """
        Servicio de datos para el módulo de Inventario.

        Responsabilidades:
        - Consultar KPIs y referencias para el dashboard de inventario.
        - Decidir si la consulta debe ir a una vista resumen o a una vista detalle.
        - Refrescar las vistas materializadas usadas por el módulo.
        - Cargar catálogos de filtros para el frontend.

        Regla de negocio:
        - La pantalla trabaja a nivel referencia_sku.
        - referencias_snapshot_actual define el universo total de referencias.
        - inventario_actual define existencia/disponible por talla, tienda y EAN.
        - mv_inventario_resumen_referencia resume inventario por referencia.
        - mv_inventario_base_detalle permite filtrar por cliente y punto de venta.

        Decisión de rendimiento:
        - Si no hay filtros de tienda, se usa mv_inventario_resumen_referencia.
        - Si hay cliente o punto_venta, se usa mv_inventario_base_detalle.
    """

    def __init__(self, repo: PostgresRepository):
        self._repo = repo

    def obtener_dashboard(self, filtros: Dict[str, Any]) -> Dict[str, Any]:
        """
        Construye la respuesta principal del dashboard de inventario.

        Dependiendo de los filtros recibidos, consulta:
        - mv_inventario_resumen_referencia para consultas generales.
        - mv_inventario_base_detalle cuando se filtra por cliente o punto de venta.

        Retorna:
        - data.kpis: indicadores agregados.
        - data.referencias: listado de referencias con inventario.
        - data.catalogos: valores disponibles para filtros, si se solicitan.
        - meta: información técnica de la consulta.
        """
        filtros_limpios = self._normalizar_filtros(filtros)

        usa_detalle = self._usa_base_detalle(filtros_limpios)

        if usa_detalle:
            referencias = self._obtener_referencias_desde_detalle(filtros_limpios)
            kpis = self._obtener_kpis_desde_detalle(filtros_limpios)
            fuente = "mv_inventario_base_detalle"
        else:
            referencias = self._obtener_referencias_desde_resumen(filtros_limpios)
            kpis = self._obtener_kpis_desde_resumen(filtros_limpios)
            fuente = "mv_inventario_resumen_referencia"

        data = {
            "kpis": kpis,
            "referencias": referencias,
        }

        if filtros_limpios.get("incluir_catalogos") == "true":
            data["catalogos"] = self._obtener_catalogos()

        return {
            "data": data,
            "meta": {
                "total_referencias": int(kpis.get("referencias_totales") or 0),
                "fuente": fuente,
            },
        }
    
    def _usa_base_detalle(self, filtros: Dict[str, str]) -> bool:
        """
        Solo usamos la base detalle cuando realmente se filtra por tienda.
        Si no hay cliente ni punto_venta, la base resumen es mucho más rápida.
        """
        return bool(filtros.get("cliente") or filtros.get("punto_venta"))

    def _obtener_referencias_desde_resumen(self, filtros: Dict[str, str]) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        where_extra = self._construir_where_resumen(filtros, params)

        sql = f"""
            SELECT
                referencia_sku,
                referencia_base,
                descripcion,
                categoria,
                color,
                codigo_color,
                perfil_prenda,
                estado,
                tipo_inventario,
                tipo_portafolio,
                linea,
                cuento,
                precio_unitario,
                fecha_creacion,
                cantidad_tallas,
                tallas,

                existencia_total,
                disponible_total,
                sku_disponibles,
                tallas_disponibles,
                puntos_venta_con_inventario,
                fecha_ultima_actualizacion,
                tiene_inventario,
                estado_inventario,

                CASE
                    WHEN puntos_venta_con_inventario > 0
                    THEN ROUND(existencia_total::numeric / puntos_venta_con_inventario)::integer
                    ELSE 0
                END AS promedio_tienda

            FROM public.mv_inventario_resumen_referencia
            WHERE 1 = 1
            {where_extra}
            ORDER BY
                disponible_total DESC,
                referencia_sku ASC;
        """

        return self._repo.fetch_all(sql, params)
    
    def _obtener_referencias_desde_detalle(self, filtros: Dict[str, str]) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        where_extra = self._construir_where_detalle(filtros, params)

        sql = f"""
            WITH base_filtrada AS (
                SELECT *
                FROM public.mv_inventario_base_detalle
                WHERE 1 = 1
                {where_extra}
            ),
            referencias_agrupadas AS (
                SELECT
                    referencia_sku,
                    referencia_base,
                    descripcion,
                    categoria,
                    color,
                    codigo_color,
                    perfil_prenda,
                    estado,
                    tipo_inventario,
                    tipo_portafolio,
                    linea,
                    cuento,
                    precio_unitario,
                    fecha_creacion,
                    cantidad_tallas,
                    tallas,

                    SUM(COALESCE(existencia, 0))::integer AS existencia_total,
                    SUM(COALESCE(disponible, 0))::integer AS disponible_total,

                    COUNT(DISTINCT sku_key)
                        FILTER (
                            WHERE sku_key IS NOT NULL
                            AND COALESCE(disponible, 0) > 0
                        )::integer AS sku_disponibles,

                    COUNT(DISTINCT UPPER(TRIM(talla)))
                        FILTER (
                            WHERE TRIM(COALESCE(talla, '')) <> ''
                            AND COALESCE(disponible, 0) > 0
                        )::integer AS tallas_disponibles,

                    COUNT(DISTINCT tienda_key)
                        FILTER (
                            WHERE tienda_key IS NOT NULL
                            AND COALESCE(disponible, 0) > 0
                        )::integer AS puntos_venta_con_inventario,

                    MAX(fecha_ultima_actualizacion) AS fecha_ultima_actualizacion

                FROM base_filtrada
                GROUP BY
                    referencia_sku,
                    referencia_base,
                    descripcion,
                    categoria,
                    color,
                    codigo_color,
                    perfil_prenda,
                    estado,
                    tipo_inventario,
                    tipo_portafolio,
                    linea,
                    cuento,
                    precio_unitario,
                    fecha_creacion,
                    cantidad_tallas,
                    tallas
            )
            SELECT
                *,
                CASE
                    WHEN disponible_total > 0 THEN true
                    ELSE false
                END AS tiene_inventario,

                CASE
                    WHEN disponible_total > 0 THEN 'Con inventario'
                    ELSE 'Sin inventario'
                END AS estado_inventario,

                CASE
                    WHEN puntos_venta_con_inventario > 0
                    THEN ROUND(existencia_total::numeric / puntos_venta_con_inventario)::integer
                    ELSE 0
                END AS promedio_tienda

            FROM referencias_agrupadas
            WHERE 1 = 1
            AND (
                    %(solo_con_inventario_sql)s = ''
                    OR disponible_total > 0
                )
            AND (
                    %(solo_sin_inventario_sql)s = ''
                    OR disponible_total <= 0
                )
            ORDER BY
                disponible_total DESC,
                referencia_sku ASC;
        """

        params["solo_con_inventario_sql"] = "1" if filtros.get("solo_con_inventario") == "true" else ""
        params["solo_sin_inventario_sql"] = "1" if filtros.get("solo_sin_inventario") == "true" else ""

        return self._repo.fetch_all(sql, params)


    def _obtener_kpis_desde_resumen(self, filtros: Dict[str, str]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        where_extra = self._construir_where_resumen(filtros, params)

        sql = f"""
            SELECT
                COUNT(*)::integer AS referencias_totales,
                COUNT(*) FILTER (WHERE tiene_inventario)::integer AS referencias_con_inventario,
                COUNT(*) FILTER (WHERE NOT tiene_inventario)::integer AS referencias_sin_inventario,
                COALESCE(SUM(sku_disponibles), 0)::integer AS sku_disponibles,
                COALESCE(SUM(disponible_total), 0)::integer AS disponible_total,
                COALESCE(SUM(existencia_total), 0)::integer AS existencia_total
            FROM public.mv_inventario_resumen_referencia
            WHERE 1 = 1
            {where_extra};
        """

        rows = self._repo.fetch_all(sql, params)
        return rows[0] if rows else self._kpis_vacios()
    
    def _obtener_kpis_desde_detalle(self, filtros: Dict[str, str]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        where_extra = self._construir_where_detalle(filtros, params)

        sql = f"""
            WITH base_filtrada AS (
                SELECT *
                FROM public.mv_inventario_base_detalle
                WHERE 1 = 1
                {where_extra}
            ),
            refs AS (
                SELECT
                    referencia_sku,
                    SUM(COALESCE(existencia, 0)) AS existencia_total,
                    SUM(COALESCE(disponible, 0)) AS disponible_total,
                    COUNT(DISTINCT sku_key)
                        FILTER (
                            WHERE sku_key IS NOT NULL
                            AND COALESCE(disponible, 0) > 0
                        ) AS sku_disponibles
                FROM base_filtrada
                GROUP BY referencia_sku
            ),
            refs_filtradas AS (
                SELECT *
                FROM refs
                WHERE 1 = 1
                AND (
                        %(solo_con_inventario_sql)s = ''
                        OR disponible_total > 0
                    )
                AND (
                        %(solo_sin_inventario_sql)s = ''
                        OR disponible_total <= 0
                    )
            )
            SELECT
                COUNT(*)::integer AS referencias_totales,
                COUNT(*) FILTER (WHERE disponible_total > 0)::integer AS referencias_con_inventario,
                COUNT(*) FILTER (WHERE disponible_total <= 0)::integer AS referencias_sin_inventario,
                COALESCE(SUM(sku_disponibles), 0)::integer AS sku_disponibles,
                COALESCE(SUM(disponible_total), 0)::integer AS disponible_total,
                COALESCE(SUM(existencia_total), 0)::integer AS existencia_total
            FROM refs_filtradas;
        """

        params["solo_con_inventario_sql"] = "1" if filtros.get("solo_con_inventario") == "true" else ""
        params["solo_sin_inventario_sql"] = "1" if filtros.get("solo_sin_inventario") == "true" else ""

        rows = self._repo.fetch_all(sql, params)
        return rows[0] if rows else self._kpis_vacios()
    
    def _kpis_vacios(self) -> Dict[str, Any]:
        return {
            "referencias_totales": 0,
            "referencias_con_inventario": 0,
            "referencias_sin_inventario": 0,
            "sku_disponibles": 0,
            "disponible_total": 0,
            "existencia_total": 0,
        }

    def _normalizar_filtros(self, filtros: Dict[str, Any]) -> Dict[str, str]:
        """
        Limpia y limita los filtros aceptados por el dashboard.

        Esta función evita que el frontend envíe campos inesperados hacia la construcción
        dinámica de condiciones SQL. Los nombres de columnas usados en SQL siguen
        controlados por diccionarios internos, no por el payload del usuario.
        """
        if not isinstance(filtros, dict):
            return {}

        campos_permitidos = {
            "tipo_portafolio",
            "linea",
            "estado",
            "cuento",
            "categoria",
            "referencia_sku",
            "cliente",
            "punto_venta",
            "solo_con_inventario",
            "solo_sin_inventario",
            "incluir_catalogos",
        }

        salida: Dict[str, str] = {}

        for campo in campos_permitidos:
            valor = filtros.get(campo)
            salida[campo] = "" if valor is None else str(valor).strip()

        return salida
    
    def _construir_where_resumen(
        self,
        filtros: Dict[str, str],
        params: Dict[str, Any],
    ) -> str:
        """
        Construye condiciones SQL para la vista resumen.

        La vista resumen no tiene información a nivel tienda, por eso no acepta filtros
        como cliente o punto_venta.
        """
        condiciones: List[str] = []

        filtros_exactos = {
            "tipo_portafolio": "tipo_portafolio",
            "linea": "linea",
            "estado": "estado",
        }

        for nombre_filtro, columna_sql in filtros_exactos.items():
            valor = filtros.get(nombre_filtro, "")
            if valor:
                condiciones.append(f"AND {columna_sql} = %({nombre_filtro})s")
                params[nombre_filtro] = valor

        filtros_parciales = {
            "cuento": "cuento",
            "categoria": "categoria",
            "referencia_sku": "referencia_sku",
        }

        for nombre_filtro, columna_sql in filtros_parciales.items():
            valor = filtros.get(nombre_filtro, "")
            if valor:
                condiciones.append(f"AND COALESCE({columna_sql}, '') ILIKE %({nombre_filtro})s")
                params[nombre_filtro] = f"%{valor}%"

        if filtros.get("solo_con_inventario") == "true":
            condiciones.append("AND tiene_inventario = true")

        if filtros.get("solo_sin_inventario") == "true":
            condiciones.append("AND tiene_inventario = false")

        return "\n".join(condiciones)
    
    def _construir_where_detalle(
        self,
        filtros: Dict[str, str],
        params: Dict[str, Any],
    ) -> str:
        """
            Construye condiciones SQL para la vista detalle.

            Esta ruta se usa cuando el usuario filtra por cliente o punto_venta,
            campos que solo existen a nivel tienda.
        """
        condiciones: List[str] = []

        filtros_exactos = {
            "tipo_portafolio": "tipo_portafolio",
            "linea": "linea",
            "estado": "estado",
        }

        for nombre_filtro, columna_sql in filtros_exactos.items():
            valor = filtros.get(nombre_filtro, "")
            if valor:
                condiciones.append(f"AND {columna_sql} = %({nombre_filtro})s")
                params[nombre_filtro] = valor

        filtros_parciales = {
            "cuento": "cuento",
            "categoria": "categoria",
            "referencia_sku": "referencia_sku",
            "cliente": "cliente",
            "punto_venta": "punto_venta",
        }

        for nombre_filtro, columna_sql in filtros_parciales.items():
            valor = filtros.get(nombre_filtro, "")
            if valor:
                condiciones.append(f"AND COALESCE({columna_sql}, '') ILIKE %({nombre_filtro})s")
                params[nombre_filtro] = f"%{valor}%"

        return "\n".join(condiciones)

    def refrescar_base(self) -> Dict[str, Any]:
        """
        Refresca las vistas materializadas de inventario.

        Debe ejecutarse después del job de inventario o cuando se necesite recalcular
        manualmente la información consultada por el dashboard.
        """
        self._repo.execute(
            "REFRESH MATERIALIZED VIEW public.mv_inventario_base_detalle;",
            {},
        )

        self._repo.execute(
            "REFRESH MATERIALIZED VIEW public.mv_inventario_resumen_referencia;",
            {},
        )

        rows = self._repo.fetch_all(
            """
            SELECT
                COUNT(*)::integer AS referencias_totales,
                COUNT(*) FILTER (WHERE tiene_inventario)::integer AS referencias_con_inventario,
                COUNT(*) FILTER (WHERE NOT tiene_inventario)::integer AS referencias_sin_inventario,
                COALESCE(SUM(sku_disponibles), 0)::integer AS sku_disponibles,
                COALESCE(SUM(disponible_total), 0)::integer AS disponible_total,
                COALESCE(SUM(existencia_total), 0)::integer AS existencia_total
            FROM public.mv_inventario_resumen_referencia;
            """,
            {},
        )

        return rows[0] if rows else self._kpis_vacios()
    
    def _obtener_catalogos(self) -> Dict[str, Any]:
        """
        Obtiene valores disponibles para filtros.
        Se consulta desde la base detalle porque Cliente y Punto de Venta
        existen a nivel tienda, no a nivel referencia agrupada.
        """

        rows = self._repo.fetch_all(
            """
            SELECT
                ARRAY_AGG(DISTINCT tipo_portafolio ORDER BY tipo_portafolio)
                    FILTER (WHERE TRIM(COALESCE(tipo_portafolio, '')) <> '') AS tipos_portafolio,

                ARRAY_AGG(DISTINCT linea ORDER BY linea)
                    FILTER (WHERE TRIM(COALESCE(linea, '')) <> '') AS lineas,

                ARRAY_AGG(DISTINCT estado ORDER BY estado)
                    FILTER (WHERE TRIM(COALESCE(estado, '')) <> '') AS estados,

                ARRAY_AGG(DISTINCT cuento ORDER BY cuento)
                    FILTER (WHERE TRIM(COALESCE(cuento, '')) <> '') AS cuentos,

                ARRAY_AGG(DISTINCT categoria ORDER BY categoria)
                    FILTER (WHERE TRIM(COALESCE(categoria, '')) <> '') AS categorias,

                ARRAY_AGG(DISTINCT referencia_sku ORDER BY referencia_sku)
                    FILTER (WHERE TRIM(COALESCE(referencia_sku, '')) <> '') AS referencias,

                ARRAY_AGG(DISTINCT cliente ORDER BY cliente)
                    FILTER (
                        WHERE TRIM(COALESCE(cliente, '')) <> ''
                        AND cliente <> 'Sin cliente'
                    ) AS clientes,

                ARRAY_AGG(DISTINCT punto_venta ORDER BY punto_venta)
                    FILTER (
                        WHERE TRIM(COALESCE(punto_venta, '')) <> ''
                        AND punto_venta <> 'Sin punto de venta'
                    ) AS puntos_venta

            FROM public.mv_inventario_base_detalle;
            """,
            {},
        )

        if not rows:
            return {}

        row = rows[0]

        return {
            "tipos_portafolio": row.get("tipos_portafolio") or [],
            "lineas": row.get("lineas") or [],
            "estados": row.get("estados") or [],
            "cuentos": row.get("cuentos") or [],
            "categorias": row.get("categorias") or [],
            "referencias": row.get("referencias") or [],
            "clientes": row.get("clientes") or [],
            "puntos_venta": row.get("puntos_venta") or [],
        }