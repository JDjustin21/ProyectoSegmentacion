# backend/modules/analiticas/agotados_db_service.py

from __future__ import annotations

from typing import Any, Dict, List

from backend.modules.analiticas.agotados_metricas import construir_dashboard_agotados
from backend.repositories.postgres_repository import PostgresRepository


class AgotadosDbService:
    """
    Servicio de datos para Analíticas de Agotados.

    Responsabilidad:
    - consultar Postgres
    - traer únicamente referencias segmentadas activas
    - entregar un dataset base para calcular KPIs

    Regla de negocio de esta primera versión:
    - solo se analizan referencias segmentadas
    - el grano es referencia_sku + tienda + talla
    - el inventario usado es disponible_talla
    """

    def __init__(
        self,
        repo: PostgresRepository,
        view_tiendas: str,
        view_existencia_talla: str,
    ):
        self._repo = repo
        self._view_tiendas = view_tiendas
        self._view_existencia_talla = view_existencia_talla

    def obtener_dashboard_agotados(self, filtros: Dict[str, Any]) -> Dict[str, Any]:
        filtros_limpios = self._normalizar_filtros(filtros)
        datos = self.obtener_base_agotados_segmentados(filtros_limpios)

        dashboard = construir_dashboard_agotados(
            datos=datos,
            filtros=filtros_limpios,
        )

        return dashboard

    def obtener_base_agotados_segmentados(
        self,
        filtros: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """
        Consulta la base analítica de agotados ya normalizada en PostgreSQL.

        La vista concentra el cruce entre segmentación, detalle, tiendas,
        referencias e inventario. El backend solo aplica filtros y devuelve
        el dataset base para armar la respuesta.
        """
        params: Dict[str, Any] = {}
        where_extra = self._construir_where_filtros(filtros, params)

        sql = f"""
            SELECT
                id_segmentacion,
                referencia_sku,
                referencia_base,
                descripcion,
                categoria,
                linea,
                tipo_portafolio,
                estado_sku,
                cuento,
                tipo_inventario,
                fecha_creacion,
                codigo_color,
                color,
                perfil_prenda,
                fch_act_portafolio,
                clase_agotados,

                id_detalle,
                llave_naval,
                talla,
                codigo_barras,
                cantidad_segmentada,
                fecha_actualizacion,

                cod_bodega,
                cod_dependencia,
                cliente,
                desc_dependencia,
                ciudad,
                zona,
                clima,
                clasificacion,
                testeo,

                disponible_talla,
                disponible_calculado,
                es_agotado,
                estado_agotado,
                disponible_original_nulo
            FROM public.mv_analiticas_agotados_base
            WHERE 1 = 1
            {where_extra}
            ORDER BY
                linea,
                referencia_sku,
                desc_dependencia,
                talla;
        """

        return self._repo.fetch_all(sql, params)

    def _normalizar_filtros(self, filtros: Dict[str, Any]) -> Dict[str, str]:
        """
        Limpia filtros enviados por el frontend.

        No decide reglas de negocio, solo evita espacios y valores raros.
        """
        if not isinstance(filtros, dict):
            return {}

        campos_permitidos = {
            "linea",
            "cuento",
            "referencia_sku",
            "talla",
            "llave_naval",
            "dependencia",
            "cliente",
            "ciudad",
            "zona",
            "clima",
            "clasificacion",
            "testeo",
            "tipo_portafolio",
            "estado_sku",
            "clase_agotados",
        }

        salida: Dict[str, str] = {}

        for campo in campos_permitidos:
            valor = filtros.get(campo)
            if valor is None:
                salida[campo] = ""
            else:
                salida[campo] = str(valor).strip()

        return salida

    def _construir_where_filtros(
        self,
        filtros: Dict[str, str],
        params: Dict[str, Any],
    ) -> str:
        condiciones: List[str] = []

        filtros_exactos = {
            "linea": "linea",
            "cuento": "cuento",
            "referencia_sku": "referencia_sku",
            "talla": "talla",
            "llave_naval": "llave_naval",
            "tipo_portafolio": "tipo_portafolio",
            "estado_sku": "estado_sku",
            "clase_agotados": "clase_agotados",
        }

        for nombre_filtro, columna_sql in filtros_exactos.items():
            valor = filtros.get(nombre_filtro, "")
            if valor:
                condiciones.append(f"AND {columna_sql} = %({nombre_filtro})s")
                params[nombre_filtro] = valor

        filtros_parciales = {
            "cliente": "cliente",
            "dependencia": "desc_dependencia",
            "ciudad": "ciudad",
            "zona": "zona",
            "clima": "clima",
            "clasificacion": "clasificacion",
            "testeo": "testeo",
        }

        for nombre_filtro, columna_sql in filtros_parciales.items():
            valor = filtros.get(nombre_filtro, "")
            if valor:
                condiciones.append(f"AND COALESCE({columna_sql}, '') ILIKE %({nombre_filtro})s")
                params[nombre_filtro] = f"%{valor}%"

        return "\n".join(condiciones)
    
    def refrescar_base_agotados(self) -> Dict[str, Any]:
        """
        Refresca la materialized view de agotados.

        Esta operación puede ser pesada. No debe ejecutarse en cada cambio
        de filtro, solo cuando el usuario quiera recalcular la base analítica.
        """
        sql = """
            REFRESH MATERIALIZED VIEW public.mv_analiticas_agotados_base;
        """

        self._repo.execute(sql, {})

        rows = self._repo.fetch_all(
            """
            SELECT
                COUNT(*) AS total_filas,
                COUNT(*) FILTER (WHERE es_agotado) AS total_agotadas,
                MAX(fecha_actualizacion) AS ultima_fecha_segmentacion
            FROM public.mv_analiticas_agotados_base;
            """,
            {}
        )

        return rows[0] if rows else {
            "total_filas": 0,
            "total_agotadas": 0,
            "ultima_fecha_segmentacion": None,
        }