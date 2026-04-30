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
        Consulta la base analítica de agotados.

        Se toma la última segmentación por referencia_sku y solo sus detalles
        activos con cantidad mayor a cero. Esto responde a la pregunta:
        'De lo que está segmentado actualmente, ¿qué está agotado?'.
        """
        params: Dict[str, Any] = {}

        where_extra = self._construir_where_filtros(filtros, params)

        sql = f"""
            WITH ultimas_segmentaciones AS (
                SELECT DISTINCT ON (s.referencia)
                    s.id_segmentacion,
                    s.referencia AS referencia_sku,
                    s.descripcion,
                    s.categoria,
                    s.linea,
                    s.tipo_portafolio,
                    s.estado_sku,
                    s.cuento,
                    s.tipo_inventario,
                    s.fecha_creacion
                FROM public.segmentacion s
                ORDER BY
                    s.referencia,
                    s.fecha_creacion DESC,
                    s.id_segmentacion DESC
            )
            SELECT
                s.id_segmentacion,
                s.referencia_sku,
                s.descripcion,
                s.categoria,
                s.linea,
                s.tipo_portafolio,
                s.estado_sku,
                s.cuento,
                s.tipo_inventario,

                d.llave_naval,
                d.talla,
                d.codigo_barras,
                d.cantidad AS cantidad_segmentada,
                d.fecha_actualizacion,

                t.cod_bodega,
                t.cod_dependencia,
                t.dependencia,
                t.desc_dependencia,
                t.ciudad,
                t.zona,
                t.clima,
                t.rankin_linea AS clasificacion,
                t.testeo_fnl AS testeo,

                ex.disponible_talla
            FROM ultimas_segmentaciones s
            JOIN public.segmentacion_detalle d
                ON d.id_segmentacion = s.id_segmentacion
            LEFT JOIN public.{self._view_tiendas} t
                ON t.llave_naval = d.llave_naval
            LEFT JOIN public.{self._view_existencia_talla} ex
                ON ex.llave_naval = d.llave_naval
                AND ex.referencia_sku = s.referencia_sku
                AND ex.talla = d.talla
                AND COALESCE(ex.ean, '') = COALESCE(d.codigo_barras, '')
            WHERE COALESCE(d.estado_detalle, 'Activo') = 'Activo'
              AND COALESCE(d.cantidad, 0) > 0
              {where_extra}
            ORDER BY
                s.linea,
                s.referencia_sku,
                d.llave_naval,
                d.talla;
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
            "ciudad",
            "zona",
            "clima",
            "clasificacion",
            "testeo",
            "tipo_portafolio",
            "estado_sku",
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
        """
        Construye condiciones opcionales del dashboard.

        Los filtros vacíos no afectan la consulta.
        Los textos de tienda se tratan como búsqueda parcial para facilitar uso.
        """
        condiciones: List[str] = []

        filtros_exactos = {
            "linea": "s.linea",
            "cuento": "s.cuento",
            "referencia_sku": "s.referencia_sku",
            "talla": "d.talla",
            "llave_naval": "d.llave_naval",
            "tipo_portafolio": "s.tipo_portafolio",
            "estado_sku": "s.estado_sku",
        }

        for nombre_filtro, columna_sql in filtros_exactos.items():
            valor = filtros.get(nombre_filtro, "")
            if valor:
                condiciones.append(f"AND {columna_sql} = %({nombre_filtro})s")
                params[nombre_filtro] = valor

        filtros_parciales = {
            "dependencia": "t.desc_dependencia",
            "ciudad": "t.ciudad",
            "zona": "t.zona",
            "clima": "t.clima",
            "clasificacion": "t.rankin_linea",
            "testeo": "t.testeo_fnl",
        }

        for nombre_filtro, columna_sql in filtros_parciales.items():
            valor = filtros.get(nombre_filtro, "")
            if valor:
                condiciones.append(f"AND COALESCE({columna_sql}, '') ILIKE %({nombre_filtro})s")
                params[nombre_filtro] = f"%{valor}%"

        return "\n".join(condiciones)