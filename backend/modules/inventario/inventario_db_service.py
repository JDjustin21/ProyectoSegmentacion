from __future__ import annotations

from typing import Any, Dict, List

from backend.repositories.postgres_repository import PostgresRepository


class InventarioDbService:
    """
    Servicio de datos para el módulo de Inventario.

    Regla de negocio:
    - La pantalla trabaja a nivel referencia_sku.
    - referencias_snapshot_actual define el universo total de referencias.
    - inventario_actual define existencia/disponible por talla, tienda y EAN.
    - La materialized view mv_inventario_resumen_referencia resume el inventario por referencia.
    """

    def __init__(self, repo: PostgresRepository):
        self._repo = repo

    def obtener_dashboard(self, filtros: Dict[str, Any]) -> Dict[str, Any]:
        filtros_limpios = self._normalizar_filtros(filtros)

        referencias = self._obtener_referencias(filtros_limpios)
        kpis = self._calcular_kpis(referencias)

        return {
            "data": {
                "kpis": kpis,
                "referencias": referencias,
            },
            "meta": {
                "total_referencias": len(referencias),
                "fuente": "mv_inventario_resumen_referencia",
            },
        }

    def _obtener_referencias(self, filtros: Dict[str, str]) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        where_extra = self._construir_where_filtros(filtros, params)

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
                estado_inventario
            FROM public.mv_inventario_resumen_referencia
            WHERE 1 = 1
            {where_extra}
            ORDER BY
                tiene_inventario DESC,
                disponible_total DESC,
                referencia_sku ASC;
        """

        return self._repo.fetch_all(sql, params)

    def _calcular_kpis(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        referencias_totales = len(rows)
        referencias_con_inventario = 0
        referencias_sin_inventario = 0
        sku_disponibles = 0
        disponible_total = 0
        existencia_total = 0

        for row in rows:
            tiene_inventario = bool(row.get("tiene_inventario"))
            disponible = int(row.get("disponible_total") or 0)
            existencia = int(row.get("existencia_total") or 0)

            if tiene_inventario:
                referencias_con_inventario += 1
            else:
                referencias_sin_inventario += 1

            sku_disponibles += int(row.get("sku_disponibles") or 0)
            disponible_total += disponible
            existencia_total += existencia

        return {
            "referencias_totales": referencias_totales,
            "referencias_con_inventario": referencias_con_inventario,
            "referencias_sin_inventario": referencias_sin_inventario,
            "sku_disponibles": sku_disponibles,
            "disponible_total": disponible_total,
            "existencia_total": existencia_total,
        }

    def _normalizar_filtros(self, filtros: Dict[str, Any]) -> Dict[str, str]:
        if not isinstance(filtros, dict):
            return {}

        campos_permitidos = {
            "tipo_portafolio",
            "linea",
            "estado",
            "cuento",
            "categoria",
            "referencia_sku",
            "solo_con_inventario",
            "solo_sin_inventario",
        }

        salida: Dict[str, str] = {}

        for campo in campos_permitidos:
            valor = filtros.get(campo)
            salida[campo] = "" if valor is None else str(valor).strip()

        return salida

    def _construir_where_filtros(
        self,
        filtros: Dict[str, str],
        params: Dict[str, Any],
    ) -> str:
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

    def refrescar_base(self) -> Dict[str, Any]:
        """
        Refresca la materialized view de inventario.
        Debe usarse después de correr el job de inventario o cuando se quiera recalcular la base.
        """
        self._repo.execute(
            "REFRESH MATERIALIZED VIEW public.mv_inventario_resumen_referencia;",
            {},
        )

        rows = self._repo.fetch_all(
            """
            SELECT
                COUNT(*) AS referencias_totales,
                COUNT(*) FILTER (WHERE tiene_inventario) AS referencias_con_inventario,
                COUNT(*) FILTER (WHERE NOT tiene_inventario) AS referencias_sin_inventario,
                SUM(sku_disponibles) AS sku_disponibles,
                SUM(disponible_total) AS disponible_total,
                SUM(existencia_total) AS existencia_total,
                MAX(fecha_ultima_actualizacion) AS fecha_ultima_actualizacion
            FROM public.mv_inventario_resumen_referencia;
            """,
            {},
        )

        return rows[0] if rows else {}