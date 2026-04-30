# backend/modules/analiticas/agotados_metricas.py

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, List, Optional


def construir_dashboard_agotados(
    datos: List[Dict[str, Any]],
    filtros: Dict[str, str],
) -> Dict[str, Any]:
    """
    Construye la respuesta del dashboard de agotados.

    La intención es separar el cálculo analítico del acceso a datos.
    Esto facilita probar la lógica sin depender directamente de Postgres.
    """
    datos = datos or []

    return {
        "data": {
            "kpis": _calcular_kpis(datos),
            "por_linea": _agrupar_por(datos, "linea"),
            "por_cuento": _agrupar_por(datos, "cuento"),
            "por_referencia_sku": _agrupar_por(datos, "referencia_sku"),
            "por_talla": _agrupar_por(datos, "talla"),
            "por_zona": _agrupar_por(datos, "zona"),
            "por_clasificacion": _agrupar_por(datos, "clasificacion"),
            "por_tienda": _agrupar_por(datos, "desc_dependencia"),
            "referencias_con_agotados": _referencias_con_agotados(datos),
            "detalle": _detalle_limitado(datos, limite=500),
        },
        "meta": {
            "filtros_aplicados": filtros,
            "registros_base": len(datos),
            "grano": "referencia_sku_tienda_talla",
            "inventario_usado": "disponible_talla",
            "regla_agotado": "Para referencias segmentadas, disponible_talla NULL se interpreta como 0; agotado si disponible_calculado <= 0",
        }
    }


def _to_float(valor: Any) -> Optional[float]:
    if valor is None:
        return None

    if isinstance(valor, Decimal):
        return float(valor)

    try:
        return float(valor)
    except (TypeError, ValueError):
        return None


def _disponible_para_agotado(fila: Dict[str, Any]) -> float:
    disponible_calculado = _to_float(fila.get("disponible_calculado"))

    if disponible_calculado is not None:
        return disponible_calculado

    disponible = _to_float(fila.get("disponible_talla"))

    if disponible is None:
        return 0.0

    return disponible


def _es_agotado(fila: Dict[str, Any]) -> bool:
    if "es_agotado" in fila and fila.get("es_agotado") is not None:
        return bool(fila.get("es_agotado"))

    return _disponible_para_agotado(fila) <= 0


def _tiene_dato_inventario(fila: Dict[str, Any]) -> bool:
    """
    Indica si la fila sí cruzó con la vista de inventario.

    No cambia la regla de agotado; solo sirve para diagnóstico.
    """
    return _to_float(fila.get("disponible_talla")) is not None


def _estado_agotado(fila: Dict[str, Any]) -> str:
    if _es_agotado(fila):
        return "Agotado"

    return "Con inventario"


def _porcentaje(parte: int, total: int) -> float:
    if total <= 0:
        return 0.0

    return round((parte * 100) / total, 2)


def _calcular_kpis(datos: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calcula KPIs sobre referencias segmentadas.

    Regla:
    - disponible_talla NULL se interpreta como 0.
    - Agotado si disponible calculado <= 0.
    """
    agotados = [fila for fila in datos if _es_agotado(fila)]

    filas_con_dato_inventario = [
        fila for fila in datos
        if _tiene_dato_inventario(fila)
    ]

    filas_con_disponible_nulo = [
        fila for fila in datos
        if not _tiene_dato_inventario(fila)
    ]

    referencias = {
        fila.get("referencia_sku")
        for fila in datos
        if fila.get("referencia_sku")
    }
    referencias_unicas = {
        fila.get("referencia_sku")
        for fila in datos
        if fila.get("referencia_sku")
    }

    total_referencias = len(referencias_unicas)

    referencias_con_agotado = {
        fila.get("referencia_sku")
        for fila in agotados
        if fila.get("referencia_sku")
    }

    tiendas = {
        fila.get("llave_naval")
        for fila in datos
        if fila.get("llave_naval")
    }

    tiendas_con_agotado = {
        fila.get("llave_naval")
        for fila in agotados
        if fila.get("llave_naval")
    }

    return {
        "total_puntos_venta": len(tiendas),
        "total_puntos_venta_con_agotado": len(tiendas_con_agotado),

        "total_referencias_sku": len(referencias),
        "total_referencias_sku_con_agotado": len(referencias_con_agotado),

        "total_tallas_segmentadas": len(datos),
        "total_tallas_con_dato_inventario": len(filas_con_dato_inventario),
        "total_tallas_con_disponible_nulo": len(filas_con_disponible_nulo),

        "total_tallas_agotadas": len(agotados),

        "porcentaje_agotado_tallas": _porcentaje(len(agotados), len(datos)),
        "porcentaje_referencias_con_agotado": _porcentaje(
            len(referencias_con_agotado),
            len(referencias),
        ),
    }


def _agrupar_por(datos: List[Dict[str, Any]], campo: str) -> List[Dict[str, Any]]:
    """
    Agrupa las filas segmentadas por un campo específico.

    Regla de negocio:
    - disponible_talla NULL se interpreta como disponible 0.
    - disponible_calculado <= 0 cuenta como agotado.
    - total_con_disponible_nulo no significa error; solo indica que
      el inventario no tenía fila y por negocio se tomó como 0.
    """
    grupos = defaultdict(lambda: {
        "total_segmentado": 0,
        "total_con_dato_inventario": 0,
        "total_con_disponible_nulo": 0,
        "total_agotado": 0,
    })

    for fila in datos:
        clave = fila.get(campo) or "Sin clasificar"

        grupos[clave]["total_segmentado"] += 1

        if _tiene_dato_inventario(fila):
            grupos[clave]["total_con_dato_inventario"] += 1
        else:
            grupos[clave]["total_con_disponible_nulo"] += 1

        if _es_agotado(fila):
            grupos[clave]["total_agotado"] += 1

    salida = []

    for clave, valores in grupos.items():
        total_segmentado = valores["total_segmentado"]
        total_agotado = valores["total_agotado"]

        salida.append({
            campo: clave,
            "total_segmentado": total_segmentado,
            "total_con_dato_inventario": valores["total_con_dato_inventario"],
            "total_con_disponible_nulo": valores["total_con_disponible_nulo"],
            "total_agotado": total_agotado,
            "porcentaje_agotado": _porcentaje(total_agotado, total_segmentado),
        })

    return sorted(
        salida,
        key=lambda x: (x["total_agotado"], x["porcentaje_agotado"]),
        reverse=True,
    )

def _referencias_con_agotados(
    datos: List[Dict[str, Any]],
    limite: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Agrupa el dashboard a nivel de referencia SKU.

    Esta tabla no representa un top limitado por defecto.
    Su objetivo es mostrar el resumen por referencia:
    - Referencia
    - Descripción
    - Color
    - Cantidad segmentada
    - Cantidad agotada
    - Porcentaje agotado

    La cantidad agotada se calcula sobre la cantidad segmentada,
    no sobre el número de filas.
    """
    grupos = defaultdict(lambda: {
        "referencia_sku": "",
        "descripcion": "",
        "color": "",
        "codigo_color": "",
        "cantidad_segmentada": 0,
        "cantidad_agotada": 0,
        "total_filas": 0,
        "total_filas_agotadas": 0,
    })

    for fila in datos:
        referencia_sku = fila.get("referencia_sku") or "Sin referencia"
        cantidad = int(fila.get("cantidad_segmentada") or 0)

        grupo = grupos[referencia_sku]

        grupo["referencia_sku"] = referencia_sku
        grupo["descripcion"] = fila.get("descripcion") or ""
        grupo["color"] = fila.get("color") or ""
        grupo["codigo_color"] = fila.get("codigo_color") or ""

        grupo["cantidad_segmentada"] += cantidad
        grupo["total_filas"] += 1

        if _es_agotado(fila):
            grupo["cantidad_agotada"] += cantidad
            grupo["total_filas_agotadas"] += 1

    salida = []

    for item in grupos.values():
        item["porcentaje_agotado"] = _porcentaje(
            item["cantidad_agotada"],
            item["cantidad_segmentada"],
        )
        salida.append(item)

    salida = sorted(
        salida,
        key=lambda x: (x["cantidad_agotada"], x["porcentaje_agotado"]),
        reverse=True,
    )

    if limite is not None:
        return salida[:limite]

    return salida


def _detalle_limitado(
    datos: List[Dict[str, Any]],
    limite: int = 500,
) -> List[Dict[str, Any]]:
    detalle = []

    for fila in datos[:limite]:
        fila_out = dict(fila)

        fila_out["disponible_calculado"] = _disponible_para_agotado(fila)
        fila_out["disponible_original_nulo"] = not _tiene_dato_inventario(fila)
        fila_out["es_agotado"] = _es_agotado(fila)
        fila_out["estado_agotado"] = _estado_agotado(fila)

        detalle.append(fila_out)

    return detalle