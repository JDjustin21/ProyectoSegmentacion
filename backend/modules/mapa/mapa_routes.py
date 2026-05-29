# backend/modules/mapa/mapa_routes.py

import time

from flask import Blueprint, current_app, jsonify, render_template, request

from backend.config.settings import POSTGRES_DSN
from backend.modules.auth.decorators import login_required
from backend.modules.mapa.mapa_db_service import MapaDbService
from backend.repositories.postgres_repository import PostgresRepository


mapa_bp = Blueprint(
    "mapa",
    __name__,
    url_prefix="/mapa",
)


def _pg_repo() -> PostgresRepository:
    """
    Crea el repositorio PostgreSQL usado por el módulo de Mapa.
    """
    return PostgresRepository(POSTGRES_DSN)


def _svc_mapa(repo: PostgresRepository) -> MapaDbService:
    """
    Construye el servicio de datos del mapa.

    El mapa trabaja a nivel ciudad, no a nivel coordenada de tienda.
    Por eso cruza ventas, inventario o segmentación contra una vista geográfica
    normalizada por ciudad.

    Nota de mantenimiento:
    si las vistas cambian de nombre entre ambientes, estos nombres deberían
    moverse a backend/config/settings.py.
    """
    return MapaDbService(
        repo=repo,
        view_ciudades_geo="vw_maestra_ciudades_geo_norm",
        view_tiendas="vw_maestra_tiendas_activa_norm",
        view_ventas_mapa="vw_mapa_ventas_mensual_base",
        view_inventario_tienda="vw_inventario_tienda_rotacion",
        view_inventario_referencia="vw_inventario_resumen_referencia",
    )


@mapa_bp.get("/")
@login_required
def vista_mapa():
    """
    Renderiza la pantalla principal del mapa de calor.
    """
    return render_template("mapa.html")


@mapa_bp.get("/api/filtros")
@login_required
def api_filtros_mapa():
    """
    Retorna los catálogos disponibles para los filtros del mapa.
    """
    try:
        repo = _pg_repo()
        svc = _svc_mapa(repo)

        filtros = svc.obtener_filtros()

        return jsonify({
            "ok": True,
            "data": filtros,
        })

    except Exception:
        current_app.logger.exception("[MAPA][FILTROS][ERROR]")

        return jsonify({
            "ok": False,
            "error": "No fue posible consultar los filtros del mapa.",
        }), 500


@mapa_bp.post("/api/datos")
@login_required
def api_datos_mapa():
    """
    Retorna puntos geográficos y tabla analítica del mapa.

    Tipos soportados:
    - ventas: unidades vendidas por ciudad.
    - inventario: existencia por ciudad.
    - segmentacion: cantidad segmentada por ciudad.
    """
    payload = request.get_json(silent=True) or {}

    tipo = (payload.get("tipo") or "ventas").strip().lower()
    filtros = payload.get("filtros") or {}

    anio = payload.get("anio")
    mes = payload.get("mes")

    t0 = time.perf_counter()

    try:
        repo = _pg_repo()
        svc = _svc_mapa(repo)

        result = svc.obtener_datos_mapa(
            tipo=tipo,
            filtros=filtros,
            anio=anio,
            mes=mes,
        )

        t1 = time.perf_counter()
        current_app.logger.info(
            "[MAPA][DATOS] tipo=%s total_ms=%.2f ciudades=%s filas_tabla=%s",
            tipo,
            (t1 - t0) * 1000,
            result.get("meta", {}).get("total_ciudades", 0),
            result.get("meta", {}).get("total_filas_tabla", 0),
        )

        return jsonify({
            "ok": True,
            "data": result.get("data", {}),
            "meta": result.get("meta", {}),
        })

    except ValueError as exc:
        return jsonify({
            "ok": False,
            "error": str(exc),
        }), 400

    except Exception:
        current_app.logger.exception("[MAPA][DATOS][ERROR]")

        return jsonify({
            "ok": False,
            "error": "No fue posible consultar los datos del mapa.",
        }), 500