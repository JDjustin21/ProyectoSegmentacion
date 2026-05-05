# backend/modules/analiticas/agotados_routes.py

import time

from flask import Blueprint, current_app, jsonify, render_template, request

from backend.config.settings import (
    METRICAS_EXISTENCIA_TALLA_VIEW,
    POSTGRES_DSN,
    POSTGRES_TIENDAS_VIEW,
)
from backend.modules.analiticas.agotados_db_service import AgotadosDbService
from backend.modules.auth.decorators import login_required
from backend.repositories.postgres_repository import PostgresRepository


agotados_bp = Blueprint(
    "agotados",
    __name__,
    url_prefix="/analiticas",
)


def _pg_repo() -> PostgresRepository:
    """
    Crea una instancia del repositorio PostgreSQL para el módulo de Analíticas.
    """
    return PostgresRepository(POSTGRES_DSN)


def _svc_agotados(repo: PostgresRepository) -> AgotadosDbService:
    """
    Construye el servicio de agotados con las vistas configuradas del proyecto.
    """
    return AgotadosDbService(
        repo=repo,
        view_tiendas=POSTGRES_TIENDAS_VIEW,
        view_existencia_talla=METRICAS_EXISTENCIA_TALLA_VIEW,
    )


@agotados_bp.get("/agotados")
@login_required
def vista_analiticas():
    """
    Renderiza la pantalla del dashboard de agotados.

    La vista HTML no calcula métricas. El frontend carga la información
    mediante llamadas AJAX a los endpoints JSON del módulo.
    """
    return render_template("analiticas.html")


@agotados_bp.post("/api/agotados/dashboard")
@login_required
def api_dashboard_agotados():
    """
    Calcula el dashboard de agotados sobre referencias segmentadas.

    El análisis trabaja a nivel referencia SKU + tienda + talla.
    La base analítica ya viene normalizada desde PostgreSQL y el servicio
    aplica los filtros enviados por el frontend.
    """
    payload = request.get_json(silent=True) or {}
    t0 = time.perf_counter()

    try:
        repo = _pg_repo()
        svc = _svc_agotados(repo)

        result = svc.obtener_dashboard_agotados(payload)

        t1 = time.perf_counter()
        current_app.logger.info(
            "[ANALITICAS][AGOTADOS_DASHBOARD] total_ms=%.2f registros=%s",
            (t1 - t0) * 1000,
            result.get("meta", {}).get("registros_base", 0),
        )

        return jsonify({
            "ok": True,
            "data": result.get("data", {}),
            "meta": result.get("meta", {}),
        })

    except Exception:
        current_app.logger.exception("[ANALITICAS][AGOTADOS_DASHBOARD][ERROR]")

        return jsonify({
            "ok": False,
            "error": "No fue posible calcular el dashboard de agotados.",
        }), 500


@agotados_bp.post("/api/agotados/refrescar-base")
@login_required
def api_refrescar_base_agotados():
    """
    Refresca la materialized view usada por el dashboard de agotados.

    Esta operación recalcula la base analítica completa. No debe ejecutarse
    en cada cambio de filtro, sino cuando se requiera actualizar la base.
    """
    t0 = time.perf_counter()

    try:
        repo = _pg_repo()
        svc = _svc_agotados(repo)

        result = svc.refrescar_base_agotados()

        t1 = time.perf_counter()
        current_app.logger.info(
            "[ANALITICAS][AGOTADOS_REFRESH_BASE] total_ms=%.2f filas=%s",
            (t1 - t0) * 1000,
            result.get("total_filas", 0),
        )

        return jsonify({
            "ok": True,
            "data": result,
        })

    except Exception:
        current_app.logger.exception("[ANALITICAS][AGOTADOS_REFRESH_BASE][ERROR]")

        return jsonify({
            "ok": False,
            "error": "No fue posible refrescar la base de agotados.",
        }), 500