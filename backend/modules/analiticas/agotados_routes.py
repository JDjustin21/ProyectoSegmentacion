# backend/modules/analiticas/agotados_routes.py

import time
import traceback

from flask import Blueprint, current_app, jsonify, request
from backend.config.settings import (
    POSTGRES_DSN,
    POSTGRES_TIENDAS_VIEW,
    METRICAS_EXISTENCIA_TALLA_VIEW,
)
from backend.modules.auth.decorators import login_required
from backend.modules.analiticas.agotados_db_service import AgotadosDbService
from backend.repositories.postgres_repository import PostgresRepository
from flask import Blueprint, current_app, jsonify, request, render_template


agotados_bp = Blueprint(
    "agotados",
    __name__,
    url_prefix="/analiticas"
)


def _pg_repo() -> PostgresRepository:
    return PostgresRepository(POSTGRES_DSN)


def _svc_agotados(repo: PostgresRepository) -> AgotadosDbService:
    return AgotadosDbService(
        repo=repo,
        view_tiendas=POSTGRES_TIENDAS_VIEW,
        view_existencia_talla=METRICAS_EXISTENCIA_TALLA_VIEW,
    )
@agotados_bp.get("/agotados")
@login_required
def vista_analiticas():
    """
    Vista web del dashboard de analíticas de agotados.

    La lógica de negocio se mantiene en el endpoint API.
    Esta ruta solo renderiza la pantalla.
    """
    return render_template("analiticas.html")

@agotados_bp.post("/api/agotados/dashboard")
@login_required
def api_dashboard_agotados():
    """
    Dashboard de agotados sobre referencias segmentadas.

    Objetivo de negocio:
    - medir qué parte de lo segmentado ya no tiene disponible
      en punto de venta, a nivel referencia SKU + tienda + talla.
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

    except Exception as ex:
        current_app.logger.error(
            "[ANALITICAS][AGOTADOS_DASHBOARD][ERROR] %s\n%s",
            str(ex),
            traceback.format_exc(),
        )

        return jsonify({
            "ok": False,
            "error": "No fue posible calcular el dashboard de agotados."
        }), 500