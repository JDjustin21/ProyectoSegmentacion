import time
import traceback

from flask import Blueprint, current_app, jsonify, render_template, request

from backend.config.settings import POSTGRES_DSN
from backend.modules.auth.decorators import login_required
from backend.modules.inventario.inventario_db_service import InventarioDbService
from backend.repositories.postgres_repository import PostgresRepository


inventario_bp = Blueprint(
    "inventario",
    __name__,
    url_prefix="/inventario",
)


def _pg_repo() -> PostgresRepository:
    return PostgresRepository(POSTGRES_DSN)


def _svc_inventario(repo: PostgresRepository) -> InventarioDbService:
    return InventarioDbService(repo=repo)


@inventario_bp.get("")
@login_required
def vista_inventario():
    return render_template("inventario.html")


@inventario_bp.post("/api/dashboard")
@login_required
def api_dashboard_inventario():
    payload = request.get_json(silent=True) or {}
    t0 = time.perf_counter()

    try:
        repo = _pg_repo()
        svc = _svc_inventario(repo)

        result = svc.obtener_dashboard(payload)

        t1 = time.perf_counter()
        current_app.logger.info(
            "[INVENTARIO][DASHBOARD] total_ms=%.2f referencias=%s",
            (t1 - t0) * 1000,
            result.get("meta", {}).get("total_referencias", 0),
        )

        return jsonify({
            "ok": True,
            "data": result.get("data", {}),
            "meta": result.get("meta", {}),
        })

    except Exception as ex:
        current_app.logger.error(
            "[INVENTARIO][DASHBOARD][ERROR] %s\n%s",
            str(ex),
            traceback.format_exc(),
        )

        return jsonify({
            "ok": False,
            "error": "No fue posible cargar el dashboard de inventario.",
            "detalle": str(ex),
        }), 500


@inventario_bp.post("/api/refrescar-base")
@login_required
def api_refrescar_base_inventario():
    t0 = time.perf_counter()

    try:
        repo = _pg_repo()
        svc = _svc_inventario(repo)

        result = svc.refrescar_base()

        t1 = time.perf_counter()
        current_app.logger.info(
            "[INVENTARIO][REFRESH_BASE] total_ms=%.2f",
            (t1 - t0) * 1000,
        )

        return jsonify({
            "ok": True,
            "data": result,
        })

    except Exception as ex:
        current_app.logger.error(
            "[INVENTARIO][REFRESH_BASE][ERROR] %s\n%s",
            str(ex),
            traceback.format_exc(),
        )

        return jsonify({
            "ok": False,
            "error": "No fue posible refrescar la base de inventario.",
            "detalle": str(ex),
        }), 500