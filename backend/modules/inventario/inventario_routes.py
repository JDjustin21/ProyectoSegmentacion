# backend/modules/inventario/inventario_routes.py

import time

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
    """
    Crea una instancia del repositorio PostgreSQL.

    Se define como helper local para mantener las rutas delgadas y evitar repetir
    la creación del repositorio en cada endpoint.
    """
    return PostgresRepository(POSTGRES_DSN)


def _svc_inventario(repo: PostgresRepository) -> InventarioDbService:
    """
    Construye el servicio de inventario con sus dependencias.
    """
    return InventarioDbService(repo=repo)


@inventario_bp.get("")
@login_required
def vista_inventario():
    """
    Renderiza la pantalla principal del módulo de Inventario.

    La información del dashboard se carga desde el frontend mediante llamadas
    AJAX a los endpoints del módulo.
    """
    return render_template("inventario.html")


@inventario_bp.post("/api/dashboard")
@login_required
def api_dashboard_inventario():
    """
    Retorna los datos del dashboard de inventario.

    Body JSON:
    - filtros comerciales: línea, categoría, cuento, referencia_sku, estado, etc.
    - filtros de tienda: cliente y punto_venta.
    - incluir_catalogos: indica si deben enviarse catálogos para filtros.

    El servicio decide internamente si consulta la vista resumen o la vista detalle.
    """
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

    except Exception:
        current_app.logger.exception("[INVENTARIO][DASHBOARD][ERROR]")

        return jsonify({
            "ok": False,
            "error": "No fue posible cargar el dashboard de inventario.",
        }), 500


@inventario_bp.post("/api/refrescar-base")
@login_required
def api_refrescar_base_inventario():
    """
    Refresca las vistas materializadas usadas por el módulo de Inventario.

    Este endpoint debe ejecutarse después del job de inventario o cuando se requiera
    recalcular manualmente la base consultada por el dashboard.
    """
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

    except Exception:
        current_app.logger.exception("[INVENTARIO][REFRESH_BASE][ERROR]")

        return jsonify({
            "ok": False,
            "error": "No fue posible refrescar la base de inventario.",
        }), 500