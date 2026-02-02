# backend/modules/segmentacion/routes.py
"""
Módulo de rutas de Segmentación (Flask Blueprint).

Responsabilidad:
- Renderizar la vista principal (cards + filtros) consumiendo la API de SQL Server.
- Exponer endpoints JSON para el modal (tiendas activas, última segmentación, guardar segmentación).
- Exportar CSV SOLO de lo modificado en un rango de tiempo (por defecto "hoy" en America/Bogota).

Notas de arquitectura:
- SQL Server: solo lectura, vía API .NET (passthrough).
- Postgres: base operativa del aplicativo (tiendas versionadas, segmentaciones, etc).
"""

from flask import Blueprint, render_template, request, jsonify, send_file

import csv
import io
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from config.settings import (
    SQLSERVER_API_URL,
    SEGMENTACION_CARDS_PER_PAGE,
    IMAGES_BASE_URL,
    POSTGRES_DSN,
    POSTGRES_TIENDAS_VIEW,
    DEFAULT_TALLAS_MVP,
    LINEAS_TALLAS_FIJAS,
)

from modules.segmentacion.services import SegmentacionService
from repositories.postgres_repository import PostgresRepository
from modules.segmentacion.segmentacion_db_service import SegmentacionDbService


segmentacion_bp = Blueprint(
    "segmentacion",
    __name__,
    url_prefix="/segmentacion"
)


@segmentacion_bp.route("/", methods=["GET"])
def vista_segmentacion():
    """
    Vista principal:
    - Obtiene referencias desde la API de SQL Server (.NET)
    - Renderiza HTML con el dataset embebido (script JSON)
    - Pasa configuración al frontend vía data-attributes (sin hardcode en JS)
    """
    servicio = SegmentacionService(SQLSERVER_API_URL)
    referencias = servicio.obtener_referencias()

    return render_template(
        "segmentacion.html",
        referencias=referencias,
        cards_per_page=SEGMENTACION_CARDS_PER_PAGE,
        images_base_url=IMAGES_BASE_URL,
        # Configuración para tallas/fallback en frontend
        default_tallas_mvp=DEFAULT_TALLAS_MVP,
        lineas_tallas_fijas=LINEAS_TALLAS_FIJAS,
    )


@segmentacion_bp.get("/api/tiendas/activas")
def api_tiendas_activas():
    """
    Retorna tiendas activas para una línea comercial.

    Query params:
    - linea (obligatorio): viene desde el SKU (ej: "12 - Hombre Exterior")
    - zona/ciudad/clima (opcionales): filtros para el listado del modal

    Flujo:
    - Normalización de línea se realiza en el servicio (SegmentacionDbService),
      usando la vista Postgres que ya tiene `linea_norm`.
    """
    linea = (request.args.get("linea") or "").strip()
    if not linea:
        return jsonify({"ok": False, "error": "Falta query param: linea"}), 400

    dependencia = (request.args.get("dependencia") or "").strip()
    zona = (request.args.get("zona") or "").strip()
    ciudad = (request.args.get("ciudad") or "").strip()
    clima = (request.args.get("clima") or "").strip()
    testeo = (request.args.get("testeo") or "").strip()
    clasificacion = (request.args.get("clasificacion") or "").strip()

    repo = PostgresRepository(POSTGRES_DSN)
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    data = svc.tiendas_activas_por_linea(
        linea,
        zona=zona,
        ciudad=ciudad,
        clima=clima,
        dependencia=dependencia,
        testeo=testeo,
        clasificacion=clasificacion,
    )
    return jsonify({"ok": True, "data": data})


@segmentacion_bp.get("/api/segmentaciones/ultima")
def api_ultima_segmentacion():
    """
    Retorna la última segmentación guardada para una referenciaSku.

    Query params:
    - referenciaSku (obligatorio): ejemplo "103834-01 | 857"
    """
    referencia_sku = (request.args.get("referenciaSku") or "").strip()
    if not referencia_sku:
        return jsonify({"ok": False, "error": "Falta query param: referenciaSku"}), 400

    repo = PostgresRepository(POSTGRES_DSN)
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    data = svc.ultima_segmentacion(referencia_sku)
    return jsonify({"ok": True, "data": data})


@segmentacion_bp.post("/api/segmentaciones")
def api_guardar_segmentacion():
    """
    Guarda una segmentación en Postgres.

    Body JSON:
    - Cabecera: referenciaSku, linea, categoria, etc.
    - detalle: lista de {llave_naval, talla, cantidad}

    MVP:
    - Crea una nueva cabecera cada vez.
    - Guarda solo tallas con cantidad > 0.
    """
    payload = request.get_json(silent=True) or {}

    repo = PostgresRepository(POSTGRES_DSN)
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    result = svc.guardar_segmentacion(payload)
    return jsonify(result)


@segmentacion_bp.get("/api/export/csv")
def api_export_csv():
    """
    Exporta CSV SOLO de lo modificado en un rango de tiempo, usando
    segmentacion_detalle.fecha_actualizacion como criterio.

    Soporta:
    - ?fecha=YYYY-MM-DD  -> exporta ese día completo (00:00 a 00:00 del día siguiente)
    - ?desde=ISO&hasta=ISO -> exporta rango exacto
    Default: hoy (America/Bogota)
    """
    tz = ZoneInfo("America/Bogota")

    fecha_str = (request.args.get("fecha") or "").strip()
    desde_str = (request.args.get("desde") or "").strip()
    hasta_str = (request.args.get("hasta") or "").strip()

    if desde_str and hasta_str:
        # Rango explícito (cliente define "en este momento")
        desde = datetime.fromisoformat(desde_str).astimezone(tz)
        hasta = datetime.fromisoformat(hasta_str).astimezone(tz)
    else:
        # Día completo (por defecto hoy)
        if fecha_str:
            f = date.fromisoformat(fecha_str)
        else:
            f = datetime.now(tz).date()

        desde = datetime(f.year, f.month, f.day, 0, 0, 0, tzinfo=tz)
        hasta = desde + timedelta(days=1)

    repo = PostgresRepository(POSTGRES_DSN)
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    rows = svc.export_dataset_por_rango(desde, hasta)

    # Construcción CSV en memoria (UTF-8)
    headers = [
        "fecha_actualizacion", "id_segmentacion",
        "referenciaSku", "descripcion", "categoria", "linea",
        "tipo_portafolio", "estado_sku", "cuento",
        "llave_naval", "cod_bodega", "cod_dependencia", "dependencia",
        "desc_dependencia", "rankin_linea", "testeo",
        "ciudad", "zona", "clima",
        "talla", "cantidad"
    ]

    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()

    for r in rows:
        writer.writerow({
            "fecha_actualizacion": r.get("fecha_actualizacion"),
            "id_segmentacion": r.get("id_segmentacion"),
            "referenciaSku": r.get("referencia"),
            "descripcion": r.get("descripcion"),
            "categoria": r.get("categoria"),
            "linea": r.get("linea"),
            "tipo_portafolio": r.get("tipo_portafolio"),
            "estado_sku": r.get("estado_sku"),
            "cuento": r.get("cuento"),
            "llave_naval": r.get("llave_naval"),
            "cod_bodega": r.get("cod_bodega"),
            "cod_dependencia": r.get("cod_dependencia"),
            "dependencia": r.get("dependencia"),
            "desc_dependencia": r.get("desc_dependencia"),
            "rankin_linea": r.get("rankin_linea"),
            "testeo": r.get("testeo"),
            "ciudad": r.get("ciudad"),
            "zona": r.get("zona"),
            "clima": r.get("clima"),
            "talla": r.get("talla"),
            "cantidad": r.get("cantidad"),
        })

    data_bytes = sio.getvalue().encode("utf-8")
    file_obj = io.BytesIO(data_bytes)

    filename = f"segmentacion_{desde.date().isoformat()}.csv"
    return send_file(file_obj, as_attachment=True, download_name=filename, mimetype="text/csv")
