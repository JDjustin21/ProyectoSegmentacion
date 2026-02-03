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

import os
import re
import time
import unicodedata
from pathlib import Path
from flask import current_app, request, abort, send_from_directory

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


_IMAGES_INDEX = {
    "ts": 0.0,
    "by_key": {}  # key_normalizada -> filename_real
}

_ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".webp"}


def _strip_accents(text: str) -> str:
    # convierte áéíóú -> aeiou (y similares)
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def normalizar_referencia(raw: str) -> str:
    """
    Normaliza una referencia tipo:
    "105174-00 | 628" -> "10517400628"
    """
    v = (raw or "").strip().upper()
    v = _strip_accents(v)
    # deja solo letras y números
    v = re.sub(r"[^A-Z0-9]+", "", v)
    return v

def _images_dir() -> Path:
    # usa el static_folder real de Flask (sin adivinar rutas)
    return Path(current_app.static_folder) / "assets" / "images" / "referencias"


def _rebuild_index_if_needed(force: bool = False, ttl_seconds: int = 60) -> None:
    """
    Reconstruye el índice cada 'ttl_seconds' o si force=True.
    Esto evita escanear la carpeta miles de veces al cargar muchas cards.
    """
    now = time.time()
    if not force and (now - float(_IMAGES_INDEX["ts"])) < ttl_seconds:
        return

    img_dir = _images_dir()
    by_key = {}

    if img_dir.exists():
        for p in img_dir.iterdir():
            if not p.is_file():
                continue
            ext = p.suffix.lower()
            if ext not in _ALLOWED_EXTS:
                continue

            key = normalizar_referencia(p.stem)  # nombre sin extensión
            if not key:
                continue

            # si hay duplicados (misma key con diferentes ext),
            # nos quedamos con el primero que aparezca (MVP).
            if key not in by_key:
                by_key[key] = p.name

    _IMAGES_INDEX["by_key"] = by_key
    _IMAGES_INDEX["ts"] = now


@segmentacion_bp.get("/api/imagenes/referencia")
def api_imagen_por_referencia():
    """
    Devuelve la imagen asociada a una referencia.
    - GET /segmentacion/api/imagenes/referencia?ref=105174-00%20%7C%20628
    """
    ref = (request.args.get("ref") or "").strip()
    if not ref:
        abort(400, description="Falta parámetro 'ref'.")

    key = normalizar_referencia(ref)
    if not key:
        abort(400, description="Referencia inválida.")

    _rebuild_index_if_needed(force=False, ttl_seconds=60)

    filename = _IMAGES_INDEX["by_key"].get(key)
    if not filename:
        # no hay imagen: devolvemos 404 para que el frontend use onerror->placeholder
        abort(404)

    img_dir = _images_dir()
    if not img_dir.exists():
        abort(404)

    return send_from_directory(img_dir, filename)


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
    repo = PostgresRepository(POSTGRES_DSN)
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    rows = svc.export_dataset_todas()

    headers = [
        "fecha_actualizacion",
        "id_segmentacion",
        "fecha_creacion",
        "id_usuario",
        "id_version_tiendas",
        "estado_segmentacion",

        "referenciaSku",
        "codigo_barras",
        "descripcion",
        "categoria",
        "linea",
        "tipo_portafolio",
        "estado_sku",
        "cuento",
        "tipo_inventario",

        "llave_naval",
        "talla",
        "cantidad",
        "estado_detalle",

        "cod_bodega",
        "cod_dependencia",
        "dependencia",
        "desc_dependencia",
        "ciudad",
        "zona",
        "clima",
        "rankin_linea",
        "testeo"
    ]

    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()

    for r in rows:
        writer.writerow({
            "fecha_actualizacion": r.get("fecha_actualizacion"),
            "id_segmentacion": r.get("id_segmentacion"),
            "fecha_creacion": r.get("fecha_creacion"),
            "id_usuario": r.get("id_usuario"),
            "id_version_tiendas": r.get("id_version_tiendas"),
            "estado_segmentacion": r.get("estado_segmentacion"),

            "referenciaSku": r.get("referencia"),
            "codigo_barras": r.get("codigo_barras"),
            "descripcion": r.get("descripcion"),
            "categoria": r.get("categoria"),
            "linea": r.get("linea"),
            "tipo_portafolio": r.get("tipo_portafolio"),
            "estado_sku": r.get("estado_sku"),
            "cuento": r.get("cuento"),
            "tipo_inventario": r.get("tipo_inventario"),

            "llave_naval": r.get("llave_naval"),
            "talla": r.get("talla"),
            "cantidad": r.get("cantidad"),
            "estado_detalle": r.get("estado_detalle"),

            "cod_bodega": r.get("cod_bodega"),
            "cod_dependencia": r.get("cod_dependencia"),
            "dependencia": r.get("dependencia"),
            "desc_dependencia": r.get("desc_dependencia"),
            "ciudad": r.get("ciudad"),
            "zona": r.get("zona"),
            "clima": r.get("clima"),
            "rankin_linea": r.get("rankin_linea"),
            "testeo": r.get("testeo"),
        })

    # Tip: utf-8-sig ayuda a Excel a abrir acentos bien (BOM)
    data_bytes = sio.getvalue().encode("utf-8-sig")
    file_obj = io.BytesIO(data_bytes)

    filename = f"segmentaciones_todas_{datetime.now(ZoneInfo('America/Bogota')).date().isoformat()}.csv"
    return send_file(file_obj, as_attachment=True, download_name=filename, mimetype="text/csv")