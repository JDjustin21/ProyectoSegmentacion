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
import csv
import io
import re
import time
import unicodedata
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import traceback

from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    render_template,
    request,
    send_file,
    send_from_directory,
)
from werkzeug.utils import secure_filename
from backend.modules.auth.decorators import login_required, role_required
from backend.modules.segmentacion.app_cache_service import AppCacheService
from backend.modules.segmentacion.services import SegmentacionService
from backend.modules.segmentacion.segmentacion_db_service import SegmentacionDbService
from backend.repositories.postgres_repository import PostgresRepository
from backend.config.settings import (
    DEFAULT_TALLAS_MVP,
    IMAGES_BASE_URL,
    LINEAS_TALLAS_FIJAS,
    POSTGRES_DSN,
    POSTGRES_TIENDAS_VIEW,
    SEGMENTACION_CARDS_PER_PAGE,
    SQLSERVER_API_URL,
)
import backend.config.settings as settings


segmentacion_bp = Blueprint(
    "segmentacion",
    __name__,
    url_prefix="/segmentacion"
)

CACHE_KEY_REFERENCIAS = "referencias_sqlserver"
CACHE_TTL_SECONDS = 120  # 2 min
TZ_BOGOTA = ZoneInfo("America/Bogota")

def _pg_repo() -> PostgresRepository:
    return PostgresRepository(POSTGRES_DSN)

def _svc_pg(repo: PostgresRepository) -> SegmentacionDbService:
    return SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)



@segmentacion_bp.route("/", methods=["GET"])
@login_required
def vista_segmentacion():
    """
    Vista principal:
    - Obtiene referencias desde la API de SQL Server (.NET)
    - Renderiza HTML con el dataset embebido (script JSON)
    - Pasa configuración al frontend vía data-attributes (sin hardcode en JS)
    """
    repo = _pg_repo()
    cache = AppCacheService(repo)

    cached = cache.get(CACHE_KEY_REFERENCIAS)
    if cache.is_fresh(cached, CACHE_TTL_SECONDS):
        referencias = cached["payload"]
    else:
        # Lock key fijo (un número) para "referencias"
        LOCK_KEY = 910001  

        got_lock = cache.try_lock(LOCK_KEY)
        if got_lock:
            try:
                try:
                    servicio = SegmentacionService(SQLSERVER_API_URL)
                    referencias = servicio.obtener_referencias()
                    cache.set(CACHE_KEY_REFERENCIAS, referencias)

                except Exception as ex:
                    # Si SQL Server API falla (VPN caída), NO tumbes la vista.
                    # Usa cache (aunque esté viejo) o lista vacía para poder entrar al módulo.
                    current_app.logger.exception("Fallo SQL Server API obteniendo referencias: %s", ex)
                    referencias = (cached or {}).get("payload") or []
            
            finally:
                cache.unlock(LOCK_KEY)
        else:
            # Otro ya está refrescando: esperamos un poquito el cache y usamos eso
            cached2 = cache.wait_for_refresh(CACHE_KEY_REFERENCIAS, CACHE_TTL_SECONDS, max_wait_seconds=8.0)
            referencias = (cached2 or {}).get("payload") or []

            # fallback extremo: si por alguna razón sigue vacío, refrescamos igual
            if not referencias:
                try:
                    servicio = SegmentacionService(SQLSERVER_API_URL)
                    referencias = servicio.obtener_referencias()
                    cache.set(CACHE_KEY_REFERENCIAS, referencias)
                except Exception as ex:
                    current_app.logger.exception("Fallo SQL Server API en fallback extremo: %s", ex)
                    referencias = []

    svc_pg = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)
    referencias = svc_pg.marcar_y_anotar_referencias_nuevas(referencias, dias_nuevo=7)
    referencias = svc_pg.anotar_segmentacion_y_conteo(referencias)

    referencias.sort(key=lambda r: (not r.get("is_new", False)))

    return render_template(
        "segmentacion.html",
        referencias=referencias,
        cards_per_page=SEGMENTACION_CARDS_PER_PAGE,
        images_base_url=IMAGES_BASE_URL,
        default_tallas_mvp=DEFAULT_TALLAS_MVP,
        lineas_tallas_fijas=LINEAS_TALLAS_FIJAS,
    )

@segmentacion_bp.get("/utilidades")
@login_required
def vista_utilidades():
    """
    Vista Utilidades:
    - Exportar segmentaciones
    - Importar imágenes a static/assets/images/referencias
    """
    return render_template("utilidades.html")

@segmentacion_bp.post("/api/cache/referencias/refresh")
@login_required
def api_refresh_cache_referencias():
    repo = _pg_repo()
    cache = AppCacheService(repo)

    servicio = SegmentacionService(SQLSERVER_API_URL)
    referencias = servicio.obtener_referencias()

    svc_pg = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)
    referencias = svc_pg.marcar_y_anotar_referencias_nuevas(referencias, dias_nuevo=7)

    cache.set(CACHE_KEY_REFERENCIAS, referencias)
    return jsonify({"ok": True, "mensaje": "Cache de referencias actualizado", "count": len(referencias)})

@segmentacion_bp.post("/api/referencias/ack")
@login_required
def api_ack_referencia():
    payload = request.get_json(silent=True) or {}
    referencia = (payload.get("referencia") or "").strip()
    if not referencia:
        return jsonify({"ok": False, "error": "Falta 'referencia'"}), 400

    repo = _pg_repo()
    now = datetime.now(TZ_BOGOTA)

    repo.execute("""
        UPDATE public.referencias_vistas
        SET acknowledged_at = %(now)s
        WHERE referencia_sku = %(ref)s;
    """, {"now": now, "ref": referencia})

    return jsonify({"ok": True})

@segmentacion_bp.post("/api/referencias/segmentar")
@login_required
def api_segmentar_referencia():
    payload = request.get_json(silent=True) or {}
    referencia_sku = (payload.get("referencia") or "").strip()
    if not referencia_sku:
        return jsonify({"ok": False, "error": "Falta 'referencia'"}), 400

    repo = _pg_repo()
    svc_pg = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    # Marcamos como segmentada en la base de datos
    svc_pg.marcar_como_segmentada(referencia_sku)

    return jsonify({"ok": True, "mensaje": "Referencia segmentada"})



@segmentacion_bp.get("/api/tiendas/activas")
@login_required
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

    repo = _pg_repo()
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
@login_required
def api_ultima_segmentacion():
    """
    Retorna la última segmentación guardada para una referenciaSku.

    Query params:
    - referenciaSku (obligatorio): ejemplo "103834-01 | 857"
    """
    referencia_sku = (request.args.get("referenciaSku") or "").strip()
    if not referencia_sku:
        return jsonify({"ok": False, "error": "Falta query param: referenciaSku"}), 400

    repo = _pg_repo()
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    data = svc.ultima_segmentacion(referencia_sku)
    return jsonify({"ok": True, "data": data})


@segmentacion_bp.route("/api/metricas", methods=["GET"])
@login_required
def api_metricas():
    try:
        referencia_sku = (request.args.get("referenciaSku") or "").strip()
        llave_naval = (request.args.get("llave_naval") or "").strip() or None

        if not referencia_sku:
            return jsonify({"ok": False, "error": "Falta referenciaSku"}), 400

        repo = PostgresRepository(settings.POSTGRES_DSN)

        resumen, detalle = repo.obtener_metricas_por_referencia(
            referencia_sku=referencia_sku,
            llave_naval=llave_naval,
            view_cpd_talla=settings.METRICAS_CPD_TALLA_VIEW,
            view_cpd_tienda=settings.METRICAS_CPD_TIENDA_VIEW,
            view_prom_talla=settings.METRICAS_VENTA_PROM_TALLA_VIEW,
            view_prom_tienda=settings.METRICAS_VENTA_PROM_TIENDA_VIEW,
            view_rotacion_talla=settings.METRICAS_ROTACION_TALLA_VIEW,
            view_rotacion_tienda=settings.METRICAS_ROTACION_TIENDA_VIEW,
        )

        return jsonify({
            "ok": True,
            "data": {
                "referenciaSku": referencia_sku,
                "resumenPorTienda": resumen,
                "detallePorTalla": detalle,
            }
        })
    except Exception as ex:
        current_app.logger.exception("Error en /api/metricas")
        return jsonify({"ok": False, "error": str(ex), "trace": traceback.format_exc()}), 500
    

@segmentacion_bp.post("/api/segmentaciones")
@login_required
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

    repo = _pg_repo()
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    result = svc.guardar_segmentacion(payload)
    if result.get("ok") is True:
        referencia_sku = (payload.get("referenciaSku") or "").strip()

        # Fuente de verdad: Postgres (última segmentación + detalle + vista tiendas activas)
        flags = svc.obtener_estado_y_conteo_segmentacion([referencia_sku]).get(
            referencia_sku,
            {"is_segmented": False, "tiendas_activas_segmentadas": 0}
        )

        result["is_segmented"] = bool(flags["is_segmented"])
        result["tiendas_activas_segmentadas"] = int(flags["tiendas_activas_segmentadas"] or 0)


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
@login_required
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

@segmentacion_bp.post("/api/imagenes/upload")
@login_required
def api_upload_imagenes():
    """
    Sube una o varias imágenes y las guarda en:
    static/assets/images/referencias/

    Reglas:
    - Acepta: .jpg, .jpeg, .png, .webp
    - Normaliza el nombre para que matchee con la referencia:
        "104535-00 | 616.png" -> "10453500616.png"
    - Sobrescribe si ya existe.
    - Refresca el índice de imágenes al final.
    """
    # 1) Tomamos archivos del form-data
    files = request.files.getlist("files")
    if not files:
        return jsonify({"ok": False, "error": "No se recibieron archivos. Usa el campo 'files'."}), 400

    img_dir = _images_dir()
    img_dir.mkdir(parents=True, exist_ok=True)

    guardadas = []
    rechazadas = []

    for f in files:
        if not f or not getattr(f, "filename", ""):
            continue

        original_name = f.filename
        safe_name = secure_filename(original_name)  # evita rutas raras o caracteres peligrosos
        if not safe_name:
            rechazadas.append({"archivo": original_name, "motivo": "Nombre inválido"})
            continue

        # 2) Validar extensión
        ext = Path(safe_name).suffix.lower()
        if ext not in _ALLOWED_EXTS:
            rechazadas.append({"archivo": original_name, "motivo": f"Extensión no permitida ({ext})"})
            continue

        # 3) Normalizar "stem" (nombre sin extensión) para que siempre sea la key
        #    Ej: "104535-00 | 616" -> "10453500616"
        stem = Path(safe_name).stem
        key = normalizar_referencia(stem)
        if not key:
            rechazadas.append({"archivo": original_name, "motivo": "No se pudo normalizar a referencia"})
            continue

        # 4) Nombre final del archivo (siempre key + ext)
        final_name = f"{key}{ext}"
        dest_path = img_dir / final_name

        # 5) Guardar (sobrescribe)
        #    Nota: f.save sobrescribe si el archivo existe.
        try:
            f.save(dest_path)
            guardadas.append({
                "archivo_original": original_name,
                "archivo_guardado": final_name
            })
        except Exception as ex:
            rechazadas.append({"archivo": original_name, "motivo": f"Error guardando: {str(ex)}"})

    # 6) Refrescar índice (para que se vea inmediato sin esperar TTL)
    _rebuild_index_if_needed(force=True, ttl_seconds=60)

    return jsonify({
        "ok": True,
        "resumen": {
            "recibidos": len(files),
            "guardadas": len(guardadas),
            "rechazadas": len(rechazadas),
        },
        "guardadas": guardadas,
        "rechazadas": rechazadas
    })

@segmentacion_bp.post("/api/segmentaciones/reset")
@login_required
@role_required("admin")
def api_reset_segmentaciones():
    """
    Reinicia (TRUNCATE) las tablas de segmentación.
    Acción destructiva: solo ADMIN.
    """
    repo = _pg_repo()
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    result = svc.reset_segmentaciones()
    return jsonify(result)


@segmentacion_bp.get("/api/export/csv")
@login_required
def api_export_csv():
    """
    Exporta CSV SOLO de lo modificado en un rango de tiempo, usando
    segmentacion_detalle.fecha_actualizacion como criterio.

    Soporta:
    - ?fecha=YYYY-MM-DD  -> exporta ese día completo (00:00 a 00:00 del día siguiente)
    - ?desde=ISO&hasta=ISO -> exporta rango exacto
    Default: hoy (America/Bogota)
    """
    repo = _pg_repo()
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW)

    rows = svc.export_dataset_todas()

    headers = [
        
        "id_segmentacion",
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
        "precio_unitario",

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
        "testeo",
        "fecha_creacion",
        "fecha_actualizacion"
    ]

    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()

    for r in rows:
        writer.writerow({
           
            "id_segmentacion": r.get("id_segmentacion"),
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
            "precio_unitario": int(round(float(r.get("precio_unitario") or 0))),

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
            "fecha_actualizacion": r.get("fecha_actualizacion"),
            "fecha_creacion": r.get("fecha_creacion"),
        })

    # Tip: utf-8-sig ayuda a Excel a abrir acentos bien (BOM)
    data_bytes = sio.getvalue().encode("utf-8-sig")
    file_obj = io.BytesIO(data_bytes)

    filename = f"segmentaciones_todas_{datetime.now(TZ_BOGOTA).date().isoformat()}.csv"
    return send_file(file_obj, as_attachment=True, download_name=filename, mimetype="text/csv")