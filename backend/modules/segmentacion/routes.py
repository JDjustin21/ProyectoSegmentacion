# backend/modules/segmentacion/routes.py
"""
Módulo de rutas de Segmentación (Flask Blueprint).

Responsabilidad:
- Renderizar la vista principal (shell HTML de cards + filtros).
- Exponer endpoints JSON para:
  - listado resumido de referencias
  - detalle de una referencia para el modal
  - tiendas activas
  - última segmentación
  - guardar segmentación
  - exportación CSV
- Mantener las rutas HTTP del módulo delgadas, delegando la lógica de negocio
  a SegmentacionDbService.

Notas de arquitectura:
- SQL Server: solo lectura, vía API .NET / job de snapshot.
- Postgres: base operativa del aplicativo.
- La pantalla principal ya no consulta la API remota en caliente.
- El listado inicial usa un resumen liviano.
- El detalle del modal se carga bajo demanda.
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
from backend.modules.segmentacion.segmentacion_db_service import SegmentacionDbService
from backend.repositories.postgres_repository import PostgresRepository
from backend.config.settings import (
    DEFAULT_TALLAS_MVP,
    IMAGES_BASE_URL,
    LINEAS_TALLAS_FIJAS,
    POSTGRES_DSN,
    POSTGRES_TIENDAS_VIEW,
    SEGMENTACION_CARDS_PER_PAGE,
    METRICAS_EXISTENCIA_TALLA_VIEW,
)
import backend.config.settings as settings


segmentacion_bp = Blueprint(
    "segmentacion",
    __name__,
    url_prefix="/segmentacion"
)

TZ_BOGOTA = ZoneInfo("America/Bogota")

def _pg_repo() -> PostgresRepository:
    return PostgresRepository(POSTGRES_DSN)

def _svc_pg(repo: PostgresRepository) -> SegmentacionDbService:
    return SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW, METRICAS_EXISTENCIA_TALLA_VIEW)

def _obtener_snapshot_updated_at():
    """
    Retorna solo la fecha del snapshot vigente.
    No lee todo el dataset.
    """
    repo = _pg_repo()
    svc_pg = _svc_pg(repo)
    return svc_pg.obtener_snapshot_updated_at()

def _obtener_referencias_resumen_cards():
    """
    Retorna solo el dataset liviano para cards.
    La composición del resumen vive en SegmentacionDbService.
    """
    repo = _pg_repo()
    svc_pg = _svc_pg(repo)
    return svc_pg.listar_referencias_resumen_cards(dias_nuevo=7)

@segmentacion_bp.route("/", methods=["GET"])
@login_required
def vista_segmentacion():
    """
    Vista principal:
    - Renderiza el shell HTML
    - Pasa configuración al frontend vía data-attributes
    - Solo consulta la fecha del snapshot vigente
    - El dataset de cards se carga por AJAX desde /api/referencias
    """
    cache_updated_at = _obtener_snapshot_updated_at()

    return render_template(
        "segmentacion.html",
        cards_per_page=SEGMENTACION_CARDS_PER_PAGE,
        images_base_url=IMAGES_BASE_URL,
        default_tallas_mvp=DEFAULT_TALLAS_MVP,
        lineas_tallas_fijas=LINEAS_TALLAS_FIJAS,
        cache_updated_at=cache_updated_at,
    )

@segmentacion_bp.get("/api/referencias")
@login_required
def api_referencias():
    """
    Devuelve SOLO el resumen de referencias para las cards.

    Importante:
    - no trae campos pesados del modal
    - no hace escrituras al leer
    - la lógica de composición vive en SegmentacionDbService
    """
    t0 = time.perf_counter()

    referencias, cache_updated_at = _obtener_referencias_resumen_cards()

    t1 = time.perf_counter()

    current_app.logger.info(
        "[SEGMENTACION][API_REFERENCIAS] total_ms=%.2f refs=%s",
        (t1 - t0) * 1000,
        len(referencias),
    )

    return jsonify({
        "ok": True,
        "data": referencias,
        "meta": {
            "count": len(referencias),
            "cache_updated_at": cache_updated_at.isoformat() if cache_updated_at else None,
        }
    })

@segmentacion_bp.get("/api/referencias/detalle")
@login_required
def api_referencia_detalle():
    """
    Retorna el detalle de UNA referencia para abrir el modal.

    Este endpoint carga bajo demanda los campos pesados
    que no deben viajar en el listado principal.
    """
    referencia_sku = (request.args.get("referenciaSku") or "").strip()
    if not referencia_sku:
        return jsonify({"ok": False, "error": "Falta query param: referenciaSku"}), 400

    t0 = time.perf_counter()

    repo = _pg_repo()
    svc_pg = _svc_pg(repo)
    data = svc_pg.obtener_referencia_detalle_snapshot(referencia_sku)

    t1 = time.perf_counter()

    if not data:
        return jsonify({"ok": False, "error": "Referencia no encontrada"}), 404

    current_app.logger.info(
        "[SEGMENTACION][API_REFERENCIA_DETALLE] ref=%s total_ms=%.2f",
        referencia_sku,
        (t1 - t0) * 1000,
    )

    return jsonify({
        "ok": True,
        "data": data,
    })

@segmentacion_bp.get("/utilidades")
@login_required
def vista_utilidades():
    """
    Vista Utilidades:
    - Exportar segmentaciones
    - Importar imágenes a static/assets/images/referencias
    """
    return render_template("utilidades.html")

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
    svc_pg = _svc_pg(repo)

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
    svc = _svc_pg(repo)

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
    svc = _svc_pg(repo)

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

        linea = (request.args.get("linea") or "").strip()
        dependencia = (request.args.get("dependencia") or "").strip() or None
        llaves_raw = (request.args.get("llaves") or "").strip()
        llaves = [x.strip() for x in llaves_raw.split(",") if x.strip()] if llaves_raw else []

        t0 = time.perf_counter()
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
        t1 = time.perf_counter()

        existencia = repo.obtener_existencia_por_talla(
            referencia_sku=referencia_sku,
            llave_naval=llave_naval,
            view_existencia_talla=settings.METRICAS_EXISTENCIA_TALLA_VIEW,
        )
        t2 = time.perf_counter()

        current_app.logger.info(
            "[METRICAS] ref=%s resumen+detalle=%.2fms existencia=%.2fms total=%.2fms",
            referencia_sku,
            (t1 - t0) * 1000,
            (t2 - t1) * 1000,
            (t2 - t0) * 1000
        )

        part_linea = []
        if linea:
            part_linea = repo.obtener_participacion_linea_por_tiendas(
                linea=linea,
                dependencia=dependencia,
                view_part_linea=settings.METRICAS_PART_VENTA_LINEA_VIEW,
            )

        return jsonify({
            "ok": True,
            "data": {
                "referenciaSku": referencia_sku,
                "resumenPorTienda": resumen,
                "detallePorTalla": detalle,
                "existenciaPorTalla": existencia,
                "participacionLineaPorTienda": part_linea
            }
        })
    
    except Exception as ex:
        current_app.logger.exception("Error en /api/metricas")
        return jsonify({"ok": False, "error": str(ex), "trace": traceback.format_exc()}), 500
    

@segmentacion_bp.get("/api/metricas/participacion-linea")
@login_required
def api_metricas_participacion_linea():
    """
    Devuelve participación por línea (3 meses) por tienda.

    Query params:
    - linea 
    - dependencia -> si viene, limita resultados a ese cliente
    """
    try:
        linea = (request.args.get("linea") or "").strip()
        if not linea:
            return jsonify({"ok": False, "error": "Falta query param: linea"}), 400

        dependencia = (request.args.get("dependencia") or "").strip() or None

        repo = PostgresRepository(settings.POSTGRES_DSN)
        rows = repo.obtener_participacion_linea_por_tiendas(
            linea=linea,
            dependencia=dependencia,
            view_part_linea=settings.METRICAS_PART_VENTA_LINEA_VIEW,
        )

        return jsonify({"ok": True, "data": rows})
    except Exception as ex:
        current_app.logger.exception("Error en /api/metricas/participacion-linea")
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
    svc = _svc_pg(repo)

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

@segmentacion_bp.get("/api/segmentaciones/candidatas")
@login_required
def api_segmentaciones_candidatas():
    referencia_base = (request.args.get("referenciaBase") or "").strip()
    referencia_sku_actual = (request.args.get("referenciaSkuActual") or "").strip()

    if not referencia_base:
        return jsonify({"ok": True, "data": []})

    repo = _pg_repo()
    svc = _svc_pg(repo)

    rows = svc.listar_segmentaciones_candidatas_por_base(
        referencia_base=referencia_base,
        referencia_sku_actual=referencia_sku_actual,
    )
    return jsonify({"ok": True, "data": rows})


@segmentacion_bp.get("/api/segmentaciones/copia")
@login_required
def api_segmentacion_para_copiar():
    raw_id = (request.args.get("idSegmentacion") or "").strip()
    if not raw_id:
        return jsonify({"ok": False, "error": "Falta query param: idSegmentacion"}), 400

    repo = _pg_repo()
    svc = _svc_pg(repo)

    result = svc.obtener_segmentacion_para_copiar(int(raw_id))
    return jsonify({"ok": True, "data": result})

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
    svc = _svc_pg(repo)

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
    svc = SegmentacionDbService(repo, POSTGRES_TIENDAS_VIEW, METRICAS_EXISTENCIA_TALLA_VIEW,)

    rows = svc.export_dataset_todas()

    def _norm_digits(v: str) -> str:
        return re.sub(r"\D+", "", (v or "").strip())

    # 1) Keys únicas para consultar la vista semanal
    keys = []
    seen = set()

    for r in rows:
        referencia_sku = (r.get("referencia_sku") or "").strip()
        referencia_base = (r.get("referencia_base") or "").strip()
        codigo_color = (r.get("codigo_color") or "").strip()
        color = (r.get("color") or "").strip()
        perfil_prenda = (r.get("perfil_prenda") or "").strip()

        llave = (r.get("llave_naval") or "").strip()
        ean = _norm_digits(r.get("codigo_barras"))

        if not referencia_sku or not llave or not ean:
            continue

        k = (referencia_sku, llave, ean)
        if k not in seen:
            seen.add(k)
            keys.append(k)

    # 2) Traemos semanas desde la vista (solo para esas keys)
    ventas_map = {}  # (ref, llave, ean) -> semanas

    if keys:
        values_sql = ",".join([f"(%(r{i})s, %(l{i})s, %(e{i})s)" for i in range(len(keys))])
        params = {}
        for i, (rr, ll, ee) in enumerate(keys):
            params[f"r{i}"] = rr
            params[f"l{i}"] = ll
            params[f"e{i}"] = ee

        sql = f"""
        WITH input(referencia_sku, llave_naval, ean) AS (
            VALUES {values_sql}
        )
        SELECT
            v.referencia_sku,
            v.llave_naval,
            v.ean,
            v.semana1, v.semana2, v.semana3, v.semana4,
            v.semana5, v.semana6, v.semana7, v.semana8
        FROM public.vw_ventas_semanales_8w_por_ean v
        JOIN input i
          ON i.referencia_sku = v.referencia_sku
         AND i.llave_naval = v.llave_naval
         AND i.ean = v.ean
        """

        result = repo.fetch_all(sql, params)
        for row in result:
            k = (
                (row.get("referencia_sku") or "").strip(),
                (row.get("llave_naval") or "").strip(),
                _norm_digits(row.get("ean"))
            )
            ventas_map[k] = row

    current_app.logger.info("Export rows: %s | Weekly map: %s", len(rows), len(ventas_map))

    headers = [
        "Id Segmentacion",
        "Estado Segmentacion",

        "Referencia Sku",
        "Referencia Base",
        "Codigo Color",
        "Color",
        "Talla",
        "Codigo Barras",
        "Cantidad",
        "Existencia",
        "Linea",
        "Descripcion",
        "Categoria",
        "Tipo Portafolio",
        "Estado Sku",
        "Cuento",
        "Tipo Inventario",
        "Precio Unitario",
        "Perfil Prenda",
        
        "Llave Naval",
        "Estado Detalle",
        "Codigo Bodega",
        "Codigo Dependencia",
        "Dependencia",
        "Descripcion Dependencia",
        "Ciudad",
        "Zona",
        "Clima",
        "Ranking Linea",
        "Testeo",

        "Fecha Creacion",
        "Fecha Actualizacion",

        "Semana1",
        "Semana2",
        "Semana3",
        "Semana4",
        "Semana5",
        "Semana6",
        "Semana7",
        "Semana8",
        "TotalVentasSemanal"
    ]

    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()

    for r in rows:
        referencia_sku = (r.get("referencia_sku") or "").strip()
        referencia_base = (r.get("referencia_base") or "").strip()
        codigo_color = (r.get("codigo_color") or "").strip()
        color = (r.get("color") or "").strip()
        perfil_prenda = (r.get("perfil_prenda") or "").strip()

        llave = (r.get("llave_naval") or "").strip()
        ean = _norm_digits(r.get("codigo_barras"))

        wk = ventas_map.get((referencia_sku, llave, ean), {}) or {}

        def _to_int(v):
            try:
                return int(float(v))
            except Exception:
                return 0

        if r.get("estado_segmentacion") == "INACTIVO":
            r["estado_segmentacion"] = "INACTIVO"
            r["estado_detalle"] = "INACTIVO"
            r["fecha_actualizacion"] = datetime.now(TZ_BOGOTA).isoformat()

        writer.writerow({
            "Id Segmentacion": r.get("id_segmentacion"),
            "Estado Segmentacion": r.get("estado_segmentacion"),

            "Referencia Sku": referencia_sku,
            "Referencia Base": referencia_base,
            "Codigo Color": codigo_color,
            "Color": color,
            "Talla": r.get("talla"),
            "Codigo Barras": r.get("codigo_barras"),
            "Cantidad": r.get("cantidad"),
            "Existencia": r.get("existencia"),
            "Linea": r.get("linea"),
            "Descripcion": r.get("descripcion"),
            "Categoria": r.get("categoria"),
            "Tipo Portafolio": r.get("tipo_portafolio"),
            "Estado Sku": r.get("estado_sku"),
            "Cuento": r.get("cuento"),
            "Tipo Inventario": r.get("tipo_inventario"),
            "Precio Unitario": int(round(float(r.get("precio_unitario") or 0))),
            "Perfil Prenda": perfil_prenda,

            "Llave Naval": r.get("llave_naval"),
            "Estado Detalle": r.get("estado_detalle"),
            "Codigo Bodega": r.get("cod_bodega"),
            "Codigo Dependencia": r.get("cod_dependencia"),
            "Dependencia": r.get("dependencia"),
            "Descripcion Dependencia": r.get("desc_dependencia"),
            "Ciudad": r.get("ciudad"),
            "Zona": r.get("zona"),
            "Clima": r.get("clima"),
            "Ranking Linea": r.get("rankin_linea"),
            "Testeo": r.get("testeo"),

            "Fecha Creacion": r.get("fecha_creacion"),
            "Fecha Actualizacion": r.get("fecha_actualizacion"),

            "Semana1": _to_int(wk.get("semana1", 0)),
            "Semana2": _to_int(wk.get("semana2", 0)),
            "Semana3": _to_int(wk.get("semana3", 0)),
            "Semana4": _to_int(wk.get("semana4", 0)),
            "Semana5": _to_int(wk.get("semana5", 0)),
            "Semana6": _to_int(wk.get("semana6", 0)),
            "Semana7": _to_int(wk.get("semana7", 0)),
            "Semana8": _to_int(wk.get("semana8", 0)),
            "TotalVentasSemanal": sum(_to_int(wk.get(f"semana{i}", 0)) for i in range(1, 9))
        })

    # Tip: utf-8-sig ayuda a Excel a abrir acentos bien (BOM)
    data_bytes = sio.getvalue().encode("utf-8-sig")
    file_obj = io.BytesIO(data_bytes)

    filename = f"segmentaciones_todas_{datetime.now(TZ_BOGOTA).date().isoformat()}.csv"
    return send_file(file_obj, as_attachment=True, download_name=filename, mimetype="text/csv")