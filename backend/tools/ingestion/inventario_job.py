import os
import math
import json
import hashlib
import requests
from datetime import datetime
import logging
from pathlib import Path
from backend.repositories.postgres_repository import PostgresRepository
import backend.config.settings as settings
from psycopg2.extras import execute_values


def setup_logger(job_name: str = "inventario_job") -> logging.Logger:
    """
    Logs a: backend/logs/<job_name>_YYYYMMDD.log
    y también a consola.
    """
    backend_dir = Path(__file__).resolve().parents[2]  # Ajuste para resolver la ruta correctamente
    logs_dir = backend_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / f"{job_name}_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

    # Evitar handlers duplicados si se ejecuta en la misma sesión
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.info(f"Logger iniciado. Archivo: {log_file}")
    return logger


def norm(v: str) -> str:
    return (v or "").strip()


def md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def build_hash_fila(row: dict) -> str:
    # Clave estable: referencia_sku|ean|talla|bodega|codigo_siesa
    parts = [
        norm(row.get("referencia_sku")),
        norm(row.get("ean")),
        norm(row.get("talla")),
        norm(row.get("bodega")),
        norm(row.get("codigo_siesa")),
    ]
    return md5("|".join(parts).upper())


def chunk_list(arr, size):
    for i in range(0, len(arr), size):
        yield arr[i:i + size]


def get_referencias_universo(repo: PostgresRepository) -> list[str]:
    """
    MVP: universo = referencias que han tenido ventas en últimos 120 días.
    (Puedes ajustar a otro criterio sin tocar el resto del job)
    """
    q = """
    SELECT DISTINCT referencia_sku
    FROM public.vw_ventas_movimientos_normalizados
    WHERE fecha_movimiento >= (CURRENT_DATE - INTERVAL '120 days')
      AND TRIM(COALESCE(referencia_sku,'')) <> '';
    """
    rows = repo.fetch_all(q, {})
    return [r["referencia_sku"] for r in rows if r.get("referencia_sku")]


def crear_version(repo: PostgresRepository, observaciones: str = "") -> int:
    row = repo.fetch_one("""
        INSERT INTO public.inventario_version (estado_version, observaciones)
        VALUES ('Inactiva', %(obs)s)
        RETURNING id_version_inventario;
    """, {"obs": observaciones})
    return int(row["id_version_inventario"])


def marcar_version_activa(repo: PostgresRepository, id_version: int, filas_total: int):
    repo.execute("""
        UPDATE public.inventario_version
        SET estado_version='Inactiva'
        WHERE estado_version='Activa';
    """, {})

    repo.execute("""
        UPDATE public.inventario_version
        SET estado_version='Activa',
            filas_total=%(filas)s
        WHERE id_version_inventario=%(id)s;
    """, {"filas": int(filas_total), "id": int(id_version)})


def insertar_lote_bulk(repo: PostgresRepository, id_version: int, rows: list[dict]) -> int:
    """
    Inserta un lote completo en 1 sola operación (mucho más rápido).
    Idempotente por (id_version_inventario, hash_fila).
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO public.inventario_existencias
    (
      id_version_inventario,
      referencia_sku, ean, talla, bodega,
      disponible, existencia,
      linea, codigo_siesa,
      llave_naval, cod_dependencia,
      fecha_ultima_actualizacion,
      hash_fila
    )
    VALUES %s
    ON CONFLICT (id_version_inventario, hash_fila)
    DO UPDATE SET
      disponible = EXCLUDED.disponible,
      existencia = EXCLUDED.existencia,
      linea = EXCLUDED.linea,
      codigo_siesa = EXCLUDED.codigo_siesa,
      llave_naval = EXCLUDED.llave_naval,
      cod_dependencia = EXCLUDED.cod_dependencia,
      fecha_ultima_actualizacion = now();
    """

    values = []
    for r in rows:
        values.append((
            int(id_version),
            norm(r.get("referencia_sku")),
            norm(r.get("ean")),
            norm(r.get("talla")),
            norm(r.get("bodega")),
            int(r.get("disponible") or 0),
            int(r.get("existencia") or 0),
            norm(r.get("linea")),
            norm(r.get("codigo_siesa")),
            norm(r.get("llave_naval")),
            norm(r.get("cod_dependencia")),
            norm(r.get("hash_fila")),
        ))

    def _tx(cur):
        template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s)"
        execute_values(cur, sql, values, template=template, page_size=2000)

    repo.run_in_transaction(_tx)
    return len(rows)


def pick(d: dict, *keys, default=""):
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return default


def mapear_datos_api(item: dict) -> dict:
    """
    Normaliza campos desde tu DTO .NET a lo que guardamos en Postgres.
    Soporta PascalCase y camelCase, y nombres con underscore.
    """
    ref = norm(pick(item, "ReferenciaSku", "referenciaSku", "referencia_sku"))
    return {
        "referencia_sku": ref,
        "ean": norm(pick(item, "Ean", "ean")),
        "talla": norm(pick(item, "Talla", "talla")).upper(),
        "bodega": norm(pick(item, "Bodega", "bodega")),
        "disponible": int(pick(item, "Disponible", "disponible", default=0) or 0),
        "existencia": int(pick(item, "Existencia", "existencia", default=0) or 0),
        "linea": norm(pick(item, "Linea", "linea")),
        "codigo_siesa": norm(pick(item, "Codigo_siesa", "codigo_siesa", "CodigoSiesa", "codigoSiesa")),
        "llave_naval": norm(pick(item, "Llave_naval", "llave_naval", default="")),
        "cod_dependencia": norm(pick(item, "Cod_dependencia", "cod_dependencia", default="")),
    }


def llamar_api_inventario(referencias: list[str], timeout_sec: int = 120) -> tuple[list[dict], bool]:
    url = f"{settings.SQLSERVER_API_URL}/api/sqlserver/inventario-existencias/consultar"
    body = {
        "referenciasSku": referencias,
        "maxFilas": settings.METRICAS_ROTACION_MAX_FILAS_INV,
    }

    r = requests.post(url, json=body, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json()

    datos = data.get("datos") or []
    truncado = bool(data.get("truncado"))
    return datos, truncado


def main():
    logger = setup_logger("inventario_job")

    repo = PostgresRepository(settings.POSTGRES_DSN)

    observ = f"Job inventario lotes {datetime.now().isoformat()}"
    id_version = crear_version(repo, observaciones=observ)
    logger.info(f"Version creada (Inactiva): id_version_inventario={id_version}")

    refs = get_referencias_universo(repo)
    logger.info(f"Universo referencias: {len(refs)}")

    if not refs:
        raise SystemExit("No hay referencias en el universo. Revisa criterio de universo.")

    batch_size = int(os.getenv("INV_BATCH_SIZE", "200"))
    timeout = int(os.getenv("INV_API_TIMEOUT", "120"))

    logger.info(f"Config: INV_BATCH_SIZE={batch_size} INV_API_TIMEOUT={timeout} METRICAS_ROTACION_MAX_FILAS_INV={settings.METRICAS_ROTACION_MAX_FILAS_INV}")

    total_insertadas = 0
    total_batches = math.ceil(len(refs) / batch_size)

    for idx, batch in enumerate(chunk_list(refs, batch_size), start=1):
        t0 = datetime.now()
        logger.info(f"Lote {idx}/{total_batches}: refs={len(batch)} -> llamando API...")

        datos, truncado = llamar_api_inventario(batch, timeout_sec=timeout)
        logger.info(f"Lote {idx}/{total_batches}: API filas={len(datos)} truncado={truncado}")

        if truncado:
            raise SystemExit(
                f"API devolvió truncado=true. Reduce INV_BATCH_SIZE o aumenta METRICAS_ROTACION_MAX_FILAS_INV. "
                f"Lote size={len(batch)} maxFilas={settings.METRICAS_ROTACION_MAX_FILAS_INV}"
            )

        filas = []
        for it in datos:
            row = mapear_datos_api(it)
            row["hash_fila"] = build_hash_fila(row)
            filas.append(row)

        ins = insertar_lote_bulk(repo, id_version, filas)
        total_insertadas += ins

        dt = (datetime.now() - t0).total_seconds()
        logger.info(f"Lote {idx}/{total_batches}: insertadas={ins} acumulado={total_insertadas} tiempo_s={dt:.2f}")

    # Activar versión + refrescar inventario_actual
    marcar_version_activa(repo, id_version, total_insertadas)
    logger.info(f"Version marcada Activa: id_version_inventario={id_version} filas_total={total_insertadas}")

    # ===== FIX CLAVE =====
    # No usar fetch_one para ejecutar un SELECT que modifica tablas,
    # porque tu fetch_one no hace COMMIT si empieza por SELECT.
    repo.execute("SELECT public.refresh_inventario_actual();", {})
    logger.info("refresh_inventario_actual ejecutado y confirmado (COMMIT).")

    logger.info(f"OK inventario. version={id_version} filas={total_insertadas}")


if __name__ == "__main__":
    main()
