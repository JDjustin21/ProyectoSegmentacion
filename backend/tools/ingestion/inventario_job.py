# backend/tools/ingestion/inventario_job.py

import os
import math
import hashlib
import logging
from datetime import datetime

import requests
from psycopg2.extras import execute_values

from backend.repositories.postgres_repository import PostgresRepository
import backend.config.settings as settings


def setup_logger(job_name: str = "inventario_job") -> logging.Logger:
    """
    Logger solo a consola.
    El .bat redirige stdout/stderr al archivo log único del job.
    """
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.info("Logger iniciado en consola/stdout.")
    return logger


def norm(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def build_hash_fila(row: dict) -> str:
    """
    Clave estable para el snapshot actual de inventario.
    """
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
    Universo de referencias a consultar.
    Por ahora: referencias con ventas en los últimos 120 días.
    """
    q = """
    SELECT DISTINCT referencia_sku
    FROM public.vw_ventas_movimientos_normalizados
    WHERE fecha_movimiento >= (CURRENT_DATE - INTERVAL '120 days')
      AND TRIM(COALESCE(referencia_sku,'')) <> '';
    """
    rows = repo.fetch_all(q, {})
    return [r["referencia_sku"] for r in rows if r.get("referencia_sku")]


def preparar_snapshot_inventario_actual(repo: PostgresRepository) -> None:
    """
    Regla de negocio:
    - inventario_actual representa solo el snapshot vigente.
    - no se conserva histórico en inventario_existencias.
    - antes de cargar el nuevo snapshot se limpia completamente inventario_actual.
    """
    repo.execute("TRUNCATE TABLE public.inventario_actual;", {})


def insertar_lote_bulk(repo: PostgresRepository, rows: list[dict]) -> int:
    """
    Inserta un lote completo directamente en inventario_actual.
    Como inventario_actual es el snapshot vigente, no usa versionado.
    Idempotencia por hash_fila dentro del mismo snapshot.
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO public.inventario_actual
    (
      referencia_sku,
      ean,
      talla,
      bodega,
      disponible,
      existencia,
      linea,
      codigo_siesa,
      llave_naval,
      cod_dependencia,
      fecha_ultima_actualizacion,
      hash_fila
    )
    VALUES %s
    ON CONFLICT (hash_fila)
    DO UPDATE SET
      referencia_sku = EXCLUDED.referencia_sku,
      ean = EXCLUDED.ean,
      talla = EXCLUDED.talla,
      bodega = EXCLUDED.bodega,
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
        template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s)"
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
    Normaliza campos desde el DTO .NET a lo que guardamos en Postgres.
    Soporta PascalCase, camelCase y nombres con underscore.
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
        "referenciasSku": referencias
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

    refs = get_referencias_universo(repo)
    logger.info(f"Universo referencias: {len(refs)}")

    if not refs:
        raise SystemExit("No hay referencias en el universo. Revisa criterio de universo.")

    batch_size = int(os.getenv("INV_BATCH_SIZE", "200"))
    timeout = int(os.getenv("INV_API_TIMEOUT", "120"))

    logger.info(f"Config: INV_BATCH_SIZE={batch_size} INV_API_TIMEOUT={timeout}")

    # Limpiar snapshot actual antes de reconstruirlo.
    preparar_snapshot_inventario_actual(repo)
    logger.info("inventario_actual truncada correctamente antes de iniciar la carga.")

    total_insertadas = 0
    total_batches = math.ceil(len(refs) / batch_size)

    for idx, batch in enumerate(chunk_list(refs, batch_size), start=1):
        t0 = datetime.now()
        logger.info(f"Lote {idx}/{total_batches}: refs={len(batch)} -> llamando API...")

        datos, truncado = llamar_api_inventario(batch, timeout_sec=timeout)
        logger.info(f"Lote {idx}/{total_batches}: API filas={len(datos)} truncado={truncado}")

        if truncado:
            raise SystemExit(
                f"API devolvió truncado=true. Reduce INV_BATCH_SIZE. "
                f"Lote size={len(batch)}"
            )

        filas = []
        for it in datos:
            row = mapear_datos_api(it)
            row["hash_fila"] = build_hash_fila(row)
            filas.append(row)

        ins = insertar_lote_bulk(repo, filas)
        total_insertadas += ins

        dt = (datetime.now() - t0).total_seconds()
        logger.info(
            f"Lote {idx}/{total_batches}: insertadas={ins} "
            f"acumulado={total_insertadas} tiempo_s={dt:.2f}"
        )

    logger.info(f"OK inventario_actual. filas={total_insertadas}")


if __name__ == "__main__":
    main()