# backend/tools/ingestion/inventario_job.py

import os
import math
import hashlib
import logging
import time
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


def asegurar_tabla_staging(repo: PostgresRepository) -> None:
    """
    Crea la tabla staging si no existe.

    Esta tabla NO es histórica.
    Solo sirve como área de trabajo para armar el nuevo snapshot antes
    de reemplazar inventario_actual.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS public.inventario_actual_staging
    (LIKE public.inventario_actual INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);
    """
    repo.execute(sql, {})


def limpiar_staging(repo: PostgresRepository) -> None:
    """
    Limpia únicamente la staging.

    Importante:
    No toca inventario_actual.
    """
    repo.execute("TRUNCATE TABLE public.inventario_actual_staging;", {})


def reemplazar_snapshot_actual_desde_staging(repo: PostgresRepository) -> None:
    """
    Reemplaza inventario_actual únicamente cuando staging ya fue cargada completa.

    Esta es la parte crítica:
    - Si la API falla antes, esta función nunca se ejecuta.
    - Por tanto, inventario_actual no se borra por errores de API.
    """
    sql = """
    TRUNCATE TABLE public.inventario_actual;

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
    SELECT
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
    FROM public.inventario_actual_staging;

    TRUNCATE TABLE public.inventario_actual_staging;
    """

    def _tx(cur):
        cur.execute(sql)

    repo.run_in_transaction(_tx)


def insertar_lote_staging_bulk(repo: PostgresRepository, rows: list[dict]) -> int:
    """
    Inserta un lote completo en inventario_actual_staging.

    No se inserta directamente en inventario_actual porque esa tabla debe
    conservar el último snapshot válido hasta que la nueva carga termine completa.
    """
    if not rows:
        return 0

    sql = """
    INSERT INTO public.inventario_actual_staging
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


def llamar_api_inventario(
    referencias: list[str],
    timeout_sec: int = 120,
    max_reintentos: int = 2,
) -> tuple[list[dict], bool]:
    """
    Llama la API de inventario.

    Mejora importante:
    Si la API responde 500, se imprime el body de respuesta para diagnosticar.
    Eso ayuda a saber si falló SQL Server, la consulta, el DTO o el backend.
    """
    url = f"{settings.SQLSERVER_API_URL}/api/sqlserver/inventario-existencias/consultar"
    body = {
        "referenciasSku": referencias
    }

    ultimo_error = None

    for intento in range(1, max_reintentos + 2):
        try:
            r = requests.post(url, json=body, timeout=timeout_sec)

            if r.status_code >= 400:
                response_text = r.text[:2000] if r.text else ""
                raise RuntimeError(
                    f"API inventario respondió HTTP {r.status_code}. "
                    f"Intento={intento}. "
                    f"URL={url}. "
                    f"Refs={len(referencias)}. "
                    f"Respuesta={response_text}"
                )

            data = r.json()
            datos = data.get("datos") or []
            truncado = bool(data.get("truncado"))

            return datos, truncado

        except Exception as ex:
            ultimo_error = ex

            if intento <= max_reintentos:
                time.sleep(2 * intento)
                continue

            raise ultimo_error


def contar_filas_staging(repo: PostgresRepository) -> int:
    rows = repo.fetch_all("SELECT COUNT(*) AS total FROM public.inventario_actual_staging;", {})
    return int(rows[0]["total"]) if rows else 0


def contar_filas_actual(repo: PostgresRepository) -> int:
    rows = repo.fetch_all("SELECT COUNT(*) AS total FROM public.inventario_actual;", {})
    return int(rows[0]["total"]) if rows else 0


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

    asegurar_tabla_staging(repo)
    limpiar_staging(repo)
    logger.info("inventario_actual_staging truncada correctamente antes de iniciar la carga.")

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
                f"Lote size={len(batch)}. "
                "No se reemplazó inventario_actual."
            )

        filas = []
        for it in datos:
            row = mapear_datos_api(it)
            row["hash_fila"] = build_hash_fila(row)
            filas.append(row)

        ins = insertar_lote_staging_bulk(repo, filas)
        total_insertadas += ins

        dt = (datetime.now() - t0).total_seconds()
        logger.info(
            f"Lote {idx}/{total_batches}: insertadas_staging={ins} "
            f"acumulado_staging={total_insertadas} tiempo_s={dt:.2f}"
        )

    filas_staging = contar_filas_staging(repo)

    if filas_staging <= 0:
        raise SystemExit(
            "La carga terminó sin errores, pero inventario_actual_staging quedó vacía. "
            "Por seguridad NO se reemplaza inventario_actual."
        )

    logger.info(f"Staging cargada completamente. filas_staging={filas_staging}")
    logger.info("Reemplazando inventario_actual desde inventario_actual_staging...")

    reemplazar_snapshot_actual_desde_staging(repo)

    filas_actual = contar_filas_actual(repo)

    logger.info(
        f"OK inventario_actual reemplazada correctamente. "
        f"filas_actual={filas_actual} filas_staging_original={filas_staging}"
    )


if __name__ == "__main__":
    main()