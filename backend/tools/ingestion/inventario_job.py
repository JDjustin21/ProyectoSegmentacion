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

def consolidar_filas_por_hash(rows: list[dict]) -> tuple[list[dict], int]:
    """
    Consolida filas duplicadas antes de insertar en staging.

    Motivo:
    PostgreSQL no permite que un mismo INSERT con ON CONFLICT intente
    actualizar dos veces la misma clave única. En este job la clave única
    es hash_fila, que representa el grano lógico del inventario.

    Regla aplicada:
    - Si una fila llega repetida con el mismo hash_fila, se conserva una sola.
    - disponible y existencia se suman para no perder inventario cuando el SP
      devuelve varias filas del mismo producto/bodega/talla/ubicación.
    - Los demás atributos se conservan desde la primera aparición válida.
    """
    consolidadas: dict[str, dict] = {}
    duplicadas = 0

    for row in rows:
        hash_fila = norm(row.get("hash_fila"))

        if not hash_fila:
            continue

        if hash_fila not in consolidadas:
            nueva = dict(row)
            nueva["disponible"] = int(nueva.get("disponible") or 0)
            nueva["existencia"] = int(nueva.get("existencia") or 0)
            consolidadas[hash_fila] = nueva
            continue

        duplicadas += 1

        actual = consolidadas[hash_fila]

        actual["disponible"] = int(actual.get("disponible") or 0) + int(row.get("disponible") or 0)
        actual["existencia"] = int(actual.get("existencia") or 0) + int(row.get("existencia") or 0)

        # Conservamos valores descriptivos si la primera fila los traía vacíos.
        for campo in [
            "referencia_sku",
            "ean",
            "talla",
            "bodega",
            "linea",
            "codigo_siesa",
            "llave_naval",
            "cod_dependencia",
        ]:
            if not norm(actual.get(campo)) and norm(row.get(campo)):
                actual[campo] = row.get(campo)

    return list(consolidadas.values()), duplicadas


def chunk_list(arr, size):
    for i in range(0, len(arr), size):
        yield arr[i:i + size]


def get_referencias_universo(repo: PostgresRepository) -> list[str]:
    q = """
        SELECT DISTINCT referencia_sku
        FROM public.referencias_snapshot_actual
        WHERE TRIM(COALESCE(referencia_sku, '')) <> ''
        ORDER BY referencia_sku;
    """

    rows = repo.fetch_all(q, {})

    return [
        row["referencia_sku"]
        for row in rows
        if row.get("referencia_sku")
    ]


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

    Las filas deben venir sin duplicados por hash_fila. Esto evita que
    PostgreSQL intente resolver dos conflictos contra la misma clave dentro
    del mismo INSERT masivo.
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


def llamar_api_inventario_completo(
    timeout_sec: int = 300,
    max_reintentos: int = 2,
) -> tuple[list[dict], bool]:
    """
    Llama la API de inventario en modo completo.

    Este modo replica la lógica base del Excel:
    - ejecutar el SP de inventario
    - filtrar únicamente por bodegas permitidas
    - devolver el inventario completo para alimentar inventario_actual
    """
    url = f"{settings.SQLSERVER_API_URL}/api/sqlserver/inventario-existencias/consultar"

    body = {
        "modoInventarioCompleto": True,
        "maxFilas": 200000
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
                    f"ModoInventarioCompleto=True. "
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

    timeout = int(os.getenv("INV_API_TIMEOUT", "300"))

    logger.info(f"Config: MODO_INVENTARIO_COMPLETO=True INV_API_TIMEOUT={timeout}")

    asegurar_tabla_staging(repo)
    limpiar_staging(repo)
    logger.info("inventario_actual_staging truncada correctamente antes de iniciar la carga.")

    t0 = datetime.now()
    logger.info("Llamando API de inventario en modo completo...")

    datos, truncado = llamar_api_inventario_completo(timeout_sec=timeout)

    logger.info(
        f"API inventario completo: filas={len(datos)} truncado={truncado}"
    )

    if truncado:
        raise SystemExit(
            "API devolvió truncado=true en modo inventario completo. "
            "Aumenta maxFilas en el body de la API o revisa el volumen real antes de reemplazar inventario_actual."
        )

    filas = []

    for it in datos:
        row = mapear_datos_api(it)
        row["hash_fila"] = build_hash_fila(row)
        filas.append(row)

    filas_consolidadas, duplicadas = consolidar_filas_por_hash(filas)

    logger.info(
        f"Consolidación inventario: filas_api={len(filas)} "
        f"filas_consolidadas={len(filas_consolidadas)} "
        f"duplicadas={duplicadas}"
    )

    total_insertadas = insertar_lote_staging_bulk(repo, filas_consolidadas)

    dt = (datetime.now() - t0).total_seconds()

    logger.info(
        f"Inventario completo insertado en staging. "
        f"insertadas_staging={total_insertadas} tiempo_s={dt:.2f}"
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