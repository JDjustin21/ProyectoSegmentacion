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

# Configuración del logger
def setup_logger(job_name: str = "inventario_movimiento_job") -> logging.Logger:
    backend_dir = Path(__file__).resolve().parents[2]  # Ajuste para resolver la ruta correctamente
    logs_dir = backend_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_file = logs_dir / f"{job_name}_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

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


# Función para normalizar datos
def norm(v: str) -> str:
    return (v or "").strip()


# Función para generar el hash de la fila (para asegurar la idempotencia)
def md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# Función para crear el hash de cada fila de movimiento
def build_hash_fila(row: dict) -> str:
    parts = [
        norm(row.get("referencia_sku")),
        norm(row.get("talla")),
        norm(row.get("bodega")),
        str(row.get("fecha_movimiento") or ""),
        norm(row.get("tipo_movimiento")),
        str(row.get("cantidad_entrada") or 0),
        str(row.get("cantidad_salida") or 0),
        norm(row.get("docto")),
        norm(row.get("codigo_barra")),
    ]
    return md5("|".join(parts).upper())


# Función para dividir en lotes
def chunk_list(arr, size):
    for i in range(0, len(arr), size):
        yield arr[i:i + size]


# Obtener las referencias del universo (referencias que han tenido movimientos en los últimos 120 días)
def get_referencias_universo(repo: PostgresRepository) -> list[str]:
    q = """
    SELECT DISTINCT referencia_sku
    FROM public.vw_ventas_movimientos_normalizados
    WHERE fecha_movimiento >= (CURRENT_DATE - INTERVAL '300 days')
      AND TRIM(COALESCE(referencia_sku,'')) <> '';
    """
    rows = repo.fetch_all(q, {})
    return [r["referencia_sku"] for r in rows if r.get("referencia_sku")]


# Crear una nueva versión para los movimientos de inventario
def crear_version(repo: PostgresRepository, observaciones: str = "") -> int:
    row = repo.fetch_one("""
        INSERT INTO public.inventario_movimientos_version (estado_version, observaciones)
        VALUES ('Inactiva', %(obs)s)
        RETURNING id_version_movimiento;
    """, {"obs": observaciones})
    return int(row["id_version_movimiento"])


# Marcar la versión activa (para los movimientos de inventario)
def marcar_version_activa(repo: PostgresRepository, id_version: int, filas_total: int):
    repo.execute("""
        UPDATE public.inventario_movimientos_version
        SET estado_version='Inactiva'
        WHERE estado_version='Activa';
    """, {})

    repo.execute("""
        UPDATE public.inventario_movimientos_version
        SET estado_version='Activa',
            filas_total=%(filas)s
        WHERE id_version_movimiento=%(id)s;
    """, {"filas": int(filas_total), "id": int(id_version)})


# Insertar los datos de los movimientos de inventario en la tabla
def insertar_lote_bulk(repo: PostgresRepository, id_version: int, rows: list[dict]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO public.inventario_movimientos
    (
      id_version_movimiento,
      fecha_movimiento, tipo_movimiento,
      cantidad_entrada, cantidad_salida, cantidad_neta,
      referencia_sku, talla, bodega, cod_siesa, hash_fila, codigo_barra
    )
    VALUES %s
    """

    values = []
    for r in rows:
        # fecha_movimiento
        fecha_movimiento_str = r.get("fecha_movimiento")
        fecha_movimiento = datetime.fromisoformat(fecha_movimiento_str) if fecha_movimiento_str else None
        values.append((
            int(id_version),
            fecha_movimiento,
            norm(r.get("tipo_movimiento")),
            float(r.get("cantidad_entrada") or 0),
            float(r.get("cantidad_salida") or 0),
            float(r.get("cantidad_neta") or 0),
            norm(r.get("referencia_sku")),
            norm(r.get("talla")),
            norm(r.get("bodega")),
            norm(r.get("codigo_siesa")),
            norm(r.get("hash_fila")),
            norm(r.get("codigo_barra")),
        ))

    def _tx(cur):
        template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        execute_values(cur, sql, values, template=template, page_size=2000)

    repo.run_in_transaction(_tx)
    return len(rows)


# Función para llamar a la API de los movimientos de inventario
def llamar_api_movimiento(referencias: list[str], timeout_sec: int = 120) -> tuple[list[dict], bool]:
    url = f"{settings.SQLSERVER_API_URL}/api/sqlserver/inventario/consultar"
    body = {
        "referenciasSku": referencias,
    }

    r = requests.post(url, json=body, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json()

    datos = data.get("datos") or []
    truncado = bool(data.get("truncado"))
    return datos, truncado


# Función principal que ejecuta el proceso
def main():
    logger = setup_logger("inventario_movimiento_job")

    repo = PostgresRepository(settings.POSTGRES_DSN)

    observ = f"Job movimientos de inventario {datetime.now().isoformat()}"
    id_version = crear_version(repo, observaciones=observ)
    logger.info(f"Version creada (Inactiva): id_version_movimiento={id_version}")

    refs = get_referencias_universo(repo)
    logger.info(f"Universo referencias: {len(refs)}")

    if not refs:
        raise SystemExit("No hay referencias en el universo. Revisa criterio de universo.")

    batch_size = int(os.getenv("INV_BATCH_SIZE", "200"))
    timeout = int(os.getenv("INV_API_TIMEOUT", "120"))

    logger.info(f"Config: INV_BATCH_SIZE={batch_size} INV_API_TIMEOUT={timeout}")

    total_insertadas = 0
    total_batches = math.ceil(len(refs) / batch_size)

    for idx, batch in enumerate(chunk_list(refs, batch_size), start=1):
        t0 = datetime.now()
        logger.info(f"Lote {idx}/{total_batches}: refs={len(batch)} -> llamando API...")

        datos, truncado = llamar_api_movimiento(batch, timeout_sec=timeout)
        logger.info(f"Lote {idx}/{total_batches}: API filas={len(datos)} truncado={truncado}")

        if truncado:
            raise SystemExit(
                f"API devolvió truncado=true. Reduce INV_BATCH_SIZE"
                f"Lote size={len(batch)}"
            )

        filas = []
        for it in datos:
            row = {
                "fecha_movimiento": it.get("fechaMov"),
                "tipo_movimiento": norm(it.get("tipoMovimiento")).upper(),
                "referencia_sku": norm(it.get("referenciaSku")),
                "talla": norm(it.get("talla")),
                "bodega": norm(it.get("bodega")),
                "codigo_siesa": norm(it.get("codigoSiesa")),
                "cantidad_entrada": float(it.get("cantidadEntrada") or 0),
                "cantidad_salida": float(it.get("cantidadSalida") or 0),
                "cantidad_neta": float(it.get("cantidadNeta") or 0),
                "docto": norm(it.get("docto")),
                "codigo_barra": norm(it.get("codigoBarra")),
            }
            row["hash_fila"] = build_hash_fila(row)
            filas.append(row)

           

        ins = insertar_lote_bulk(repo, id_version, filas)
        total_insertadas += ins

        dt = (datetime.now() - t0).total_seconds()
        logger.info(f"Lote {idx}/{total_batches}: insertadas={ins} acumulado={total_insertadas} tiempo_s={dt:.2f}")

    marcar_version_activa(repo, id_version, total_insertadas)
    logger.info(f"Version marcada Activa: id_version_movimiento={id_version} filas_total={total_insertadas}")

    logger.info(f"OK movimientos de inventario. version={id_version} filas={total_insertadas}")


if __name__ == "__main__":
    main()