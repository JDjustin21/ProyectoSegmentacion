# backend/tools/ingestion/maestra_tiendas_job.py

import argparse
import csv
import hashlib
from pathlib import Path

import psycopg2
import psycopg2.extras


EXPECTED_COLUMNS = [
    "COD_BODEGA",
    "COD_DEPENDENCIA",
    "RAZON_SOCIAL",
    "DEPENDENCIA",
    "LLAVE_DEP",
    "DESC_DEPENDENCIA",
    "LLAVE_DEP2",
    "ESTADO_TIENDA",
    "CIUDAD",
    "LINEA",
    "LLAVE_NAVAL",
    "ESTADO_LINEA",
    "RANKIN_LINEA",
    "TESTEO_FNL?",
    "CLIMA",
    "ZONA",
    "LATITUD",
    "LONGITUD",
]


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def open_pg(dsn: str):
    return psycopg2.connect(dsn)


def get_active_version_hash(cur) -> str | None:
    cur.execute(
        """
        SELECT hash_archivo
        FROM public.maestra_tiendas_version
        WHERE estado_version = 'Activa'
        LIMIT 1;
        """
    )
    row = cur.fetchone()
    return row[0] if row else None


def create_new_version(cur, file_name: str, file_hash: str, filas_total: int) -> int:
    cur.execute(
        """
        INSERT INTO public.maestra_tiendas_version (
            nombre_archivo,
            hash_archivo,
            filas_total,
            estado_version
        )
        VALUES (%s, %s, %s, 'Inactiva')
        RETURNING id_version;
        """,
        (file_name, file_hash, filas_total),
    )
    return int(cur.fetchone()[0])


def set_version_active(cur, new_id_version: int):
    cur.execute(
        """
        UPDATE public.maestra_tiendas_version
        SET estado_version = 'Inactiva'
        WHERE estado_version = 'Activa';
        """
    )

    cur.execute(
        """
        UPDATE public.maestra_tiendas_version
        SET estado_version = 'Activa'
        WHERE id_version = %s;
        """,
        (new_id_version,),
    )


def truncate_staging(cur):
    cur.execute("TRUNCATE TABLE public.maestra_tiendas_staging_raw;")


def truncate_filas(cur):
    cur.execute("TRUNCATE TABLE public.maestra_tiendas_filas;")


def load_staging(cur, rows: list[dict]):
    payload = []
    for r in rows:
        payload.append(
            (
                r.get("COD_BODEGA"),
                r.get("COD_DEPENDENCIA"),
                r.get("RAZON_SOCIAL"),
                r.get("DEPENDENCIA"),
                r.get("LLAVE_DEP"),
                r.get("DESC_DEPENDENCIA"),
                r.get("LLAVE_DEP2"),
                r.get("ESTADO_TIENDA"),
                r.get("CIUDAD"),
                r.get("LINEA"),
                r.get("LLAVE_NAVAL"),
                r.get("ESTADO_LINEA"),
                r.get("RANKIN_LINEA"),
                r.get("TESTEO_FNL?"),
                r.get("CLIMA"),
                r.get("ZONA"),
                r.get("LATITUD"),
                r.get("LONGITUD"),
            )
        )

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO public.maestra_tiendas_staging_raw (
            cod_bodega,
            cod_dependencia,
            razon_social,
            dependencia,
            llave_dep,
            desc_dependencia,
            llave_dep2,
            estado_tienda,
            ciudad,
            linea,
            llave_naval,
            estado_linea,
            rankin_linea,
            testeo_fnl,
            clima,
            zona,
            latitud,
            longitud
        )
        VALUES %s;
        """,
        payload,
        page_size=5000,
    )


def validate_headers(rows: list[dict]):
    if not rows:
        raise ValueError("El archivo no contiene filas de datos.")

    headers = set(rows[0].keys())
    missing = [col for col in EXPECTED_COLUMNS if col not in headers]
    if missing:
        raise ValueError(
            f"El archivo no contiene todas las columnas esperadas. Faltan: {missing}"
        )


def validate_staging(cur):
    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.maestra_tiendas_staging_raw;
        """
    )
    total_staging = int(cur.fetchone()[0])

    if total_staging == 0:
        raise ValueError("Staging quedó vacío. Se cancela la carga.")

    cur.execute(
        """
        SELECT COUNT(*)
        FROM public.maestra_tiendas_staging_raw
        WHERE COALESCE(TRIM(llave_naval), '') <> '';
        """
    )
    total_con_llave = int(cur.fetchone()[0])

    if total_con_llave == 0:
        raise ValueError("Ninguna fila en staging tiene llave_naval. Se cancela la carga.")


def insert_filas(cur):
    cur.execute(
        """
        INSERT INTO public.maestra_tiendas_filas (
            llave_naval,
            cod_bodega,
            cod_dependencia,
            dependencia,
            desc_dependencia,
            ciudad,
            zona,
            clima,
            linea,
            rankin_linea,
            estado_linea,
            estado_tienda,
            testeo_fnl,
            llave_dep2
        )
        SELECT
            COALESCE(TRIM(llave_naval), '') AS llave_naval,
            COALESCE(TRIM(cod_bodega), '') AS cod_bodega,
            COALESCE(TRIM(cod_dependencia), '') AS cod_dependencia,
            COALESCE(TRIM(dependencia), '') AS dependencia,
            COALESCE(TRIM(desc_dependencia), '') AS desc_dependencia,
            NULLIF(TRIM(ciudad), '') AS ciudad,
            NULLIF(TRIM(zona), '') AS zona,
            NULLIF(TRIM(clima), '') AS clima,
            COALESCE(TRIM(linea), '') AS linea,
            COALESCE(TRIM(rankin_linea), '') AS rankin_linea,
            CASE
                WHEN LOWER(COALESCE(TRIM(estado_linea), '')) IN ('activa', 'activo')
                    THEN 'Activo'
                ELSE 'Inactivo'
            END AS estado_linea,
            CASE
                WHEN LOWER(COALESCE(TRIM(estado_tienda), '')) IN ('activa', 'activo')
                    THEN 'Activo'
                ELSE 'Inactivo'
            END AS estado_tienda,
            CASE
                WHEN LOWER(COALESCE(TRIM(testeo_fnl), '')) = 'testeo'
                    THEN 'Testeo'
                ELSE 'No Testeo'
            END AS testeo_fnl,
            COALESCE(TRIM(llave_dep2), '') AS llave_dep2
        FROM public.maestra_tiendas_staging_raw
        WHERE COALESCE(TRIM(llave_naval), '') <> '';
        """
    )


def read_tsv(path: Path, encoding: str) -> list[dict]:
    with path.open("r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")

        if not reader.fieldnames:
            raise ValueError("El archivo no tiene header. Este loader requiere header.")

        return list(reader)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-dsn", required=True, help="DSN de Postgres")
    parser.add_argument(
        "--source-file",
        required=True,
        help="Ruta al archivo MAESTRA_TIENDAS_POR_LINEA (ideal UNC)",
    )
    parser.add_argument("--encoding", default="utf-8", help="utf-8 / cp1252 / latin-1")
    args = parser.parse_args()

    file_path = Path(args.source_file)
    if not file_path.exists():
        raise FileNotFoundError(f"No existe o no hay permisos para: {file_path}")

    file_hash = sha256_file(file_path)
    rows = read_tsv(file_path, args.encoding)
    filas_total = len(rows)

    validate_headers(rows)

    conn = open_pg(args.pg_dsn)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            active_hash = get_active_version_hash(cur)
            if active_hash == file_hash:
                conn.rollback()
                print("NO CHANGES: la versión activa ya corresponde a este archivo (hash igual).")
                return

            new_id_version = create_new_version(
                cur,
                file_path.name,
                file_hash,
                filas_total,
            )

            truncate_staging(cur)
            load_staging(cur, rows)
            validate_staging(cur)

            truncate_filas(cur)
            insert_filas(cur)

            set_version_active(cur, new_id_version)

        conn.commit()
        print(
            f"OK: carga completada. "
            f"id_version={new_id_version}, filas_archivo={filas_total}"
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()