# backend/tools/ingestion/maestra_tiendas_job.py

import argparse
import csv
import hashlib
from pathlib import Path
from datetime import datetime

import psycopg2
import psycopg2.extras


EXPECTED_COLUMNS = [
    "COD_BODEGA", "COD_DEPENDENCIA", "RAZON_SOCIAL", "DEPENDENCIA", "LLAVE_DEP",
    "DESC_DEPENDENCIA", "LLAVE_DEP2", "ESTADO_TIENDA", "CIUDAD", "LINEA",
    "LLAVE_NAVAL", "ESTADO_LINEA", "RANKIN_LINEA", "TESTEO_FNL?", "CLIMA",
    "ZONA", "LATITUD", "LONGITUD"
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
        INSERT INTO public.maestra_tiendas_version (nombre_archivo, hash_archivo, filas_total, estado_version)
        VALUES (%s, %s, %s, 'Inactiva')
        RETURNING id_version;
        """,
        (file_name, file_hash, filas_total),
    )
    return int(cur.fetchone()[0])


def set_version_active(cur, new_id_version: int):
    # 1) Inactivar la actual (si existe)
    cur.execute(
        """
        UPDATE public.maestra_tiendas_version
        SET estado_version = 'Inactiva'
        WHERE estado_version = 'Activa';
        """
    )
    # 2) Activar la nueva
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


def load_staging(cur, rows: list[dict]):
    payload = []
    for r in rows:
        payload.append((
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
        ))

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO public.maestra_tiendas_staging_raw (
            cod_bodega, cod_dependencia, razon_social, dependencia, llave_dep,
            desc_dependencia, llave_dep2, estado_tienda, ciudad, linea,
            llave_naval, estado_linea, rankin_linea, testeo_fnl, clima,
            zona, latitud, longitud
        )
        VALUES %s;
        """,
        payload,
        page_size=5000,
    )


def insert_filas(cur, id_version: int):
    # Inserta desde staging_raw hacia filas con el id_version nuevo.
    # Nota: aquí respetamos tus nombres y constraints existentes.
    cur.execute(
        """
        INSERT INTO public.maestra_tiendas_filas (
            id_version, llave_naval, cod_bodega, cod_dependencia, dependencia,
            desc_dependencia, ciudad, zona, clima, linea, rankin_linea,
            estado_linea, estado_tienda, testeo_fnl
        )
        SELECT
            %s AS id_version,
            COALESCE(llave_naval,'') AS llave_naval,
            COALESCE(cod_bodega,'') AS cod_bodega,
            COALESCE(cod_dependencia,'') AS cod_dependencia,
            COALESCE(dependencia,'') AS dependencia,
            COALESCE(desc_dependencia,'') AS desc_dependencia,
            ciudad,
            zona,
            clima,
            COALESCE(linea,'') AS linea,
            COALESCE(rankin_linea,'') AS rankin_linea,
            CASE
                WHEN lower(estado_linea) IN ('activa','activo') THEN 'Activo'
                ELSE 'Inactivo'
            END AS estado_linea,
            CASE
                WHEN lower(estado_tienda) IN ('activa','activo') THEN 'Activo'
                ELSE 'Inactivo'
            END AS estado_tienda,
            CASE
                WHEN lower(testeo_fnl) IN ('testeo') THEN 'Testeo'
                ELSE 'No Testeo'
            END AS testeo_fnl
        FROM public.maestra_tiendas_staging_raw
        WHERE COALESCE(llave_naval,'') <> '';
        """,
        (id_version,),
    )


def upsert_actual(cur, id_version: int):
    # Actualiza la "foto actual" por llave_naval. No borramos; hacemos UPSERT.
    cur.execute(
        """
        INSERT INTO public.maestra_tiendas_actual (
            llave_naval, cod_bodega, cod_dependencia, dependencia, ciudad, zona, clima,
            linea, estado_linea, estado_tienda, testeo_fnl, id_version_ultima
        )
        SELECT
            f.llave_naval, f.cod_bodega, f.cod_dependencia, f.dependencia, f.ciudad, f.zona, f.clima,
            f.linea, f.estado_linea, f.estado_tienda,
            CASE
                WHEN lower(f.testeo_fnl) = 'testeo' THEN 'Testeo'
                ELSE 'No testeo'
            END AS testeo_fnl,
            f.id_version
        FROM public.maestra_tiendas_filas f
        WHERE f.id_version = %s
        ON CONFLICT (llave_naval) DO UPDATE
        SET
            cod_bodega = EXCLUDED.cod_bodega,
            cod_dependencia = EXCLUDED.cod_dependencia,
            dependencia = EXCLUDED.dependencia,
            ciudad = EXCLUDED.ciudad,
            zona = EXCLUDED.zona,
            clima = EXCLUDED.clima,
            linea = EXCLUDED.linea,
            estado_linea = EXCLUDED.estado_linea,
            estado_tienda = EXCLUDED.estado_tienda,
            testeo_fnl = EXCLUDED.testeo_fnl,
            id_version_ultima = EXCLUDED.id_version_ultima,
            fecha_ultima_actualizacion = now();
        """,
        (id_version,),
    )


def read_tsv(path: Path, encoding: str) -> list[dict]:
    with path.open("r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        # Validación mínima: que existan las columnas clave
        if not reader.fieldnames:
            raise ValueError("El archivo no tiene header. Este loader requiere header.")
        return list(reader)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-dsn", required=True, help="DSN de Postgres")
    parser.add_argument("--source-file", required=True, help="Ruta al archivo MAESTRA_TIENDAS_POR_LINEA (ideal UNC)")
    parser.add_argument("--encoding", default="utf-8", help="utf-8 / cp1252 / latin-1")
    args = parser.parse_args()

    file_path = Path(args.source_file)
    if not file_path.exists():
        raise FileNotFoundError(f"No existe o no hay permisos para: {file_path}")

    file_hash = sha256_file(file_path)
    rows = read_tsv(file_path, args.encoding)
    filas_total = len(rows)

    conn = open_pg(args.pg_dsn)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            active_hash = get_active_version_hash(cur)
            if active_hash == file_hash:
                conn.rollback()
                print("NO CHANGES: la versión activa ya corresponde a este archivo (hash igual).")
                return

            new_id_version = create_new_version(cur, file_path.name, file_hash, filas_total)

            truncate_staging(cur)
            load_staging(cur, rows)

            insert_filas(cur, new_id_version)
            upsert_actual(cur, new_id_version)

            set_version_active(cur, new_id_version)

        conn.commit()
        print(f"OK: nueva versión activa id_version={new_id_version}, filas={filas_total}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
