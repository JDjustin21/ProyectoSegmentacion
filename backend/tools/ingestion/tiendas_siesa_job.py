# backend/tools/ingestion/tiendas_siesa_job.py

import argparse
import csv
import hashlib
from pathlib import Path

import psycopg2
import psycopg2.extras


EXPECTED_COLUMNS = [
    "COD_BODEGA", "RAZON_SOCIAL", "DEPENDENCIA", "COD_DEPENDENCIA", "TIPO_DEPENDENCIA",
    "GTIN_ALMACEN", "COD_SIESA", "LLAVE_DEP", "DESC_DEPENDENCIA", "CLIMA", "DEPARTAMENTO",
    "CIUDAD", "ZONA", "ZONA_EX", "FECHA_INICIO", "FECHA_FIN", "ESTADO_DEPENDENCIA",
    "LLAVE_DEP2", "ESTADO_TIENDA", "LATITUD", "LONGITUD"
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
        FROM public.tiendas_siesa_version
        WHERE estado_version = 'Activa'
        ORDER BY id_version_siesa DESC
        LIMIT 1;
        """
    )
    row = cur.fetchone()
    return row[0] if row else None


def create_new_version(cur, file_name: str, file_hash: str, filas_total: int, ruta_origen: str | None) -> int:
    cur.execute(
        """
        INSERT INTO public.tiendas_siesa_version (nombre_archivo, ruta_origen, hash_archivo, filas_total, estado_version)
        VALUES (%s, %s, %s, %s, 'Inactiva')
        RETURNING id_version_siesa;
        """,
        (file_name, ruta_origen, file_hash, filas_total),
    )
    return int(cur.fetchone()[0])


def set_version_active(cur, new_id_version: int):
    cur.execute(
        """
        UPDATE public.tiendas_siesa_version
        SET estado_version = 'Inactiva'
        WHERE estado_version = 'Activa';
        """
    )
    cur.execute(
        """
        UPDATE public.tiendas_siesa_version
        SET estado_version = 'Activa'
        WHERE id_version_siesa = %s;
        """,
        (new_id_version,),
    )


def truncate_tmp(cur):
    # staging temporal en memoria (no creamos tabla staging: MVP)
    # Si prefieres staging físico, lo hacemos después.
    pass


def read_tsv(path: Path, encoding: str) -> list[dict]:
    with path.open("r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        if not reader.fieldnames:
            raise ValueError("El archivo no tiene header. Este loader requiere header.")
        return list(reader)


def insert_filas(cur, id_version_siesa: int, rows: list[dict]):
    payload = []

    for r in rows:
        cod_bodega = (r.get("COD_BODEGA") or "").strip()
        cod_dep = (r.get("COD_DEPENDENCIA") or "").strip()
        cod_siesa = (r.get("COD_SIESA") or "").strip()
        if not cod_bodega or not cod_dep or not cod_siesa:
            # filas incompletas no sirven para el mapping
            continue

        llave_dep = f"{cod_bodega}{cod_dep}"
        llave_naval = ""

        payload.append((
            id_version_siesa,
            cod_bodega,
            cod_siesa,
            cod_dep,
            llave_dep,
            llave_naval,
            (r.get("ESTADO_TIENDA") or "").strip(),
            (r.get("DEPENDENCIA") or "").strip(),
            (r.get("DESC_DEPENDENCIA") or "").strip(),
        ))

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO public.tiendas_siesa_filas (
          id_version_siesa,
          cod_bodega, cod_siesa, cod_dependencia,
          llave_dep, llave_naval,
          estado_tienda, dependencia, desc_dependencia
        )
        VALUES %s;
        """,
        payload,
        page_size=5000,
    )

    return len(payload)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-dsn", required=True, help="DSN de Postgres")
    parser.add_argument("--source-file", required=True, help="Ruta al archivo MAESTRA_TIENDAS (con COD_SIESA)")
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

            new_id_version = create_new_version(
                cur,
                file_path.name,
                file_hash,
                filas_total,
                str(file_path),
            )

            inserted = insert_filas(cur, new_id_version, rows)

            set_version_active(cur, new_id_version)

        conn.commit()
        print(f"OK: nueva versión activa id_version_siesa={new_id_version}, filas_archivo={filas_total}, filas_insertadas={inserted}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
