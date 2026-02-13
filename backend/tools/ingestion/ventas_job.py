#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import hashlib
import os
import sys
from datetime import datetime
from typing import List, Tuple, Optional

import psycopg2
import psycopg2.extras


def compute_sha256(file_path: str, chunk_size: int) -> str:
    """
    Calcula hash SHA-256 del archivo para detectar cambios.
    chunk_size se deja configurable para no hardcodear comportamiento.
    """
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def norm_header_cell(v: str) -> str:
    """
    Normaliza encabezados:
    - trim
    - uppercase
    """
    return (v or "").strip().upper()


def parse_tsv_lines(
    file_path: str,
    encoding: str,
    delimiter: str,
    expected_headers: List[str],
) -> Tuple[List[str], int]:
    """
    Lee header y valida que tenga TODAS las columnas esperadas.
    Retorna: (headers_en_archivo_normalizados, numero_columnas)
    """
    with open(file_path, "r", encoding=encoding, errors="replace") as f:
        first_line = f.readline()
        if not first_line:
            raise ValueError("El archivo está vacío; no hay encabezado.")

        raw_headers = [norm_header_cell(x) for x in first_line.rstrip("\n\r").split(delimiter)]
        if len(raw_headers) < 2:
            raise ValueError("Encabezado inválido: parece que el separador/delimitador no es correcto.")

        expected_set = set(norm_header_cell(x) for x in expected_headers)
        got_set = set(raw_headers)

        missing = sorted(list(expected_set - got_set))
        if missing:
            raise ValueError(f"Faltan columnas en el header del TXT: {missing}")

        return raw_headers, len(raw_headers)


def build_header_index(headers_in_file: List[str]) -> dict:
    """
    Crea un diccionario {NOMBRE_COLUMNA_NORMALIZADO: índice}
    para mapear valores por nombre sin depender del orden.
    """
    idx = {}
    for i, h in enumerate(headers_in_file):
        if h and h not in idx:
            idx[h] = i
    return idx


def safe_get(parts: List[str], idx: Optional[int]) -> str:
    """
    Obtiene una celda por índice.
    Si no existe, devuelve ''.
    """
    if idx is None:
        return ""
    if idx < 0 or idx >= len(parts):
        return ""
    return (parts[idx] or "").strip()


def get_active_version_hash(cur) -> Optional[str]:
    """
    Retorna hash_archivo de la versión activa, si existe.
    """
    cur.execute("""
        SELECT hash_archivo
        FROM public.ventas_version_archivo
        WHERE estado_version = 'Activa'
        LIMIT 1
    """)
    row = cur.fetchone()
    return row[0] if row else None


def insert_new_version(cur, nombre_archivo: str, ruta_origen: str, hash_archivo: str, id_usuario: Optional[int]) -> int:
    """
    Inserta versión nueva en estado Inactiva. Devuelve id_version_ventas.
    """
    cur.execute("""
        INSERT INTO public.ventas_version_archivo
            (nombre_archivo, ruta_origen, hash_archivo, estado_version, id_usuario_carga)
        VALUES
            (%s, %s, %s, 'Inactiva', %s)
        RETURNING id_version_ventas
    """, (nombre_archivo, ruta_origen, hash_archivo, id_usuario))
    return int(cur.fetchone()[0])


def update_version_rowcount(cur, id_version_ventas: int, filas_total: int) -> None:
    """
    Actualiza conteo de filas cargadas.
    """
    cur.execute("""
        UPDATE public.ventas_version_archivo
        SET filas_total = %s
        WHERE id_version_ventas = %s
    """, (filas_total, id_version_ventas))


def activate_version(cur, id_version_ventas: int) -> None:
    """
    Deja activa la nueva versión y desactiva las demás.
    """
    cur.execute("""
        UPDATE public.ventas_version_archivo
        SET estado_version = 'Inactiva'
        WHERE estado_version = 'Activa'
          AND id_version_ventas <> %s
    """, (id_version_ventas,))

    cur.execute("""
        UPDATE public.ventas_version_archivo
        SET estado_version = 'Activa'
        WHERE id_version_ventas = %s
    """, (id_version_ventas,))


def load_staging_raw(
    cur,
    file_path: str,
    encoding: str,
    delimiter: str,
    headers_in_file: List[str],
    id_version_ventas: int,
    batch_size: int,
) -> int:
    """
    Inserta filas en ventas_staging_raw con batch.
    Retorna total de filas (sin header).
    """
    idx = build_header_index(headers_in_file)

    # Mapeo por nombre para staging_raw (debe coincidir con tu header real)
    col = lambda name: idx.get(norm_header_cell(name))

    # Lista ordenada de columnas staging (sin id_version_ventas/numero_fila)
    # (Nombres entendibles, 1 a 1 con tu tabla ventas_staging_raw)
    staging_cols = [
        ("origen", col("ORIGEN")),
        ("cod_dependencia", col("COD_DEPENDENCIA")),
        ("dep_destino", col("DEP_DESTINO")),
        ("desc_dep_destino", col("DESC_DEP_DESTINO")),
        ("plu", col("PLU")),
        ("ean", col("EAN")),
        ("fecha_mvto", col("FECHA_MVTO")),
        ("desc_movimiento", col("DESC_MOVIMIENTO")),
        ("signo", col("SIGNO")),
        ("cantidad", col("CANTIDAD")),
        ("fecha_prod", col("FECHA_PROD")),
        ("reproceso_vtas", col("REPROCESO_VTAS")),
        ("dependencia", col("DEPENDENCIA")),
        ("cod_bodega", col("COD_BODEGA")),
        ("razon_social", col("RAZON_SOCIAL")),
        ("tipo_dependencia", col("TIPO_DEPENDENCIA")),
        ("gtin_almacen", col("GTIN_ALMACEN")),
        ("cod_siesa", col("COD_SIESA")),
        ("desc_dependencia", col("DESC_DEPENDENCIA")),
        ("clima", col("CLIMA")),
        ("departamento", col("DEPARTAMENTO")),
        ("ciudad", col("CIUDAD")),
        ("zona", col("ZONA")),
        ("zona_ex", col("ZONA_EX")),
        ("llave_dep2", col("LLAVE_DEP2")),
        ("estado_tienda", col("ESTADO_TIENDA")),
        ("llave_dep", col("LLAVE_DEP")),
        ("referencia", col("REFERENCIA")),
        ("desc_item", col("DESC_ITEM")),
        ("cod_color", col("COD_COLOR")),
        ("color", col("COLOR")),
        ("talla", col("TALLA")),
        ("linea_gen", col("LINEA_GEN")),
        ("linea_detll", col("LINEA_DETLL")),
        ("estilo_item", col("ESTILO_ITEM")),
        ("grupo", col("GRUPO")),
        ("linea", col("LINEA")),
        ("marca", col("MARCA")),
        ("tipo_de_negocio", col("TIPO_DE_NEGOCIO")),
        ("cuento", col("CUENTO")),
        ("tipo_portafolio_mod", col("TIPO_PORTAFOLIO_MOD")),
        ("fch_act_portafolio", col("FCH_ACT_PORTAFOLIO")),
        ("estado_sku_mod", col("ESTADO_SKU_MOD")),
        ("fch_act_sku", col("FCH_ACT_SKU")),
        ("perfil_prenda", col("PERFIL_PRENDA")),
        ("cambio_portafolio", col("CAMBIO_PORTAFOLIO?")),
        ("pvp", col("PVP")),
        ("pvp_lista", col("PVP LISTA")),
        ("pvp_hist", col("PVP HIST")),
        ("pvp_hist_lista", col("PVP HIST LISTA")),
        ("venta_pvp_lista", col("VENTA $ PVP LISTA")),
        ("venta_pvp_hist_lista", col("VENTA $ PVP HIST LISTA")),
        ("desc_grupo", col("DESC_GRUPO")),
        ("modelo", col("MODELO")),
        ("linea_my", col("LINEA_MY")),
        ("llave_naval", col("LLAVE_NAVAL")),
        ("estado_linea", col("ESTADO_LINEA")),
        ("anio", col("Año")),
        ("mes", col("Mes")),
        ("tipo_portafolio_mod_2", col("TIPO_PORTAFOLIO_MOD_2")),
    ]

    insert_sql = """
        INSERT INTO public.ventas_staging_raw (
            id_version_ventas, numero_fila,
            origen, cod_dependencia, dep_destino, desc_dep_destino, plu, ean,
            fecha_mvto, desc_movimiento, signo, cantidad, fecha_prod, reproceso_vtas,
            dependencia, cod_bodega, razon_social, tipo_dependencia, gtin_almacen, cod_siesa,
            desc_dependencia, clima, departamento, ciudad, zona, zona_ex, llave_dep2,
            estado_tienda, llave_dep, referencia, desc_item, cod_color, color, talla,
            linea_gen, linea_detll, estilo_item, grupo, linea, marca, tipo_de_negocio,
            cuento, tipo_portafolio_mod, fch_act_portafolio, estado_sku_mod, fch_act_sku,
            perfil_prenda, cambio_portafolio, pvp, pvp_lista, pvp_hist, pvp_hist_lista,
            venta_pvp_lista, venta_pvp_hist_lista, desc_grupo, modelo, linea_my,
            llave_naval, estado_linea, anio, mes, tipo_portafolio_mod_2
        ) VALUES %s
    """

    total_rows = 0
    batch: List[Tuple] = []

    with open(file_path, "r", encoding=encoding, errors="replace") as f:
        _ = f.readline()  # consumir header

        for line_no, line in enumerate(f, start=1):
            parts = line.rstrip("\n\r").split(delimiter)

            row_values = []
            for _, idx_col in staging_cols:
                row_values.append(safe_get(parts, idx_col))

            batch.append((id_version_ventas, line_no, *row_values))
            total_rows += 1

            if len(batch) >= batch_size:
                psycopg2.extras.execute_values(cur, insert_sql, batch, page_size=batch_size)
                batch.clear()

    if batch:
        psycopg2.extras.execute_values(cur, insert_sql, batch, page_size=batch_size)

    return total_rows


def stage_to_final(cur, id_version_ventas: int) -> int:
    """
    Convierte staging_raw -> ventas_movimientos.
    Retorna cantidad insertada (aproximación por rowcount de cursor).
    """
    cur.execute("""
        INSERT INTO public.ventas_movimientos (
            id_version_ventas,
            fecha_movimiento,
            descripcion_movimiento,
            signo,
            cantidad,
            llave_naval,
            cod_bodega,
            cod_dependencia,
            dependencia,
            desc_dependencia,
            ciudad,
            zona,
            clima,
            estado_tienda,
            estado_linea,
            referencia,
            cod_color,
            color,
            talla,
            ean,
            plu,
            linea,
            cuento,
            tipo_portafolio,
            estado_sku,
            pvp,
            pvp_lista,
            pvp_hist,
            pvp_hist_lista,
            venta_pvp_lista,
            venta_pvp_hist_lista,
            hash_fila
        )
        SELECT
            sr.id_version_ventas,
            CASE
                WHEN NULLIF(trim(sr.fecha_mvto), '') IS NULL THEN NULL
                ELSE to_date(trim(sr.fecha_mvto), 'DD/MM/YYYY')
            END AS fecha_movimiento,
            NULLIF(trim(sr.desc_movimiento), '') AS descripcion_movimiento,
            CASE
                WHEN NULLIF(trim(sr.signo), '') IS NULL THEN NULL
                ELSE left(trim(sr.signo), 1)
            END AS signo,
            COALESCE(NULLIF(trim(sr.cantidad), '')::int, 0) AS cantidad,

            NULLIF(trim(sr.llave_naval), '') AS llave_naval,
            NULLIF(trim(sr.cod_bodega), '') AS cod_bodega,
            NULLIF(trim(sr.cod_dependencia), '') AS cod_dependencia,
            NULLIF(trim(sr.dependencia), '') AS dependencia,
            NULLIF(trim(sr.desc_dependencia), '') AS desc_dependencia,
            NULLIF(trim(sr.ciudad), '') AS ciudad,
            NULLIF(trim(sr.zona), '') AS zona,
            NULLIF(trim(sr.clima), '') AS clima,
            NULLIF(trim(sr.estado_tienda), '') AS estado_tienda,
            NULLIF(trim(sr.estado_linea), '') AS estado_linea,

            NULLIF(trim(sr.referencia), '') AS referencia,
            NULLIF(trim(sr.cod_color), '') AS cod_color,
            NULLIF(trim(sr.color), '') AS color,
            NULLIF(trim(sr.talla), '') AS talla,
            NULLIF(trim(sr.ean), '') AS ean,
            NULLIF(trim(sr.plu), '') AS plu,

            NULLIF(trim(sr.linea), '') AS linea,
            NULLIF(trim(sr.cuento), '') AS cuento,
            NULLIF(trim(sr.tipo_portafolio_mod), '') AS tipo_portafolio,
            NULLIF(trim(sr.estado_sku_mod), '') AS estado_sku,

            NULLIF(trim(sr.pvp), '')::numeric(18,2) AS pvp,
            NULLIF(trim(sr.pvp_lista), '')::numeric(18,2) AS pvp_lista,
            NULLIF(trim(sr.pvp_hist), '')::numeric(18,2) AS pvp_hist,
            NULLIF(trim(sr.pvp_hist_lista), '')::numeric(18,2) AS pvp_hist_lista,
            NULLIF(trim(sr.venta_pvp_lista), '')::numeric(18,2) AS venta_pvp_lista,
            NULLIF(trim(sr.venta_pvp_hist_lista), '')::numeric(18,2) AS venta_pvp_hist_lista,

            md5(
                concat_ws('|',
                    trim(coalesce(sr.origen,'')),
                    trim(coalesce(sr.cod_dependencia,'')),
                    trim(coalesce(sr.ean,'')),
                    trim(coalesce(sr.fecha_mvto,'')),
                    trim(coalesce(sr.desc_movimiento,'')),
                    trim(coalesce(sr.signo,'')),
                    trim(coalesce(sr.cantidad,'')),
                    trim(coalesce(sr.llave_naval,'')),
                    trim(coalesce(sr.referencia,'')),
                    trim(coalesce(sr.cod_color,'')),
                    trim(coalesce(sr.talla,''))
                )
            ) AS hash_fila
        FROM public.ventas_staging_raw sr
        WHERE sr.id_version_ventas = %s
        ON CONFLICT (hash_fila) DO NOTHING
    """, (id_version_ventas,))

    # rowcount en INSERT puede ser aproximado según driver/plan, pero sirve como indicador
    return int(cur.rowcount) if cur.rowcount is not None else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Carga TXT de ventas (versionado + staging + final).")
    parser.add_argument("--pg-dsn", required=True, help="DSN de Postgres (ej: dbname=... user=... host=... port=...)")
    parser.add_argument("--source-file", required=True, help="Ruta del TXT (preferible UNC).")
    parser.add_argument("--encoding", default="cp1252", help="Encoding del TXT (default cp1252).")
    parser.add_argument("--delimiter", default="\t", help="Delimitador (default TAB).")
    parser.add_argument("--batch-size", type=int, default=5000, help="Tamaño de lote para inserción staging.")
    parser.add_argument("--hash-chunk-size", type=int, default=1024 * 1024, help="Chunk size para hash (bytes).")
    parser.add_argument("--user-id", type=int, default=None, help="id_usuario que ejecuta la carga (opcional).")

    args = parser.parse_args()

    source_file = args.source_file
    if not os.path.exists(source_file):
        print(f"ERROR: No existe el archivo: {source_file}", file=sys.stderr)
        return 2

    expected_headers = [
        "ORIGEN","COD_DEPENDENCIA","DEP_DESTINO","DESC_DEP_DESTINO","PLU","EAN","FECHA_MVTO","DESC_MOVIMIENTO","SIGNO",
        "CANTIDAD","FECHA_PROD","REPROCESO_VTAS","DEPENDENCIA","COD_BODEGA","RAZON_SOCIAL","TIPO_DEPENDENCIA",
        "GTIN_ALMACEN","COD_SIESA","DESC_DEPENDENCIA","CLIMA","DEPARTAMENTO","CIUDAD","ZONA","ZONA_EX","LLAVE_DEP2",
        "ESTADO_TIENDA","LLAVE_DEP","REFERENCIA","DESC_ITEM","COD_COLOR","COLOR","TALLA","LINEA_GEN","LINEA_DETLL",
        "ESTILO_ITEM","GRUPO","LINEA","MARCA","TIPO_DE_NEGOCIO","CUENTO","TIPO_PORTAFOLIO_MOD","FCH_ACT_PORTAFOLIO",
        "ESTADO_SKU_MOD","FCH_ACT_SKU","PERFIL_PRENDA","CAMBIO_PORTAFOLIO?","PVP","PVP LISTA","PVP HIST",
        "PVP HIST LISTA","VENTA $ PVP LISTA","VENTA $ PVP HIST LISTA","DESC_GRUPO","MODELO","LINEA_MY","LLAVE_NAVAL",
        "ESTADO_LINEA","Año","Mes","TIPO_PORTAFOLIO_MOD_2"
    ]

    headers_in_file, _ = parse_tsv_lines(
        file_path=source_file,
        encoding=args.encoding,
        delimiter=args.delimiter,
        expected_headers=expected_headers,
    )

    file_hash = compute_sha256(source_file, args.hash_chunk_size)

    nombre_archivo = os.path.basename(source_file)
    ruta_origen = os.path.dirname(source_file)

    conn = psycopg2.connect(args.pg_dsn)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            active_hash = get_active_version_hash(cur)
            if active_hash and active_hash == file_hash:
                print("NO CHANGES: la versión activa ya corresponde a este archivo (hash igual).")
                conn.rollback()
                return 0

            new_id = insert_new_version(cur, nombre_archivo, ruta_origen, file_hash, args.user_id)

            filas = load_staging_raw(
                cur=cur,
                file_path=source_file,
                encoding=args.encoding,
                delimiter=args.delimiter,
                headers_in_file=headers_in_file,
                id_version_ventas=new_id,
                batch_size=args.batch_size,
            )

            update_version_rowcount(cur, new_id, filas)

            inserted_final = stage_to_final(cur, new_id)

            activate_version(cur, new_id)

            conn.commit()
            print(f"OK: nueva versión activa id_version_ventas={new_id}, filas_staging={filas}, filas_final_insertadas~={inserted_final}")
            return 0

    except Exception as ex:
        conn.rollback()
        print(f"ERROR: {ex}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
