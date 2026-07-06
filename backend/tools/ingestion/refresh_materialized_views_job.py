from __future__ import annotations

import logging
import sys
import time
from pathlib import Path


# Permite ejecutar este job desde .bat, Programador de tareas o consola
# sin depender del directorio actual de ejecución.
ROOT_DIR = Path(__file__).resolve().parents[3]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from backend.config.settings import POSTGRES_DSN
from backend.repositories.postgres_repository import PostgresRepository


def setup_logger(name: str = "refresh_materialized_views_job") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def ejecutar_sql(
    repo: PostgresRepository,
    sql: str,
    descripcion: str,
    logger: logging.Logger,
) -> None:
    inicio = time.perf_counter()

    logger.info("INICIO | %s", descripcion)
    repo.execute(sql, {})

    duracion = time.perf_counter() - inicio
    logger.info("OK | %s | %.2f segundos", descripcion, duracion)


def main() -> int:
    logger = setup_logger()

    logger.info("====================================================")
    logger.info("INICIO refresh materialized views")
    logger.info("POSTGRES_DSN configurado: %s", "SI" if POSTGRES_DSN else "NO")

    if not POSTGRES_DSN:
        logger.error("Falta POSTGRES_DSN")
        return 10

    repo = PostgresRepository(POSTGRES_DSN)

    try:
        ejecutar_sql(
            repo,
            "REFRESH MATERIALIZED VIEW public.mv_inventario_base_detalle;",
            "REFRESH mv_inventario_base_detalle",
            logger,
        )

        ejecutar_sql(
            repo,
            "ANALYZE public.mv_inventario_base_detalle;",
            "ANALYZE mv_inventario_base_detalle",
            logger,
        )

        ejecutar_sql(
            repo,
            "REFRESH MATERIALIZED VIEW public.mv_inventario_resumen_referencia;",
            "REFRESH mv_inventario_resumen_referencia",
            logger,
        )

        ejecutar_sql(
            repo,
            "ANALYZE public.mv_inventario_resumen_referencia;",
            "ANALYZE mv_inventario_resumen_referencia",
            logger,
        )

        ejecutar_sql(
            repo,
            "REFRESH MATERIALIZED VIEW public.mv_metricas_existencia_por_talla;",
            "REFRESH mv_metricas_existencia_por_talla",
            logger,
        )

        ejecutar_sql(
            repo,
            "ANALYZE public.mv_metricas_existencia_por_talla;",
            "ANALYZE mv_metricas_existencia_por_talla",
            logger,
        )

        ejecutar_sql(
            repo,
            "REFRESH MATERIALIZED VIEW public.mv_analiticas_agotados_base;",
            "REFRESH mv_analiticas_agotados_base",
            logger,
        )

        ejecutar_sql(
            repo,
            "ANALYZE public.mv_analiticas_agotados_base;",
            "ANALYZE mv_analiticas_agotados_base",
            logger,
        )

        ejecutar_sql(
            repo,
            "REFRESH MATERIALIZED VIEW public.mv_cpd_ventas_30d_tienda_ref_dia;",
            "REFRESH mv_cpd_ventas_30d_tienda_ref_dia",
            logger,
        )

        ejecutar_sql(
            repo,
            "ANALYZE public.mv_cpd_ventas_30d_tienda_ref_dia;",
            "ANALYZE mv_cpd_ventas_30d_tienda_ref_dia",
            logger,
        )

        ejecutar_sql(
            repo,
            "REFRESH MATERIALIZED VIEW public.mv_metricas_venta_promedio_3_meses_resumen_tienda;",
            "REFRESH mv_metricas_venta_promedio_3_meses_resumen_tienda",
            logger,
        )

        ejecutar_sql(
            repo,
            "ANALYZE public.mv_metricas_venta_promedio_3_meses_resumen_tienda;",
            "ANALYZE mv_metricas_venta_promedio_3_meses_resumen_tienda",
            logger,
        )

        ejecutar_sql(
            repo,
            "REFRESH MATERIALIZED VIEW public.mv_rotacion_base_ref_tienda_talla;",
            "REFRESH mv_rotacion_base_ref_tienda_talla",
            logger,
        )

        ejecutar_sql(
            repo,
            "ANALYZE public.mv_rotacion_base_ref_tienda_talla;",
            "ANALYZE mv_rotacion_base_ref_tienda_talla",
            logger,
        )

        ejecutar_sql(
            repo,
            "REFRESH MATERIALIZED VIEW public.mv_mapa_filtros;",
            "REFRESH mv_mapa_filtros",
            logger,
        )

        ejecutar_sql(
            repo,
            "ANALYZE public.mv_mapa_filtros;",
            "ANALYZE mv_mapa_filtros",
            logger,
        )

    except Exception:
        logger.exception("ERROR ejecutando refresh de materialized views")
        return 1

    logger.info("FIN refresh materialized views")
    logger.info("====================================================")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())