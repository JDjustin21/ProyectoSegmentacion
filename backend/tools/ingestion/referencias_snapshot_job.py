#backend/tools/ingestion/referencias_snapshot_job.py
from __future__ import annotations

import logging
import sys

from backend.config.settings import POSTGRES_DSN, SQLSERVER_API_URL
from backend.modules.segmentacion.referencias_snapshot_service import ReferenciasSnapshotService
from backend.repositories.postgres_repository import PostgresRepository


def setup_logger(name: str = "referencias_snapshot_job") -> logging.Logger:
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


def main() -> None:
    logger = setup_logger()

    logger.info("Iniciando refresh de referencias snapshot.")
    logger.info("SQLSERVER_API_URL=%s", SQLSERVER_API_URL)

    repo = PostgresRepository(POSTGRES_DSN)
    service = ReferenciasSnapshotService(repo, SQLSERVER_API_URL)

    result = service.refresh_snapshot(
        trigger_type=ReferenciasSnapshotService.TRIGGER_MANUAL,
        created_by="local_job",
    )

    logger.info(
        "Refresh exitoso. refresh_id=%s source_count=%s loaded_count=%s promoted_count=%s duration_ms=%s",
        result["refresh_id"],
        result["source_count"],
        result["loaded_count"],
        result["promoted_count"],
        result["duration_ms"],
    )


if __name__ == "__main__":
    main()