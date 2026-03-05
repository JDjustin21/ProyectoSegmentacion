import logging
from pathlib import Path
from datetime import datetime

def build_log_path(job_name: str, logs_dir: Path | None = None, dated: bool = True) -> Path:
    """
    Construye la ruta del log dentro del proyecto.
    job_name: ej 'inventario_job'
    dated: si True -> inventario_job_2026-03-04.log
    """
    job = (job_name or "job").strip().replace(" ", "_")
    if logs_dir is None:
        # .../backend/tools/ingestion/logging_utils.py -> .../backend/logs
        logs_dir = Path(__file__).resolve().parents[2] / "logs"

    logs_dir.mkdir(parents=True, exist_ok=True)

    if dated:
        d = datetime.now().strftime("%Y-%m-%d")
        return logs_dir / f"{job}_{d}.log"
    return logs_dir / f"{job}.log"


def setup_job_logger(job_name: str, level: int = logging.INFO, dated: bool = True) -> logging.Logger:
    """
    Logger estándar:
    - FileHandler a backend/logs/
    - StreamHandler a consola (para que si corres manual, lo veas)
    - Evita duplicación de handlers si el job se ejecuta varias veces
    """
    logger = logging.getLogger(job_name)
    logger.setLevel(level)
    logger.propagate = False

    # Evita duplicar handlers si se llama 2 veces
    if getattr(logger, "_configured", False):
        return logger

    log_path = build_log_path(job_name, dated=dated)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)

    logger._configured = True
    logger.info("Logger inicializado. Archivo: %s", str(log_path))

    return logger