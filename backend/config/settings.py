import os
from pathlib import Path

from dotenv import load_dotenv


# Ruta raíz del proyecto.
# Permite cargar el archivo .env sin depender de la carpeta desde donde se ejecute Flask,
# Waitress o cualquiera de los jobs de ingesta.
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")


# ---------------------------------------------------------------------
# API externa C# / SQL Server / Siesa
# ---------------------------------------------------------------------
# URL raíz de la API C#.
#
# Regla del proyecto:
# SQLSERVER_API_URL NO debe incluir /api al final.
#
# Correcto:
#   Local:    http://localhost:5031
#   Servidor: http://127.0.0.1:5001
#
# Los endpoints internos agregan /api/... desde el código.
SQLSERVER_API_URL = os.getenv("SQLSERVER_API_URL", "http://localhost:5031").strip().rstrip("/")


SEGMENTACION_CARDS_PER_PAGE = int(
    os.getenv("SEGMENTACION_CARDS_PER_PAGE", "16").strip()
)

IMAGES_BASE_URL = os.getenv("IMAGES_BASE_URL", "/ref_images").strip()


SECRET_KEY = os.getenv("SECRET_KEY", "").strip()
if not SECRET_KEY:
    raise RuntimeError("Falta SECRET_KEY en .env")


POSTGRES_DSN = os.getenv("POSTGRES_DSN", "").strip()

POSTGRES_TIENDAS_VIEW = os.getenv(
    "POSTGRES_TIENDAS_VIEW",
    "vw_maestra_tiendas_activa_norm"
).strip()

METRICAS_CPD_TALLA_VIEW = os.getenv(
    "METRICAS_CPD_TALLA_VIEW",
    "vw_metricas_cpd_30_dias_por_talla"
).strip()

METRICAS_CPD_TIENDA_VIEW = os.getenv(
    "METRICAS_CPD_TIENDA_VIEW",
    "vw_metricas_cpd_30_dias_resumen_tienda"
).strip()

METRICAS_VENTA_PROM_TALLA_VIEW = os.getenv(
    "METRICAS_VENTA_PROM_TALLA_VIEW",
    "vw_metricas_venta_promedio_3_meses_por_talla"
).strip()

METRICAS_VENTA_PROM_TIENDA_VIEW = os.getenv(
    "METRICAS_VENTA_PROM_TIENDA_VIEW",
    "vw_metricas_venta_promedio_3_meses_resumen_tienda"
).strip()

METRICAS_ROTACION_TALLA_VIEW = os.getenv(
    "METRICAS_ROTACION_TALLA_VIEW",
    "vw_metricas_rotacion_por_talla"
).strip()

METRICAS_ROTACION_TIENDA_VIEW = os.getenv(
    "METRICAS_ROTACION_TIENDA_VIEW",
    "vw_metricas_rotacion_por_tienda"
).strip()

METRICAS_PART_VENTA_LINEA_VIEW = os.getenv(
    "METRICAS_PART_VENTA_LINEA_VIEW",
    "vw_metricas_participacion_venta_linea_3m"
).strip()

METRICAS_EXISTENCIA_TALLA_VIEW = os.getenv(
    "METRICAS_EXISTENCIA_TALLA_VIEW",
    "vw_metricas_existencia_por_talla"
).strip()


DEFAULT_TALLAS_MVP = os.getenv("DEFAULT_TALLAS_MVP", "S,M,L,XL").strip()

LINEAS_TALLAS_FIJAS = os.getenv(
    "LINEAS_TALLAS_FIJAS",
    "Dama Exterior;Dama Deportivo;Hombre Exterior;Hombre Deportivo"
).strip()


DEFAULT_USER_ID = int(os.getenv("DEFAULT_USER_ID", "1").strip())