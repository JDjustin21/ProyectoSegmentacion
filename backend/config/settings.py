# backend/config/settings.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env desde la raíz del proyecto
ROOT_DIR = Path(__file__).resolve().parents[2]  # .../proyecto/
load_dotenv(ROOT_DIR / ".env")

SQLSERVER_API_URL = "http://localhost:5031"
#SQLSERVER_API_URL = os.getenv("SQLSERVER_API_URL", "http://localhost")
SEGMENTACION_CARDS_PER_PAGE = 16

IMAGES_BASE_URL = "/ref_images"



SECRET_KEY = os.getenv("SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError("Falta SECRET_KEY en .env")

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "").strip()
POSTGRES_TIENDAS_VIEW = os.getenv("POSTGRES_TIENDAS_VIEW", "vw_maestra_tiendas_activa_norm").strip()
METRICAS_CPD_TALLA_VIEW = os.getenv("METRICAS_CPD_TALLA_VIEW", "vw_metricas_cpd_30_dias_por_talla")
METRICAS_CPD_TIENDA_VIEW = os.getenv("METRICAS_CPD_TIENDA_VIEW", "vw_metricas_cpd_30_dias_resumen_tienda")
METRICAS_VENTA_PROM_TALLA_VIEW = os.getenv("METRICAS_VENTA_PROM_TALLA_VIEW", "vw_metricas_venta_promedio_3_meses_por_talla")
METRICAS_VENTA_PROM_TIENDA_VIEW = os.getenv("METRICAS_VENTA_PROM_TIENDA_VIEW", "vw_metricas_venta_promedio_3_meses_resumen_tienda")


DEFAULT_TALLAS_MVP = os.getenv("DEFAULT_TALLAS_MVP", "S,M,L,XL").strip()
LINEAS_TALLAS_FIJAS = os.getenv(
    "LINEAS_TALLAS_FIJAS",
    "Dama Exterior;Dama Deportivo;Hombre Exterior;Hombre Deportivo"
).strip()



DEFAULT_USER_ID = int(os.getenv("DEFAULT_USER_ID", "1").strip())

print("SQLSERVER_API_URL =", SQLSERVER_API_URL)