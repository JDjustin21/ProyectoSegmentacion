# backend/config/settings.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env desde la raíz del proyecto
ROOT_DIR = Path(__file__).resolve().parents[2]  # .../proyecto/
load_dotenv(ROOT_DIR / ".env")

SQLSERVER_API_URL = "http://localhost:5031"
SEGMENTACION_CARDS_PER_PAGE = 8

REF_IMAGES_DIR = r"C:\Creytex\ImagenesReferencias"
REF_IMAGES_ALLOWED_EXTENSIONS = [".jpg", ".jpeg", ".png", ".webp"]
IMAGES_BASE_URL = "/ref_images"

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "").strip()
POSTGRES_TIENDAS_VIEW = os.getenv("POSTGRES_TIENDAS_VIEW", "vw_maestra_tiendas_activa_norm").strip()

DEFAULT_TALLAS_MVP = os.getenv("DEFAULT_TALLAS_MVP", "S,M,L,XL").strip()
LINEAS_TALLAS_FIJAS = os.getenv(
    "LINEAS_TALLAS_FIJAS",
    "Dama Exterior;Dama Deportivo;Hombre Exterior;Hombre Deportivo"
).strip()

DEFAULT_USER_ID = int(os.getenv("DEFAULT_USER_ID", "1").strip())

