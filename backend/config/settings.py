#backend/config/settings.py
# Configuracion de ajustes para la conexion con SQL Server
SQLSERVER_API_URL = "http://localhost:5031"

SEGMENTACION_CARDS_PER_PAGE = 3

# Imágenes de referencias (carpeta externa)
# Regla: el archivo se llama igual que la referencia (E47439.jpg / .png / etc.)
REF_IMAGES_DIR = r"C:\Creytex\ImagenesReferencias"
REF_IMAGES_ALLOWED_EXTENSIONS = [".jpg", ".jpeg", ".png", ".webp"]

# Endpoint base para construir la URL en el frontend
IMAGES_BASE_URL = "/ref_images"