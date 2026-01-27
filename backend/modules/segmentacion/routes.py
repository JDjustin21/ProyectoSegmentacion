# backend/modules/segmentacion/routes.py
from flask import Blueprint, render_template
from config.settings import SQLSERVER_API_URL, SEGMENTACION_CARDS_PER_PAGE, IMAGES_BASE_URL
from modules.segmentacion.services import SegmentacionService

segmentacion_bp = Blueprint(
    "segmentacion",
    __name__,
    url_prefix="/segmentacion"
)

@segmentacion_bp.route("/", methods=["GET"])
def vista_segmentacion():
    """
    Vista principal de segmentación.
    - Obtiene referencias desde la API
    - Renderiza la vista HTML
    """ 
    servicio = SegmentacionService(SQLSERVER_API_URL)
    referencias = servicio.obtener_referencias()

    return render_template(
        "segmentacion.html",
        referencias=referencias,
        cards_per_page=SEGMENTACION_CARDS_PER_PAGE,
        images_base_url=IMAGES_BASE_URL
    )
