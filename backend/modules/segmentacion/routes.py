from flask import Blueprint, render_template
from modules.segmentacion.services import obtener_referencias

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
    referencias = obtener_referencias()
    return render_template(
        "segmentation.html",
        referencias=referencias
    )
