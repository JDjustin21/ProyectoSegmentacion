# backend/app.py

from flask import Flask, send_file, abort
from modules.segmentacion.services import SegmentacionService
from modules.segmentacion.routes import segmentacion_bp 
from config.settings import SQLSERVER_API_URL
from config.settings import REF_IMAGES_DIR, REF_IMAGES_ALLOWED_EXTENSIONS
import os

def create_app():
    app = Flask(
        __name__,
        template_folder="../frontend/templates",
        static_folder="../frontend/static"
    )

    # Registrar el módulo de segmentación
    app.register_blueprint(segmentacion_bp)
    @app.get("/ref_images/<string:ref>")
    def ref_image(ref: str):
        """
        Sirve una imagen por referencia desde REF_IMAGES_DIR.

        Convención:
        - El archivo se llama igual que la referencia.
        - La extensión puede ser .jpg/.jpeg/.png/.webp
        """
        # Seguridad mínima: evitar path traversal
        if not ref or ".." in ref or "/" in ref or "\\" in ref:
            abort(400)

        # Buscar el primer archivo existente por extensión
        for ext in REF_IMAGES_ALLOWED_EXTENSIONS:
            filename = f"{ref}{ext}"
            full_path = os.path.join(REF_IMAGES_DIR, filename)
            if os.path.isfile(full_path):
                response = send_file(full_path)

                # Mantener "actualizado": forzar revalidación en cada carga
                # (Si luego quieres performance, lo cambiamos a cache corto)
                response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
                response.headers["Pragma"] = "no-cache"
                response.headers["Expires"] = "0"
                return response

        # Si no existe imagen para esa referencia
        abort(404)
    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
