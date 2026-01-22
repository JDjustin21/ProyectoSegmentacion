from flask import Flask, render_template
from modules.segmentacion.services import SegmentacionService
from config.settings import SQLSERVER_API_URL


def create_app():
    app = Flask(
        __name__,
        template_folder="../frontend/templates",
        static_folder="../frontend/static"
    )

    @app.route("/segmentacion")
    def segmentacion():
        servicio = SegmentacionService(SQLSERVER_API_URL)
        referencias = servicio.obtener_referencias()

        return render_template(
            "segmentacion.html",
            referencias=referencias
        )
    
    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
