# backend/app.py

from flask import Flask

from backend.config.settings import SECRET_KEY
from backend.modules.admin.admin_routes import admin_bp
from backend.modules.analiticas.agotados_routes import agotados_bp
from backend.modules.auth.auth_routes import auth_bp
from backend.modules.inventario.inventario_routes import inventario_bp
from backend.modules.segmentacion.routes import segmentacion_bp


def create_app():
    """
    Crea y configura la aplicación Flask principal.

    Esta función centraliza el registro de blueprints del sistema:
    autenticación, administración, segmentación, analíticas e inventario.

    Se usa tanto en ejecución local como en producción mediante WSGI.
    """
    app = Flask(
        __name__,
        template_folder="../frontend/templates",
        static_folder="../frontend/static"
    )

    # Clave usada por Flask para sesiones, mensajes flash y protección básica.
    app.config["SECRET_KEY"] = SECRET_KEY

    # Módulos transversales del sistema.
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)

    # Módulos funcionales del aplicativo.
    app.register_blueprint(segmentacion_bp)
    app.register_blueprint(agotados_bp)
    app.register_blueprint(inventario_bp)

    return app


if __name__ == "__main__":
    # Ejecución local para desarrollo. En producción se usa backend/wsgi.py.
    app = create_app()
    app.run(debug=True)
