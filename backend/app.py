# backend/app.py

from backend.modules.segmentacion.routes import segmentacion_bp
from backend.modules.auth.auth_routes import auth_bp
from flask import Flask
from backend.config.settings import SECRET_KEY
from backend.modules.admin.admin_routes import admin_bp
import os

def create_app():
    app = Flask(
        __name__,
        template_folder="../frontend/templates",
        static_folder="../frontend/static"
    )
    
    app.config["SECRET_KEY"] = SECRET_KEY  # viene de settings / env

    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)

    # Registrar el módulo de segmentación
    app.register_blueprint(segmentacion_bp)
    
    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
