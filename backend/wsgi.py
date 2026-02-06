# backend/wsgi.py

from backend.app import create_app

# WSGI callable que Waitress/IIS reverse proxy van a usar
app = create_app()