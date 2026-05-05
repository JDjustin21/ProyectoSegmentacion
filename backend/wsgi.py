# backend/wsgi.py

from backend.app import create_app

# Punto de entrada WSGI usado por Waitress u otro servidor compatible.
app = create_app()