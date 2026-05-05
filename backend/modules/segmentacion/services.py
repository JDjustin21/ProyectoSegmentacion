# backend/modules/segmentacion/services.py

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List

import requests


class SegmentacionService:
    """
    Servicio de lectura de referencias desde la API C# / SQL Server.

    Este servicio no alimenta directamente la pantalla principal de Segmentación.
    Su uso actual está asociado al refresh del snapshot de referencias, donde la API
    externa se consulta, se transforma y luego se carga en PostgreSQL.

    La pantalla principal consume PostgreSQL, no la API remota en caliente.
    """

    def __init__(self, api_base_url: str):
        """
        Recibe la URL base de la API C#.

        Ejemplos:
        - Local: http://localhost:5031
        - Servidor: http://cyt-0108/api
        """
        self.api_base_url = (api_base_url or "").rstrip("/")

    def obtener_referencias(self) -> List[Dict[str, Any]]:
        """
        Consulta el endpoint de referencias expuesto por la API C#.

        SQLSERVER_API_URL debe ser la URL raíz del servicio C#, sin /api al final.
        Ejemplos válidos:
        - http://localhost:5031
        - http://127.0.0.1:5001
        """
        if not self.api_base_url:
            raise RuntimeError("No se configuró SQLSERVER_API_URL.")

        url = f"{self.api_base_url}/api/sqlserver/referencias/consultar"

        response = requests.post(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        datos = data.get("datos", [])

        if not isinstance(datos, list):
            return []

        return json.loads(json.dumps(datos, default=self._json_safe))

    @staticmethod
    def _json_safe(obj: Any) -> Any:
        """
        Convierte tipos especiales a valores serializables en JSON.

        Esto protege el flujo cuando la API devuelve fechas, decimales u otros
        valores que no siempre son compatibles directamente con JSON.
        """
        if isinstance(obj, datetime):
            return obj.isoformat()

        if isinstance(obj, Decimal):
            return float(obj)

        return str(obj)