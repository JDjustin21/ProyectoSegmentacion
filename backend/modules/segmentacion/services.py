# backend/modules/segmentacion/services.py

import requests
import json
from datetime import datetime
from decimal import Decimal 

import re
from typing import Any, Dict, List, Optional, Tuple
from config.settings import POSTGRES_DSN, POSTGRES_TIENDAS_VIEW, DEFAULT_TALLAS_MVP
from repositories.postgres_repository import PostgresRepository


class SegmentacionService:
    """
    Servicio encargado de consumir la API de SQL Server.
    No contiene lógica de negocio.
    Solo obtiene datos.
    """

    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url

    def obtener_referencias(self):
        """
        Llama al endpoint de la API que devuelve las referencias.
        Retorna una lista de referencias (JSON).
        """

        url = f"{self.api_base_url}/api/sqlserver/referencias/consultar"

        response = requests.post(url, timeout=30)
        response.raise_for_status()

        def _json_safe(obj):
            if isinstance(obj, (datetime,)):
                return obj.isoformat()
            if isinstance(obj, Decimal):
                return float(obj)
            return str(obj)

        data = response.json()
        datos = data.get("datos", [])

        # “Re-serializa” para forzar compatibilidad JSON
        datos = json.loads(json.dumps(datos, default=_json_safe))

        return datos

    
        
