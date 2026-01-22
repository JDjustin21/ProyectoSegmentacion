import requests

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

        data = response.json()
        return data.get("datos", [])
