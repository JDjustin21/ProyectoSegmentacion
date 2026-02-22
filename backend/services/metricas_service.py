# backend/services/metricas_service.py
import re
import unicodedata
from typing import Dict, List, Tuple, Any

import backend.config.settings as settings
from backend.repositories.postgres_repository import PostgresRepository
from backend.modules.segmentacion.services import SegmentacionService


def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text or "")
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def normalizar_linea_para_llave(linea_raw: str) -> str:
    """
    Convierte:
      '10 - Dama Exterior' -> 'DAMA EXTERIOR'
    Mantiene un solo espacio entre palabras, sin acentos, en mayúscula.
    """
    s = (linea_raw or "").strip()
    s = re.sub(r"^\s*\d+\s*-\s*", "", s)  # quita "10 - "
    s = _strip_accents(s).upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s


class MetricasService:
    def __init__(self, pg_repo: PostgresRepository, sql_service: SegmentacionService):
        self.pg = pg_repo
        self.sql = sql_service

    def _get_mapping_coddep(self) -> Dict[Tuple[str, str], str]:
        """
        (cod_bodega, cod_siesa) -> cod_dependencia (version activa)
        """
        rows = self.pg.fetch_all("""
            SELECT f.cod_bodega, f.cod_siesa, f.cod_dependencia
            FROM public.tiendas_siesa_filas f
            JOIN public.tiendas_siesa_version v
              ON v.id_version_siesa = f.id_version_siesa
            WHERE v.estado_version = 'Activa'
              AND COALESCE(f.cod_bodega,'') <> ''
              AND COALESCE(f.cod_siesa,'') <> ''
              AND COALESCE(f.cod_dependencia,'') <> '';
        """)

        out: Dict[Tuple[str, str], str] = {}
        for r in rows:
            b = (r.get("cod_bodega") or "").strip()
            s = (r.get("cod_siesa") or "").strip()
            d = (r.get("cod_dependencia") or "").strip()
            if b and s and d:
                out[(b, s)] = d
        return out

    def _get_ventas_ventana(self, referencia_sku: str, dias: int) -> Dict[Tuple[str, str], int]:
        """
        (llave_naval, ean) -> ventas unidades (VENTAS POS)
        """
        view = (settings.METRICAS_VENTAS_MOV_VIEW or "").strip()
        if not view:
            raise ValueError("METRICAS_VENTAS_MOV_VIEW está vacío en settings.")

        rows = self.pg.fetch_all(f"""
            SELECT
              v.llave_naval,
              v.ean,
              SUM(ABS(COALESCE(v.cantidad,0)))::int AS ventas_unidades
            FROM public.{view} v
            WHERE v.referencia_sku = %(ref)s
              AND v.fecha_movimiento >= (CURRENT_DATE - (%(dias)s || ' days')::interval)
              AND v.descripcion_movimiento = 'VENTAS POS'
            GROUP BY v.llave_naval, v.ean;
        """, {"ref": referencia_sku, "dias": int(dias)})

        out: Dict[Tuple[str, str], int] = {}
        for r in rows:
            ln = (r.get("llave_naval") or "").strip()
            ean = (r.get("ean") or "").strip()
            ventas = int(r.get("ventas_unidades") or 0)
            if ln and ean:
                out[(ln, ean)] = ventas
        return out

    def calcular_rotacion(self, referencia_sku: str, dias: int) -> Dict[str, Any]:
        """
        Retorna:
          - resumenPorTienda: [{llave_naval, ventas_ventana, inventario_actual, despacho, rotacion_hist}]
          - detallePorTalla: [{llave_naval, talla, ean, ventas_ventana, inventario_actual, despacho, rotacion_hist}]
        """
        ref = (referencia_sku or "").strip()
        if not ref:
            return {"ok": False, "error": "referencia_sku vacío"}

        dias = int(dias or settings.METRICAS_ROTACION_DIAS_DEFAULT)
        max_filas_inv = int(settings.METRICAS_ROTACION_MAX_FILAS_INV)

        # 1) Inventario desde .NET (MVP: filtrar aquí por referenciaSku para no depender de truncado)
        inv_rows = self.sql.obtener_inventario_existencias({
            "MaxFilas": max_filas_inv,
            # Si luego lo agregas en .NET, se lo pasamos también:
            # "ReferenciaSku": ref
        })

        # 2) mapping cod_dependencia
        map_coddep = self._get_mapping_coddep()

        # 3) ventas ventana
        ventas_map = self._get_ventas_ventana(ref, dias)

        # 4) inventario por (llave_naval, talla, ean)
        inv_map: Dict[Tuple[str, str, str], int] = {}

        for r in inv_rows:
            bodega = (r.get("bodega") or "").strip()
            cod_siesa = (r.get("codigo_siesa") or "").strip()
            linea_raw = (r.get("linea") or "").strip()
            ean = (r.get("ean") or "").strip()
            talla = (r.get("talla") or "").strip().upper()
            ref_sku_inv = (r.get("referenciaSku") or "").strip()

            # filtro MVP si el .NET aún no filtra por referencia
            if ref_sku_inv and ref_sku_inv != ref:
                continue

            try:
                existencia = int(r.get("existencia") or 0)
            except Exception:
                existencia = 0

            if not bodega or not cod_siesa or not linea_raw or not ean or not talla:
                continue

            cod_dep = map_coddep.get((bodega, cod_siesa))
            if not cod_dep:
                continue

            linea_norm = normalizar_linea_para_llave(linea_raw)
            llave_naval = f"{bodega}{cod_dep}{linea_norm}"

            k = (llave_naval, talla, ean)
            inv_map[k] = inv_map.get(k, 0) + existencia

        # 5) construir detallePorTalla y resumenPorTienda
        resumen: Dict[str, Dict[str, Any]] = {}
        detalle: List[Dict[str, Any]] = []

        # keys vienen del inventario (porque ahí tenemos talla); ventas se cruzan por (llave, ean)
        for (llave_naval, talla, ean), inv in inv_map.items():
            vta = int(ventas_map.get((llave_naval, ean), 0))
            despacho = int(inv) + int(vta)
            rot = (vta / despacho) if despacho > 0 else 0.0

            detalle.append({
                "llave_naval": llave_naval,
                "talla": talla,
                "ean": ean,
                "inventario_actual": int(inv),
                "ventas_ventana": int(vta),
                "despacho": int(despacho),
                "rotacion_hist": float(rot),
            })

            if llave_naval not in resumen:
                resumen[llave_naval] = {
                    "llave_naval": llave_naval,
                    "ventas_ventana": 0,
                    "inventario_actual": 0,
                    "despacho": 0,
                    "rotacion_hist": 0.0,
                }

            resumen[llave_naval]["ventas_ventana"] += int(vta)
            resumen[llave_naval]["inventario_actual"] += int(inv)

        # cerrar resumen: despacho + rotación
        for ln, obj in resumen.items():
            despacho = int(obj["ventas_ventana"]) + int(obj["inventario_actual"])
            obj["despacho"] = despacho
            obj["rotacion_hist"] = (obj["ventas_ventana"] / despacho) if despacho > 0 else 0.0

        resumen_list = list(resumen.values())
        resumen_list.sort(key=lambda x: (-float(x["rotacion_hist"]), -int(x["ventas_ventana"])))
        detalle.sort(key=lambda x: (x["llave_naval"], x["talla"]))

        return {
            "referenciaSku": ref,
            "dias": int(dias),
            "resumenPorTienda": resumen_list,
            "detallePorTalla": detalle,
            "debug": {
                "inventario_rows_recibidas": len(inv_rows),
                "inventario_keys_talla": len(inv_map),
                "ventas_keys": len(ventas_map),
                "mapping_keys": len(map_coddep),
            }
        }
