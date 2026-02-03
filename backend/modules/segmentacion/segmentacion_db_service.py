# backend/modules/segmentacion/segmentacion_db_service.py
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple
from config.settings import DEFAULT_USER_ID


from repositories.postgres_repository import PostgresRepository


class SegmentacionDbService:
    """
    Servicio de negocio para el mundo Postgres:
    - Tiendas activas por línea
    - Guardar segmentación
    - Consultar última segmentación
    - Exportar CSV por ventana de tiempo
    """

    def __init__(self, repo: PostgresRepository, view_tiendas: str):
        self._repo = repo
        self._view_tiendas = view_tiendas

    # -------------------------
    # Normalización de línea
    # -------------------------
    def normalizar_linea(self, linea_raw: str) -> str:
        """
        Convierte:
        "17 - Bebito" -> "bebito"
        "Bebito" -> "bebito"
        """
        v = (linea_raw or "").strip()
        if " - " in v:
            v = v.split(" - ", 1)[1]
        return v.strip().lower()

    # -------------------------
    # Tiendas activas por línea
    # -------------------------
    def tiendas_activas_por_linea(
        self,
        linea_raw: str,
        zona: str = "",
        ciudad: str = "",
        clima: str = "",
        dependencia: str = "",
        testeo: str = "",
        clasificacion: str = ""
    ) -> Dict[str, Any]:

        linea_norm = self.normalizar_linea(linea_raw)

        sql = f"""
           SELECT
                v.llave_naval,
                v.cod_bodega,
                v.cod_dependencia,
                v.dependencia,
                v.desc_dependencia,
                v.ciudad,
                v.zona,
                v.clima,
                v.linea,
                v.estado_linea,
                v.estado_tienda,
                v.testeo_fnl,
                v.rankin_linea
            FROM {self._view_tiendas} v
            WHERE v.linea_norm = %(linea_norm)s
              AND v.estado_tienda_norm = 'activo'
              AND v.estado_linea_norm  = 'activo'

              -- filtros tipo "buscador" (parcial)
              AND (%(dependencia)s = '' OR COALESCE(v.desc_dependencia, v.dependencia) ILIKE %(dependencia_like)s)
              AND (%(zona)s = '' OR v.zona ILIKE %(zona_like)s)
              AND (%(ciudad)s = '' OR v.ciudad ILIKE %(ciudad_like)s)
              AND (%(clima)s = '' OR v.clima ILIKE %(clima_like)s)
              AND (%(testeo)s = '' OR COALESCE(v.testeo_fnl,'') ILIKE %(testeo_like)s)
              AND (%(clasificacion)s = '' OR COALESCE(v.rankin_linea,'') ILIKE %(clasificacion_like)s)

            ORDER BY COALESCE(v.desc_dependencia, v.dependencia);
        """

        zona_v = (zona or "").strip()
        ciudad_v = (ciudad or "").strip()
        clima_v = (clima or "").strip()
        dependencia_v = (dependencia or "").strip()
        testeo_v = (testeo or "").strip()
        clasificacion_v = (clasificacion or "").strip()

        tiendas = self._repo.fetch_all(sql, {
            "linea_norm": linea_norm,

            "dependencia": dependencia_v,
            "dependencia_like": f"%{dependencia_v}%",

            "zona": zona_v,
            "zona_like": f"%{zona_v}%",

            "ciudad": ciudad_v,
            "ciudad_like": f"%{ciudad_v}%",

            "clima": clima_v,
            "clima_like": f"%{clima_v}%",

            "testeo": testeo_v,
            "testeo_like": f"%{testeo_v}%",

            "clasificacion": clasificacion_v,
            "clasificacion_like": f"%{clasificacion_v}%",
        })

        return {
            "linea": linea_norm,
            "filtros_aplicados": {
                "dependencia": dependencia_v or None,
                "zona": zona_v or None,
                "ciudad": ciudad_v or None,
                "clima": clima_v or None,
                "testeo": testeo_v or None,
                "clasificacion": clasificacion_v or None,
            },
            "tiendas": tiendas
        }

    # -------------------------
    # Última segmentación por referenciaSku
    # -------------------------
    def ultima_segmentacion(self, referencia_sku: str) -> Dict[str, Any]:
        ref = (referencia_sku or "").strip()
        if not ref:
            return {"existe": False, "segmentacion": None}

        sql_head = """
            SELECT id_segmentacion, id_usuario, fecha_creacion, estado_segmentacion,
                   referencia, codigo_barras, descripcion, categoria, linea,
                   tipo_portafolio, estado_sku, cuento, tipo_inventario
            FROM segmentacion
            WHERE referencia = %(ref)s
            ORDER BY fecha_creacion DESC
            LIMIT 1;
        """
        head = self._repo.fetch_one(sql_head, {"ref": ref})
        if not head:
            return {"existe": False, "segmentacion": None}

        sql_det = """
            SELECT llave_naval, talla, cantidad
            FROM segmentacion_detalle
            WHERE id_segmentacion = %(id)s
              AND COALESCE(estado_detalle,'Activo') = 'Activo';
        """
        det = self._repo.fetch_all(sql_det, {"id": head["id_segmentacion"]})

        head["detalle"] = det
        head["referenciaSku"] = head.pop("referencia")

        return {"existe": True, "segmentacion": head}

    # -------------------------
    # Guardar segmentación (crea nueva cabecera)
    # -------------------------
    def guardar_segmentacion(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(ZoneInfo("America/Bogota"))

        # 1) Id versión activa
        row = self._repo.fetch_one("""
            SELECT id_version
            FROM maestra_tiendas_version
            WHERE estado_version='Activa'
            ORDER BY id_version DESC
            LIMIT 1;
        """)
        if not row:
            raise RuntimeError("No existe versión Activa en maestra_tiendas_version.")
        id_version_activa = int(row["id_version"])

        # 2) Refrescar snapshot para que la FK de segmentacion_detalle no falle
        self._repo.execute("SELECT * FROM public.refresh_maestra_tiendas_actual();")

        ref = (payload.get("referenciaSku") or "").strip()
        if not ref:
            raise RuntimeError("Falta referenciaSku para guardar.")

        detalles = payload.get("detalle") or []
        if not isinstance(detalles, list):
            raise RuntimeError("El campo 'detalle' debe ser una lista.")

        # Si quieres: validar que exista al menos una cantidad > 0
        any_positive = any(int(d.get("cantidad") or 0) > 0 for d in detalles if isinstance(d, dict))
        if not any_positive:
            raise RuntimeError("No hay cantidades para guardar (todas están en 0).")

        # Cabecera (siempre nueva para MVP)
        sql_insert_head = """
            INSERT INTO segmentacion (
                id_usuario, fecha_creacion, id_version_tiendas, estado_segmentacion,
                referencia, codigo_barras, descripcion, categoria, linea,
                tipo_portafolio, estado_sku, cuento, tipo_inventario
            )
            VALUES (
                %(id_usuario)s, %(fecha_creacion)s, %(id_version_tiendas)s, %(estado_segmentacion)s,
                %(referencia)s, %(codigo_barras)s, %(descripcion)s, %(categoria)s, %(linea)s,
                %(tipo_portafolio)s, %(estado_sku)s, %(cuento)s, %(tipo_inventario)s
            )
            RETURNING id_segmentacion;
        """

        head_params = {
            "id_usuario": int(payload.get("id_usuario") or DEFAULT_USER_ID),
            "fecha_creacion": now,
            "id_version_tiendas": id_version_activa,
            "estado_segmentacion": payload.get("estado_segmentacion") or "Activa",
            "referencia": ref,
            "codigo_barras": (payload.get("codigo_barras") or "").strip(),
            "descripcion": (payload.get("descripcion") or "").strip(),
            "categoria": (payload.get("categoria") or "").strip(),
            "linea": (payload.get("linea") or "").strip(),
            "tipo_portafolio": (payload.get("tipo_portafolio") or "").strip(),
            "estado_sku": (payload.get("estado_sku") or "").strip(),
            "cuento": (payload.get("cuento") or "").strip(),
            "tipo_inventario": (payload.get("tipo_inventario") or "").strip(),
        }

        id_seg = self._repo.execute_returning_id(
            sql_insert_head,
            head_params,
            "id_segmentacion"
        )

        # Detalle: guardamos solo cantidades > 0 (MVP limpio)
        detalles = payload.get("detalle") or []
        filas = []
        total_units = 0
        tiendas_con_cantidad = set()
        tallas_usadas = set()

        for d in detalles:
            llave = (d.get("llave_naval") or "").strip()
            talla = (d.get("talla") or "").strip()
            cantidad = int(d.get("cantidad") or 0)

            if not llave or not talla:
                continue
            if cantidad <= 0:
                continue

            total_units += cantidad
            tiendas_con_cantidad.add(llave)
            tallas_usadas.add(talla)

            filas.append({
                "id_segmentacion": id_seg,
                "llave_naval": llave,
                "talla": talla,
                "cantidad": cantidad,
                "estado_detalle": "Activo",
                "fecha_actualizacion": now
            })

        sql_insert_det = """
            INSERT INTO segmentacion_detalle (
                id_segmentacion, llave_naval, talla, cantidad, estado_detalle, fecha_actualizacion
            )
            VALUES (
                %(id_segmentacion)s, %(llave_naval)s, %(talla)s, %(cantidad)s, %(estado_detalle)s, %(fecha_actualizacion)s
            );
        """
        self._repo.execute_many(sql_insert_det, filas)

        return {
            "ok": True,
            "id_segmentacion": id_seg,
            "mensaje": "Segmentación guardada",
            "resumen": {
                "tiendas_con_cantidad": len(tiendas_con_cantidad),
                "total_unidades": total_units,
                "tallas_usadas": sorted(list(tallas_usadas))
            }
        }

    # -------------------------
    # Dataset para export CSV por fecha/rango
    # -------------------------
    def export_dataset_todas(self) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT
                s.id_segmentacion,
                s.fecha_creacion,
                s.id_usuario,
                s.id_version_tiendas,
                s.referencia,
                s.codigo_barras,
                s.descripcion,
                s.categoria,
                s.linea,
                s.tipo_portafolio,
                s.estado_sku,
                s.cuento,
                s.tipo_inventario,

                d.llave_naval,
                d.talla,
                d.cantidad,
                d.estado_detalle,
                d.fecha_actualizacion,

                -- datos tienda (desde la vista)
                t.cod_bodega,
                t.cod_dependencia,
                t.dependencia,
                t.desc_dependencia,
                t.ciudad,
                t.zona,
                t.clima,
                t.rankin_linea,
                t.testeo_fnl AS testeo
            FROM segmentacion_detalle d
            JOIN segmentacion s
              ON s.id_segmentacion = d.id_segmentacion
            LEFT JOIN {self._view_tiendas} t
              ON t.llave_naval = d.llave_naval
            WHERE COALESCE(d.estado_detalle,'Activo') = 'Activo'
              AND COALESCE(s.estado_segmentacion,'Activa') = 'Activa'
            ORDER BY d.fecha_actualizacion ASC, s.id_segmentacion ASC;
        """
        return self._repo.fetch_all(sql)

    # -------------------------
    # Dataset para export CSV por fecha/rango (si luego lo vuelves a necesitar)
    # -------------------------
    def export_dataset_por_rango(self, desde: datetime, hasta: datetime) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT
                s.id_segmentacion,
                s.fecha_creacion,
                s.id_usuario,
                s.id_version_tiendas,
                s.referencia,
                s.codigo_barras,
                s.descripcion,
                s.categoria,
                s.linea,
                s.tipo_portafolio,
                s.estado_sku,
                s.cuento,
                s.tipo_inventario,
                s.estado_segmentacion,


                d.llave_naval,
                d.talla,
                d.cantidad,
                d.estado_detalle,
                d.fecha_actualizacion,

                t.cod_bodega,
                t.cod_dependencia,
                t.dependencia,
                t.desc_dependencia,
                t.ciudad,
                t.zona,
                t.clima,
                t.rankin_linea,
                t.testeo_fnl AS testeo
            FROM segmentacion_detalle d
            JOIN segmentacion s
              ON s.id_segmentacion = d.id_segmentacion
            LEFT JOIN {self._view_tiendas} t
              ON t.llave_naval = d.llave_naval
            WHERE d.fecha_actualizacion >= %(desde)s
              AND d.fecha_actualizacion <  %(hasta)s
              AND COALESCE(d.estado_detalle,'Activo') = 'Activo'
              AND COALESCE(s.estado_segmentacion,'Activa') = 'Activa'
            ORDER BY d.fecha_actualizacion ASC, s.id_segmentacion ASC;
        """
        return self._repo.fetch_all(sql, {"desde": desde, "hasta": hasta})
