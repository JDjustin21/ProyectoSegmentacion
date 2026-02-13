# backend/modules/segmentacion/segmentacion_db_service.py
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List

from backend.config.settings import DEFAULT_USER_ID
from backend.repositories.postgres_repository import PostgresRepository


TZ_BOGOTA = ZoneInfo("America/Bogota")
CLASIFICACIONES_EXACTAS = {"AA", "A", "B", "C", "NA"}


class SegmentacionDbService:
    """
    Servicio de negocio para Postgres (Segmentación):

    - Consultar tiendas activas por línea (desde una vista normalizada)
    - Guardar segmentaciones (cabecera + detalle) con histórico
    - Consultar última segmentación por referencia
    - Exportar dataset para CSV
    - Reset (TRUNCATE) de tablas de segmentación (admin)
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
          "Bebito"      -> "bebito"
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
        clasificacion: str = "",
    ) -> Dict[str, Any]:
        linea_norm = self.normalizar_linea(linea_raw)

        # Limpiar valores de filtros (evita espacios raros)
        dependencia_v = (dependencia or "").strip()
        zona_v = (zona or "").strip()
        ciudad_v = (ciudad or "").strip()
        clima_v = (clima or "").strip()
        testeo_v = (testeo or "").strip()
        clasificacion_v = (clasificacion or "").strip()

        # Normalización para decidir si la clasificación debe ser exacta (AA/A/B/C/NA)
        clasif_up = clasificacion_v.upper().replace(" ", "")
        if clasif_up == "N/A":
            clasif_up = "NA"

        clasificacion_is_exact = clasif_up in CLASIFICACIONES_EXACTAS

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
              AND (%(dependencia)s = '' OR v.dependencia ILIKE %(dependencia_like)s)
              AND (%(zona)s = '' OR v.zona ILIKE %(zona_like)s)
              AND (%(ciudad)s = '' OR v.ciudad ILIKE %(ciudad_like)s)
              AND (%(clima)s = '' OR v.clima ILIKE %(clima_like)s)
              AND (%(testeo)s = '' OR COALESCE(v.testeo_fnl,'') ILIKE %(testeo_like)s)

              -- clasificación:
              -- si es exacta (AA/A/B/C/NA) -> compara exacto
              -- si no, hace ILIKE parcial
              AND (
                    %(clasificacion)s = ''
                    OR (
                        %(clasificacion_is_exact)s = TRUE
                        AND UPPER(COALESCE(v.rankin_linea,'')) = %(clasificacion_exact)s
                    )
                    OR (
                        %(clasificacion_is_exact)s = FALSE
                        AND COALESCE(v.rankin_linea,'') ILIKE %(clasificacion_like)s
                    )
              )
            ORDER BY COALESCE(v.desc_dependencia, v.dependencia);
        """

        tiendas = self._repo.fetch_all(
            sql,
            {
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
                "clasificacion_is_exact": clasificacion_is_exact,
                "clasificacion_exact": clasif_up,
                "clasificacion_like": f"%{clasificacion_v}%",
            },
        )

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
            "tiendas": tiendas,
        }

    def contar_tiendas_activas_por_linea(self, linea_raw: str) -> int:
        linea_norm = self.normalizar_linea(linea_raw)
        sql = f"""
            SELECT COUNT(*) AS n
            FROM {self._view_tiendas} v
            WHERE v.linea_norm = %(linea_norm)s
              AND v.estado_tienda_norm = 'activo'
              AND v.estado_linea_norm  = 'activo';
        """
        row = self._repo.fetch_one(sql, {"linea_norm": linea_norm})
        return int(row["n"]) if row and row.get("n") is not None else 0

    # -------------------------
    # Última segmentación por referenciaSku
    # -------------------------
    def ultima_segmentacion(self, referencia_sku: str) -> Dict[str, Any]:
        ref = (referencia_sku or "").strip()
        if not ref:
            return {"existe": False, "segmentacion": None}

        sql_head = """
            SELECT
                id_segmentacion, id_usuario, fecha_creacion, estado_segmentacion,
                referencia, codigo_barras, descripcion, categoria, linea,
                tipo_portafolio, precio_unitario, estado_sku, cuento, tipo_inventario
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
        now = datetime.now(TZ_BOGOTA)

        ref = (payload.get("referenciaSku") or "").strip()
        if not ref:
            raise RuntimeError("Falta referenciaSku para guardar.")

        detalles = payload.get("detalle") or []
        if not isinstance(detalles, list):
            raise RuntimeError("El campo 'detalle' debe ser una lista.")

        # 1) Lista activa desde payload: solo cantidad > 0
        filas_activo: List[Dict[str, Any]] = []
        total_units = 0
        tiendas_con_cantidad = set()
        tallas_usadas = set()

        for d in detalles:
            llave = (d.get("llave_naval") or "").strip()
            talla = (d.get("talla") or "").strip()
            cantidad = int(d.get("cantidad") or 0)

            if not llave or not talla or cantidad <= 0:
                continue

            total_units += cantidad
            tiendas_con_cantidad.add(llave)
            tallas_usadas.add(talla)

            filas_activo.append({"llave_naval": llave, "talla": talla, "cantidad": cantidad})

        new_keys = {(r["llave_naval"], r["talla"]) for r in filas_activo}
        nueva_activa = bool(new_keys)

        def _tx(cur):
            # 1) Versión activa (misma transacción)
            cur.execute("""
                SELECT id_version
                FROM maestra_tiendas_version
                WHERE estado_version='Activa'
                ORDER BY id_version DESC
                LIMIT 1;
            """)
            row = cur.fetchone()
            if not row:
                raise RuntimeError("No existe versión Activa en maestra_tiendas_version.")
            id_version_activa = int(row["id_version"])

            # 2) Refrescar snapshot (misma transacción)
            cur.execute("SELECT * FROM public.refresh_maestra_tiendas_actual();")

            # 3) Tomar última cabecera ACTIVA y bloquearla para evitar carreras
            cur.execute("""
                SELECT id_segmentacion
                FROM segmentacion
                WHERE referencia = %(ref)s
                  AND COALESCE(estado_segmentacion,'Activa') = 'Activa'
                ORDER BY fecha_creacion DESC
                LIMIT 1
                FOR UPDATE;
            """, {"ref": ref})
            last = cur.fetchone()
            last_id = int(last["id_segmentacion"]) if last else None

            # 4) Keys previas activas (cantidad > 0)
            prev_keys = set()
            if last_id:
                cur.execute("""
                    SELECT llave_naval, talla
                    FROM segmentacion_detalle
                    WHERE id_segmentacion = %(id)s
                      AND COALESCE(estado_detalle,'Activo') = 'Activo'
                      AND cantidad > 0;
                """, {"id": last_id})
                prev_rows = cur.fetchall() or []
                prev_keys = {(r["llave_naval"], r["talla"]) for r in prev_rows}

            desactivadas = prev_keys - new_keys

            # 5) Inactivar cabecera anterior (histórico)
            if last_id:
                cur.execute("""
                    UPDATE segmentacion
                    SET estado_segmentacion = 'Inactiva'
                    WHERE id_segmentacion = %(id)s;
                """, {"id": last_id})

            # 6) Insertar nueva cabecera
            cur.execute("""
                INSERT INTO segmentacion (
                    id_usuario, fecha_creacion, id_version_tiendas, estado_segmentacion,
                    referencia, codigo_barras, descripcion, categoria, linea,
                    tipo_portafolio, precio_unitario, estado_sku, cuento, tipo_inventario
                )
                VALUES (
                    %(id_usuario)s, %(fecha_creacion)s, %(id_version_tiendas)s, %(estado_segmentacion)s,
                    %(referencia)s, %(codigo_barras)s, %(descripcion)s, %(categoria)s, %(linea)s,
                    %(tipo_portafolio)s, %(precio_unitario)s, %(estado_sku)s, %(cuento)s, %(tipo_inventario)s
                )
                RETURNING id_segmentacion;
            """, {
                "id_usuario": int(payload.get("id_usuario") or DEFAULT_USER_ID),
                "fecha_creacion": now,
                "id_version_tiendas": id_version_activa,
                "estado_segmentacion": "Activa" if nueva_activa else "Inactiva",
                "referencia": ref,
                "codigo_barras": (payload.get("codigo_barras") or "").strip(),
                "descripcion": (payload.get("descripcion") or "").strip(),
                "categoria": (payload.get("categoria") or "").strip(),
                "linea": (payload.get("linea") or "").strip(),
                "tipo_portafolio": (payload.get("tipo_portafolio") or "").strip(),
                "precio_unitario": float(payload.get("precio_unitario") or 0.0),
                "estado_sku": (payload.get("estado_sku") or "").strip(),
                "cuento": (payload.get("cuento") or "").strip(),
                "tipo_inventario": (payload.get("tipo_inventario") or "").strip(),
            })
            head_row = cur.fetchone()
            if not head_row or "id_segmentacion" not in head_row:
                raise RuntimeError("No se retornó id_segmentacion al insertar cabecera.")
            id_seg = int(head_row["id_segmentacion"])

            # 7) Insertar detalle:
            # - Activos: cantidad>0 (Activo)
            # - Desactivadas: cantidad=0 (Inactivo) para que aparezcan en CSV
            filas_insert: List[Dict[str, Any]] = []

            for r in filas_activo:
                filas_insert.append({
                    "id_segmentacion": id_seg,
                    "llave_naval": r["llave_naval"],
                    "talla": r["talla"],
                    "cantidad": int(r["cantidad"]),
                    "estado_detalle": "Activo",
                    "fecha_actualizacion": now,
                })

            for (llave, talla) in desactivadas:
                filas_insert.append({
                    "id_segmentacion": id_seg,
                    "llave_naval": llave,
                    "talla": talla,
                    "cantidad": 0,
                    "estado_detalle": "Inactivo",
                    "fecha_actualizacion": now,
                })

            if filas_insert:
                cur.executemany("""
                    INSERT INTO segmentacion_detalle (
                        id_segmentacion, llave_naval, talla, cantidad, estado_detalle, fecha_actualizacion
                    )
                    VALUES (
                        %(id_segmentacion)s, %(llave_naval)s, %(talla)s, %(cantidad)s, %(estado_detalle)s, %(fecha_actualizacion)s
                    );
                """, filas_insert)

            # 8) segmented_at (badge):
            # si NO hay activos -> segmented_at NULL
            if nueva_activa:
                cur.execute("""
                    INSERT INTO public.referencias_vistas (referencia_sku, first_seen, last_seen, segmented_at)
                    VALUES (%(ref)s, %(now)s, %(now)s, %(now)s)
                    ON CONFLICT (referencia_sku)
                    DO UPDATE SET
                        last_seen = EXCLUDED.last_seen,
                        segmented_at = EXCLUDED.segmented_at;
                """, {"ref": ref, "now": now})
            else:
                cur.execute("""
                    INSERT INTO public.referencias_vistas (referencia_sku, first_seen, last_seen, segmented_at)
                    VALUES (%(ref)s, %(now)s, %(now)s, NULL)
                    ON CONFLICT (referencia_sku)
                    DO UPDATE SET
                        last_seen = EXCLUDED.last_seen,
                        segmented_at = NULL;
                """, {"ref": ref, "now": now})

            return {"id_seg": id_seg, "desactivadas_count": len(desactivadas)}

        tx_result = self._repo.run_in_transaction(_tx)

        return {
            "ok": True,
            "id_segmentacion": tx_result["id_seg"],
            "mensaje": "Segmentación guardada",
            "resumen": {
                "tiendas_con_cantidad": len(tiendas_con_cantidad),
                "total_unidades": total_units,
                "tallas_usadas": sorted(list(tallas_usadas)),
                "desactivadas": int(tx_result["desactivadas_count"]),
                "is_segmented": nueva_activa,
            },
        }

    # -------------------------
    # Dataset para export CSV
    # -------------------------
    def export_dataset_todas(self) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT
                s.id_segmentacion,
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
                s.precio_unitario,
                s.fecha_creacion,

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
            ORDER BY d.fecha_actualizacion ASC, s.id_segmentacion ASC;
        """
        return self._repo.fetch_all(sql)

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
                s.precio_unitario,
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
            ORDER BY d.fecha_actualizacion ASC, s.id_segmentacion ASC;
        """
        return self._repo.fetch_all(sql, {"desde": desde, "hasta": hasta})

    # -------------------------
    # Referencias nuevas / anotaciones UI
    # -------------------------
    def marcar_y_anotar_referencias_nuevas(
        self, referencias: List[Dict[str, Any]], dias_nuevo: int = 7
    ) -> List[Dict[str, Any]]:
        if not referencias:
            return referencias

        now = datetime.now(TZ_BOGOTA)

        ref_list: List[str] = []
        for r in referencias:
            ref = (r.get("referencia") or r.get("referenciaSku") or r.get("Referencia") or "").strip()
            if ref:
                ref_list.append(ref)

        if not ref_list:
            return referencias

        sql_upsert = """
            INSERT INTO public.referencias_vistas (referencia_sku, last_seen)
            VALUES (%(referencia_sku)s, %(last_seen)s)
            ON CONFLICT (referencia_sku)
            DO UPDATE SET last_seen = EXCLUDED.last_seen;
        """
        rows = [{"referencia_sku": ref, "last_seen": now} for ref in ref_list]
        self._repo.execute_many(sql_upsert, rows)

        cutoff = now - timedelta(days=int(dias_nuevo))
        sql_nuevas = """
            SELECT referencia_sku
            FROM public.referencias_vistas
            WHERE first_seen >= %(cutoff)s
              AND acknowledged_at IS NULL;
        """
        nuevas_rows = self._repo.fetch_all(sql_nuevas, {"cutoff": cutoff})
        nuevas_set = {(x.get("referencia_sku") or "").strip() for x in nuevas_rows}

        for r in referencias:
            ref = (r.get("referencia") or "").strip()
            r["is_new"] = bool(ref and ref in nuevas_set)

        return referencias

    def anotar_segmentacion_y_conteo(self, referencias: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not referencias:
            return referencias

        ref_list: List[str] = []
        for r in referencias:
            ref = (r.get("referencia") or "").strip()
            if ref:
                ref_list.append(ref)

        if not ref_list:
            return referencias

        flags = self.obtener_estado_y_conteo_segmentacion(ref_list)

        for r in referencias:
            ref = (r.get("referencia") or r.get("referenciaSku") or r.get("Referencia") or "").strip()
            meta = flags.get(ref, {"is_segmented": False, "tiendas_activas_segmentadas": 0})
            r["is_segmented"] = bool(meta["is_segmented"])
            r["tiendas_activas_segmentadas"] = int(meta["tiendas_activas_segmentadas"] or 0)

        return referencias

    def obtener_estado_y_conteo_segmentacion(self, ref_list: List[str]) -> Dict[str, Dict[str, Any]]:
        if not ref_list:
            return {}

        sql = f"""
            WITH last_seg AS (
                SELECT DISTINCT ON (s.referencia)
                    s.referencia,
                    s.id_segmentacion,
                    s.linea
                FROM segmentacion s
                WHERE s.referencia = ANY(%(refs)s)
                ORDER BY s.referencia, s.fecha_creacion DESC
            ),
            seg_conteo AS (
                SELECT
                    ls.referencia,
                    COUNT(DISTINCT d.llave_naval) AS tiendas_activas_segmentadas
                FROM last_seg ls
                JOIN segmentacion_detalle d
                  ON d.id_segmentacion = ls.id_segmentacion
                JOIN {self._view_tiendas} v
                  ON v.llave_naval = d.llave_naval
                 AND v.estado_tienda_norm = 'activo'
                 AND v.estado_linea_norm  = 'activo'
                 AND (
                        COALESCE(NULLIF(TRIM(ls.linea), ''), '') = ''
                        OR v.linea_norm = (
                            CASE
                                WHEN POSITION(' - ' IN COALESCE(ls.linea,'')) > 0
                                    THEN LOWER(TRIM(SPLIT_PART(ls.linea, ' - ', 2)))
                                ELSE LOWER(TRIM(COALESCE(ls.linea,'')))
                            END
                        )
                 )
                WHERE COALESCE(d.estado_detalle,'Activo') = 'Activo'
                  AND COALESCE(d.cantidad,0) > 0
                GROUP BY ls.referencia
            )
            SELECT
                r.ref AS referencia,
                COALESCE(sc.tiendas_activas_segmentadas, 0) AS tiendas_activas_segmentadas,
                (COALESCE(sc.tiendas_activas_segmentadas, 0) > 0) AS is_segmented
            FROM (SELECT UNNEST(%(refs)s) AS ref) r
            LEFT JOIN seg_conteo sc
              ON sc.referencia = r.ref;
        """

        rows = self._repo.fetch_all(sql, {"refs": ref_list})

        out: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            ref = (r.get("referencia") or "").strip()
            out[ref] = {
                "is_segmented": bool(r.get("is_segmented")),
                "tiendas_activas_segmentadas": int(r.get("tiendas_activas_segmentadas") or 0),
            }

        # asegurar que todas queden
        for ref in ref_list:
            out.setdefault(ref, {"is_segmented": False, "tiendas_activas_segmentadas": 0})

        return out

    def marcar_como_segmentada(self, referencia_sku: str) -> None:
        ref = (referencia_sku or "").strip()
        if not ref:
            return

        now = datetime.now(TZ_BOGOTA)

        sql = """
            INSERT INTO public.referencias_vistas (referencia_sku, first_seen, last_seen, segmented_at)
            VALUES (%(ref)s, %(now)s, %(now)s, %(now)s)
            ON CONFLICT (referencia_sku)
            DO UPDATE SET
                last_seen = EXCLUDED.last_seen,
                segmented_at = EXCLUDED.segmented_at;
        """
        self._repo.execute(sql, {"ref": ref, "now": now})

    def reset_segmentaciones(self) -> Dict[str, Any]:
        """
        Limpia por completo las tablas de segmentación.
        Nota: TRUNCATE para reiniciar rápido.
        """
        def _tx(cur):
            cur.execute("""
                TRUNCATE TABLE
                    public.segmentacion_detalle,
                    public.segmentacion
                RESTART IDENTITY;
            """)
            return {"ok": True, "mensaje": "Segmentaciones reiniciadas correctamente."}

        return self._repo.run_in_transaction(_tx)
