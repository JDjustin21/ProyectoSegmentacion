# backend/modules/segmentacion/segmentacion_db_service.py
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

from backend.config.settings import DEFAULT_USER_ID
from backend.repositories.postgres_repository import PostgresRepository


TZ_BOGOTA = ZoneInfo("America/Bogota")
CLASIFICACIONES_EXACTAS = {"AA", "A", "B", "C", "NA"}
TESTEO_EXACTO = {"TESTEO", "NO TESTEO"}


class SegmentacionDbService:
    """
    Servicio de negocio para Postgres (Segmentación):

    - Consultar tiendas activas por línea (desde una vista normalizada)
    - Guardar segmentaciones (cabecera + detalle) con histórico
    - Consultar última segmentación por referencia
    - Exportar dataset para CSV
    - Reset (TRUNCATE) de tablas de segmentación (admin)
    """

    def __init__(self, repo: PostgresRepository, view_tiendas: str, view_existencia_talla: str,):
        self._repo = repo
        self._view_tiendas = view_tiendas
        self._view_existencia_talla = view_existencia_talla

    def obtener_snapshot_updated_at(self) -> Optional[datetime]:
        """
        Retorna la fecha/hora más reciente del snapshot vigente.

        Responsabilidad:
        - solo traer el metadato de actualización
        - evitar leer todas las referencias cuando la vista shell
          solo necesita mostrar la fecha del snapshot
        """
        sql = """
            SELECT MAX(loaded_at) AS loaded_at
            FROM public.referencias_snapshot_actual;
        """
        row = self._repo.fetch_one(sql)

        loaded_at = row.get("loaded_at") if row else None
        if not loaded_at:
            return None

        if getattr(loaded_at, "tzinfo", None) is not None:
            return loaded_at.astimezone(TZ_BOGOTA)

        return loaded_at

    def listar_referencias_resumen_cards(
        self
    ) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
        """
        Retorna SOLO el dataset liviano para cards.

        Importante:
        - no trae campos pesados del modal como tallasConteo o codigosBarrasPorTalla
        - anota referencias nuevas solo en lectura
        - anota estado y conteo de segmentación
        """
        sql = """
            SELECT
                referencia_sku AS referencia,
                descripcion,
                categoria,
                color,
                estado,
                tipo_portafolio AS "tipoPortafolio",
                linea,
                cuento,
                precio_unitario AS "precioUnitario",
                fecha_creacion AS "fechaCreacion",
                cantidad_tallas AS "cantidadTallas"
            FROM public.referencias_snapshot_actual
            ORDER BY referencia_sku;
        """
        referencias = self._repo.fetch_all(sql)
        cache_updated_at = self.obtener_snapshot_updated_at()

        referencias = self.anotar_referencias_nuevas(referencias)
        referencias = self.anotar_segmentacion_y_conteo(referencias)

        def _fecha_sort_value(v):
            if not v:
                return ""
            try:
                return str(v)
            except Exception:
                return ""

        referencias.sort(
            key=lambda r: (
                0 if r.get("is_new") else 1,
                _fecha_sort_value(r.get("fechaCreacion")) if r.get("is_new") else "",
                r.get("referencia", "")
            ),
            reverse=False
        )

        # Reordenar nuevas por fecha descendente manteniendo resto abajo
        nuevas = [r for r in referencias if r.get("is_new")]
        resto = [r for r in referencias if not r.get("is_new")]

        nuevas.sort(
            key=lambda r: (_fecha_sort_value(r.get("fechaCreacion")), r.get("referencia", "")),
            reverse=True
        )
        resto.sort(key=lambda r: r.get("referencia", ""))

        referencias = nuevas + resto

        return referencias, cache_updated_at
    
    def listar_segmentaciones_candidatas_por_base(
        self,
        referencia_base: str,
        cuento: str = "",
        referencia_sku_actual: str = ""
    ) -> List[Dict[str, Any]]:
        base = (referencia_base or "").strip()
        cuento_val = (cuento or "").strip()
        actual = (referencia_sku_actual or "").strip()

        if not base and not cuento_val:
            return []

        sql = """
            WITH last_seg_por_sku AS (
                SELECT DISTINCT ON (s.referencia)
                    s.id_segmentacion,
                    s.referencia AS referencia_sku,
                    s.referencia_base,
                    s.cuento,
                    s.color,
                    s.codigo_color,
                    s.descripcion,
                    s.linea,
                    s.fecha_creacion,
                    s.estado_segmentacion,
                    CASE
                        WHEN (%(base)s <> '' AND COALESCE(s.referencia_base, '') = %(base)s)
                        AND (%(cuento)s <> '' AND COALESCE(s.cuento, '') = %(cuento)s)
                            THEN 3
                        WHEN (%(base)s <> '' AND COALESCE(s.referencia_base, '') = %(base)s)
                            THEN 2
                        WHEN (%(cuento)s <> '' AND COALESCE(s.cuento, '') = %(cuento)s)
                            THEN 1
                        ELSE 0
                    END AS prioridad_copia,
                    CASE
                        WHEN (%(base)s <> '' AND COALESCE(s.referencia_base, '') = %(base)s)
                        AND (%(cuento)s <> '' AND COALESCE(s.cuento, '') = %(cuento)s)
                            THEN 'base + cuento'
                        WHEN (%(base)s <> '' AND COALESCE(s.referencia_base, '') = %(base)s)
                            THEN 'base'
                        WHEN (%(cuento)s <> '' AND COALESCE(s.cuento, '') = %(cuento)s)
                            THEN 'cuento'
                        ELSE ''
                    END AS tipo_coincidencia
                FROM segmentacion s
                WHERE
                    (
                        (%(base)s <> '' AND COALESCE(s.referencia_base, '') = %(base)s)
                        OR
                        (%(cuento)s <> '' AND COALESCE(s.cuento, '') = %(cuento)s)
                    )
                    AND (%(actual)s = '' OR s.referencia <> %(actual)s)
                ORDER BY
                    s.referencia,
                    CASE
                        WHEN (%(base)s <> '' AND COALESCE(s.referencia_base, '') = %(base)s)
                        AND (%(cuento)s <> '' AND COALESCE(s.cuento, '') = %(cuento)s)
                            THEN 3
                        WHEN (%(base)s <> '' AND COALESCE(s.referencia_base, '') = %(base)s)
                            THEN 2
                        WHEN (%(cuento)s <> '' AND COALESCE(s.cuento, '') = %(cuento)s)
                            THEN 1
                        ELSE 0
                    END DESC,
                    s.fecha_creacion DESC
            ),
            det AS (
                SELECT
                    d.id_segmentacion,
                    COUNT(DISTINCT CASE
                        WHEN COALESCE(d.estado_detalle, 'Activo') = 'Activo'
                        AND COALESCE(d.cantidad, 0) > 0
                        THEN d.llave_naval
                    END) AS tiendas_segmentadas,
                    COALESCE(SUM(CASE
                        WHEN COALESCE(d.estado_detalle, 'Activo') = 'Activo'
                        AND COALESCE(d.cantidad, 0) > 0
                        THEN d.cantidad
                        ELSE 0
                    END), 0) AS total_unidades
                FROM segmentacion_detalle d
                GROUP BY d.id_segmentacion
            )
            SELECT
                l.id_segmentacion,
                l.referencia_sku,
                l.referencia_base,
                l.cuento,
                l.color,
                l.codigo_color,
                l.descripcion,
                l.linea,
                l.fecha_creacion,
                l.estado_segmentacion,
                l.prioridad_copia,
                l.tipo_coincidencia,
                COALESCE(det.tiendas_segmentadas, 0) AS tiendas_segmentadas,
                COALESCE(det.total_unidades, 0) AS total_unidades
            FROM last_seg_por_sku l
            LEFT JOIN det ON det.id_segmentacion = l.id_segmentacion
            WHERE l.prioridad_copia > 0
            ORDER BY l.prioridad_copia DESC, l.fecha_creacion DESC;
        """
        return self._repo.fetch_all(sql, {
            "base": base,
            "cuento": cuento_val,
            "actual": actual,
        })
    
    def obtener_segmentacion_para_copiar(self, id_segmentacion: int) -> Dict[str, Any]:
        sql_head = """
            SELECT
                id_segmentacion,
                referencia AS referencia_sku,
                referencia_base,
                color,
                codigo_color,
                descripcion,
                linea,
                fecha_creacion,
                estado_segmentacion
            FROM segmentacion
            WHERE id_segmentacion = %(id)s
            LIMIT 1;
        """
        head = self._repo.fetch_one(sql_head, {"id": id_segmentacion})
        if not head:
            return {"existe": False, "segmentacion": None}

        sql_det = """
            SELECT
                llave_naval,
                talla,
                cantidad,
                codigo_barras,
                estado_detalle
            FROM segmentacion_detalle
            WHERE id_segmentacion = %(id)s
            AND COALESCE(estado_detalle, 'Activo') = 'Activo'
            AND COALESCE(cantidad, 0) > 0;
        """
        detalle = self._repo.fetch_all(sql_det, {"id": id_segmentacion})

        head["detalle"] = detalle
        return {"existe": True, "segmentacion": head}
    

    def _listar_llaves_activas_por_linea(self, linea_raw: str) -> set[str]:
        linea_norm = self.normalizar_linea(linea_raw)

        sql = f"""
            SELECT DISTINCT v.llave_naval
            FROM {self._view_tiendas} v
            WHERE v.linea_norm = %(linea_norm)s
            AND v.estado_tienda_norm = 'activo'
            AND v.estado_linea_norm  = 'activo';
        """
        rows = self._repo.fetch_all(sql, {"linea_norm": linea_norm})
        return {
            (r.get("llave_naval") or "").strip()
            for r in rows
            if (r.get("llave_naval") or "").strip()
        }
    def listar_tiendas_candidatas_para_copiar(
            self,
            linea_raw: str
        ) -> List[Dict[str, Any]]:
            """
            Lista tiendas que ya están segmentadas en referencias de la línea dada.
            Sirve para alimentar el menú principal de 'copiar tienda'.
            """
            linea = (linea_raw or "").strip()
            if not linea:
                return []

            llaves_activas = self._listar_llaves_activas_por_linea(linea)
            if not llaves_activas:
                return []

            sql = """
                WITH ultimas AS (
                    SELECT DISTINCT ON (s.referencia)
                        s.id_segmentacion,
                        s.referencia,
                        s.linea,
                        s.fecha_creacion
                    FROM segmentacion s
                    WHERE COALESCE(s.linea, '') = %(linea)s
                    ORDER BY s.referencia, s.fecha_creacion DESC
                ),
                det AS (
                    SELECT
                        u.referencia,
                        u.fecha_creacion,
                        d.llave_naval,
                        COUNT(*) AS filas_talla,
                        COALESCE(SUM(d.cantidad), 0) AS total_unidades
                    FROM ultimas u
                    JOIN segmentacion_detalle d
                    ON d.id_segmentacion = u.id_segmentacion
                    WHERE COALESCE(d.estado_detalle, 'Activo') = 'Activo'
                    AND COALESCE(d.cantidad, 0) > 0
                    GROUP BY u.referencia, u.fecha_creacion, d.llave_naval
                ),
                ref_ejemplo AS (
                    SELECT DISTINCT ON (d.llave_naval)
                        d.llave_naval,
                        d.referencia AS referencia_ejemplo
                    FROM det d
                    ORDER BY d.llave_naval, d.fecha_creacion DESC, d.referencia ASC
                )
                SELECT
                    d.llave_naval,
                    MIN(t.desc_dependencia) AS desc_dependencia,
                    MIN(t.dependencia) AS dependencia,
                    MIN(t.ciudad) AS ciudad,
                    MIN(t.zona) AS zona,
                    COUNT(DISTINCT d.referencia) AS referencias_segmentadas,
                    COALESCE(SUM(d.total_unidades), 0) AS total_unidades,
                    MAX(r.referencia_ejemplo) AS referencia_ejemplo
                FROM det d
                LEFT JOIN public.vw_maestra_tiendas_activa_norm t
                ON t.llave_naval = d.llave_naval
                LEFT JOIN ref_ejemplo r
                ON r.llave_naval = d.llave_naval
                WHERE d.llave_naval = ANY(%(llaves)s)
                GROUP BY d.llave_naval
                ORDER BY referencias_segmentadas DESC, desc_dependencia ASC NULLS LAST;
            """

            return self._repo.fetch_all(sql, {
                "linea": linea,
                "llaves": list(llaves_activas),
            })
    
    def preview_copiar_tienda_a_linea_segmentada(
        self,
        linea_raw: str,
        llave_naval_origen: str
    ) -> Dict[str, Any]:
        """
        No guarda nada.
        Solo calcula si existe plantilla para esa tienda y cuántas referencias
        segmentadas de la línea serían afectadas.
        """
        linea = (linea_raw or "").strip()
        llave = (llave_naval_origen or "").strip()

        if not linea:
            return {"ok": False, "error": "Falta línea."}
        if not llave:
            return {"ok": False, "error": "Falta llave_naval_origen."}

        llaves_activas = self._listar_llaves_activas_por_linea(linea)
        if llave not in llaves_activas:
            return {
                "ok": False,
                "error": "La tienda no está activa para la línea seleccionada."
            }

        sql_plantilla = """
            WITH ultimas AS (
                SELECT DISTINCT ON (s.referencia)
                    s.id_segmentacion,
                    s.referencia,
                    s.linea,
                    s.fecha_creacion
                FROM segmentacion s
                WHERE COALESCE(s.linea, '') = %(linea)s
                ORDER BY s.referencia, s.fecha_creacion DESC
            )
            SELECT
                u.id_segmentacion,
                u.referencia,
                u.fecha_creacion
            FROM ultimas u
            JOIN segmentacion_detalle d
            ON d.id_segmentacion = u.id_segmentacion
            WHERE d.llave_naval = %(llave)s
            AND COALESCE(d.estado_detalle, 'Activo') = 'Activo'
            AND COALESCE(d.cantidad, 0) > 0
            ORDER BY u.fecha_creacion DESC
            LIMIT 1;
        """
        plantilla = self._repo.fetch_one(sql_plantilla, {
            "linea": linea,
            "llave": llave
        })

        if not plantilla:
            return {
                "ok": False,
                "error": "No existe una plantilla de segmentación para esa tienda en la línea."
            }

        sql_destino = """
            WITH ultimas AS (
                SELECT DISTINCT ON (s.referencia)
                    s.id_segmentacion,
                    s.referencia,
                    s.linea
                FROM segmentacion s
                WHERE COALESCE(s.linea, '') = %(linea)s
                ORDER BY s.referencia, s.fecha_creacion DESC
            )
            SELECT COUNT(*) AS total
            FROM ultimas;
        """
        row = self._repo.fetch_one(sql_destino, {"linea": linea})
        total_refs = int(row["total"]) if row and row.get("total") is not None else 0

        return {
            "ok": True,
            "linea": linea,
            "llave_naval_origen": llave,
            "referencia_origen": plantilla.get("referencia"),
            "id_segmentacion_origen": plantilla.get("id_segmentacion"),
            "referencias_afectadas": total_refs,
        }
    
    def ejecutar_copiar_tienda_a_linea_segmentada(
        self,
        linea_raw: str,
        llave_naval_origen: str
    ) -> Dict[str, Any]:
        """
        Copia una tienda segmentada a todas las referencias ya segmentadas
        de la misma línea. Solo afecta esa tienda.
        """
        linea = (linea_raw or "").strip()
        llave = (llave_naval_origen or "").strip()

        preview = self.preview_copiar_tienda_a_linea_segmentada(
            linea_raw=linea,
            llave_naval_origen=llave,
        )
        if not preview.get("ok"):
            return preview

        id_seg_origen = int(preview["id_segmentacion_origen"])
        llaves_activas = self._listar_llaves_activas_por_linea(linea)

        def _tx(cur):
            now = datetime.now(TZ_BOGOTA)

            # 1) Leer plantilla (solo la tienda origen)
            cur.execute("""
                SELECT talla, cantidad, codigo_barras
                FROM segmentacion_detalle
                WHERE id_segmentacion = %(id_seg)s
                AND llave_naval = %(llave)s
                AND COALESCE(estado_detalle, 'Activo') = 'Activo'
                AND COALESCE(cantidad, 0) > 0
                ORDER BY talla;
            """, {
                "id_seg": id_seg_origen,
                "llave": llave
            })
            plantilla = cur.fetchall() or []
            if not plantilla:
                raise RuntimeError("La plantilla de la tienda origen no contiene tallas activas.")

            # 2) Referencias destino = últimas segmentaciones de la línea
            cur.execute("""
                WITH ultimas AS (
                    SELECT DISTINCT ON (s.referencia)
                        s.id_segmentacion,
                        s.referencia
                    FROM segmentacion s
                    WHERE COALESCE(s.linea, '') = %(linea)s
                    ORDER BY s.referencia, s.fecha_creacion DESC
                )
                SELECT id_segmentacion, referencia
                FROM ultimas
                ORDER BY referencia;
            """, {"linea": linea})
            destinos = cur.fetchall() or []

            actualizadas = 0
            omitidas = 0

            for dest in destinos:
                id_seg_dest = int(dest["id_segmentacion"])
                referencia_dest = (dest["referencia"] or "").strip()

                if not referencia_dest:
                    omitidas += 1
                    continue

                if llave not in llaves_activas:
                    omitidas += 1
                    continue

                # 3) Obtener las tallas que vienen en la plantilla
                tallas_plantilla = {
                    (fila["talla"] or "").strip()
                    for fila in plantilla
                    if (fila.get("talla") or "").strip()
                }

                # 4) Inactivar SOLO las tallas actuales de esa tienda que ya no estén en la plantilla
                cur.execute("""
                    UPDATE segmentacion_detalle
                    SET
                        estado_detalle = 'Inactivo',
                        fecha_actualizacion = %(now)s
                    WHERE id_segmentacion = %(id_seg)s
                    AND llave_naval = %(llave)s
                    AND talla <> ALL(%(tallas_plantilla)s);
                """, {
                    "id_seg": id_seg_dest,
                    "llave": llave,
                    "tallas_plantilla": list(tallas_plantilla),
                    "now": now
                })

                # 5) Upsert de las tallas de la plantilla:
                #    si ya existe esa talla para esa tienda/referencia -> UPDATE
                #    si no existe -> INSERT
                for fila in plantilla:
                    talla = (fila.get("talla") or "").strip()
                    if not talla:
                        continue

                    cur.execute("""
                        INSERT INTO segmentacion_detalle (
                            id_segmentacion,
                            llave_naval,
                            talla,
                            cantidad,
                            codigo_barras,
                            estado_detalle,
                            fecha_actualizacion
                        )
                        VALUES (
                            %(id_seg)s,
                            %(llave)s,
                            %(talla)s,
                            %(cantidad)s,
                            %(codigo_barras)s,
                            'Activo',
                            %(now)s
                        )
                        ON CONFLICT (id_segmentacion, llave_naval, talla)
                        DO UPDATE SET
                            cantidad = EXCLUDED.cantidad,
                            codigo_barras = EXCLUDED.codigo_barras,
                            estado_detalle = 'Activo',
                            fecha_actualizacion = EXCLUDED.fecha_actualizacion;
                    """, {
                        "id_seg": id_seg_dest,
                        "llave": llave,
                        "talla": talla,
                        "cantidad": int(fila.get("cantidad") or 0),
                        "codigo_barras": (fila.get("codigo_barras") or None),
                        "now": now
                    })

                actualizadas += 1

            return {
                "ok": True,
                "linea": linea,
                "llave_naval_origen": llave,
                "referencia_origen": preview.get("referencia_origen"),
                "referencias_afectadas": preview.get("referencias_afectadas", 0),
                "referencias_actualizadas": actualizadas,
                "referencias_omitidas": omitidas,
            }

        return self._repo.run_in_transaction(_tx)

    def obtener_referencia_detalle_snapshot(
        self,
        referencia_sku: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retorna el detalle de UNA referencia desde snapshot.

        Este método alimenta el modal bajo demanda.
        Aquí sí traemos los campos pesados que no deben viajar
        en el listado principal.
        """
        ref = (referencia_sku or "").strip()
        if not ref:
            return None

        sql = """
            SELECT
                referencia_sku AS "referenciaSku",
                referencia_base AS "referenciaBase",
                descripcion,
                categoria,
                color,
                codigo_color AS "codigoColor",
                perfil_prenda AS "perfilPrenda",
                estado,
                tipo_inventario AS "tipoInventario",
                tipo_portafolio AS "tipoPortafolio",
                linea,
                cuento,
                precio_unitario AS "precioUnitario",
                fecha_creacion AS "fechaCreacion",
                cantidad_tallas AS "cantidadTallas",
                tallas,
                tallas_conteo_json AS "tallasConteo",
                codigos_barras_por_talla_json AS "codigosBarrasPorTalla"
            FROM public.referencias_snapshot_actual
            WHERE referencia_sku = %(ref)s
            LIMIT 1;
        """
        return self._repo.fetch_one(sql, {"ref": ref})
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
        testeo_up = " ".join(testeo_v.upper().split())
        testeo_is_exact = testeo_up in TESTEO_EXACTO

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
              AND (
                    %(testeo)s = ''
                    OR (
                        %(testeo_is_exact)s = TRUE
                        AND UPPER(regexp_replace(COALESCE(v.testeo_fnl,''), '\s+', ' ', 'g')) = %(testeo_exact)s
                    )
                    OR (
                        %(testeo_is_exact)s = FALSE
                        AND COALESCE(v.testeo_fnl,'') ILIKE %(testeo_like)s
                    )
                )

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
                "testeo_is_exact": testeo_is_exact,
                "testeo_exact": testeo_up,
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

        referencia_base = (payload.get("referencia") or "").strip()
        codigo_color = (payload.get("codigo_color") or payload.get("codigoColor") or "").strip()
        color = (payload.get("color") or "").strip()
        perfil_prenda = (payload.get("perfil_prenda") or payload.get("perfilPrenda") or "").strip()

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
            codigo_barras = (d.get("codigo_barras") or "").strip()

            filas_activo.append({"llave_naval": llave, 
                                 "talla": talla, 
                                 "cantidad": cantidad, 
                                 "codigo_barras": codigo_barras or None})

        new_keys = {(r["llave_naval"], r["talla"]) for r in filas_activo}
        nueva_activa = bool(new_keys)

        tiendas_estado = payload.get("tiendas_estado") or {}
        if not isinstance(tiendas_estado, dict):
            tiendas_estado = {}

        llaves_inactivas = [k.strip() for k, v in tiendas_estado.items() if str(v).lower() in ("false", "0") or v is False]
        llaves_inactivas = [k for k in llaves_inactivas if k]

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

           # 3) Determinar id_segmentacion sobre el cual vamos a trabajar
            payload_id = payload.get("id_segmentacion") or payload.get("id_segmentacion_actual")
            id_seg = int(payload_id) if payload_id else None

            if id_seg:
                # validar que exista y corresponda a la referencia
                cur.execute("""
                    SELECT id_segmentacion
                    FROM segmentacion
                    WHERE id_segmentacion = %(id)s AND referencia = %(ref)s
                    FOR UPDATE;
                """, {"id": id_seg, "ref": ref})
                ok = cur.fetchone()
                if not ok:
                    # si el frontend mandó un id que no existe, caemos a "usar la última"
                    id_seg = None

            if not id_seg:
                # tomar última (activa o la más reciente)
                cur.execute("""
                    SELECT id_segmentacion
                    FROM segmentacion
                    WHERE referencia = %(ref)s
                    ORDER BY fecha_creacion DESC
                    LIMIT 1
                    FOR UPDATE;
                """, {"ref": ref})
                last = cur.fetchone()
                id_seg = int(last["id_segmentacion"]) if last else None

            # 4) Si no existe ninguna, creamos cabecera nueva (solo primera vez)
            if not id_seg:
                cur.execute("""
                    INSERT INTO segmentacion (
                        id_usuario,
                        fecha_creacion,
                        id_version_tiendas,
                        estado_segmentacion,
                        referencia,
                        referencia_base,
                        codigo_color,
                        color,
                        perfil_prenda,
                        codigo_barras,
                        descripcion,
                        categoria,
                        linea,
                        tipo_portafolio,
                        precio_unitario,
                        estado_sku,
                        cuento,
                        tipo_inventario
                    )
                    VALUES (
                        %(id_usuario)s,
                        %(fecha_creacion)s,
                        %(id_version_tiendas)s,
                        %(estado_segmentacion)s,
                        %(referencia)s,
                        %(referencia_base)s,
                        %(codigo_color)s,
                        %(color)s,
                        %(perfil_prenda)s,
                        %(codigo_barras)s,
                        %(descripcion)s,
                        %(categoria)s,
                        %(linea)s,
                        %(tipo_portafolio)s,
                        %(precio_unitario)s,
                        %(estado_sku)s,
                        %(cuento)s,
                        %(tipo_inventario)s
                    )
                    RETURNING id_segmentacion;
                """, {
                    "id_usuario": int(payload.get("id_usuario") or DEFAULT_USER_ID),
                    "fecha_creacion": now,
                    "id_version_tiendas": id_version_activa,
                    "estado_segmentacion": "Activa" if nueva_activa else "Inactiva",
                    "referencia": ref,
                    "referencia_base": referencia_base or None,
                    "codigo_color": codigo_color or None,
                    "color": color or None,
                    "perfil_prenda": perfil_prenda or None,
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
                id_seg = int(head_row["id_segmentacion"])

            else:
                # 5) Si ya existe cabecera, SOLO actualizamos metadata mínima si quieres
                # (esto NO crea filas nuevas)
                cur.execute("""
                    UPDATE segmentacion
                    SET
                        id_version_tiendas = %(id_version)s,
                        referencia_base = %(referencia_base)s,
                        codigo_color = %(codigo_color)s,
                        color = %(color)s,
                        perfil_prenda = %(perfil_prenda)s,
                        codigo_barras = %(codigo_barras)s,
                        descripcion = %(descripcion)s,
                        categoria = %(categoria)s,
                        linea = %(linea)s,
                        tipo_portafolio = %(tipo_portafolio)s,
                        precio_unitario = %(precio_unitario)s,
                        estado_sku = %(estado_sku)s,
                        cuento = %(cuento)s,
                        tipo_inventario = %(tipo_inventario)s
                    WHERE id_segmentacion = %(id_seg)s;
                """, {
                    "id_version": id_version_activa,
                    "id_seg": id_seg,
                    "referencia_base": referencia_base or None,
                    "codigo_color": codigo_color or None,
                    "color": color or None,
                    "perfil_prenda": perfil_prenda or None,
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

            # 6) Upsert de detalle para activos (cantidad>0) -> Activo, actualiza qty y barcode
            if filas_activo:
                cur.executemany("""
                    INSERT INTO segmentacion_detalle (
                        id_segmentacion, llave_naval, talla, cantidad, codigo_barras, estado_detalle, fecha_actualizacion
                    )
                    VALUES (
                        %(id_segmentacion)s, %(llave_naval)s, %(talla)s, %(cantidad)s, %(codigo_barras)s, 'Activo', %(fecha_actualizacion)s
                    )
                    ON CONFLICT (id_segmentacion, llave_naval, talla)
                    DO UPDATE SET
                        cantidad = EXCLUDED.cantidad,
                        codigo_barras = EXCLUDED.codigo_barras,
                        estado_detalle = 'Activo',
                        fecha_actualizacion = EXCLUDED.fecha_actualizacion;
                """, [{
                    "id_segmentacion": id_seg,
                    "llave_naval": r["llave_naval"],
                    "talla": r["talla"],
                    "cantidad": int(r["cantidad"]),
                    "codigo_barras": r.get("codigo_barras"),
                    "fecha_actualizacion": now,
                } for r in filas_activo])

            # 7) Para llaves inactivas: SOLO cambiar estado_detalle y fecha_actualizacion
            # (NO tocar cantidad, NO tocar codigo_barras)
            if llaves_inactivas:
                cur.execute("""
                    UPDATE segmentacion_detalle
                    SET estado_detalle = 'Inactivo',
                        fecha_actualizacion = %(now)s
                    WHERE id_segmentacion = %(id)s
                    AND llave_naval = ANY(%(llaves)s);
                """, {"now": now, "id": id_seg, "llaves": llaves_inactivas})

            # 8) estado_segmentacion (regla sugerida):
            # - Activa si existe al menos un detalle Activo con cantidad>0
            # - Inactiva si no hay ninguno
            cur.execute("""
                SELECT 1
                FROM segmentacion_detalle
                WHERE id_segmentacion = %(id)s
                AND COALESCE(estado_detalle,'Activo') = 'Activo'
                AND cantidad > 0
                LIMIT 1;
            """, {"id": id_seg})
            has_any = cur.fetchone() is not None
            cur.execute("""
                UPDATE segmentacion
                SET estado_segmentacion = %(estado)s
                WHERE id_segmentacion = %(id)s;
            """, {"estado": "Activa" if has_any else "Inactiva", "id": id_seg})

            # 9) referencias_vistas badge (igual que tenías)
            if has_any:
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

            return {"id_seg": id_seg}

        tx_result = self._repo.run_in_transaction(_tx)

        return {
            "ok": True,
            "id_segmentacion": tx_result["id_seg"],
            "mensaje": "Segmentación guardada",
            "resumen": {
                "tiendas_con_cantidad": len(tiendas_con_cantidad),
                "total_unidades": total_units,
                "tallas_usadas": sorted(list(tallas_usadas)),
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
                CASE
                    WHEN COALESCE(d.estado_detalle, 'Activo') = 'Inactivo'
                        THEN 'Inactivo'
                    ELSE s.estado_segmentacion
                END AS estado_segmentacion,

                s.referencia AS referencia_sku,
                s.referencia_base,
                s.codigo_color,
                s.color,
                s.perfil_prenda,
                s.codigo_barras AS codigo_barras_sku,
                s.descripcion,
                s.categoria,
                s.linea,
                s.tipo_portafolio,
                s.estado_sku,
                s.cuento,
                s.tipo_inventario,
                s.precio_unitario,
                s.fecha_creacion,

                d.llave_naval,
                d.talla,
                d.codigo_barras AS codigo_barras,
                d.cantidad,
                ex.existencia_talla AS existencia,
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
            LEFT JOIN {self._view_existencia_talla} ex
            ON ex.llave_naval = d.llave_naval
            AND ex.referencia_sku = s.referencia
            AND ex.talla = d.talla
            AND COALESCE(ex.ean, '') = COALESCE(d.codigo_barras, '')
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
                CASE
                    WHEN COALESCE(d.estado_detalle, 'Activo') = 'Inactivo'
                        THEN 'Inactivo'
                    ELSE s.estado_segmentacion
                END AS estado_segmentacion,
                d.llave_naval,
                d.talla,
                d.cantidad,
                d.estado_detalle,
                d.fecha_actualizacion,

                ex.existencia_talla AS existencia,

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
            LEFT JOIN {self._view_existencia_talla} ex
            ON ex.llave_naval = d.llave_naval
            AND ex.referencia_sku = s.referencia
            AND ex.talla = d.talla
            AND COALESCE(ex.ean, '') = COALESCE(d.codigo_barras, '')
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
            INSERT INTO public.referencias_vistas (
                referencia_sku,
                first_seen,
                last_seen
            )
            VALUES (
                %(referencia_sku)s,
                %(now)s,
                %(now)s
            )
            ON CONFLICT (referencia_sku)
            DO UPDATE SET
                last_seen = EXCLUDED.last_seen;
        """
        rows = [{"referencia_sku": ref, "now": now} for ref in ref_list]
        self._repo.execute_many(sql_upsert, rows)

        cutoff = now - timedelta(days=int(dias_nuevo))
        sql_nuevas = """
            SELECT referencia_sku
            FROM public.referencias_vistas
            WHERE referencia_sku = ANY(%(refs)s)
            AND viewed_at IS NULL;
        """
        nuevas_rows = self._repo.fetch_all(sql_nuevas, {"cutoff": cutoff})
        nuevas_set = {(x.get("referencia_sku") or "").strip() for x in nuevas_rows}

        for r in referencias:
            ref = (r.get("referencia") or "").strip()
            r["is_new"] = bool(ref and ref in nuevas_set)

        return referencias
    
    def sincronizar_referencias_vistas_snapshot(self, referencias: List[Dict[str, Any]]) -> int:
        """
        Sincroniza referencias_vistas con el snapshot vigente.

        Responsabilidad:
        - registrar nuevas referencias si no existían
        - actualizar last_seen de las ya existentes

        Esta operación debe ejecutarse desde el job/snapshot,
        NO desde la carga de la vista principal.
        """
        if not referencias:
            return 0

        now = datetime.now(TZ_BOGOTA)

        ref_list: List[str] = []
        for r in referencias:
            ref = (r.get("referencia") or r.get("referenciaSku") or r.get("Referencia") or "").strip()
            if ref:
                ref_list.append(ref)

        if not ref_list:
            return 0

        sql_upsert = """
            INSERT INTO public.referencias_vistas (
                referencia_sku,
                first_seen,
                last_seen,
                viewed_at
            )
            VALUES (
                %(referencia_sku)s,
                %(now)s,
                %(now)s,
                NULL
            )
            ON CONFLICT (referencia_sku)
            DO UPDATE SET
                last_seen = EXCLUDED.last_seen;
        """
        rows = [{"referencia_sku": ref, "now": now} for ref in ref_list]
        self._repo.execute_many(sql_upsert, rows)

        return len(rows)

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
    
    def anotar_referencias_nuevas(
        self,
        referencias: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Marca is_new en las referencias usando referencias_vistas.

        Regla nueva:
        - is_new = True si viewed_at IS NULL
        - is_new = False si viewed_at IS NOT NULL

        Solo lectura. No hace upserts.
        """
        if not referencias:
            return referencias

        ref_list: List[str] = []
        for r in referencias:
            ref = (r.get("referencia") or r.get("referenciaSku") or r.get("Referencia") or "").strip()
            if ref:
                ref_list.append(ref)

        if not ref_list:
            return referencias

        sql_nuevas = """
            SELECT referencia_sku
            FROM public.referencias_vistas
            WHERE referencia_sku = ANY(%(refs)s)
            AND viewed_at IS NULL;
        """
        nuevas_rows = self._repo.fetch_all(sql_nuevas, {"refs": ref_list})
        nuevas_set = {(x.get("referencia_sku") or "").strip() for x in nuevas_rows}

        for r in referencias:
            ref = (r.get("referencia") or r.get("referenciaSku") or r.get("Referencia") or "").strip()
            r["is_new"] = bool(ref and ref in nuevas_set)

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