#backend/modules/segmentacion/referencias_snapshot_service.py

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from backend.modules.segmentacion.services import SegmentacionService
from backend.repositories.postgres_repository import PostgresRepository

from backend.modules.segmentacion.segmentacion_db_service import SegmentacionDbService
from backend.config.settings import (
    POSTGRES_TIENDAS_VIEW,
    METRICAS_EXISTENCIA_TALLA_VIEW,
)

class ReferenciasSnapshotService:
    """
    Servicio responsable de refrescar el snapshot vigente de referencias.

    Responsabilidades:
    - Llamar a la API .NET de referencias
    - Registrar el proceso en referencias_refresh_control
    - Cargar staging
    - Validar el lote
    - Promover a actual
    - Marcar el refresh como success / failed

    Notas:
    - NO expone lógica HTTP.
    - NO depende de Flask request context.
    - Está diseñado para ser usado por jobs programados.
    """

    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"

    TRIGGER_SCHEDULED = "scheduled"
    TRIGGER_MANUAL = "manual"
    TRIGGER_STARTUP = "startup"

    def __init__(self, repo: PostgresRepository, sqlserver_api_url: str):
        self._repo = repo
        self._sqlserver_api_url = sqlserver_api_url

    # =========================
    # API pública principal
    # =========================
    def refresh_snapshot(
        self,
        trigger_type: str = TRIGGER_MANUAL,
        created_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ejecuta el refresh completo del snapshot.

        Flujo:
        1) crea refresh_control en running
        2) consume API .NET
        3) transforma registros
        4) carga staging
        5) valida
        6) promueve a actual
        7) marca success
        Si falla:
        - deja actual intacta
        - marca failed
        """
        refresh_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)

        self._insert_refresh_control_running(
            refresh_id=refresh_id,
            started_at=started_at,
            trigger_type=trigger_type,
            created_by=created_by,
        )

        try:
            raw_rows = self._obtener_referencias_desde_api()
            source_count = len(raw_rows)

            snapshot_rows = self._mapear_referencias_snapshot(
                raw_rows=raw_rows,
                refresh_id=refresh_id,
                loaded_at=started_at,
            )

            loaded_count = len(snapshot_rows)

            if loaded_count == 0:
                raise RuntimeError("La API devolvió 0 referencias útiles para snapshot.")

            self._cargar_staging(refresh_id=refresh_id, rows=snapshot_rows)
            self._validar_staging(refresh_id=refresh_id, expected_count=loaded_count)
            promoted_count = self._promover_a_actual(refresh_id=refresh_id)

            svc_pg = SegmentacionDbService(
                self._repo,
                POSTGRES_TIENDAS_VIEW,
                METRICAS_EXISTENCIA_TALLA_VIEW,
            )
            synced_count = svc_pg.sincronizar_referencias_vistas_snapshot(snapshot_rows)

            finished_at = datetime.now(timezone.utc)
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)

            self._update_refresh_control_success(
                refresh_id=refresh_id,
                finished_at=finished_at,
                source_count=source_count,
                loaded_count=loaded_count,
                promoted_count=promoted_count,
                duration_ms=duration_ms,
            )

            return {
                "ok": True,
                "refresh_id": refresh_id,
                "source_count": source_count,
                "loaded_count": loaded_count,
                "promoted_count": promoted_count,
                "synced_count": synced_count,
                "duration_ms": duration_ms,
            }

        except Exception as ex:
            finished_at = datetime.now(timezone.utc)
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)

            self._update_refresh_control_failed(
                refresh_id=refresh_id,
                finished_at=finished_at,
                duration_ms=duration_ms,
                error_message=str(ex),
            )
            raise

    # =========================
    # Consumo de API
    # =========================
    def _obtener_referencias_desde_api(self) -> List[Dict[str, Any]]:
        servicio = SegmentacionService(self._sqlserver_api_url)
        rows = servicio.obtener_referencias()
        return rows if isinstance(rows, list) else []

    # =========================
    # Mapeo
    # =========================
    def _mapear_referencias_snapshot(
        self,
        raw_rows: List[Dict[str, Any]],
        refresh_id: str,
        loaded_at: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Convierte la salida actual de la API .NET al shape de snapshot en Postgres.

        Este método es tolerante a snake_case / camelCase / PascalCase
        para no romper la integración si cambia el serializado.
        """
        out: List[Dict[str, Any]] = []

        for r in raw_rows:
            referencia_sku = self._pick_str(r, "referencia", "referenciaSku", "Referencia")
            if not referencia_sku:
                continue

            item = {
                "refresh_id": refresh_id,
                "loaded_at": loaded_at,
                "referencia_sku": referencia_sku,
                "referencia_base": self._pick_str(r, "referencia_base", "referenciaBase", "ReferenciaBase"),
                "descripcion": self._pick_str(r, "descripcion", "Descripcion"),
                "categoria": self._pick_str(r, "categoria", "Categoria"),
                "color": self._pick_str(r, "color", "Color"),
                "codigo_color": self._pick_str(r, "codigo_color", "codigoColor", "CodigoColor"),
                "perfil_prenda": self._pick_str(r, "perfil_prenda", "perfilPrenda", "PerfilPrenda"),
                "estado": self._pick_str(r, "estado", "Estado"),
                "tipo_inventario": self._pick_str(r, "tipo_inventario", "tipoInventario", "TipoInventario"),
                "tipo_portafolio": self._pick_str(r, "tipo_portafolio", "tipoPortafolio", "TipoPortafolio"),
                "linea": self._pick_str(r, "linea", "Linea"),
                "cuento": self._pick_str(r, "cuento", "Cuento"),
                "precio_unitario": self._pick_decimal(r, "precio_unitario", "precioUnitario", "PrecioUnitario"),
                "fecha_creacion": self._pick_datetime(r, "fecha_creacion", "fechaCreacion", "FechaCreacion"),
                "cantidad_tallas": self._pick_int(r, "cantidad_tallas", "cantidadTallas", "CantidadTallas"),
                "tallas": self._pick_str(r, "tallas", "Tallas"),
                "tallas_conteo_json": self._pick_json(r, "tallas_conteo", "tallasConteo", "TallasConteo"),
                "codigos_barras_por_talla_json": self._pick_json(
                    r,
                    "codigos_barras_por_talla",
                    "codigosBarrasPorTalla",
                    "CodigosBarrasPorTalla",
                ),
            }
            out.append(item)

        return out

    # =========================
    # Refresh control
    # =========================
    def _insert_refresh_control_running(
        self,
        refresh_id: str,
        started_at: datetime,
        trigger_type: str,
        created_by: Optional[str],
    ) -> None:
        sql = """
            INSERT INTO public.referencias_refresh_control (
                refresh_id,
                started_at,
                status,
                trigger_type,
                created_by
            )
            VALUES (
                %(refresh_id)s,
                %(started_at)s,
                %(status)s,
                %(trigger_type)s,
                %(created_by)s
            );
        """
        self._repo.execute(
            sql,
            {
                "refresh_id": refresh_id,
                "started_at": started_at,
                "status": self.STATUS_RUNNING,
                "trigger_type": trigger_type,
                "created_by": created_by,
            },
        )

    def _update_refresh_control_success(
        self,
        refresh_id: str,
        finished_at: datetime,
        source_count: int,
        loaded_count: int,
        promoted_count: int,
        duration_ms: int,
    ) -> None:
        sql = """
            UPDATE public.referencias_refresh_control
            SET
                finished_at = %(finished_at)s,
                status = %(status)s,
                source_count = %(source_count)s,
                loaded_count = %(loaded_count)s,
                promoted_count = %(promoted_count)s,
                duration_ms = %(duration_ms)s,
                error_message = NULL
            WHERE refresh_id = %(refresh_id)s;
        """
        self._repo.execute(
            sql,
            {
                "refresh_id": refresh_id,
                "finished_at": finished_at,
                "status": self.STATUS_SUCCESS,
                "source_count": source_count,
                "loaded_count": loaded_count,
                "promoted_count": promoted_count,
                "duration_ms": duration_ms,
            },
        )

    def _update_refresh_control_failed(
        self,
        refresh_id: str,
        finished_at: datetime,
        duration_ms: int,
        error_message: str,
    ) -> None:
        sql = """
            UPDATE public.referencias_refresh_control
            SET
                finished_at = %(finished_at)s,
                status = %(status)s,
                duration_ms = %(duration_ms)s,
                error_message = %(error_message)s
            WHERE refresh_id = %(refresh_id)s;
        """
        self._repo.execute(
            sql,
            {
                "refresh_id": refresh_id,
                "finished_at": finished_at,
                "status": self.STATUS_FAILED,
                "duration_ms": duration_ms,
                "error_message": error_message,
            },
        )

    # =========================
    # Staging
    # =========================
    def _cargar_staging(self, refresh_id: str, rows: List[Dict[str, Any]]) -> None:
        """
        Inserta el lote completo en staging.
        Antes limpia residuos del mismo refresh_id por seguridad.
        """
        self._repo.execute(
            "DELETE FROM public.referencias_snapshot_staging WHERE refresh_id = %(refresh_id)s;",
            {"refresh_id": refresh_id},
        )

        sql = """
            INSERT INTO public.referencias_snapshot_staging (
                refresh_id,
                loaded_at,
                referencia_sku,
                referencia_base,
                descripcion,
                categoria,
                color,
                codigo_color,
                perfil_prenda,
                estado,
                tipo_inventario,
                tipo_portafolio,
                linea,
                cuento,
                precio_unitario,
                fecha_creacion,
                cantidad_tallas,
                tallas,
                tallas_conteo_json,
                codigos_barras_por_talla_json
            )
            VALUES (
                %(refresh_id)s,
                %(loaded_at)s,
                %(referencia_sku)s,
                %(referencia_base)s,
                %(descripcion)s,
                %(categoria)s,
                %(color)s,
                %(codigo_color)s,
                %(perfil_prenda)s,
                %(estado)s,
                %(tipo_inventario)s,
                %(tipo_portafolio)s,
                %(linea)s,
                %(cuento)s,
                %(precio_unitario)s,
                %(fecha_creacion)s,
                %(cantidad_tallas)s,
                %(tallas)s,
                %(tallas_conteo_json)s::jsonb,
                %(codigos_barras_por_talla_json)s::jsonb
            );
        """
        self._repo.execute_many(sql, rows)

    def _validar_staging(self, refresh_id: str, expected_count: int) -> None:
        row = self._repo.fetch_one(
            """
            SELECT
                COUNT(*) AS n,
                COUNT(*) FILTER (WHERE referencia_sku IS NULL OR TRIM(referencia_sku) = '') AS invalid_ref_count
            FROM public.referencias_snapshot_staging
            WHERE refresh_id = %(refresh_id)s;
            """,
            {"refresh_id": refresh_id},
        )

        n = int(row["n"] or 0) if row else 0
        invalid_ref_count = int(row["invalid_ref_count"] or 0) if row else 0

        if n != expected_count:
            raise RuntimeError(
                f"Validación de staging falló: expected_count={expected_count}, staged_count={n}"
            )

        if invalid_ref_count > 0:
            raise RuntimeError(
                f"Validación de staging falló: hay {invalid_ref_count} filas con referencia_sku inválida."
            )

    # =========================
    # Promoción a actual
    # =========================
    def _promover_a_actual(self, refresh_id: str) -> int:
        """
        Reemplaza completamente la tabla actual con el lote validado del refresh_id.
        Se hace dentro de transacción para no dejar la tabla a medias.
        """
        def _tx(cur):
            cur.execute(
                """
                SELECT COUNT(*) AS n
                FROM public.referencias_snapshot_staging
                WHERE refresh_id = %(refresh_id)s;
                """,
                {"refresh_id": refresh_id},
            )
            row = cur.fetchone()
            staged_count = int(row["n"] or 0) if row else 0

            if staged_count <= 0:
                raise RuntimeError("No hay filas en staging para promover a actual.")

            cur.execute("TRUNCATE TABLE public.referencias_snapshot_actual;")

            cur.execute(
                """
                INSERT INTO public.referencias_snapshot_actual (
                    refresh_id,
                    loaded_at,
                    referencia_sku,
                    referencia_base,
                    descripcion,
                    categoria,
                    color,
                    codigo_color,
                    perfil_prenda,
                    estado,
                    tipo_inventario,
                    tipo_portafolio,
                    linea,
                    cuento,
                    precio_unitario,
                    fecha_creacion,
                    cantidad_tallas,
                    tallas,
                    tallas_conteo_json,
                    codigos_barras_por_talla_json
                )
                SELECT
                    refresh_id,
                    loaded_at,
                    referencia_sku,
                    referencia_base,
                    descripcion,
                    categoria,
                    color,
                    codigo_color,
                    perfil_prenda,
                    estado,
                    tipo_inventario,
                    tipo_portafolio,
                    linea,
                    cuento,
                    precio_unitario,
                    fecha_creacion,
                    cantidad_tallas,
                    tallas,
                    tallas_conteo_json,
                    codigos_barras_por_talla_json
                FROM public.referencias_snapshot_staging
                WHERE refresh_id = %(refresh_id)s;
                """,
                {"refresh_id": refresh_id},
            )

            cur.execute("SELECT COUNT(*) AS n FROM public.referencias_snapshot_actual;")
            row2 = cur.fetchone()
            promoted_count = int(row2["n"] or 0) if row2 else 0

            if promoted_count != staged_count:
                raise RuntimeError(
                    f"Promoción inconsistente: staging={staged_count}, actual={promoted_count}"
                )

            return promoted_count

        return int(self._repo.run_in_transaction(_tx))

    # =========================
    # Helpers de mapeo
    # =========================
    @staticmethod
    def _pick_raw(data: Dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in data:
                return data.get(key)
        return None

    @classmethod
    def _pick_str(cls, data: Dict[str, Any], *keys: str) -> Optional[str]:
        value = cls._pick_raw(data, *keys)
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    @classmethod
    def _pick_int(cls, data: Dict[str, Any], *keys: str) -> Optional[int]:
        value = cls._pick_raw(data, *keys)
        if value is None or value == "":
            return None
        try:
            return int(value)
        except Exception:
            return None

    @classmethod
    def _pick_decimal(cls, data: Dict[str, Any], *keys: str) -> Optional[Decimal]:
        value = cls._pick_raw(data, *keys)
        if value is None or value == "":
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    @classmethod
    def _pick_datetime(cls, data: Dict[str, Any], *keys: str) -> Optional[datetime]:
        value = cls._pick_raw(data, *keys)
        if value in (None, ""):
            return None

        if isinstance(value, datetime):
            return value

        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

    @classmethod
    def _pick_json(cls, data: Dict[str, Any], *keys: str) -> Optional[str]:
        value = cls._pick_raw(data, *keys)
        if value is None:
            return None
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return None