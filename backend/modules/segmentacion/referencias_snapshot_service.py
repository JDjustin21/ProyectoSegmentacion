# backend/modules/segmentacion/referencias_snapshot_service.py

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from backend.config.settings import (
    METRICAS_EXISTENCIA_TALLA_VIEW,
    POSTGRES_TIENDAS_VIEW,
)
from backend.modules.segmentacion.segmentacion_db_service import SegmentacionDbService
from backend.modules.segmentacion.services import SegmentacionService
from backend.repositories.postgres_repository import PostgresRepository


class ReferenciasSnapshotService:
    """
    Servicio encargado de refrescar el snapshot vigente de referencias.

    Este servicio conecta la información operativa de Siesa/SQL Server con
    PostgreSQL. La pantalla principal de Segmentación no consulta directamente
    la API .NET; consume la tabla public.referencias_snapshot_actual.

    Flujo principal:
    1. Registra el inicio del refresh en public.referencias_refresh_control.
    2. Consulta referencias desde la API .NET.
    3. Normaliza la respuesta al formato esperado por PostgreSQL.
    4. Reemplaza el contenido de public.referencias_snapshot_staging.
    5. Valida que el lote cargado sea consistente.
    6. Promueve el lote validado a public.referencias_snapshot_actual.
    7. Sincroniza atributos comerciales vigentes en public.segmentacion.
    8. Sincroniza public.referencias_vistas para manejar referencias nuevas.
    9. Registra el resultado final del refresh.

    Reglas importantes:
    - referencias_snapshot_staging no conserva histórico.
    - referencias_snapshot_actual representa la foto vigente de referencias.
    - referencias_refresh_control conserva la trazabilidad de cada ejecución.
    - Si el refresh falla, se marca como failed y se propaga la excepción.
    """

    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"

    TRIGGER_SCHEDULED = "scheduled"
    TRIGGER_MANUAL = "manual"
    TRIGGER_STARTUP = "startup"

    def __init__(self, repo: PostgresRepository, sqlserver_api_url: str):
        """
        Inicializa el servicio con el repositorio PostgreSQL y la URL base
        de la API C# / SQL Server.
        """
        self._repo = repo
        self._sqlserver_api_url = (sqlserver_api_url or "").strip()

    def refresh_snapshot(
        self,
        trigger_type: str = TRIGGER_MANUAL,
        created_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Ejecuta el refresh completo del snapshot de referencias.

        Este método es la entrada principal para jobs programados, ejecuciones
        manuales o cargas iniciales. Retorna conteos útiles para monitoreo.
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
            segmentacion_sync_count = self._sincronizar_estado_segmentacion_desde_snapshot()

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
                "segmentacion_sync_count": segmentacion_sync_count,
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

    def _obtener_referencias_desde_api(self) -> List[Dict[str, Any]]:
        """
        Consulta la API .NET de referencias.

        Se mantiene separado para aislar el origen externo. Si en el futuro
        cambia la API, este método y SegmentacionService serían los primeros
        puntos a revisar.
        """
        servicio = SegmentacionService(self._sqlserver_api_url)
        rows = servicio.obtener_referencias()

        return rows if isinstance(rows, list) else []

    def _mapear_referencias_snapshot(
        self,
        raw_rows: List[Dict[str, Any]],
        refresh_id: str,
        loaded_at: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Convierte la salida de la API .NET al formato de snapshot en PostgreSQL.

        El mapeo acepta nombres en snake_case, camelCase y PascalCase para hacer
        más tolerante la integración ante cambios menores de serialización.
        """
        out: List[Dict[str, Any]] = []

        for row in raw_rows:
            referencia_sku = self._pick_str(row, "referencia", "referenciaSku", "Referencia")
            if not referencia_sku:
                continue

            item = {
                "refresh_id": refresh_id,
                "loaded_at": loaded_at,
                "referencia_sku": referencia_sku,
                "referencia_base": self._pick_str(row, "referencia_base", "referenciaBase", "ReferenciaBase"),
                "descripcion": self._pick_str(row, "descripcion", "Descripcion"),
                "categoria": self._pick_str(row, "categoria", "Categoria"),
                "color": self._pick_str(row, "color", "Color"),
                "codigo_color": self._pick_str(row, "codigo_color", "codigoColor", "CodigoColor"),
                "perfil_prenda": self._pick_str(row, "perfil_prenda", "perfilPrenda", "PerfilPrenda"),
                "estado": self._pick_str(row, "estado", "Estado"),
                "tipo_inventario": self._pick_str(row, "tipo_inventario", "tipoInventario", "TipoInventario"),
                "tipo_portafolio": self._pick_str(row, "tipo_portafolio", "tipoPortafolio", "TipoPortafolio"),
                "linea": self._pick_str(row, "linea", "Linea"),
                "cuento": self._pick_str(row, "cuento", "Cuento"),
                "precio_unitario": self._pick_decimal(row, "precio_unitario", "precioUnitario", "PrecioUnitario"),
                "fecha_creacion": self._pick_datetime(row, "fecha_creacion", "fechaCreacion", "FechaCreacion"),
                "fch_act_portafolio": self._pick_date(
                    row,
                    "fch_act_portafolio",
                    "fchActPortafolio",
                    "FchActPortafolio",
                ),
                "clase_agotados": self._pick_str(
                    row,
                    "clase_agotados",
                    "claseAgotados",
                    "ClaseAgotados",
                ),
                "cantidad_tallas": self._pick_int(row, "cantidad_tallas", "cantidadTallas", "CantidadTallas"),
                "tallas": self._pick_str(row, "tallas", "Tallas"),
                "tallas_conteo_json": self._pick_json(row, "tallas_conteo", "tallasConteo", "TallasConteo"),
                "codigos_barras_por_talla_json": self._pick_json(
                    row,
                    "codigos_barras_por_talla",
                    "codigosBarrasPorTalla",
                    "CodigosBarrasPorTalla",
                ),
            }
            out.append(item)

        return out

    def _insert_refresh_control_running(
        self,
        refresh_id: str,
        started_at: datetime,
        trigger_type: str,
        created_by: Optional[str],
    ) -> None:
        """
        Registra una ejecución nueva del refresh en estado running.
        """
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
        """
        Marca el refresh como exitoso y guarda conteos de control.
        """
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
        """
        Marca el refresh como fallido y conserva el mensaje de error.
        """
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

    def _cargar_staging(self, refresh_id: str, rows: List[Dict[str, Any]]) -> None:
        """
        Reemplaza el contenido de staging con el lote actual.

        La tabla staging no se usa como histórico. Su función es recibir el lote
        recién consultado antes de validarlo y promoverlo a la tabla final.
        """
        if not rows:
            raise RuntimeError("No se recibieron filas para cargar en referencias_snapshot_staging.")

        self._repo.execute(
            "TRUNCATE TABLE public.referencias_snapshot_staging;",
            {},
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
                fch_act_portafolio,
                clase_agotados,
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
                %(fch_act_portafolio)s,
                %(clase_agotados)s,
                %(cantidad_tallas)s,
                %(tallas)s,
                %(tallas_conteo_json)s::jsonb,
                %(codigos_barras_por_talla_json)s::jsonb
            );
        """
        self._repo.execute_many(sql, rows)

    def _validar_staging(self, refresh_id: str, expected_count: int) -> None:
        """
        Valida que staging tenga el número esperado de filas y referencias válidas.
        """
        row = self._repo.fetch_one(
            """
            SELECT
                COUNT(*) AS n,
                COUNT(*) FILTER (
                    WHERE referencia_sku IS NULL OR TRIM(referencia_sku) = ''
                ) AS invalid_ref_count
            FROM public.referencias_snapshot_staging
            WHERE refresh_id = %(refresh_id)s;
            """,
            {"refresh_id": refresh_id},
        )

        staged_count = int(row["n"] or 0) if row else 0
        invalid_ref_count = int(row["invalid_ref_count"] or 0) if row else 0

        if staged_count != expected_count:
            raise RuntimeError(
                f"Validación de staging falló: expected_count={expected_count}, staged_count={staged_count}"
            )

        if invalid_ref_count > 0:
            raise RuntimeError(
                f"Validación de staging falló: hay {invalid_ref_count} filas con referencia_sku inválida."
            )

    def _promover_a_actual(self, refresh_id: str) -> int:
        """
        Reemplaza public.referencias_snapshot_actual con el lote validado.

        La operación se ejecuta en transacción para evitar que la tabla final
        quede parcialmente cargada.
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
                    fch_act_portafolio,
                    clase_agotados,
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
                    fch_act_portafolio,
                    clase_agotados,
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
            row_actual = cur.fetchone()
            promoted_count = int(row_actual["n"] or 0) if row_actual else 0

            if promoted_count != staged_count:
                raise RuntimeError(
                    f"Promoción inconsistente: staging={staged_count}, actual={promoted_count}"
                )

            return promoted_count

        return int(self._repo.run_in_transaction(_tx))

    def _sincronizar_estado_segmentacion_desde_snapshot(self) -> int:
        """
        Sincroniza atributos comerciales vigentes sobre segmentaciones existentes.

        La segmentación conserva tiendas, tallas, cantidades y estado operativo.
        Sin embargo, campos como tipo_portafolio y estado_sku deben reflejar
        la referencia vigente del snapshot, no una copia vieja guardada al momento
        de segmentar.
        """
        def _tx(cur):
            cur.execute(
                """
                UPDATE public.segmentacion s
                SET
                    tipo_portafolio = r.tipo_portafolio,
                    estado_sku = r.estado
                FROM public.referencias_snapshot_actual r
                WHERE r.referencia_sku = s.referencia
                AND (
                    COALESCE(s.tipo_portafolio, '') <> COALESCE(r.tipo_portafolio, '')
                    OR COALESCE(s.estado_sku, '') <> COALESCE(r.estado, '')
                );
                """
            )

            return cur.rowcount or 0

        return int(self._repo.run_in_transaction(_tx))

    @staticmethod
    def _pick_raw(data: Dict[str, Any], *keys: str) -> Any:
        """
        Retorna el primer valor encontrado entre varias posibles claves.
        """
        for key in keys:
            if key in data:
                return data.get(key)

        return None

    @classmethod
    def _pick_str(cls, data: Dict[str, Any], *keys: str) -> Optional[str]:
        """
        Lee un campo como texto limpio.
        """
        value = cls._pick_raw(data, *keys)

        if value is None:
            return None

        text = str(value).strip()

        return text if text else None

    @classmethod
    def _pick_int(cls, data: Dict[str, Any], *keys: str) -> Optional[int]:
        """
        Lee un campo como entero. Si no se puede convertir, retorna None.
        """
        value = cls._pick_raw(data, *keys)

        if value is None or value == "":
            return None

        try:
            return int(value)
        except Exception:
            return None

    @classmethod
    def _pick_decimal(cls, data: Dict[str, Any], *keys: str) -> Optional[Decimal]:
        """
        Lee un campo como Decimal. Si no se puede convertir, retorna None.
        """
        value = cls._pick_raw(data, *keys)

        if value is None or value == "":
            return None

        try:
            return Decimal(str(value))
        except Exception:
            return None

    @classmethod
    def _pick_datetime(cls, data: Dict[str, Any], *keys: str) -> Optional[datetime]:
        """
        Lee un campo como datetime desde valores nativos o cadenas ISO.
        """
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
    def _pick_date(cls, data: Dict[str, Any], *keys: str):
        """
        Lee un campo como date desde un datetime o una cadena ISO.
        """
        value = cls._pick_raw(data, *keys)

        if value in (None, ""):
            return None

        if isinstance(value, datetime):
            return value.date()

        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.date()
        except Exception:
            return None

    @classmethod
    def _pick_json(cls, data: Dict[str, Any], *keys: str) -> Optional[str]:
        """
        Serializa a JSON el primer valor encontrado entre las claves indicadas.

        Se usa para campos agregados como tallas_conteo_json y
        codigos_barras_por_talla_json antes de insertarlos como jsonb.
        """
        value = cls._pick_raw(data, *keys)

        if value is None:
            return None

        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return None