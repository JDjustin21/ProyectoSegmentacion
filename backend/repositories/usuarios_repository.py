#backend/repositories/usuarios_repository.py

from typing import Any, Dict, List, Optional

from backend.repositories.postgres_repository import PostgresRepository


class UsuariosRepository:
    """
    Repositorio de acceso a datos para la tabla public.usuarios.

    Esta clase encapsula las consultas SQL relacionadas con usuarios para evitar
    que las rutas o servicios de autenticación dependan directamente de sentencias SQL.
    """

    def __init__(self, repo: PostgresRepository):
        """
        Recibe una instancia de PostgresRepository.

        La conexión y ejecución de SQL se delega al repositorio genérico para mantener
        una sola forma de acceder a PostgreSQL dentro del aplicativo.
        """
        self._repo = repo

    def get_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Busca un usuario por correo electrónico.

        Se usa principalmente durante el inicio de sesión. El correo se normaliza
        en minúsculas para evitar diferencias por mayúsculas/minúsculas.
        """
        sql = """
            SELECT id_usuario, email, nombre, password_hash, estado_usuario, rol, ultimo_login
            FROM public.usuarios
            WHERE email = %(email)s
            LIMIT 1;
        """
        return self._repo.fetch_one(sql, {"email": (email or "").strip().lower()})

    def get_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Consulta los datos públicos de un usuario por su identificador.

        No retorna el password_hash porque esta consulta está pensada para recuperar
        información de sesión o perfil, no para autenticar credenciales.
        """
        sql = """
            SELECT id_usuario, email, nombre, estado_usuario, rol, ultimo_login
            FROM public.usuarios
            WHERE id_usuario = %(id)s
            LIMIT 1;
        """
        return self._repo.fetch_one(sql, {"id": int(user_id)})

    def update_last_login(self, user_id: int) -> None:
        """
        Actualiza la fecha y hora del último inicio de sesión del usuario.
        """
        sql = """
            UPDATE public.usuarios
            SET ultimo_login = now()
            WHERE id_usuario = %(id)s;
        """
        self._repo.execute(sql, {"id": int(user_id)})

    def list_all(self) -> List[Dict[str, Any]]:
        """
        Lista todos los usuarios registrados en el sistema.

        Se usa desde el módulo administrativo para consultar usuarios, roles
        y estado de acceso.
        """
        sql = """
            SELECT id_usuario, email, nombre, estado_usuario, rol, fecha_creacion, ultimo_login
            FROM public.usuarios
            ORDER BY id_usuario DESC;
        """
        return self._repo.fetch_all(sql)

    def create_user(self, email: str, nombre: str, password_hash: str, rol: str) -> int:
        """
        Crea un usuario activo y retorna su identificador.

        La contraseña debe llegar previamente convertida a hash desde la capa de servicio.
        El repositorio no debe conocer la lógica de hashing.
        """
        sql = """
            INSERT INTO public.usuarios (email, nombre, password_hash, rol, estado_usuario)
            VALUES (%(email)s, %(nombre)s, %(password_hash)s, %(rol)s, 'Activo')
            RETURNING id_usuario;
        """
        params = {
            "email": (email or "").strip().lower(),
            "nombre": (nombre or "").strip(),
            "password_hash": password_hash,
            "rol": (rol or "").strip()
        }
        row = self._repo.fetch_one(sql, params)
        return int(row["id_usuario"])

    def update_role(self, id_usuario: int, rol: str) -> None:
        """
        Actualiza el rol de un usuario.

        Esta operación debe ser llamada únicamente desde rutas protegidas
        por permisos administrativos.
        """
        sql = """
            UPDATE public.usuarios
            SET rol = %(rol)s
            WHERE id_usuario = %(id_usuario)s;
        """
        self._repo.execute(sql, {"rol": (rol or "").strip(), "id_usuario": int(id_usuario)})

    def update_password(self, id_usuario: int, password_hash: str) -> None:
        """
        Actualiza la contraseña de un usuario.

        El valor recibido debe ser un hash, nunca una contraseña en texto plano.
        """
        sql = """
            UPDATE public.usuarios
            SET password_hash = %(password_hash)s
            WHERE id_usuario = %(id_usuario)s;
        """
        self._repo.execute(
            sql,
            {
                "password_hash": password_hash,
                "id_usuario": int(id_usuario)
            }
        )