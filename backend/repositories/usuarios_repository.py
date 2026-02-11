from typing import Optional, Dict, Any, List
from backend.repositories.postgres_repository import PostgresRepository

class UsuariosRepository:
    def __init__(self, repo: PostgresRepository):
        self._repo = repo

    def get_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT id_usuario, email, nombre, password_hash, estado_usuario, rol, ultimo_login
            FROM public.usuarios
            WHERE email = %(email)s
            LIMIT 1;
        """
        return self._repo.fetch_one(sql, {"email": (email or "").strip().lower()})

    def get_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT id_usuario, email, nombre, estado_usuario, rol, ultimo_login
            FROM public.usuarios
            WHERE id_usuario = %(id)s
            LIMIT 1;
        """
        return self._repo.fetch_one(sql, {"id": int(user_id)})

    def update_last_login(self, user_id: int) -> None:
        sql = """
            UPDATE public.usuarios
            SET ultimo_login = now()
            WHERE id_usuario = %(id)s;
        """
        self._repo.execute(sql, {"id": int(user_id)})

    def list_all(self) -> List[Dict[str, Any]]:
        sql = """
            SELECT id_usuario, email, nombre, estado_usuario, rol, fecha_creacion, ultimo_login
            FROM public.usuarios
            ORDER BY id_usuario DESC;
        """
        return self._repo.fetch_all(sql)

    def create_user(self, email: str, nombre: str, password_hash: str, rol: str) -> int:
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
        sql = """
            UPDATE public.usuarios
            SET rol = %(rol)s
            WHERE id_usuario = %(id_usuario)s;
        """
        self._repo.execute(sql, {"rol": (rol or "").strip(), "id_usuario": int(id_usuario)})

    def update_password(self, id_usuario: int, password_hash: str) -> None:
        sql = """
            UPDATE public.usuarios
            SET password_hash = %(password_hash)s
            WHERE id_usuario = %(id_usuario)s;
        """
        self._repo.execute(sql, {"password_hash": password_hash, "id_usuario": int(id_usuario)})