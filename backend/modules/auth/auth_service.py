# backend/modules/auth/auth_service.py
from typing import Dict, Any, Optional
from werkzeug.security import generate_password_hash, check_password_hash

from backend.repositories.usuarios_repository import UsuariosRepository

ROLES_ALLOWED = {"admin", "user"}  # definir roles permitidos, si se agrega uno nuevo, agregarlo aquí

class AuthService:
    def __init__(self, usuarios_repo: UsuariosRepository):
        self._usuarios_repo = usuarios_repo

    def authenticate(self, email: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Retorna datos del usuario si autentica, si no retorna None.
        """
        email_n = (email or "").strip().lower()
        if not email_n or not password:
            return None

        user = self._usuarios_repo.get_by_email(email_n)
        if not user:
            return None

        if (user.get("estado_usuario") or "").strip() != "Activo":
            return None

        ph = user.get("password_hash") or ""
        if not check_password_hash(ph, password):
            return None

        self._usuarios_repo.update_last_login(int(user["id_usuario"]))

        # devolvemos lo mínimo para sesión
        return {
            "id_usuario": int(user["id_usuario"]),
            "email": user.get("email"),
            "nombre": user.get("nombre"),
            "rol": (user.get("rol") or "user").strip()
        }

    def hash_password(self, password: str) -> str:
        return generate_password_hash(password)

    def validate_role(self, rol: str) -> str:
        r = (rol or "").strip().lower()
        if r not in ROLES_ALLOWED:
            return "user"
        return r
