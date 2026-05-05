# backend/modules/auth/auth_service.py
from typing import Any, Dict, Optional

from werkzeug.security import check_password_hash, generate_password_hash

from backend.repositories.usuarios_repository import UsuariosRepository


# Roles válidos dentro del aplicativo.
# Si en el futuro se agrega otro rol, debe registrarse aquí para que pueda usarse
# desde el módulo administrativo.
ROLES_ALLOWED = {"admin", "user"}


class AuthService:
    """
    Servicio de autenticación y administración básica de credenciales.

    Esta clase concentra la lógica relacionada con:
    - Validar credenciales de usuario.
    - Generar hashes de contraseña.
    - Normalizar y validar roles.

    No ejecuta SQL directamente. Para eso usa UsuariosRepository.
    """

    def __init__(self, usuarios_repo: UsuariosRepository):
        self._usuarios_repo = usuarios_repo

    def authenticate(self, email: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Autentica un usuario por email y contraseña.

        Retorna un diccionario con los datos mínimos necesarios para la sesión
        si las credenciales son válidas. Si el usuario no existe, está inactivo
        o la contraseña no coincide, retorna None.
        """
        email_n = (email or "").strip().lower()

        if not email_n or not password:
            return None

        user = self._usuarios_repo.get_by_email(email_n)
        if not user:
            return None

        if (user.get("estado_usuario") or "").strip() != "Activo":
            return None

        password_hash = user.get("password_hash") or ""
        if not check_password_hash(password_hash, password):
            return None

        self._usuarios_repo.update_last_login(int(user["id_usuario"]))

        # Solo se retorna la información necesaria para guardar en sesión.
        return {
            "id_usuario": int(user["id_usuario"]),
            "email": user.get("email"),
            "nombre": user.get("nombre"),
            "rol": (user.get("rol") or "user").strip()
        }

    def hash_password(self, password: str) -> str:
        """
        Genera un hash seguro para almacenar contraseñas.

        Nunca debe guardarse una contraseña en texto plano en base de datos.
        """
        return generate_password_hash(password)

    def validate_role(self, rol: str) -> str:
        """
        Valida que el rol recibido pertenezca a los roles permitidos.

        Si el rol no es válido, se asigna 'user' como rol seguro por defecto.
        """
        role = (rol or "").strip().lower()

        if role not in ROLES_ALLOWED:
            return "user"

        return role