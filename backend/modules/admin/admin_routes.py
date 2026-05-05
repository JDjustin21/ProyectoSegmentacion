# backend/modules/admin/admin_routes.py

import logging

from flask import Blueprint, flash, redirect, render_template, request, url_for

from backend.config.settings import POSTGRES_DSN
from backend.modules.auth.auth_service import AuthService
from backend.modules.auth.decorators import role_required
from backend.repositories.postgres_repository import PostgresRepository
from backend.repositories.usuarios_repository import UsuariosRepository


logger = logging.getLogger(__name__)

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


def _build_usuarios_repository() -> UsuariosRepository:
    """
    Construye el repositorio de usuarios con su conexión a PostgreSQL.
    """
    repo = PostgresRepository(POSTGRES_DSN)

    return UsuariosRepository(repo)


def _build_auth_service(usuarios_repo: UsuariosRepository) -> AuthService:
    """
    Construye el servicio de autenticación usando el repositorio de usuarios.
    """
    return AuthService(usuarios_repo)


@admin_bp.get("/usuarios")
@role_required("admin")
def usuarios_index():
    """
    Muestra la pantalla administrativa de usuarios.

    Solo los usuarios con rol admin pueden consultar esta vista.
    """
    usuarios_repo = _build_usuarios_repository()
    usuarios = usuarios_repo.list_all()

    return render_template("admin/usuarios.html", usuarios=usuarios)


@admin_bp.post("/usuarios/crear")
@role_required("admin")
def usuarios_crear():
    """
    Crea un nuevo usuario desde el panel administrativo.

    La contraseña se convierte a hash antes de guardarse en base de datos.
    """
    email = (request.form.get("email") or "").strip().lower()
    nombre = (request.form.get("nombre") or "").strip()
    password = request.form.get("password") or ""
    rol = (request.form.get("rol") or "user").strip()

    if not email or not nombre or not password:
        flash("Email, nombre y contraseña son obligatorios.", "danger")
        return redirect(url_for("admin.usuarios_index"))

    usuarios_repo = _build_usuarios_repository()
    auth = _build_auth_service(usuarios_repo)

    rol_ok = auth.validate_role(rol)
    password_hash = auth.hash_password(password)

    try:
        usuarios_repo.create_user(
            email=email,
            nombre=nombre,
            password_hash=password_hash,
            rol=rol_ok
        )
        flash("Usuario creado correctamente.", "success")

    except Exception:
        logger.exception("Error creando usuario administrativo.")
        flash("No se pudo crear el usuario. Revisa que el correo no exista previamente.", "danger")

    return redirect(url_for("admin.usuarios_index"))


@admin_bp.post("/usuarios/<int:id_usuario>/rol")
@role_required("admin")
def usuarios_cambiar_rol(id_usuario: int):
    """
    Actualiza el rol de un usuario existente.
    """
    rol = (request.form.get("rol") or "").strip()

    usuarios_repo = _build_usuarios_repository()
    auth = _build_auth_service(usuarios_repo)

    rol_ok = auth.validate_role(rol)
    usuarios_repo.update_role(id_usuario=id_usuario, rol=rol_ok)

    flash("Rol actualizado.", "success")

    return redirect(url_for("admin.usuarios_index"))


@admin_bp.post("/usuarios/<int:id_usuario>/password")
@role_required("admin")
def usuarios_reset_password(id_usuario: int):
    """
    Actualiza la contraseña de un usuario desde el panel administrativo.
    """
    password = request.form.get("password") or ""

    if not password:
        flash("La contraseña no puede estar vacía.", "danger")
        return redirect(url_for("admin.usuarios_index"))

    usuarios_repo = _build_usuarios_repository()
    auth = _build_auth_service(usuarios_repo)

    password_hash = auth.hash_password(password)
    usuarios_repo.update_password(id_usuario=id_usuario, password_hash=password_hash)

    flash("Contraseña actualizada.", "success")

    return redirect(url_for("admin.usuarios_index"))