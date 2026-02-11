# backend/modules/admin/admin_routes.py

from flask import Blueprint, render_template, request, redirect, url_for, flash

from backend.config.settings import POSTGRES_DSN
from backend.repositories.postgres_repository import PostgresRepository
from backend.repositories.usuarios_repository import UsuariosRepository
from backend.modules.auth.auth_service import AuthService
from backend.modules.auth.decorators import login_required, role_required

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")


@admin_bp.get("/usuarios")
@login_required
@role_required("admin")
def usuarios_index():
    repo = PostgresRepository(POSTGRES_DSN)
    usuarios_repo = UsuariosRepository(repo)

    usuarios = usuarios_repo.list_all()
    return render_template("admin/usuarios.html", usuarios=usuarios)


@admin_bp.post("/usuarios/crear")
@login_required
@role_required("admin")
def usuarios_crear():
    email = (request.form.get("email") or "").strip().lower()
    nombre = (request.form.get("nombre") or "").strip()
    password = request.form.get("password") or ""
    rol = (request.form.get("rol") or "user").strip()

    if not email or not nombre or not password:
        flash("Email, nombre y contraseña son obligatorios.", "danger")
        return redirect(url_for("admin.usuarios_index"))

    repo = PostgresRepository(POSTGRES_DSN)
    usuarios_repo = UsuariosRepository(repo)
    auth = AuthService(usuarios_repo)

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
    except Exception as e:
        # IMPORTANTE: así ves el motivo real en consola
        print("ERROR creando usuario:", repr(e))
        flash(f"No se pudo crear el usuario: {e}", "danger")

    return redirect(url_for("admin.usuarios_index"))


@admin_bp.post("/usuarios/<int:id_usuario>/rol")
@login_required
@role_required("admin")
def usuarios_cambiar_rol(id_usuario: int):
    rol = (request.form.get("rol") or "").strip()

    repo = PostgresRepository(POSTGRES_DSN)
    usuarios_repo = UsuariosRepository(repo)
    auth = AuthService(usuarios_repo)

    rol_ok = auth.validate_role(rol)
    usuarios_repo.update_role(id_usuario=id_usuario, rol=rol_ok)

    flash("Rol actualizado.", "success")
    return redirect(url_for("admin.usuarios_index"))


@admin_bp.post("/usuarios/<int:id_usuario>/password")
@login_required
@role_required("admin")
def usuarios_reset_password(id_usuario: int):
    password = request.form.get("password") or ""
    if not password:
        flash("La contraseña no puede estar vacía.", "danger")
        return redirect(url_for("admin.usuarios_index"))

    repo = PostgresRepository(POSTGRES_DSN)
    usuarios_repo = UsuariosRepository(repo)
    auth = AuthService(usuarios_repo)

    password_hash = auth.hash_password(password)
    usuarios_repo.update_password(id_usuario=id_usuario, password_hash=password_hash)

    flash("Contraseña actualizada.", "success")
    return redirect(url_for("admin.usuarios_index"))
