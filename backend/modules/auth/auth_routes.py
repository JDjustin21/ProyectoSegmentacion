# backend/modules/auth/auth_routes.py
from urllib.parse import urljoin, urlparse

from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from backend.config.settings import POSTGRES_DSN
from backend.modules.auth.auth_service import AuthService
from backend.modules.auth.decorators import login_required
from backend.repositories.postgres_repository import PostgresRepository
from backend.repositories.usuarios_repository import UsuariosRepository


auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


def _build_auth_service() -> AuthService:
    """
    Construye el servicio de autenticación con sus dependencias.

    Se deja en una función auxiliar para evitar repetir la creación del repositorio
    cada vez que una ruta necesita autenticar o administrar credenciales.
    """
    repo = PostgresRepository(POSTGRES_DSN)
    usuarios_repo = UsuariosRepository(repo)

    return AuthService(usuarios_repo)


def _is_safe_redirect_url(target: str) -> bool:
    """
    Valida que la URL de redirección pertenezca al mismo host del aplicativo.

    Esto evita redirecciones abiertas mediante el parámetro next.
    """
    if not target:
        return False

    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))

    return test_url.scheme in {"http", "https"} and ref_url.netloc == test_url.netloc


def _get_next_url() -> str:
    """
    Obtiene la URL de retorno después del login.

    Si el parámetro next no es válido o apunta fuera del aplicativo,
    se redirige a la vista principal de segmentación.
    """
    default_url = url_for("segmentacion.vista_segmentacion")
    next_url = request.args.get("next") or request.form.get("next") or default_url

    if not _is_safe_redirect_url(next_url):
        return default_url

    return next_url


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    """
    Muestra el formulario de inicio de sesión y procesa las credenciales.

    En GET renderiza la pantalla de login.
    En POST valida el usuario, guarda los datos mínimos en sesión y redirige
    a la página solicitada originalmente.
    """
    next_url = _get_next_url()

    if request.method == "GET":
        return render_template("auth/login.html", next=next_url)

    email = (request.form.get("email") or "").strip()
    password = request.form.get("password") or ""

    auth = _build_auth_service()
    user = auth.authenticate(email, password)

    if not user:
        flash("Credenciales inválidas o usuario inactivo.", "danger")
        return render_template("auth/login.html", next=next_url), 401

    session.clear()
    session["user_id"] = user["id_usuario"]
    session["email"] = user["email"]
    session["nombre"] = user["nombre"]
    session["rol"] = user["rol"]

    return redirect(next_url)


@auth_bp.route("/logout", methods=["GET"])
@login_required
def logout():
    """
    Cierra la sesión actual y redirige al login.
    """
    session.clear()

    return redirect(url_for("auth.login"))