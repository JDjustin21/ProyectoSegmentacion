# backend/modules/auth/auth_routes.py

from flask import Blueprint, render_template, request, redirect, url_for, session, flash

from backend.repositories.postgres_repository import PostgresRepository
from backend.repositories.usuarios_repository import UsuariosRepository
from backend.modules.auth.auth_service import AuthService
from backend.modules.auth.decorators import login_required
from backend.config.settings import POSTGRES_DSN

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    next_url = request.args.get("next") or url_for("segmentacion.vista_segmentacion")

    if request.method == "GET":
        return render_template("auth/login.html", next=next_url)

    email = (request.form.get("email") or "").strip()
    password = request.form.get("password") or ""

    repo = PostgresRepository(POSTGRES_DSN)
    usuarios_repo = UsuariosRepository(repo)
    auth = AuthService(usuarios_repo)

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
    session.clear()
    return redirect(url_for("auth.login"))
