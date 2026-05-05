# backend/modules/auth/decorators.py
from functools import wraps

from flask import abort, redirect, request, session, url_for


def login_required(fn):
    """
    Protege rutas que requieren un usuario autenticado.

    Si no existe user_id en la sesión, redirige al login y conserva la ruta
    solicitada en el parámetro next para volver después de iniciar sesión.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("auth.login", next=request.path))

        return fn(*args, **kwargs)

    return wrapper


def role_required(*roles):
    """
    Protege rutas que requieren uno o varios roles específicos.

    También valida que exista una sesión activa. Por eso, técnicamente,
    una ruta con role_required no necesita además login_required, aunque se puede
    dejar por claridad si se prefiere.
    """
    roles_set = {
        role.strip().lower()
        for role in roles
        if role
    }

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not session.get("user_id"):
                return redirect(url_for("auth.login", next=request.path))

            user_role = (session.get("rol") or "").strip().lower()

            if user_role not in roles_set:
                abort(403)

            return fn(*args, **kwargs)

        return wrapper

    return decorator