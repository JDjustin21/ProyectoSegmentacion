# backend/modules/auth/decorators.py
from functools import wraps
from flask import session, redirect, url_for, request, abort

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("auth.login", next=request.path))
        return fn(*args, **kwargs)
    return wrapper

def role_required(*roles):
    roles_set = {r.strip().lower() for r in roles if r}
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not session.get("user_id"):
                return redirect(url_for("auth.login", next=request.path))
            user_role = (session.get("rol") or "").strip().lower()
            if user_role not in roles_set:
                return abort(403)
            return fn(*args, **kwargs)
        return wrapper
    return decorator
