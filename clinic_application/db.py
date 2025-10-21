_current_role = "normal"

def set_dsn(role: str):
    """ 'super' for admin, 'normal' for read-only."""
    global _current_role
    _current_role = "super" if role == "super" else "normal"

def is_admin() -> bool:
    return _current_role == "super"

def require_admin():
    if not is_admin():
        raise PermissionError("Admin only operation")
