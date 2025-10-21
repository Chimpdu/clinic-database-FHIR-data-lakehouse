from typing import Optional
from pymongo import MongoClient, ASCENDING
from mongo_config import MONGODB_URI, MONGODB_DB

import fhir_client as fhir

_client = MongoClient(MONGODB_URI)
_db = _client[MONGODB_DB]
_accounts = _db["accounts"]
_accounts.create_index([("name", ASCENDING)], unique=True)

# ---------- bootstrap: ensure default admin/admin ----------
def _ensure_default_admin():
    """Create the default admin/admin account if it doesn't exist."""
    exists = _accounts.find_one({"name": "admin"})
    if not exists:
        _accounts.insert_one({
            "name": "admin",
            "password": "admin",
            "role": "admin",       # reserved backdoor admin
            "person_id": "admin"
        })

_ensure_default_admin()

# ---------- FHIR role lookups used by UI ----------
def is_doctor_person(person_id: Optional[str]) -> bool:
    if not person_id:
        return False
    pid = person_id.strip()
    # accept 'd9005' (login-style) or 'Practitioner/9005'
    if pid.lower().startswith("practitioner/"):
        pid = pid.split("/", 1)[1]
    elif pid and pid[0].lower() == "d" and pid[1:].isdigit():
        pid = pid[1:]
    try:
        res = fhir.read(f"Practitioner/{pid}")
        return bool(res and res.get("resourceType") == "Practitioner")
    except Exception:
        return False


# login_backend.py
def is_patient_person(person_id: Optional[str]) -> bool:
    if not person_id:
        return False
    pid = person_id.strip()
    if pid.lower().startswith("patient/"):
        pid = pid.split("/", 1)[1]
    elif pid and pid[0].lower() == "p" and pid[1:].isdigit():
        pid = pid[1:]
    try:
        res = fhir.read(f"Patient/{pid}")
        return bool(res and res.get("resourceType") == "Patient")
    except Exception:
        return False


# ---------- helpers ----------
def _make_account(name: str, password: str, role: str, person_id: str, source: str):
    """
    Idempotent upsert for an account.
    """
    _accounts.update_one(
        {"name": name},
        {"$setOnInsert": {
            "password": password,
            "role": role,            # "super" (doctor), "normal" (patient), or "admin"
            "person_id": person_id,  # raw FHIR resource id
            "source": source
        }},
        upsert=True
    )

def _parse_prefixed_username(uname: str) -> Optional[tuple[str, str]]:
    """
    Accepts 'p<id>' or 'd<id>'.
    Returns tuple (kind, id) where kind in {'patient','doctor'} or None if not matching.
    """
    if not uname or len(uname) < 2:
        return None
    if uname[0] in ("p", "P"):
        return ("patient", uname[1:])
    if uname[0] in ("d", "D"):
        return ("doctor", uname[1:])
    return None

def _auto_create_user_account_from_prefixed(name: str) -> bool:
    """
    Auto-provision rule:
      - If username == password and
        - starts with 'p' -> validate Patient/{id} exists, then create role 'normal'
        - starts with 'd' -> validate Practitioner/{id} exists, then create role 'super'
    """
    parsed = _parse_prefixed_username(name)
    if not parsed:
        return False

    kind, pid = parsed
    if kind == "patient":
        if not is_patient_person(pid):
            return False
        _make_account(
            name=name,
            password=name,         # initial password equals username
            role="normal",         # patients see messaging + account only
            person_id=pid,
            source="fhir-patient"
        )
        return True

    if kind == "doctor":
        if not is_doctor_person(pid):
            return False
        _make_account(
            name=name,
            password=name,         # initial password equals username
            role="super",          # practitioners get admin UI
            person_id=pid,
            source="fhir-doctor"
        )
        return True

    return False

# ---------- public API used by UI ----------
def get_role(name: str) -> str:
    acc = _accounts.find_one({"name": name}, {"role": 1})
    return (acc or {}).get("role", "normal")

def check_admin(name: str, password: str) -> bool:
    # Keep legacy behavior: admin/admin should always work and be present
    if name == "admin" and password == "admin":
        _ensure_default_admin()
        return True
    acc = _accounts.find_one({"name": name, "password": password, "role": "admin"})
    return acc is not None

def check_user(name: str, password: str) -> bool:
    # 1) Already registered?
    acc = _accounts.find_one({"name": name})
    if acc:
        # Accept roles used by the UI routing
        return acc.get("password") == password and acc.get("role") in ("normal", "super", "admin")

    # 2) Auto-provision path: username == password == p<Patient.id> or d<Practitioner.id>
    if name and password and name == password:
        if _auto_create_user_account_from_prefixed(name):
            return True

    # 3) Otherwise not a valid user
    return False

def insert_user(name: str, password: str, *, person_id: Optional[str] = None, as_admin: bool = False):
    """
    Manual registration (Register). If as_admin=True, creates an admin.
    NOTE: For FHIR-linked users, prefer using the p<id>/d<id> convention and auto-provision.
    """
    role = "admin" if as_admin else "normal"
    _accounts.update_one(
        {"name": name},
        {"$setOnInsert": {
            "password": password,
            "role": role,
            "person_id": (person_id or name)
        }},
        upsert=True
    )

def get_account_person_id(name: str) -> Optional[str]:
    acc = _accounts.find_one({"name": name}, {"person_id": 1})
    return (acc or {}).get("person_id")

def change_own_credentials(old_name: str, *, role: str,
                           new_name: Optional[str] = None,
                           new_password: Optional[str] = None):
    """
    Keep old API. Allows user to change their own username/password.
    'role' parameter is ignored here (role changes should be admin-only elsewhere).
    """
    if not new_name and not new_password:
        return
    q = {"name": old_name}
    updates = {}
    if new_name:
        if _accounts.find_one({"name": new_name}):
            raise ValueError("Username already taken")
        updates["name"] = new_name
    if new_password:
        updates["password"] = new_password
    if updates:
        _accounts.update_one(q, {"$set": updates})
