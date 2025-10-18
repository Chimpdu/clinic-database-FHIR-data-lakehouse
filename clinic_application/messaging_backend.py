from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from pymongo import MongoClient, ASCENDING, DESCENDING
from mongo_config import MONGODB_URI, MONGODB_DB, COLL_MESSAGES
import backend  # lo_save_file + FHIR helpers
from login_backend import get_account_person_id, is_doctor_person, is_patient_person

# ---- Mongo client / indices ----
_client = MongoClient(MONGODB_URI)
_db = _client[MONGODB_DB]
_messages = _db[COLL_MESSAGES]

# Ensure indexes (safe on repeated runs)
_messages.create_index([("sender_id", ASCENDING), ("receiver_id", ASCENDING), ("created_at", DESCENDING)])
_messages.create_index([("participants", ASCENDING), ("created_at", DESCENDING)])


# ----------------- normalization helpers (single source of truth) -----------------
def _norm_patient_id(x: Optional[str]) -> str:
    x = (x or "").strip()
    if x.lower().startswith("patient/"):
        x = x.split("/", 1)[1]
    if x and x[0].lower() == "p" and x[1:].isdigit():
        x = x[1:]
    return x

def _norm_practitioner_id(x: Optional[str]) -> str:
    x = (x or "").strip()
    if x.lower().startswith("practitioner/"):
        x = x.split("/", 1)[1]
    if x and x[0].lower() == "d" and x[1:].isdigit():
        x = x[1:]
    return x

def _now_utc():
    return datetime.utcnow()


def _normalize_identity(login_name: str, *, role: str) -> Dict[str, str]:
    """
    Convert login -> domain identity.
    We determine doctor/patient by FHIR existence, not UI role labels,
    and we normalize ids (strip p/d prefixes and resourceType/).
    """
    raw = get_account_person_id(login_name) or login_name

    doc_id = _norm_practitioner_id(raw)
    pat_id = _norm_patient_id(raw)

    if is_doctor_person(doc_id):
        return {"user_type": "doctor", "person_id": doc_id, "login_name": login_name, "role": role}
    if is_patient_person(pat_id):
        return {"user_type": "patient", "person_id": pat_id, "login_name": login_name, "role": role}
    return {"user_type": "admin_only", "person_id": raw, "login_name": login_name, "role": role}


def _encounter_partners(person_id: str, user_type: str) -> List[Dict[str, str]]:
    """
    Partners are strictly those sharing an Encounter.
    Always return **normalized bare ids** in 'id'.
    """
    if user_type == "doctor":
        rows = backend.get_encounter_patients_for_doctor(_norm_practitioner_id(person_id))
        return [{"id": pid, "name": pname} for (pid, pname) in rows]  # pid already bare
    if user_type == "patient":
        rows = backend.get_encounter_doctors_for_patient(_norm_patient_id(person_id))
        return [{"id": did, "name": dname} for (did, dname) in rows]  # did already bare
    return []


def list_recipients_for_user(login_name: str, *, role: str) -> List[Dict[str, str]]:
    me = _normalize_identity(login_name, role=role)
    return _encounter_partners(me["person_id"], me["user_type"])


def _normalize_other_id_for_me(me: Dict[str, str], other_id: str) -> str:
    """
    Normalize the 'other person' id according to my type.
    - If I'm a doctor, 'other' must be a Patient id
    - If I'm a patient, 'other' must be a Practitioner id
    """
    if me["user_type"] == "doctor":
        return _norm_patient_id(other_id)
    if me["user_type"] == "patient":
        return _norm_practitioner_id(other_id)
    return (other_id or "").strip()


def _ensure_allowed_recipient(me: Dict[str, str], to_person_id: str):
    to_norm = _normalize_other_id_for_me(me, to_person_id)
    partners = {r["id"] for r in _encounter_partners(me["person_id"], me["user_type"])}  # already bare
    if to_norm not in partners:
        raise PermissionError("Recipient is not in your Encounter history (not allowed to message).")


def send_message(login_name: str, *, role: str,
                 to_person_id: str,
                 text: Optional[str] = None,
                 file_path: Optional[str] = None) -> str:
    """
    Save a message to MongoDB.
    - Restrict pairs to doctor<->patient who share at least one Encounter
    - Persist attachments via backend.lo_save_file (stored under ./files/)
    All ids persisted are **normalized bare FHIR logical ids**.
    """
    if (not text or text.strip() == "") and not file_path:
        raise ValueError("Provide text or attach a file.")

    me = _normalize_identity(login_name, role=role)
    if me["user_type"] not in ("doctor", "patient"):
        raise PermissionError("Only doctors or patients can send messages.")

    # Normalize and validate recipient
    to_norm = _normalize_other_id_for_me(me, to_person_id)
    _ensure_allowed_recipient(me, to_norm)

    # Optional file attachment
    file_url = None
    if file_path:
        oid = backend.lo_save_file(file_path)
        file_url = f"files/{Path(str(oid)).name}"

    # Store normalized ids for stable thread key
    a, b = sorted([me["person_id"], to_norm])
    doc: Dict[str, Any] = {
        "sender_id": me["person_id"],   # bare id
        "receiver_id": to_norm,         # bare id
        "participants": [a, b],         # bare ids
        "text": (text or "").strip() or None,
        "file_url": file_url,
        "created_at": _now_utc(),
    }
    res = _messages.insert_one(doc)
    return str(res.inserted_id)


def get_conversation(login_name: str, *, role: str,
                     other_person_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Load latest messages between me and other_person_id (ascending by time).
    Accepts prefixed ids, stores/queries with normalized bare ids.
    """
    me = _normalize_identity(login_name, role=role)
    if me["user_type"] not in ("doctor", "patient"):
        return []

    other_norm = _normalize_other_id_for_me(me, other_person_id)
    _ensure_allowed_recipient(me, other_norm)

    a, b = sorted([me["person_id"], other_norm])
    cur = _messages.find({"participants": [a, b]}).sort("created_at", ASCENDING).limit(int(limit or 200))

    out: List[Dict[str, Any]] = []
    for m in cur:
        out.append({
            "sender_id": m.get("sender_id"),
            "receiver_id": m.get("receiver_id"),
            "text": m.get("text"),
            "file_url": m.get("file_url"),
            "created_at": m.get("created_at"),
        })
    return out
