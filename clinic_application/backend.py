import os
import base64
import mimetypes
import hashlib
from typing import Any, Dict, List, Tuple, Optional, Set

import fhir_client as fhir

NO_DATA = "no data found"

# ----------------------------- helpers -----------------------------
def _s(x) -> str:
    if isinstance(x, str):
        return x
    if isinstance(x, list):
        parts = []
        for n in x:
            if isinstance(n, dict):
                if "text" in n and isinstance(n["text"], str) and n["text"].strip():
                    parts.append(n["text"].strip()); continue
                given = " ".join(n.get("given", []) or [])
                fam = n.get("family", "") or ""
                s = (given + " " + fam).strip()
                if s: parts.append(s)
            else:
                parts.append(str(n))
        return ", ".join(parts) if parts else str(x)
    if isinstance(x, dict):
        if "text" in x and isinstance(x["text"], str) and x["text"].strip():
            return x["text"].strip()
        given = " ".join(x.get("given", []) or [])
        fam = x.get("family", "") or ""
        s = (given + " " + fam).strip()
        return s or str(x)
    return "" if x is None else str(x)

def _text_any(obj: Any) -> str:
    if obj is None: return ""
    if isinstance(obj, str): return obj
    if isinstance(obj, dict): return obj.get("text") if isinstance(obj.get("text"), str) else ""
    if isinstance(obj, list):
        for it in obj:
            if isinstance(it, dict) and isinstance(it.get("text"), str):
                return it["text"]
    return ""

def _nz(s: Optional[str]) -> str:
    if s is None: return NO_DATA
    if isinstance(s, str):
        s2 = s.strip(); return s2 if s2 else NO_DATA
    return str(s)

def _res_id(res: Dict[str, Any]) -> str:
    return res.get("id") or ""

def _name_of(res: Dict[str, Any]) -> str:
    return _s(res.get("name") or res.get("id"))

def _inc_get(inc: Dict[Tuple[str, str], Dict[str, Any]], ref: Optional[str]) -> Optional[Dict[str, Any]]:
    if not ref or "/" not in ref: return None
    rt, rid = ref.split("/", 1)
    return inc.get((rt, rid))

def _digest_key(parts: List[str]) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update((p or "").encode("utf-8")); h.update(b"|")
    return h.hexdigest()

def _guess_mime(path: str) -> str:
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"

def _resource_exists(resource: str, rid: str) -> bool:
    try:
        fhir.read(f"{resource}/{rid}")
        return True
    except Exception:
        return False

def _make_media_from_file(file_path: str,
                          patient_id: Optional[str] = None,
                          encounter_id: Optional[str] = None) -> str:
    if not os.path.isfile(file_path):
        raise ValueError("File not found")
    with open(file_path, "rb") as f:
        raw = f.read()
    b64 = base64.b64encode(raw).decode("utf-8")
    ctype = _guess_mime(file_path)
    title = os.path.basename(file_path)
    media: Dict[str, Any] = {
        "resourceType": "Media",
        "status": "completed",
        "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/media-type", "code": "image"}]},
        "content": {"contentType": ctype, "data": b64, "title": title}
    }
    if patient_id:
        media["subject"] = {"reference": f"Patient/{patient_id}"}
    if encounter_id:
        media["partOf"] = [{"reference": f"Encounter/{encounter_id}"}]
    created = fhir.create("Media", media)  # server assigns id
    mid = created.get("id")
    if not mid:
        raise ValueError("Failed to obtain Media id from server response")
    return mid

# ===================================================================
#                            PATIENTS
# ===================================================================
def patient_view() -> List[Tuple]:
    rows: List[Tuple] = []
    seen: Set[Tuple] = set()
    pats, inc = fhir.search(
        "Patient",
        {"_count": 200},
        elements=["id", "name", "generalPractitioner"],
        includes=["Patient:general-practitioner:Practitioner"],
    )
    for p in pats:
        pid = _res_id(p); pname = _name_of(p)
        did = dname = None
        gp_list = p.get("generalPractitioner", []) or []
        if gp_list:
            ref = gp_list[0].get("reference")
            pr = _inc_get(inc, ref)
            if pr and pr.get("resourceType") == "Practitioner":
                did = _res_id(pr); dname = _name_of(pr)
        row = (_nz(pid), _nz(_s(pname)), _nz(did), _nz(_s(dname)))
        if row not in seen:
            seen.add(row); rows.append(row)
    return rows

def patient_search(patient_name: str = "", patient_id: str = "",
                   practitioner_name: str = "", practitioner_id: str = "") -> List[Tuple]:
    all_rows = patient_view(); out: List[Tuple] = []
    pn = patient_name.lower() if patient_name else ""
    dn = practitioner_name.lower() if practitioner_name else ""
    for pid, pname, did, dname in all_rows:
        if patient_id and (pid == NO_DATA or patient_id not in pid): continue
        if pn and (pname == NO_DATA or pn not in pname.lower()): continue
        if practitioner_id and (did == NO_DATA or practitioner_id not in did): continue
        if dn and (dname == NO_DATA or dn not in dname.lower()): continue
        out.append((pid, pname, did, dname))
    return out

def patient_insert(full_name: str, practitioner_id: str) -> None:
    """POST /Patient (server assigns id)."""
    pat: Dict[str, Any] = {"resourceType": "Patient"}
    full_name = (full_name or "").strip()
    if full_name:
        given = full_name.split()[:-1]; family = full_name.split()[-1]
        pat["name"] = [{"given": given, "family": family}]
    if practitioner_id and _resource_exists("Practitioner", practitioner_id):
        pat["generalPractitioner"] = [{"reference": f"Practitioner/{practitioner_id}"}]
    fhir.create("Patient", pat)

def patient_delete(patient_id: str) -> None:
    if not patient_id: raise ValueError("patient id is required")
    fhir.delete(f"Patient/{patient_id}")

def patient_update(patient_id: str, new_practitioner_id: Optional[str], new_full_name: Optional[str]) -> None:
    if not patient_id: raise ValueError("patient id is required")
    pat = fhir.read(f"Patient/{patient_id}"); changed = False
    if new_full_name is not None and new_full_name.strip():
        given = new_full_name.strip().split()[:-1]
        family = new_full_name.strip().split()[-1]
        pat["name"] = [{"given": given, "family": family}]; changed = True
    if new_practitioner_id is not None:
        npid = new_practitioner_id.strip()
        if npid and _resource_exists("Practitioner", npid):
            pat["generalPractitioner"] = [{"reference": f"Practitioner/{npid}"}]
            changed = True
        # empty or invalid id => no change
    if changed:
        fhir.update(f"Patient/{patient_id}", pat)

# ===================================================================
#                         PRACTITIONERS
# ===================================================================
def practitioner_view() -> List[Tuple]:
    rows: List[Tuple] = []; seen: Set[Tuple] = set()
    practitioners, _ = fhir.search("Practitioner", {"_count": 200}, elements=["id", "name"])
    for pr in practitioners:
        did = _res_id(pr); dname = _name_of(pr)
        row = (_nz(did), _nz(_s(dname)))
        if row not in seen: seen.add(row); rows.append(row)
    return rows

def practitioner_search(practitioner_name: str = "", practitioner_id: str = "") -> List[Tuple]:
    params = {"_count": 200}
    if practitioner_name: params["name"] = practitioner_name
    practitioners, _ = fhir.search("Practitioner", params, elements=["id", "name"])
    rows: List[Tuple] = []; seen: Set[Tuple] = set()
    for pr in practitioners:
        did = _res_id(pr); dname = _name_of(pr)
        if practitioner_id and practitioner_id not in did: continue
        row = (_nz(did), _nz(_s(dname)))
        if row not in seen: seen.add(row); rows.append(row)
    return rows

def doctor_insert(full_name: str) -> None:
    """POST /Practitioner (server assigns id)."""
    pr: Dict[str, Any] = {"resourceType": "Practitioner"}
    full_name = (full_name or "").strip()
    if full_name:
        given = full_name.split()[:-1]; family = full_name.split()[-1]
        pr["name"] = [{"given": given, "family": family}]
    fhir.create("Practitioner", pr)

def doctor_delete(practitioner_id: str) -> None:
    if not practitioner_id: raise ValueError("practitioner id is required")
    fhir.delete(f"Practitioner/{practitioner_id}")

def doctor_update(practitioner_id: str, _unused_department_id: Optional[str], new_full_name: Optional[str]) -> None:
    if not practitioner_id: raise ValueError("practitioner id is required")
    pr = fhir.read(f"Practitioner/{practitioner_id}"); changed = False
    if new_full_name is not None and new_full_name.strip():
        given = new_full_name.strip().split()[:-1]
        family = new_full_name.strip().split()[-1]
        pr["name"] = [{"given": given, "family": family}]; changed = True
    if changed:
        fhir.update(f"Practitioner/{practitioner_id}", pr)

# ===================================================================
#                         APPOINTMENTS (Encounter)
# ===================================================================
def appointment_view() -> List[Tuple]:
    rows: List[Tuple] = []; seen_ids: Set[str] = set()
    encs, inc = fhir.search(
        "Encounter",
        {"_count": 200},
        elements=["id", "status", "period", "participant", "serviceProvider", "subject"],
        includes=["Encounter:subject", "Encounter:participant:Practitioner", "Encounter:service-provider"],
    )
    for e in encs:
        enc_id = e["id"]
        if enc_id in seen_ids: continue
        seen_ids.add(enc_id)
        per = e.get("period", {}) or {}
        dt = (per.get("start") or "")[:10]
        year, month, day = (dt[0:4] or ""), (dt[5:7] or ""), (dt[8:10] or "")
        loc = ""
        sp = e.get("serviceProvider") or {}
        org = _inc_get(inc, sp.get("reference", ""))
        if sp.get("display"): loc = sp["display"]
        elif org: loc = org.get("name", "") or org.get("id", "")
        pat = _inc_get(inc, (e.get("subject") or {}).get("reference", ""))
        pid = _res_id(pat) if pat else None
        pname = _name_of(pat) if pat else None
        did = dname = None
        for part in e.get("participant", []) or []:
            ref = (part.get("individual") or {}).get("reference", "")
            if ref.startswith("Practitioner/"):
                pr = _inc_get(inc, ref)
                if pr:
                    did = _res_id(pr); dname = _name_of(pr); break
        rows.append((_nz(enc_id), _nz(year), _nz(month), _nz(day), _nz(loc),
                     _nz(pid), _nz(_s(pname)), _nz(did), _nz(_s(dname))))
    return rows

def appointment_search(appoint_id: str = "", year: str = "", month: str = "", day: str = "",
                       patient_name: str = "", patient_id: str = "",
                       practitioner_name: str = "", practitioner_id: str = "") -> List[Tuple]:
    rows = appointment_view()
    pn = patient_name.lower() if patient_name else ""
    dn = practitioner_name.lower() if practitioner_name else ""
    out: List[Tuple] = []
    for (enc_id, y, m, d, loc, pid, pname, did, dname) in rows:
        if appoint_id and appoint_id not in enc_id: continue
        if year and (not y or year != y): continue
        if month and (not m or month != m): continue
        if day and (not d or day != d): continue
        if patient_id and (pid == NO_DATA or patient_id not in pid): continue
        if pn and (pname == NO_DATA or pn not in pname.lower()): continue
        if practitioner_id and (did == NO_DATA or practitioner_id not in did): continue
        if dn and (dname == NO_DATA or dn not in dname.lower()): continue
        out.append((enc_id, y, m, d, loc, pid, pname, did, dname))
    return out

def appointment_insert(y: str, m: str, d: str, location_text: str,
                       patient_id: str, practitioner_id: str) -> None:
    """POST /Encounter (server assigns id)."""
    enc: Dict[str, Any] = {"resourceType": "Encounter"}
    enc["status"] = "finished"
    enc["class"] = {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB"}
    if patient_id and _resource_exists("Patient", patient_id):
        enc["subject"] = {"reference": f"Patient/{patient_id}"}
    if practitioner_id and _resource_exists("Practitioner", practitioner_id):
        enc["participant"] = [{"individual": {"reference": f"Practitioner/{practitioner_id}"}}]
    if y and m and d:
        enc["period"] = {"start": f"{int(y):04d}-{int(m):02d}-{int(d):02d}T09:00:00+00:00"}
    if location_text:
        enc["serviceProvider"] = {"display": location_text}
    fhir.create("Encounter", enc)

def appointment_delete(aid: str) -> None:
    fhir.delete(f"Encounter/{aid}")

def appointment_update(aid: str, y: str, m: str, d: str, location_text: str,
                       patient_id: Optional[str], practitioner_id: Optional[str]) -> None:
    enc = fhir.read(f"Encounter/{aid}"); changed = False
    if y and m and d:
        enc["period"] = {"start": f"{int(y):04d}-{int(m):02d}-{int(d):02d}T09:00:00+00:00"}; changed = True
    # empty location => no change
    if location_text is not None and location_text.strip():
        enc["serviceProvider"] = {"display": location_text.strip()}
        changed = True
    if patient_id and _resource_exists("Patient", patient_id):
        enc["subject"] = {"reference": f"Patient/{patient_id}"}; changed = True
    if practitioner_id and _resource_exists("Practitioner", practitioner_id):
        enc["participant"] = [{"individual": {"reference": f"Practitioner/{practitioner_id}"}}]; changed = True
    if changed: fhir.update(f"Encounter/{aid}", enc)

# ===================================================================
#                         OBSERVATIONS
# ===================================================================
def _obs_comment(o: Dict[str, Any]) -> str:
    note_txt = _text_any(o.get("note"))
    if note_txt.strip(): return note_txt.strip()
    if isinstance(o.get("valueString"), str) and o["valueString"].strip():
        return o["valueString"].strip()
    vcc = o.get("valueCodeableConcept")
    if isinstance(vcc, dict):
        t = vcc.get("text")
        if isinstance(t, str) and t.strip(): return t.strip()
        cds = vcc.get("coding") or []
        if cds and isinstance(cds, list):
            disp = cds[0].get("display") or cds[0].get("code") or ""
            if disp: return disp
    return ""

def observation_view() -> List[Tuple]:
    rows: List[Tuple] = []; seen_keys: Set[str] = set()
    obs, inc = fhir.search(
        "Observation",
        {"_count": 200},
        elements=["id","effectiveDateTime","issued","encounter","subject","note","valueString","valueCodeableConcept","derivedFrom"],
        includes=["Observation:encounter","Observation:subject","Observation:derived-from:Media"],
    )
    def _media_ids_from(o: Dict[str, Any]) -> str:
        ids: List[str] = []
        for ref in o.get("derivedFrom", []) or []:
            r = ref.get("reference", "")
            if r.startswith("Media/"): ids.append(r.split("/", 1)[1])
        return ",".join(ids) if ids else ""
    for o in obs:
        oid = o["id"]
        dt_full = o.get("effectiveDateTime") or o.get("issued") or ""
        y = dt_full[:4] if len(dt_full) >= 4 else ""
        m = dt_full[5:7] if len(dt_full) >= 7 else ""
        d = dt_full[8:10] if len(dt_full) >= 10 else ""
        enc_ref = (o.get("encounter") or {}).get("reference")
        apid = enc_ref.split("/", 1)[1] if enc_ref and "/" in enc_ref else ""
        pat = _inc_get(inc, (o.get("subject") or {}).get("reference"))
        pid = _res_id(pat) if pat else ""; pname = _name_of(pat) if pat else ""
        comment = _obs_comment(o).strip()
        media_ids = _media_ids_from(o)
        norm_comment = " ".join(comment.lower().split())
        key = _digest_key([y, m, d, apid or "", pid or "", norm_comment])
        if key in seen_keys: continue
        seen_keys.add(key)
        rows.append((_nz(oid), _nz(y), _nz(m), _nz(d), _nz(apid), _nz(pid), _nz(_s(pname)), _nz(comment), _nz(media_ids)))
    return rows

def observation_search(obser_id: str = "", year: str = "", month: str = "", day: str = "", appoint_id: str = "",
                       patient_name: str = "", patient_id: str = "") -> List[Tuple]:
    rows = observation_view(); pn = patient_name.lower() if patient_name else ""
    out: List[Tuple] = []
    for (oid, y, m, d, ap, pid, pname, comment, foid) in rows:
        if obser_id and (oid == NO_DATA or obser_id not in oid): continue
        if year and (y == NO_DATA or year != y): continue
        if month and (m == NO_DATA or month != m): continue
        if day and (d == NO_DATA or day != d): continue
        if appoint_id and (ap == NO_DATA or appoint_id not in ap): continue
        if patient_id and (pid == NO_DATA or patient_id not in pid): continue
        if pn and (pname == NO_DATA or pn not in pname.lower()): continue
        out.append((oid, y, m, d, ap, pid, pname, comment, foid))
    return out

def observation_insert(y: str, m: str, d: str, appoint_id: Optional[str],
                       comment_text: Optional[str], file_path: Optional[str]) -> None:
    """POST /Observation (server assigns id)."""
    obs: Dict[str, Any] = {"resourceType": "Observation"}
    obs["status"] = "final"; obs["code"] = {"text": "Observation note"}
    if y and m and d:
        obs["effectiveDateTime"] = f"{int(y):04d}-{int(m):02d}-{int(d):02d}T09:00:00+00:00"
    if appoint_id: obs["encounter"] = {"reference": f"Encounter/{appoint_id}"}
    if comment_text and comment_text.strip():
        obs["note"] = [{"text": comment_text.strip()}]
    if file_path and file_path.strip():
        mid = _make_media_from_file(file_path.strip(), encounter_id=appoint_id)
        obs["derivedFrom"] = [{"reference": f"Media/{mid}"}]
    fhir.create("Observation", obs)

def observation_update(obser_id: str, y: str, m: str, d: str, appoint_id: Optional[str],
                       new_comment_text: Optional[str], new_file_path: Optional[str]) -> None:
    obs = fhir.read(f"Observation/{obser_id}"); changed = False
    if y and m and d:
        obs["effectiveDateTime"] = f"{int(y):04d}-{int(m):02d}-{int(d):02d}T09:00:00+00:00"; changed = True
    # empty appoint_id => no change
    if appoint_id is not None and appoint_id.strip():
        obs["encounter"] = {"reference": f"Encounter/{appoint_id.strip()}"}; changed = True
    # empty comment => no change
    if new_comment_text is not None and new_comment_text.strip():
        obs["note"] = [{"text": new_comment_text.strip()}]; changed = True
    # attachments: empty string => no change
    if new_file_path is not None and new_file_path.strip():
        mid = _make_media_from_file(new_file_path.strip(),
                                    encounter_id=(appoint_id.strip() if appoint_id else None))
        obs["derivedFrom"] = [{"reference": f"Media/{mid}"}]; changed = True
    if changed: fhir.update(f"Observation/{obser_id}", obs)

def observation_delete(obser_id: str) -> None:
    fhir.delete(f"Observation/{obser_id}")

# ===================================================================
#                           DIAGNOSES (Condition)
# ===================================================================
def diagnosis_view() -> List[Tuple]:
    rows: List[Tuple] = []; seen_keys: Set[str] = set()
    conds, inc = fhir.search(
        "Condition",
        {"_count": 200},
        elements=["id","recordedDate","onsetDateTime","encounter","subject","note","evidence"],
        includes=["Condition:subject","Condition:encounter","Condition:evidence-detail:Media"],
    )
    def _media_ids_from_c(c: Dict[str, Any]) -> str:
        ids: List[str] = []
        for ev in c.get("evidence", []) or []:
            for det in ev.get("detail", []) or []:
                r = det.get("reference", "")
                if r.startswith("Media/"): ids.append(r.split("/", 1)[1])
        return ",".join(ids) if ids else ""
    for c in conds:
        cid = c["id"]
        date = c.get("recordedDate") or c.get("onsetDateTime") or ""
        y = date[:4] if len(date) >= 4 else ""
        m = date[5:7] if len(date) >= 7 else ""
        d = date[8:10] if len(date) >= 10 else ""
        enc_ref = (c.get("encounter") or {}).get("reference")
        apid = enc_ref.split("/", 1)[1] if enc_ref and "/" in enc_ref else ""
        pat = _inc_get(inc, (c.get("subject") or {}).get("reference"))
        pid = _res_id(pat) if pat else ""; pname = _name_of(pat) if pat else ""
        comment = _text_any(c.get("note")).strip()
        media_ids = _media_ids_from_c(c)
        norm_comment = " ".join(comment.lower().split())
        key = _digest_key([y, m, d, apid or "", pid or "", norm_comment])
        if key in seen_keys: continue
        seen_keys.add(key)
        rows.append((_nz(cid), _nz(y), _nz(m), _nz(d), _nz(apid), _nz(pid), _nz(_s(pname)), _nz(comment), _nz(media_ids)))
    return rows

def diagnosis_search(diagn_id: str = "", year: str = "", month: str = "", day: str = "",
                     appoint_id: str = "", patient_name: str = "", patient_id: str = "") -> List[Tuple]:
    rows = diagnosis_view(); pn = patient_name.lower() if patient_name else ""
    out: List[Tuple] = []
    for (cid, y, m, d, apid, pid, pname, comment, foid) in rows:
        if diagn_id and (cid == NO_DATA or diagn_id not in cid): continue
        if year and (y == NO_DATA or year != y): continue
        if month and (m == NO_DATA or month != m): continue
        if day and (d == NO_DATA or day != d): continue
        if appoint_id and (apid == NO_DATA or appoint_id not in apid): continue
        if patient_id and (pid == NO_DATA or patient_id not in pid): continue
        if pn and (pname == NO_DATA or pn not in pname.lower()): continue
        out.append((cid, y, m, d, apid, pid, pname, comment, foid))
    return out

def diagnosis_insert(y: str, m: str, d: str, appoint_id: Optional[str],
                     comment_text: Optional[str], file_path: Optional[str]) -> None:
    """POST /Condition (server assigns id)."""
    c: Dict[str, Any] = {"resourceType": "Condition"}
    c["code"] = {"text": "Diagnosis"}
    if y and m and d:
        c["recordedDate"] = f"{int(y):04d}-{int(m):02d}-{int(d):02d}T09:00:00+00:00"
    if appoint_id: c["encounter"] = {"reference": f"Encounter/{appoint_id}"}
    if comment_text and comment_text.strip():
        c["note"] = [{"text": comment_text.strip()}]
    if file_path and file_path.strip():
        mid = _make_media_from_file(file_path.strip(), encounter_id=appoint_id)
        c["evidence"] = [{"detail": [{"reference": f"Media/{mid}"}]}]
    fhir.create("Condition", c)

def diagnosis_update(diagn_id: str, y: str, m: str, d: str, appoint_id: Optional[str],
                     new_comment_text: Optional[str], new_file_path: Optional[str]) -> None:
    c = fhir.read(f"Condition/{diagn_id}"); changed = False
    if y and m and d:
        c["recordedDate"] = f"{int(y):04d}-{int(m):02d}-{int(d):02d}T09:00:00+00:00"; changed = True
    # empty appoint_id => no change
    if appoint_id is not None and appoint_id.strip():
        c["encounter"] = {"reference": f"Encounter/{appoint_id.strip()}"}; changed = True
    # empty comment => no change
    if new_comment_text is not None and new_comment_text.strip():
        c["note"] = [{"text": new_comment_text.strip()}]; changed = True
    # attachments: empty string => no change
    if new_file_path is not None and new_file_path.strip():
        mid = _make_media_from_file(new_file_path.strip(),
                                    encounter_id=(appoint_id.strip() if appoint_id else None))
        c["evidence"] = [{"detail": [{"reference": f"Media/{mid}"}]}]; changed = True
    if changed: fhir.update(f"Condition/{diagn_id}", c)

def diagnosis_delete(diagn_id: str) -> None:
    fhir.delete(f"Condition/{diagn_id}")

# ===================================================================
#                    FILE STORAGE
# ===================================================================
def lo_save_file(local_path: str) -> str:
    if not os.path.isfile(local_path): raise ValueError("File not found")
    return os.path.basename(local_path)

# ===================================================================
#                           CLINICS
# ===================================================================
def _addr_string(addr: Dict[str, Any]) -> str:
    if not addr: return ""
    parts = []
    if addr.get("line"): parts.extend(addr["line"])
    for k in ("city", "state", "postalCode", "country"):
        if addr.get(k): parts.append(str(addr[k]))
    return ", ".join([p for p in parts if p])

def clinic_view() -> List[Tuple]:
    rows: List[Tuple] = []; seen = set()
    orgs, _ = fhir.search("Organization", {"_count": 200}, elements=["id", "name", "address"])
    for o in orgs:
        cid = o.get("id"); cname = o.get("name") or ""
        addr = _addr_string((o.get("address") or [{}])[0] if o.get("address") else {})
        row = (_nz(cid), _nz(cname), _nz(addr))
        if row not in seen: seen.add(row); rows.append(row)
    return rows

def clinic_search(cli_id: str = "", cli_name: str = "", address_kw: str = "") -> List[Tuple]:
    rows = clinic_view(); kw = (address_kw or "").lower(); out = []
    for cid, cname, addr in rows:
        if cli_id and (cid == NO_DATA or cli_id not in cid): continue
        if cli_name and (cname == NO_DATA or cname.lower() not in cname.lower()): continue
        if address_kw and (addr == NO_DATA or kw not in addr.lower()): continue
        out.append((cid, cname, addr))
    return out

def clinic_insert(cli_name: str, address_text: str) -> None:
    """POST /Organization (server assigns id)."""
    org: Dict[str, Any] = {"resourceType": "Organization"}
    if cli_name: org["name"] = cli_name
    if address_text and address_text.strip():
        org["address"] = [{"text": address_text.strip(), "line": [address_text.strip()]}]
    fhir.create("Organization", org)

def clinic_update(cli_id: str, new_name: Optional[str], new_address_text: Optional[str]) -> None:
    if not cli_id: raise ValueError("cli_id is required for update")
    org = fhir.read(f"Organization/{cli_id}"); changed = False
    # empty new_name => no change
    if new_name is not None and new_name.strip():
        org["name"] = new_name.strip(); changed = True
    # empty new_address_text => no change
    if new_address_text is not None and new_address_text.strip():
        txt = new_address_text.strip()
        org["address"] = [{"text": txt, "line": [txt]}]; changed = True
    if changed: fhir.update(f"Organization/{cli_id}", org)

def clinic_delete(cli_id: str) -> None:
    if not cli_id: raise ValueError("cli_id is required")
    fhir.delete(f"Organization/{cli_id}")

# --- Messaging helpers rewritten to use FHIR ---
def get_encounter_patients_for_doctor(doctor_id: str):
    """
    Returns list[(patient_ID, patient_name)] for all Patients who had an Encounter
    with Practitioner {doctor_id}. Uses server-side participant filter.
    """
    # accept "d9005", "Practitioner/9005", or "9005"
    pid = (doctor_id or "").strip()
    if pid.lower().startswith("practitioner/"):
        pid = pid.split("/", 1)[1]
    elif pid and pid[0].lower() == "d" and pid[1:].isdigit():
        pid = pid[1:]
    if not pid:
        return []

    rows, seen = [], set()

    # filter by practitioner so we don't miss Encounters past the first 200
    encs, inc = fhir.search(
        "Encounter",
        {"_count": 200, "participant": f"Practitioner/{pid}"},
        elements=["id", "subject"],                     # only what we need
        includes=["Encounter:subject"],                 # pull Patient into 'inc'
    )

    for e in encs:
        subj_ref = ((e.get("subject") or {}).get("reference") or "")
        if not subj_ref.startswith("Patient/"):
            continue

        pat = _inc_get(inc, subj_ref)
        pat_id = subj_ref.split("/", 1)[1]
        pat_name = _name_of(pat) if pat else pat_id

        row = (_nz(pat_id), _nz(_s(pat_name)))
        if row not in seen:
            seen.add(row)
            rows.append(row)

   
    return rows






def get_encounter_doctors_for_patient(patient_id: str):
    """
    Returns list[(doctor_ID, doctor_name)] for all Practitioners who had an Encounter
    with the given Patient.
    """
    rows = []
    seen = set()

    encs, inc = fhir.search(
        "Encounter",
        {"_count": 200, "subject": f"Patient/{patient_id}"},
        elements=["id", "participant"],
        includes=["Encounter:participant:Practitioner"],  # resolve Practitioner refs
    )

    for e in encs:
        for part in e.get("participant", []) or []:
            ind = part.get("individual") or {}
            ref = ind.get("reference", "")
            if not ref.startswith("Practitioner/"):
                continue
            pr = _inc_get(inc, ref)
            did = _res_id(pr) if pr else (ref.split("/", 1)[1] if "/" in ref else "")
            dname = _name_of(pr) if pr else did

            row = (_nz(did), _nz(_s(dname)))
            if row not in seen:
                seen.add(row)
                rows.append(row)

    return rows
