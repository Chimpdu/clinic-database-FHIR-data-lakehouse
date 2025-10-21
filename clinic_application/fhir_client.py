import base64, json, mimetypes
from typing import Any, Dict, Iterable, List, Optional, Tuple
import requests
from fhir_config import FHIR_BASE, FHIR_BASIC_USER, FHIR_BASIC_PASS, FHIR_BEARER, TIMEOUT, HEADERS_JSON  # noqa: F401

Auth = (FHIR_BASIC_USER, FHIR_BASIC_PASS) if (FHIR_BASIC_USER and FHIR_BASIC_PASS) else None

_session = requests.Session()
# Auth
if FHIR_BEARER:
    _session.headers.update({"Authorization": f"Bearer {FHIR_BEARER}"})
# Performance headers
_session.headers.update({
    "Accept": "application/fhir+json",
    "Accept-Encoding": "gzip",
    "Connection": "keep-alive",
})


# -------- tiny LRU-ish cache for read-heavy refs --------
_cache: Dict[str, Dict[str, Any]] = {}
def _cache_get(ref: str) -> Optional[Dict[str, Any]]:
    return _cache.get(ref)
def _cache_put(ref: str, res: Dict[str, Any]):
    if len(_cache) > 1000:
        _cache.clear()
    _cache[ref] = res

def _url(path: str) -> str:
    return f"{FHIR_BASE}/{path.strip('/')}"

# ======================= basic HTTP wrappers =======================
def _raise_with_detail(resp: requests.Response):
    try:
        detail = resp.text
    except Exception:
        detail = "<no body>"
    resp.raise_for_status()  # will raise HTTPError; requests attaches resp
    # Safety: if somehow not raised, raise with detail
    raise requests.HTTPError(f"{resp.status_code} {resp.reason}: {detail}", response=resp)

def get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    # cache only for canonical refs like Resource/id (no params)
    if not params and "/" in path and "?" not in path:
        c = _cache_get(path)
        if c is not None:
            return c
    r = _session.get(_url(path), params=params, timeout=TIMEOUT, auth=Auth)
    if not r.ok:
        _raise_with_detail(r)
    data = r.json()
    if not params and "/" in path and "?" not in path:
        _cache_put(path, data)
    return data

def post(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    r = _session.post(_url(path), data=json.dumps(body), headers=HEADERS_JSON, timeout=TIMEOUT, auth=Auth)
    if not r.ok:
        _raise_with_detail(r)
    return r.json()

def put(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    r = _session.put(_url(path), data=json.dumps(body), headers=HEADERS_JSON, timeout=TIMEOUT, auth=Auth)
    if not r.ok:
        _raise_with_detail(r)
    return r.json()

def delete(path: str) -> None:
    r = _session.delete(_url(path), timeout=TIMEOUT, auth=Auth)
    if r.status_code not in (200, 202, 204):
        _raise_with_detail(r)

# ================= convenience helpers expected by backend.py =================
def create(resource: str, body: Dict[str, Any]) -> Dict[str, Any]:
    return post(resource, body)

def update(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    return put(path, body)

def read(path: str) -> Dict[str, Any]:
    return get(path)


# =========== optimized searches with _include/_elements/_revinclude, mainly for viewall function ===========
# _include adds things this resource points to, _revinclude adds things point to this resource
def search(resource: str, params: Dict[str, Any], *,
           elements: Optional[List[str]] = None,
           includes: Optional[List[str]] = None,
           revincludes: Optional[List[str]] = None
           ) -> Tuple[List[Dict[str,str]], Dict[Tuple[str,str], Dict[str,Any]]]:
    q = dict(params or {})
    q.setdefault("_summary", "data")

    send_elements = (elements and not includes and not revincludes)
    if send_elements:
        q["_elements"] = ",".join(elements)

    if includes:
        for inc in includes:
            q.setdefault("_include", []).append(inc)
    if revincludes:
        for inc in revincludes:
            q.setdefault("_revinclude", []).append(inc)

    def _fetch(qparams: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return get(resource, qparams)
        except requests.HTTPError:
            if "_summary" in qparams or "_elements" in qparams:
                q2 = dict(qparams)
                q2.pop("_summary", None)
                q2.pop("_elements", None)
                return get(resource, q2)
            raise

    res = _fetch(q)

    all_entries: List[Dict[str, Any]] = []
    inc_map: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def _collect(page: Dict[str, Any]):
        for e in page.get("entry", []) or []:
            r = e.get("resource")
            if not r:
                continue
            rt, rid = r.get("resourceType"), r.get("id")
            if not rt or not rid:
                continue
            inc_map[(rt, rid)] = r
            _cache_put(f"{rt}/{rid}", r)
            if rt == resource.rstrip("/"):
                all_entries.append(r)
    #  inc_map stores all resouces (including the _include and _revinclude), all_enties store only the main type we searched for 
    _collect(res)
    #  link to next bundle, we do this because of paging
    next_link = next((l.get("url") for l in res.get("link", []) if l.get("relation") == "next"), None)
    while next_link:
        res = _session.get(next_link, timeout=TIMEOUT, auth=Auth).json()
        _collect(res)
        next_link = next((l.get("url") for l in res.get("link", []) if l.get("relation") == "next"), None)

    return all_entries, inc_map


# -------- Binary/Media helpers  --------
def upload_binary_from_file(path: str, content_type: Optional[str] = None) -> str:
    if not content_type:
        content_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
    with open(path, "rb") as f:
        data_b64 = base64.b64encode(f.read()).decode("ascii")
    res = post("Binary", {"resourceType": "Binary", "contentType": content_type, "data": data_b64})
    return res["id"]


