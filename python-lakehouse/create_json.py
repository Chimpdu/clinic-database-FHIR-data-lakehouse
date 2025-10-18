#!/usr/bin/env python3
"""
Auto-ingest populated FHIR resource types from a HAPI FHIR server into a Hudi-backed data lake,
WITHOUT using Hive Metastore.

Flow:
1) Discover resource types from /metadata.
2) Count each type (_summary=count) and keep only non-empty.
3) Incremental pull since last watermark (_lastUpdated), with paging.
4) Upsert to Hudi by resourceType path (no Hive sync).
5) Persist watermark per type.

Set via env (defaults shown):
- FHIR_BASE_URL     = https://hapi-fhir.app.cloud.cbh.kth.se/fhir
- HDFS_BASE_URI     = hdfs://namenode:8020
- LAKE_BASE_PATH    = /fhir
- STATE_DIR         = .state
- PAGE_SIZE         = 200
- REQUEST_TIMEOUT_S = 30
- SLEEP_BETWEEN_REQ = 0.2
"""

import os
import json
import time
import datetime as dt
from typing import Dict, List, Optional
import requests
from urllib.parse import urljoin

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from session import spark

# ---------- Config ----------
FHIR_BASE_URL     = os.getenv("FHIR_BASE_URL", "https://hapi-fhir.app.cloud.cbh.kth.se/fhir").rstrip("/") + "/"
HDFS_BASE_URI     = os.getenv("HDFS_BASE_URI", "hdfs://namenode:8020").rstrip("/")
LAKE_BASE_PATH    = os.getenv("LAKE_BASE_PATH", "/fhir")
STATE_DIR         = os.getenv("STATE_DIR", ".state")
PAGE_SIZE         = int(os.getenv("PAGE_SIZE", "200"))
REQUEST_TIMEOUT_S = int(os.getenv("REQUEST_TIMEOUT_S", "30"))
SLEEP_BETWEEN_REQ = float(os.getenv("SLEEP_BETWEEN_REQ", "0.2"))

os.makedirs(STATE_DIR, exist_ok=True)

def _utcnow_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _wm_path(rt: str) -> str:
    return os.path.join(STATE_DIR, f"{rt}.watermark.txt")

def load_watermark(resource_type: str) -> Optional[str]:
    p = _wm_path(resource_type)
    if os.path.exists(p):
        s = open(p, "r", encoding="utf-8").read().strip()
        return s or None
    return None

def save_watermark(resource_type: str, iso_ts: str) -> None:
    with open(_wm_path(resource_type), "w", encoding="utf-8") as f:
        f.write(iso_ts)

def get_capability_statement() -> Dict:
    url = urljoin(FHIR_BASE_URL, "metadata")
    r = requests.get(url, timeout=REQUEST_TIMEOUT_S, headers={"Accept": "application/fhir+json"})
    r.raise_for_status()
    return r.json()

def discover_resource_types_from_capability() -> List[str]:
    cs = get_capability_statement()
    types = []
    for rest in cs.get("rest", []):
        for res in rest.get("resource", []):
            t = res.get("type")
            if t:
                types.append(t)
    return sorted(set(types))

def get_resource_count(resource_type: str) -> int:
    url = urljoin(FHIR_BASE_URL, resource_type)
    params = {"_summary": "count", "_total": "accurate", "_format": "json"}
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_S, headers={"Accept": "application/fhir+json"})
    r.raise_for_status()
    return int(r.json().get("total", 0))

def discover_non_empty_resource_types() -> List[str]:
    all_types = discover_resource_types_from_capability()
    non_empty = []
    print("=== Probing resource types for non-empty counts ===")
    for t in all_types:
        try:
            c = get_resource_count(t)
            print(f"- {t:<22} {c}")
            if c > 0:
                non_empty.append(t)
        except Exception as e:
            print(f"  [WARN] Skipping {t}: {e}")
        time.sleep(SLEEP_BETWEEN_REQ)
    return non_empty

def iter_bundle_entries(resource_type: str, since_iso: Optional[str]) -> List[Dict]:
    base_url = urljoin(FHIR_BASE_URL, resource_type)
    params = {"_format": "json", "_count": str(PAGE_SIZE), "_sort": "_lastUpdated"}
    if since_iso:
        params["_lastUpdated"] = f"gt{since_iso}"

    url = base_url
    out: List[Dict] = []
    while True:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_S, headers={"Accept": "application/fhir+json"})
        r.raise_for_status()
        bundle = r.json()
        for e in bundle.get("entry", []) or []:
            res = e.get("resource")
            if res:
                out.append(res)
        # follow next link
        next_url = None
        for link in bundle.get("link", []) or []:
            if link.get("relation") == "next":
                next_url = link.get("url")
                break
        if not next_url:
            break
        url, params = next_url, None
        time.sleep(SLEEP_BETWEEN_REQ)
    return out

def hudi_base_path(resource_type: str) -> str:
    return f"{HDFS_BASE_URI}{LAKE_BASE_PATH}/{resource_type.lower()}-json"

def try_read_existing_schema(resource_type: str) -> Optional[StructType]:
    path = hudi_base_path(resource_type)
    try:
        return spark.read.format("hudi").load(path).limit(0).schema
    except Exception:
        return None

def resources_to_spark_df(resource_list: List[Dict], enforce_schema: Optional[StructType]):
    if not resource_list:
        return None
    # Load via JSON to keep nested fields
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in resource_list])
    reader = spark.read
    # If table already exists, enforce its schema (avoids MissingSchemaFieldException)
    if enforce_schema is not None:
        reader = reader.schema(enforce_schema)
    df = reader.json(rdd)
    # Precombine field (guaranteed non-null)
    df = df.withColumn("meta_lastUpdated", F.coalesce(F.col("meta.lastUpdated"), F.lit(_utcnow_iso())))
    # Partition field (kept for convenience)
    df = df.withColumn("resourceType", F.col("resourceType"))
    # Record key must exist; if missing, drop (very rare)
    df = df.filter(F.col("id").isNotNull())
    return df

def hudi_write(df, resource_type: str):
    base_path  = hudi_base_path(resource_type)

    options = {
        "hoodie.table.name": f"fhir_raw_{resource_type.lower()}",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "meta_lastUpdated",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.partitionpath.field": "resourceType",

        # Make writes tolerant to field additions/removals
        "hoodie.datasource.write.reconcile.schema": "true",
        "hoodie.avro.schema.validate": "false",
    }

    (df.write
        .format("hudi")
        .options(**options)
        .mode("append")
        .save(base_path))

    print(f"[OK] Upserted {resource_type} -> path={base_path} (no Hive sync)")

def main():
    print(f"FHIR_BASE_URL   = {FHIR_BASE_URL}")
    print(f"HDFS_BASE_URI   = {HDFS_BASE_URI}")
    print(f"LAKE_BASE_PATH  = {LAKE_BASE_PATH}")
    print(f"STATE_DIR       = {STATE_DIR}\n")

    populated = discover_non_empty_resource_types()
    if not populated:
        print("No non-empty resource types detected. Nothing to ingest.")
        return

    for rtype in populated:
        print(f"\n=== Loading {rtype} incrementally ===")
        since = load_watermark(rtype)
        if since:
            print(f"Using watermark: {since}")
        else:
            print("No watermark found — full backfill for this type.")

        resources = iter_bundle_entries(rtype, since)
        if not resources:
            print("Nothing new to ingest.")
            save_watermark(rtype, _utcnow_iso())
            continue

        existing_schema = try_read_existing_schema(rtype)
        df = resources_to_spark_df(resources, enforce_schema=existing_schema)
        if df is None or df.rdd.isEmpty():
            print("Parsed DataFrame is empty; skipping write.")
            save_watermark(rtype, _utcnow_iso())
            continue

        hudi_write(df, rtype)

        max_ts = df.agg(F.max("meta_lastUpdated").alias("max_ts")).collect()[0]["max_ts"]
        save_watermark(rtype, max_ts or _utcnow_iso())
        print(f"Updated watermark for {rtype}: {max_ts}")

    print("\nAll done.")

if __name__ == "__main__":
    main()
