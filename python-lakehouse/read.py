#!/usr/bin/env python3
"""
Interactive reader for Hudi datasets

- Scans HDFS for folders ending with "-json" that contain ".hoodie" (valid Hudi tables).
- Lets you pick one.
- PROMPTS for how many records to fetch.
- Shows a preview.
- Saves to Parquet (full schema), CSV (complex cols stringified as JSON), and JSONL.
"""

import os
import argparse
import datetime as dt
from session import spark
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, BooleanType,
    TimestampType, DateType, FloatType, ShortType, ByteType, DecimalType, DataType
)

HDFS_BASE_URI  = os.getenv("HDFS_BASE_URI", "hdfs://namenode:8020").rstrip("/")
LAKE_BASE_PATH = os.getenv("LAKE_BASE_PATH", "/fhir")
OUTPUT_ROOT    = os.getenv("OUTPUT_ROOT", "./outputs")

# ---------- HDFS helpers ----------
def hdfs_fs():
    jvm  = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    return jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(HDFS_BASE_URI), conf)

def jpath(p: str):
    return spark._jvm.org.apache.hadoop.fs.Path(p)

# ---------- Discovery ----------
def list_hudi_datasets(max_depth=3) -> dict:
    """
    Recursively scan likely roots for *-json dirs that contain a '.hoodie' folder.
    Returns: { display_name : full_hdfs_path }
    """
    fs = hdfs_fs()

    candidates = [
        f"{HDFS_BASE_URI}{LAKE_BASE_PATH}".rstrip("/"),
    ]

    seen = set()
    found = {}

    def walk(dir_path: str, depth: int):
        if depth > max_depth or dir_path in seen:
            return
        seen.add(dir_path)
        p = jpath(dir_path)
        if not fs.exists(p) or not fs.isDirectory(p):
            return
        for st in fs.listStatus(p) or []:
            if not st.isDirectory():
                continue
            name = st.getPath().getName()
            full = st.getPath().toString()
            # Our convention: "<resource>-json" and must contain ".hoodie"
            if name.endswith("-json"):
                hoodie = jpath(full + "/.hoodie")
                if fs.exists(hoodie) and fs.isDirectory(hoodie):
                    found[name] = full
            # continue walking
            walk(full, depth + 1)

    for root in candidates:
        walk(root, depth=0)

    return dict(sorted(found.items()))

# ---------- UI helpers ----------
def pick_from_list(options: dict) -> str:
    keys = list(options.keys())
    for i, k in enumerate(keys, 1):
        print(f"{i:>2}. {k}  -> {options[k]}")
    while True:
        raw = input("Select a dataset by number: ").strip()
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(keys):
                return keys[idx - 1]
        print("Invalid selection, try again.")

def prompt_limit(default_val=20) -> int:
    while True:
        raw = input(f"How many records to fetch? [{default_val}]: ").strip()
        if raw == "":
            return default_val
        if raw.isdigit() and int(raw) > 0:
            return int(raw)
        print("Please enter a positive integer.")

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

# ---------- Format helpers ----------
# Is the Spark type atomic/CSV-safe?
_ATOMIC = (StringType, IntegerType, LongType, DoubleType, BooleanType,
           TimestampType, DateType, FloatType, ShortType, ByteType, DecimalType)

def csv_friendly(df):
    """
    For CSV: keep atomic types as-is; stringify complex types via to_json.
    """
    cols = []
    for f in df.schema.fields:
        dt: DataType = f.dataType
        if isinstance(dt, _ATOMIC):
            cols.append(F.col(f.name).alias(f.name))
        else:
            cols.append(F.to_json(F.col(f.name)).alias(f.name))
    return df.select(cols)

def save_all_formats(df, name: str):
    import glob, os, datetime as dt
    from pyspark.sql import functions as F

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base_dir = os.path.abspath(os.path.join(OUTPUT_ROOT, name, ts))

    # Local filesystem URIs for Spark
    base_uri = "file://" + base_dir
    pq_dir = os.path.join(base_dir, "parquet")
    cs_dir = os.path.join(base_dir, "csv")
    jl_dir = os.path.join(base_dir, "jsonl")
    for d in (pq_dir, cs_dir, jl_dir):
        os.makedirs(d, exist_ok=True)

    # Parquet (single file)
    df.coalesce(1).write.mode("overwrite").parquet("file://" + pq_dir)
    pq_files = sorted(glob.glob(os.path.join(pq_dir, "part-*")))

    # CSV (stringify complex cols first, single file)
    df_csv = csv_friendly(df)
    df_csv.coalesce(1).write.mode("overwrite").option("header", "true").csv("file://" + cs_dir)
    cs_files = sorted(glob.glob(os.path.join(cs_dir, "part-*.csv"))) or sorted(glob.glob(os.path.join(cs_dir, "part-*")))

    # JSONL (single file)
    df_jsonl = df.select(F.to_json(F.struct(*df.columns)).alias("json"))
    df_jsonl.coalesce(1).write.mode("overwrite").text("file://" + jl_dir)
    jl_files = sorted(glob.glob(os.path.join(jl_dir, "part-*")))

    print("\nSaved results to (LOCAL):")
    print(f"  Parquet : {pq_files[-1] if pq_files else '(none)'}")
    print(f"  CSV     : {cs_files[-1] if cs_files else '(none)'}")
    print(f"  JSONL   : {jl_files[-1] if jl_files else '(none)'}")


# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Read Hudi datasets directly from HDFS (no Hive).")
    ap.add_argument("--name", help="Dataset folder name, e.g. patient-json")
    ap.add_argument("--path", help="Full HDFS path, e.g. hdfs://namenode:8020/fhir/patient-json")
    args = ap.parse_args()

    # Resolve dataset path
    if args.path:
        chosen_name = os.path.basename(args.path.rstrip("/"))
        hdfs_path = args.path
    else:
        datasets = list_hudi_datasets()
        if not datasets:
            print(f"No datasets found under {HDFS_BASE_URI}{LAKE_BASE_PATH} (or nearby).")
            print("Tip: pass --path hdfs://namenode:8020/fhir/<something>-json to read a known table.")
            return
        if args.name:
            if args.name not in datasets:
                print(f"'{args.name}' not found. Available: {', '.join(datasets.keys())}")
                return
            chosen_name = args.name
        else:
            print("=== Available datasets ===")
            chosen_name = pick_from_list(datasets)
        hdfs_path = datasets[chosen_name]

    # Prompt for limit
    limit = prompt_limit(default_val=20)

    print(f"\nReading {limit} from: {chosen_name}  ({hdfs_path})")
    df = spark.read.format("hudi").load(hdfs_path).limit(limit)

    print("\n=== Preview ===")
    df.show(n=min(limit, 20), truncate=80)

    save_all_formats(df, chosen_name)

if __name__ == "__main__":
    main()