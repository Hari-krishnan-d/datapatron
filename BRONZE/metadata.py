import json
import os
import math
import boto3
import requests
import pyodbc
import io
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# 5 minutes per source maximum to prevent endless loops
PIPELINE_TIMEOUT_SECONDS = 300


# ------------------------------------------------------------
# NaN CLEANER
# pandas reads empty cells as float NaN.
# NaN is isinstance(value, float) → True, so schema_mapping
# incorrectly types every empty column as DOUBLE.
# Replace all NaN with None before saving so schema_mapping
# sees null and defaults to STRING instead.
# ------------------------------------------------------------
def _clean_sample(rows: list) -> list:
    """
    Clean sample rows so they are valid JSON and honest about nulls.

    What pandas gives us from a CSV sample:
      float NaN      → None   (empty cell)
      float inf      → None   (infinity — invalid JSON)
      int 6453301881 → int    (real phone/ID value — kept)
      float 35000.0  → float  (real amount — kept)
      str "   debit" → str    (real value with spaces — kept as-is)
      str ""         → None   (empty string — becomes null)

    The LLM sees the actual values so it can decide types correctly.
    e.g. PHOnE_numbER=6453301881 (int) → LLM knows it looks numeric
         but column name says phone → LLM should assign STRING.
    """
    import math as _math
    cleaned = []
    for row in rows:
        clean_row = {}
        for k, v in row.items():
            if v is None:
                clean_row[k] = None
            elif isinstance(v, float):
                if _math.isnan(v) or _math.isinf(v):
                    clean_row[k] = None       # NaN / Inf → null
                else:
                    clean_row[k] = v          # real float → keep
            elif isinstance(v, str):
                clean_row[k] = v if v.strip() else None  # blank → null
            else:
                clean_row[k] = v              # int, bool → keep
        cleaned.append(clean_row)
    return cleaned


# ------------------------------------------------------------
# DRIVER DETECTION
# ------------------------------------------------------------
def get_sql_driver():
    drivers = pyodbc.drivers()
    for d in drivers:
        if "SQL Server" in d:
            return d
    raise Exception(f"No SQL Server ODBC driver found. Installed: {drivers}")


# ------------------------------------------------------------
# AZURE SQL SAMPLE EXTRACTION
# ------------------------------------------------------------
def extract_azure_samples(conn):
    driver = get_sql_driver()
    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={conn['server']};"
        f"DATABASE={conn['database']};"
        f"UID={conn['username']};"
        f"PWD={conn['password']};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    connection = pyodbc.connect(connection_string, timeout=5)
    cursor     = connection.cursor()
    schema     = conn.get("schema") or "dbo"

    cursor.execute("""
        SELECT TOP 5 TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
    """, schema)

    tables  = [row[0] for row in cursor.fetchall()]
    samples = {}

    for table in tables:
        try:
            cursor.execute(f"SELECT TOP 5 * FROM {schema}.{table}")
            columns = [col[0] for col in cursor.description]
            rows    = cursor.fetchall()
            samples[f"{schema}.{table}"] = _clean_sample([
                dict(zip(columns, row)) for row in rows
            ])
        except Exception as e:
            samples[f"{schema}.{table}"] = {"error": str(e)}

    connection.close()
    return samples


# ------------------------------------------------------------
# S3 SAMPLE EXTRACTION
# ------------------------------------------------------------
def extract_s3_samples(conn):
    import pandas as pd

    s3 = boto3.client(
        "s3",
        aws_access_key_id     = conn["aws_access_key_id"],
        aws_secret_access_key = conn["aws_secret_access_key"],
        region_name           = conn["region"],
    )

    paginator   = s3.get_paginator("list_objects_v2")
    pages       = paginator.paginate(Bucket=conn["bucket"], Prefix=conn.get("prefix", ""))
    all_objects = []
    for page in pages:
        all_objects.extend(page.get("Contents", []))

    print(f"  Found {len(all_objects)} file(s) in S3 bucket.")

    samples = {}

    for obj in all_objects:
        key = obj["Key"]
        if key.endswith("/"):
            continue

        try:
            obj_data = s3.get_object(Bucket=conn["bucket"], Key=key, Range="bytes=0-50000")
            data     = obj_data["Body"].read()
            fmt      = conn.get("file_format", "csv")

            if fmt == "json":
                df = pd.read_json(io.BytesIO(data), lines=True)
            elif fmt == "csv":
                df = pd.read_csv(io.BytesIO(data), nrows=5)
            else:
                samples[key] = {"info": "Unsupported format"}
                continue

            # Clean NaN → None before storing
            samples[key] = _clean_sample(df.head(5).to_dict(orient="records"))

        except Exception as e:
            samples[key] = {"error": str(e)}

    return samples


# ------------------------------------------------------------
# REST API SAMPLE EXTRACTION
# ------------------------------------------------------------
def extract_api_samples(conn):
    endpoint = conn["endpoint"]
    params   = {"n": conn.get("sample_n", 5)}
    headers  = {}

    auth_type = conn.get("auth_type", "")
    if auth_type == "api_key":
        key_name          = conn.get("header_name", "api_key")
        headers[key_name] = conn.get("api_key", "")
    elif auth_type == "bearer":
        headers["Authorization"] = f"Bearer {conn.get('api_key', '')}"
    elif auth_type == "basic":
        headers["Authorization"] = f"Basic {conn.get('api_key', '')}"

    try:
        response = requests.get(endpoint, params=params, headers=headers, timeout=10)
        data     = response.json()

        if isinstance(data, list):
            return {"api_result": _clean_sample(data[:5])}

        if isinstance(data, dict):
            for key in ("data", "results", "records", "items"):
                if isinstance(data.get(key), list):
                    return {"api_result": _clean_sample(data[key][:5])}
            return {"api_result": _clean_sample([data])}

    except Exception as e:
        return {"error": str(e)}

    return {"api_result": []}


# ------------------------------------------------------------
# DEDUPLICATION
# ------------------------------------------------------------
def _deduplicate_sample_keys(metadata_sources: list) -> list:
    key_counts = {}
    for src in metadata_sources:
        if src.get("source_type") != "rest_api":
            continue
        for key in src.get("sample_data", {}).keys():
            key_counts[key] = key_counts.get(key, 0) + 1

    duplicate_keys = {k for k, v in key_counts.items() if v > 1}
    key_counter    = {k: 0 for k in duplicate_keys}

    for src in metadata_sources:
        if src.get("source_type") != "rest_api":
            continue
        new_sample_data = {}
        for key, rows in src.get("sample_data", {}).items():
            if key in duplicate_keys:
                key_counter[key] += 1
                new_key              = f"{key}_{key_counter[key]:02d}"
                new_sample_data[new_key] = rows
            else:
                new_sample_data[key] = rows
        src["sample_data"] = new_sample_data

    return metadata_sources


# ------------------------------------------------------------
# PARALLEL PIPELINE PROCESSING
# ------------------------------------------------------------
def process_pipeline(pipeline: dict) -> dict:
    source      = pipeline["source_type"]
    conn        = pipeline["connection_template"]
    pipeline_id = pipeline.get("pipeline_id", "")

    print(f"  Processing {source}  [{pipeline_id}]")

    source_entry = {
        "source_type": source,
        "pipeline_id": pipeline_id,
    }

    try:
        if source == "azure_sql":
            source_entry["sample_data"] = extract_azure_samples(conn)
        elif source == "s3":
            source_entry["sample_data"] = extract_s3_samples(conn)
        elif source in ("rest_api", "api"):
            source_entry["sample_data"] = extract_api_samples(conn)
    except Exception as e:
        source_entry["error"] = str(e)

    return source_entry


# ------------------------------------------------------------
# MAIN METADATA BUILDER
# ------------------------------------------------------------
def create_metadata_file(input_json: str, output_metadata: str = "metadata.json"):
    with open(input_json, "r") as f:
        config = json.load(f)

    print("\n  Starting metadata extraction...\n")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_pipeline, p) for p in config["pipelines"]]
        results = []
        for future in futures:
            try:
                results.append(future.result(timeout=PIPELINE_TIMEOUT_SECONDS))
            except TimeoutError:
                results.append({"error": f"Extraction timed out after {PIPELINE_TIMEOUT_SECONDS}s"})

    metadata = {"sources": results}
    metadata["sources"] = _deduplicate_sample_keys(metadata["sources"])

    with open(output_metadata, "w") as f:
        json.dump(metadata, f, indent=2, allow_nan=False)

    print(f"\n  Metadata saved → {output_metadata}")


# ------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------
def run_metadata(credentials_path: str, output_dir: str = "."):
    os.makedirs(output_dir, exist_ok=True)
    metadata_path = os.path.join(output_dir, "metadata.json")
    create_metadata_file(credentials_path, metadata_path)

    with open(metadata_path, "r") as f:
        metadata = json.load(f)

    return metadata, metadata_path


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
if __name__ == "__main__":
    file_path = input("Enter credential JSON path: ").strip()
    run_metadata(file_path)