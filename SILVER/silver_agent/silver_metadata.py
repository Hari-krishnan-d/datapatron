"""
SILVER/metadata/metadata.py — Silver Metadata Agent
=====================================================
Extracts profiling statistics from Databricks bronze tables
to inform the silver transformation pipeline.

Stats extracted per table:
  - Row count total
  - Column data types
  - Null value count per column
  - Duplicate row count
  - 5 sample rows per table (used by silver_transform.py type casting)

Called by orchestrator.py after silver validation.

Input : silver_credentials.json
Output: silver_metadata.json

{
  "session_id": "...",
  "sources": [
    {
      "pipeline_id": "...",
      "source_type": "databricks_bronze",
      "catalog":     "...",
      "schema":      "...",
      "tables": [
        {
          "table":           "inspection_data",
          "full_table":      "catalog.schema.inspection_data",
          "row_count":       12345,
          "duplicate_count": 23,
          "sample":          [
            {"col1": "val1", "col2": "val2", ...},
            ...  (up to 5 rows)
          ],
          "columns": [
            {
              "name":          "customer_id",
              "type":          "BIGINT",
              "null_count":    0,
              "null_pct":      0.0,
              "sample_values": ["1001", "1002", "1003", "1004", "1005"]
            },
            ...
          ]
        }
      ]
    }
  ]
}
"""

import json
import os
import time
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, TimeoutError

PIPELINE_TIMEOUT_SECONDS = 300  # 5 minutes max per pipeline extraction

logger = logging.getLogger("Silver.Metadata")


# =============================================================================
# SQL RUNNER
# =============================================================================

def _auth_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


def _run_sql(
    workspace_url: str,
    token:         str,
    warehouse_id:  str,
    sql:           str,
    timeout:       int = 120,
) -> list:
    """
    Submit SQL, poll until done, return data_array rows.
    Each row is a list of string values.
    Raises Exception on failure or timeout.
    """
    submit_url = f"{workspace_url}/api/2.0/sql/statements"
    payload    = {
        "statement":    sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
    }
    resp = requests.post(
        submit_url,
        headers=_auth_headers(token),
        json=payload,
        timeout=30,
    )
    if resp.status_code != 200:
        raise Exception(
            f"SQL submission failed ({resp.status_code}): {resp.text[:300]}"
        )

    result = resp.json()
    state  = result.get("status", {}).get("state", "")

    if state not in ("SUCCEEDED", "FAILED", "CANCELED"):
        stmt_id    = result["statement_id"]
        status_url = f"{workspace_url}/api/2.0/sql/statements/{stmt_id}"
        start      = time.time()
        while True:
            sr    = requests.get(status_url, headers=_auth_headers(token), timeout=15)
            state = sr.json().get("status", {}).get("state", "")
            if state == "SUCCEEDED":
                result = sr.json()
                break
            if state in ("FAILED", "CANCELED"):
                raise Exception(
                    f"SQL failed: {sr.json().get('status', {}).get('error', {})}"
                )
            if time.time() - start > timeout:
                raise Exception(f"SQL timed out after {timeout}s")
            time.sleep(2)
    elif state != "SUCCEEDED":
        raise Exception(
            f"SQL failed immediately: {result.get('status', {}).get('error', {})}"
        )

    return result.get("result", {}).get("data_array", [])


def _run_sql_with_schema(
    workspace_url: str,
    token:         str,
    warehouse_id:  str,
    sql:           str,
    timeout:       int = 120,
) -> tuple:
    """
    Same as _run_sql but also returns column schema so we can
    build named dicts from data_array rows.

    Returns:
        (columns: list of {"name": str, "type_name": str}, rows: list of lists)
    """
    submit_url = f"{workspace_url}/api/2.0/sql/statements"
    payload    = {
        "statement":    sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
    }
    resp = requests.post(
        submit_url,
        headers=_auth_headers(token),
        json=payload,
        timeout=30,
    )
    if resp.status_code != 200:
        raise Exception(
            f"SQL submission failed ({resp.status_code}): {resp.text[:300]}"
        )

    result = resp.json()
    state  = result.get("status", {}).get("state", "")

    if state not in ("SUCCEEDED", "FAILED", "CANCELED"):
        stmt_id    = result["statement_id"]
        status_url = f"{workspace_url}/api/2.0/sql/statements/{stmt_id}"
        start      = time.time()
        while True:
            sr    = requests.get(status_url, headers=_auth_headers(token), timeout=15)
            state = sr.json().get("status", {}).get("state", "")
            if state == "SUCCEEDED":
                result = sr.json()
                break
            if state in ("FAILED", "CANCELED"):
                raise Exception(
                    f"SQL failed: {sr.json().get('status', {}).get('error', {})}"
                )
            if time.time() - start > timeout:
                raise Exception(f"SQL timed out after {timeout}s")
            time.sleep(2)
    elif state != "SUCCEEDED":
        raise Exception(
            f"SQL failed immediately: {result.get('status', {}).get('error', {})}"
        )

    schema_cols = result.get("manifest", {}).get("schema", {}).get("columns", [])
    data_rows   = result.get("result",   {}).get("data_array", [])

    return schema_cols, data_rows


# =============================================================================
# TABLE DISCOVERY
# =============================================================================

def _get_tables(
    workspace_url: str,
    token:         str,
    warehouse_id:  str,
    catalog:       str,
    schema:        str,
    table:         str,
) -> list:
    """
    Return list of table names to profile.
    If table is given → just that one.
    If blank          → all tables in schema.
    """
    if table:
        return [table]

    rows = _run_sql(
        workspace_url, token, warehouse_id,
        f"SHOW TABLES IN {catalog}.{schema}",
    )
    # SHOW TABLES returns: database, tableName, isTemporary
    return [row[1] for row in rows if row]


# =============================================================================
# SAMPLE ROWS
# =============================================================================

def _fetch_sample_rows(
    workspace_url: str,
    token:         str,
    warehouse_id:  str,
    full_table:    str,
    columns_raw:   list,
    n:             int = 5,
) -> tuple:
    """
    Fetch up to n sample rows from the table.

    Returns:
        sample_rows  : list of dicts  [{col_name: value, ...}, ...]
        sample_values: dict           {col_name: [val1, val2, ...], ...}

    sample_rows     → stored on the table profile (for auto modeller / LLM context)
    sample_values   → stored per column (for type casting recommendations in transform)
    """
    if not columns_raw:
        return [], {}

    try:
        schema_cols, data_rows = _run_sql_with_schema(
            workspace_url, token, warehouse_id,
            f"SELECT * FROM {full_table} LIMIT {n}",
        )

        # Use schema column names if available, else fall back to columns_raw
        if schema_cols:
            col_names = [c.get("name", "") for c in schema_cols]
        else:
            col_names = [c["name"] for c in columns_raw]

        # Build list of row dicts
        sample_rows = []
        for row in data_rows:
            row_dict = {}
            for j, col_name in enumerate(col_names):
                row_dict[col_name] = row[j] if j < len(row) else None
            sample_rows.append(row_dict)

        # Build per-column sample value lists
        sample_values = {col_name: [] for col_name in col_names}
        for row_dict in sample_rows:
            for col_name, val in row_dict.items():
                if val is not None and str(val).strip() != "":
                    sample_values[col_name].append(val)

        return sample_rows, sample_values

    except Exception as exc:
        logger.warning("Could not fetch sample rows from %s: %s", full_table, exc)
        return [], {}


# =============================================================================
# TABLE PROFILER
# =============================================================================

def _profile_table(
    workspace_url: str,
    token:         str,
    warehouse_id:  str,
    catalog:       str,
    schema:        str,
    table:         str,
) -> dict:
    """
    Run profiling queries against one Databricks table.

    Returns:
    {
        "table":           "...",
        "full_table":      "catalog.schema.table",
        "row_count":       int,
        "duplicate_count": int,
        "sample":          [{col: val, ...}, ...],   ← up to 5 rows
        "columns": [
            {
                "name":          "...",
                "type":          "...",
                "null_count":    int,
                "null_pct":      float,
                "sample_values": [val1, val2, ...]   ← up to 5 non-null values
            }
        ]
    }
    """
    full_table = f"{catalog}.{schema}.{table}"
    print(f"\n  Profiling: {full_table}")

    # ── 1. Row count ──────────────────────────────────────────────────────────
    rows      = _run_sql(workspace_url, token, warehouse_id, f"SELECT COUNT(*) FROM {full_table}")
    row_count = int(rows[0][0]) if rows else 0
    print(f"     Row count       : {row_count:,}")

    # ── 2. Column types via DESCRIBE ─────────────────────────────────────────
    desc_rows = _run_sql(
        workspace_url, token, warehouse_id,
        f"DESCRIBE TABLE {full_table}",
    )
    # DESCRIBE returns: col_name, data_type, comment
    # Stop at blank separator row that Databricks adds before partition info
    columns_raw = []
    for row in desc_rows:
        col_name = row[0].strip() if row[0] else ""
        col_type = row[1].strip() if len(row) > 1 and row[1] else "STRING"
        if not col_name or col_name.startswith("#"):
            break
        columns_raw.append({"name": col_name, "type": col_type})

    print(f"     Columns         : {len(columns_raw)}")

    # ── 3. Null counts ────────────────────────────────────────────────────────
    if columns_raw:
        null_exprs = ", ".join(
            f"SUM(CASE WHEN `{c['name']}` IS NULL THEN 1 ELSE 0 END) AS `null_{c['name']}`"
            for c in columns_raw
        )
        null_rows   = _run_sql(workspace_url, token, warehouse_id,
                               f"SELECT {null_exprs} FROM {full_table}")
        null_values = null_rows[0] if null_rows else []
    else:
        null_values = []

    # ── 4. Sample rows (5 rows) ───────────────────────────────────────────────
    print(f"     Fetching samples: up to 5 rows...")
    sample_rows, sample_values = _fetch_sample_rows(
        workspace_url, token, warehouse_id,
        full_table, columns_raw, n=5,
    )
    print(f"     Sample rows     : {len(sample_rows)} fetched")

    # ── 5. Assemble column profiles ───────────────────────────────────────────
    columns = []
    for i, col in enumerate(columns_raw):
        null_count   = int(null_values[i]) if i < len(null_values) and null_values[i] else 0
        null_pct     = round((null_count / row_count * 100), 2) if row_count > 0 else 0.0
        col_samples  = sample_values.get(col["name"], [])

        columns.append({
            "name"         : col["name"],
            "type"         : col["type"],
            "null_count"   : null_count,
            "null_pct"     : null_pct,
            "sample_values": col_samples,   # up to 5 non-null values for type casting
        })

    nulled = [c for c in columns if c["null_count"] > 0]
    if nulled:
        print(f"     Columns w/ nulls: {[c['name'] for c in nulled]}")
    else:
        print(f"     Nulls           : none")

    # ── 6. Duplicate row count ────────────────────────────────────────────────
    all_cols        = ", ".join(f"`{c['name']}`" for c in columns_raw)
    dup_sql         = f"""
        SELECT COUNT(*) FROM (
            SELECT {all_cols}, COUNT(*) AS cnt
            FROM {full_table}
            GROUP BY {all_cols}
            HAVING cnt > 1
        )
    """
    dup_rows        = _run_sql(workspace_url, token, warehouse_id, dup_sql)
    duplicate_count = int(dup_rows[0][0]) if dup_rows else 0
    print(f"     Duplicate rows  : {duplicate_count:,}")

    return {
        "table"          : table,
        "full_table"     : full_table,
        "row_count"      : row_count,
        "duplicate_count": duplicate_count,
        "sample"         : sample_rows,    # list of up to 5 row dicts
        "columns"        : columns,
    }


def _process_pipeline(pipeline: dict) -> dict:
    """Helper to process a single silver pipeline's metadata extraction."""
    pipeline_id = pipeline["pipeline_id"]
    conn        = pipeline.get("connection_template", {})

    workspace_url = conn.get("workspace_url", "").rstrip("/")
    warehouse_id  = conn.get("warehouse_id", "")
    token         = conn.get("token", "")
    catalog       = conn.get("catalog", "")
    schema        = conn.get("schema", "")
    table         = conn.get("table", "")

    source_entry = {
        "pipeline_id": pipeline_id,
        "source_type": "databricks_bronze",
        "catalog"    : catalog,
        "schema"     : schema,
        "tables"     : [],
    }

    try:
        table_names = _get_tables(workspace_url, token, warehouse_id, catalog, schema, table)
        if not table_names:
            raise Exception(f"No tables found in {catalog}.{schema}")

        for tbl in table_names:
            profile = _profile_table(workspace_url, token, warehouse_id, catalog, schema, tbl)
            source_entry["tables"].append(profile)
    except Exception as exc:
        logger.error("Metadata failed for %s: %s", pipeline_id, exc)
        source_entry["error"] = str(exc)

    return source_entry


# =============================================================================
# SILVER METADATA AGENT
# =============================================================================

def run_silver_metadata(
    credentials_path: str,
    output_dir:       str = ".",
) -> tuple:
    """
    Profile all bronze tables referenced by silver pipelines.

    Args:
        credentials_path : Path to silver_credentials.json.
        output_dir       : Where to save silver_metadata.json.

    Returns:
        (metadata_dict, metadata_path)
    """
    logger.info("Silver Metadata Agent started.")

    with open(credentials_path, "r", encoding="utf-8") as f:
        plan = json.load(f)

    session_id = plan.get("session_id", "unknown")
    metadata   = {
        "session_id": session_id,
        "sources":    [],
    }

    print("\n" + "=" * 57)
    print("  SILVER — METADATA EXTRACTION")
    print(f"  Session   : {session_id}")
    print(f"  Pipelines : {len(plan.get('pipelines', []))}")
    print("=" * 57)
    print("\n  Profiling bronze tables...")
    print("  (row count, types, nulls, duplicates, 5 sample rows)\n")

    # Parallelize extraction using a ThreadPool to prevent sequential blocking
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pid = {
            executor.submit(_process_pipeline, p): p["pipeline_id"]
            for p in plan.get("pipelines", [])
        }

        for future in future_to_pid:
            pid = future_to_pid[future]
            try:
                source_entry = future.result(timeout=PIPELINE_TIMEOUT_SECONDS)
                metadata["sources"].append(source_entry)
            except TimeoutError:
                logger.error("Metadata extraction timed out for pipeline %s", pid)
                metadata["sources"].append({
                    "pipeline_id": pid,
                    "error": f"Extraction timed out after {PIPELINE_TIMEOUT_SECONDS}s"
                })

    # ── Save ──────────────────────────────────────────────────────────────────
    os.makedirs(output_dir, exist_ok=True)
    metadata_path = os.path.join(output_dir, "silver_metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    logger.info("Silver metadata → %s", metadata_path)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_tables  = sum(len(s.get("tables", [])) for s in metadata["sources"])
    total_rows    = sum(
        t.get("row_count", 0)
        for s in metadata["sources"]
        for t in s.get("tables", [])
    )
    total_dups    = sum(
        t.get("duplicate_count", 0)
        for s in metadata["sources"]
        for t in s.get("tables", [])
    )
    total_samples = sum(
        len(t.get("sample", []))
        for s in metadata["sources"]
        for t in s.get("tables", [])
    )

    print(f"\n  {'─' * 55}")
    print(f"  SILVER METADATA COMPLETE")
    print(f"  Tables profiled : {total_tables}")
    print(f"  Total rows      : {total_rows:,}")
    print(f"  Total duplicates: {total_dups:,}")
    print(f"  Sample rows     : {total_samples} total ({total_samples // max(total_tables, 1)} avg/table)")
    print(f"  Saved → {metadata_path}")
    print(f"  {'─' * 55}\n")

    return metadata, metadata_path