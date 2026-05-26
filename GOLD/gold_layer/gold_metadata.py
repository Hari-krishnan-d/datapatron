"""
gold_metadata.py — Gold Layer Metadata Builder (Integrated)
============================================================
Combines:
  - gold_metadata.py  : init_gold_metadata() credential injection +
                        build_metadata() table-level enrichment
  - metadata.py       : fetch_metadata_grouped() fast parallel engine
                        (batched SQL stats + batched LLM enrichment)

The orchestrator calls:
  1. init_gold_metadata(hostname, sql_warehouse_id, access_token, model_endpoint)
       → stores credentials in module-level variables
  2. fetch_metadata_grouped(catalog, schema, hostname=..., ...)
       → runs the fast parallel metadata extraction and returns enriched dict

Both entry points are available in this single file.
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from databricks import sql as databricks_sql
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()


# =============================================================================
# MODULE-LEVEL CREDENTIAL STORE
# Populated by init_gold_metadata() — mirrors the lazy-init pattern used
# by data_model.py so the orchestrator can inject credentials once.
# =============================================================================

_HOSTNAME:     str = ""
_ACCESS_TOKEN: str = ""
_WAREHOUSE_ID: str = ""
_MODEL_NAME:   str = os.getenv("MODEL_NAME", "")

_llm_client: "OpenAI | None" = None

description_cache: dict = {}


def init_gold_metadata(
    hostname: str,
    sql_warehouse_id: str,
    access_token: str,
    model_endpoint: str = "",
) -> None:
    """
    Inject Databricks credentials into this module.
    Called once by the orchestrator after credential collection.
    """
    global _HOSTNAME, _ACCESS_TOKEN, _WAREHOUSE_ID, _MODEL_NAME, _llm_client

    _HOSTNAME     = _sanitise_hostname(hostname)
    _ACCESS_TOKEN = access_token
    _WAREHOUSE_ID = sql_warehouse_id

    if model_endpoint:
        _MODEL_NAME = model_endpoint

    _llm_client = OpenAI(
        api_key  = access_token,
        base_url = f"https://{_HOSTNAME}/serving-endpoints",
    )


def _get_client() -> OpenAI:
    if _llm_client is None:
        raise RuntimeError(
            "gold_metadata has not been initialised — "
            "call init_gold_metadata(hostname, sql_warehouse_id, access_token) first."
        )
    return _llm_client


# =============================================================================
# CREDENTIAL HELPERS
# =============================================================================

def _sanitise_hostname(hostname: str) -> str:
    """Strip accidental https:// prefix and trailing slashes."""
    h = hostname.strip()
    for prefix in ("https://", "http://"):
        if h.startswith(prefix):
            h = h[len(prefix):]
    return h.rstrip("/")


def _validate_credentials(
    hostname: str,
    sql_warehouse_id: str,
    access_token: str,
    model_endpoint: str,
) -> None:
    errors = []

    if not hostname or hostname.strip() in ("", "YOUR_HOSTNAME"):
        errors.append(
            "DATABRICKS_HOST is empty or not set. "
            "Expected format: dbc-xxxxxxxx.cloud.databricks.com"
        )
    if not access_token or not access_token.startswith("dapi"):
        errors.append(
            "DATABRICKS_TOKEN looks invalid (should start with 'dapi'). "
            "Generate a new token in Databricks → User Settings → Developer → Access Tokens."
        )
    if not sql_warehouse_id or not sql_warehouse_id.startswith("/sql/"):
        errors.append(
            f"SQL_WAREHOUSE_ID '{sql_warehouse_id}' looks wrong. "
            "Expected format: /sql/1.0/warehouses/<warehouse-id>"
        )
    if not model_endpoint or model_endpoint.strip() in ("", "YOUR_MODEL"):
        errors.append(
            "MODEL_NAME / model_endpoint is empty. "
            "Expected value: the exact serving endpoint name shown in "
            "Databricks → Serving (e.g. 'databricks-llama-4-maverick')."
        )

    if errors:
        msg = "\n".join(f"  ✗ {e}" for e in errors)
        raise ValueError(
            f"\n[gold_metadata.py] Credential validation failed:\n{msg}\n"
            "Fix the values above and retry."
        )


# =============================================================================
# CONNECTION HELPERS
# =============================================================================

def _make_llm_client(
    hostname: str,
    access_token: str,
    llm_host: str = "",
    llm_token: str = "",
) -> OpenAI:
    resolved_host  = llm_host  if llm_host  else hostname
    resolved_token = llm_token if llm_token else access_token
    return OpenAI(
        api_key  = resolved_token,
        base_url = f"https://{resolved_host}/serving-endpoints",
    )


def _get_connection(hostname: str, sql_warehouse_id: str, access_token: str):
    return databricks_sql.connect(
        server_hostname = hostname,
        http_path       = sql_warehouse_id,
        access_token    = access_token,
    )


def get_connection():
    """Module-level connection using stored credentials (for build_metadata)."""
    if not _HOSTNAME or not _ACCESS_TOKEN:
        raise RuntimeError(
            "gold_metadata credentials not set — call init_gold_metadata() first."
        )
    return _get_connection(_HOSTNAME, _WAREHOUSE_ID, _ACCESS_TOKEN)


# =============================================================================
# RULE-BASED HELPERS  (fast — no I/O)
# =============================================================================

def _rule_description(col: str) -> str | None:
    c = col.lower()
    if c.endswith("_id"):
        return f"Unique identifier for each {c.replace('_id', '')} record"
    if c.endswith("_date"):
        return f"Date when {c.replace('_date', '')} occurred"
    if c.endswith("_amount"):
        return f"Monetary value for {c.replace('_amount', '')}"
    if c.startswith("is_"):
        return f"Indicates whether {c.replace('is_', '')}"
    return None


def _rule_role(col: str, distinct_count: int, row_count: int, dtype: str) -> str | None:
    ratio = distinct_count / row_count if row_count else 0
    c     = col.lower()
    dt    = dtype.lower()

    if c.endswith("_id") and ratio > 0.95:
        return "primary_key"
    if c.endswith("_id") and ratio > 0.2:
        return "foreign_key"
    if any(x in dt for x in ("int", "decimal", "double", "float", "bigint", "smallint")):
        return "measure"
    if ratio < 0.05:
        return "low_cardinality_dimension"
    if ratio > 0.95:
        return "high_cardinality_dimension"
    return None


# =============================================================================
# BATCHED SQL  — one query per table instead of one per column
# =============================================================================

def _fetch_table_stats_batch(conn, full_table: str, columns: list[str]) -> dict[str, dict]:
    parts = ["COUNT(*) AS __total__"]
    for col in columns:
        safe = f"`{col}`"
        parts.append(f"SUM(CASE WHEN {safe} IS NULL THEN 1 ELSE 0 END) AS `{col}__nulls`")
        parts.append(f"COUNT(DISTINCT {safe}) AS `{col}__distinct`")

    select_sql = "SELECT " + ", ".join(parts) + f" FROM {full_table}"
    cursor = conn.cursor()
    try:
        cursor.execute(select_sql)
        row        = cursor.fetchone()
        desc       = [c[0] for c in cursor.description]
        result_map = dict(zip(desc, row))
    finally:
        cursor.close()

    total  = result_map.get("__total__", 0) or 0
    output = {}
    for col in columns:
        null_count     = result_map.get(f"{col}__nulls",    0) or 0
        distinct_count = result_map.get(f"{col}__distinct", 0) or 0
        null_rate      = round(null_count / total, 4) if total > 0 else 0.0
        output[col]    = {
            "null_count":     int(null_count),
            "null_rate":      null_rate,
            "distinct_count": int(distinct_count),
        }
    return output


def _fetch_row_count(conn, full_table: str) -> int:
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def _fetch_sample_data(conn, full_table: str, limit: int = 3) -> list[dict]:
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT * FROM {full_table} LIMIT {limit}")
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


# =============================================================================
# BATCHED LLM  — one call per table for all residual columns
# =============================================================================

_VALID_ROLES = {
    "primary_key", "foreign_key", "measure",
    "dimension", "low_cardinality_dimension", "high_cardinality_dimension",
}


def _batch_llm_enrich(
    table: str,
    residual_columns: list[dict],
    client: OpenAI,
    model_endpoint: str,
    max_retries: int = 3,
) -> dict[str, dict]:
    if not residual_columns:
        return {}

    col_lines = []
    for c in residual_columns:
        needs = []
        if c["needs_desc"]:
            needs.append("description")
        if c["needs_role"]:
            needs.append("role")
        col_lines.append(
            f'  - column="{c["col"]}"  dtype={c["dtype"]}'
            f'  null_rate={c["stats"]["null_rate"]}'
            f'  distinct_count={c["stats"]["distinct_count"]}'
            f'  needs=[{", ".join(needs)}]'
        )

    prompt = f"""You are a senior data engineer.

Table: {table}

For each column below, provide ONLY what is listed in its "needs" field.

Columns:
{chr(10).join(col_lines)}

RULES:
- description : max 12 words, business-focused, no quotes
- role        : EXACTLY one of: primary_key | foreign_key | measure |
                dimension | low_cardinality_dimension | high_cardinality_dimension

Respond ONLY with a valid JSON object, no markdown fences, no explanation.
Format:
{{
  "column_name": {{ "description": "...", "role": "..." }},
  ...
}}
If a field is not needed, omit it from that column's object.
"""

    for attempt in range(1, max_retries + 1):
        try:
            response = client.chat.completions.create(
                model       = model_endpoint,
                messages    = [{"role": "user", "content": prompt}],
                temperature = 0,
            )
            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            data = json.loads(raw)
            for col_data in data.values():
                if "role" in col_data and col_data["role"] not in _VALID_ROLES:
                    col_data["role"] = "dimension"
            return data
        except Exception as exc:
            if attempt == max_retries:
                print(f"  ⚠  LLM batch enrichment failed for table '{table}': {exc}")
                return {}
            time.sleep(1)

    return {}


# =============================================================================
# PER-TABLE WORKER  (runs in its own thread with its own connection)
# =============================================================================

def _process_table(
    full_table: str,
    col_rows: list[tuple],
    hostname: str,
    sql_warehouse_id: str,
    access_token: str,
    client: OpenAI,
    model_endpoint: str,
) -> tuple[str, dict]:
    conn = _get_connection(hostname, sql_warehouse_id, access_token)

    try:
        row_count    = _fetch_row_count(conn, full_table)
        sample_data  = _fetch_sample_data(conn, full_table)
        columns_list = [r[0] for r in col_rows]
        stats_map    = _fetch_table_stats_batch(conn, full_table, columns_list)

        enriched  = []
        residuals = []

        for col_name, dtype, comment in col_rows:
            stats = stats_map.get(col_name, {"null_count": 0, "null_rate": 0.0, "distinct_count": 0})

            desc       = comment or _rule_description(col_name)
            needs_desc = desc is None

            role        = _rule_role(col_name, stats["distinct_count"], row_count, dtype)
            needs_role  = role is None
            role_source = "rule" if role else "llm"

            enriched.append({
                "col":        col_name,
                "dtype":      dtype,
                "desc":       desc,
                "role":       role,
                "role_src":   role_source,
                "stats":      stats,
                "needs_desc": needs_desc,
                "needs_role": needs_role,
            })

            if needs_desc or needs_role:
                residuals.append({
                    "col":        col_name,
                    "dtype":      dtype,
                    "needs_desc": needs_desc,
                    "needs_role": needs_role,
                    "stats":      stats,
                })

        table_short = full_table.split(".")[-1]
        llm_results = _batch_llm_enrich(table_short, residuals, client, model_endpoint)

        final_columns = []
        for e in enriched:
            col_name = e["col"]
            llm_data = llm_results.get(col_name, {})

            desc        = e["desc"]     or llm_data.get("description", col_name)
            role        = e["role"]     or llm_data.get("role",        "dimension")
            role_source = e["role_src"] if e["role"] else "llm"

            final_columns.append({
                "column":      col_name,
                "type":        e["dtype"],
                "description": desc,
                "role":        role,
                "role_source": role_source,
                "stats":       e["stats"],
            })

        table_meta = {
            "row_count":   row_count,
            "sample_data": sample_data,
            "columns":     final_columns,
        }

    finally:
        conn.close()

    return full_table, table_meta


# =============================================================================
# PUBLIC API — fetch_metadata_grouped
# Called by the orchestrator's _gold_fetch_silver_metadata()
# =============================================================================

def fetch_metadata_grouped(
    catalog: str,
    schema: str,
    *,
    hostname: str,
    sql_warehouse_id: str,
    access_token: str,
    model_endpoint: str,
    llm_host: str = "",
    llm_token: str = "",
    max_workers: int = 5,
    silver_table: str | None = None,      # ← NEW
) -> dict:
    """
    Fetch and enrich column-level metadata for all tables in catalog.schema.
    Uses batched SQL stats + batched LLM enrichment + parallel table processing.

    Called by the orchestrator as:
        mod.fetch_metadata_grouped(
            silver_catalog, silver_schema,
            hostname=..., sql_warehouse_id=...,
            access_token=..., model_endpoint=...,
        )
    """
    # Sanitise and validate
    hostname = _sanitise_hostname(hostname)
    if llm_host:
        llm_host = _sanitise_hostname(llm_host)

    _validate_credentials(hostname, sql_warehouse_id, access_token, model_endpoint)

    client = _make_llm_client(
        hostname, access_token,
        llm_host  = llm_host,
        llm_token = llm_token,
    )

    # ── Step 1: fetch column definitions ────────────────────────────────────
    conn = _get_connection(hostname, sql_warehouse_id, access_token)
    try:
        cursor = conn.cursor()

        # Build optional table filter
        table_filter = f"AND table_name = '{silver_table}'" if silver_table else ""

        cursor.execute(
            f"SELECT table_name, column_name, data_type, comment "
            f"FROM `{catalog}`.information_schema.columns "
            f"WHERE table_schema = '{schema}' "
            f"AND table_name != 'silver_taxonomy_lookup' "
            f"{table_filter} "
            f"ORDER BY table_name, ordinal_position"
        )
        all_rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    # ── Step 2: group by table ───────────────────────────────────────────────
    tables_map: dict[str, list] = {}
    for table, col, dtype, comment in all_rows:
        full_table = f"{catalog}.{schema}.{table}"
        tables_map.setdefault(full_table, []).append((col, dtype, comment))

    total_tables  = len(tables_map)
    total_columns = sum(len(v) for v in tables_map.values())
    print(f"\n --- Extracting Metadata using Parallel Processing --- ")
    print(f"     Tables: {total_tables}   |   Columns: {total_columns}")

    # ── Step 3: process all tables in parallel ───────────────────────────────
    metadata:  dict = {}
    completed: int  = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                _process_table,
                full_table, col_rows,
                hostname, sql_warehouse_id, access_token,
                client, model_endpoint,
            ): full_table
            for full_table, col_rows in tables_map.items()
        }

        for future in as_completed(futures):
            full_table = futures[future]
            try:
                name, table_meta = future.result()
                metadata[name]   = table_meta
                completed += 1
                short = full_table.split(".")[-1]
                ncols = len(table_meta["columns"])
                print(f"  ✅  [{completed}/{total_tables}]  {short}  ({ncols} columns)")
            except Exception as exc:
                completed += 1
                print(f"  ❌  [{completed}/{total_tables}]  {full_table}  — ERROR: {exc}")

    return metadata


# =============================================================================
# LEGACY HELPERS  (used by build_metadata — kept for backward compatibility)
# =============================================================================

def get_row_count(cursor, table: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    return cursor.fetchone()[0]


def get_sample_data(cursor, table: str) -> list:
    cursor.execute(f"SELECT * FROM {table} LIMIT 3")
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def get_column_stats(cursor, table: str, column: str) -> dict:
    cursor.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table}")
    return {"distinct_count": cursor.fetchone()[0]}


def detect_primary_key(columns: list, row_count: int):
    if row_count > 0:
        for col in columns:
            if col["stats"]["distinct_count"] == row_count:
                return col["column"]
    for col in columns:
        if col["column"].endswith("_id"):
            return col["column"]
    return columns[0]["column"]


def assign_roles(columns: list, table_name: str, row_count: int) -> list:
    pk = detect_primary_key(columns, row_count)
    for col in columns:
        name  = col["column"].lower()
        dtype = col["type"].lower()

        if col["column"] == pk:
            col["role"] = "primary_key"
            continue
        if name.endswith("_id"):
            col["role"] = "foreign_key"
            continue
        if name in ["year", "month", "day", "week"]:
            col["role"] = "dimension"
            continue
        if dtype == "date":
            col["role"] = "dimension"
            continue
        if dtype in ["int", "long", "double", "float", "decimal"]:
            if any(x in name for x in ["amount", "sales", "price", "qty", "total"]):
                col["role"] = "measure"
                continue
        col["role"] = "dimension"
    return columns


def classify_table(columns: list) -> str:
    has_measure = any(c["role"] == "measure"     for c in columns)
    has_fk      = any(c["role"] == "foreign_key" for c in columns)
    if has_measure and has_fk:
        return "fact"
    return "dimension"


def smart_description(col_name: str, role: str, table_type: str) -> str | None:
    col = col_name.lower()

    if role == "primary_key":
        return f"Unique identifier for each {table_type} record"
    if role == "foreign_key":
        return f"Reference to related {col.replace('_id', '')}"
    if role == "measure":
        if "sales" in col or "amount" in col:
            return "Total monetary value"
        if "qty" in col:
            return "Total quantity"
        if "price" in col:
            return "Unit price value"
        return "Numeric business metric"
    if col.endswith("_date"):
        return f"Date when {col.replace('_date', '')} occurred"
    if col == "year":
        return "Calendar year"
    if col == "month":
        return "Calendar month"
    if role == "dimension":
        if "name" in col:
            return "Name of the entity"
        if "status" in col:
            return "Current status value"
        if "region" in col or "territory" in col:
            return "Geographical classification"
        if "category" in col or "group" in col:
            return "Categorization label"
        return "Descriptive attribute"
    return None


def llm_description(column: str, dtype: str, table: str) -> str:
    key = f"{table}.{column}"
    if key in description_cache:
        return description_cache[key]

    prompt = f"""You are a senior data engineer.
Write a clean business-friendly column description.
RULES:
- Max 6 words
- No punctuation
- No repetition

Column: {column}
Type: {dtype}
Table: {table}
"""
    try:
        res  = _get_client().chat.completions.create(
            model    = _MODEL_NAME,
            messages = [{"role": "user", "content": prompt}],
            temperature = 0.1,
        )
        desc = res.choices[0].message.content.strip()
        desc = desc.replace(".", "")
        desc = " ".join(desc.split()[:6])
    except Exception:
        desc = "Business attribute"

    description_cache[key] = desc
    return desc


# =============================================================================
# BUILD METADATA  (original sequential approach — kept for direct use)
# =============================================================================

def build_metadata(catalog: str, schema: str) -> dict:
    """
    Sequential metadata builder using stored module-level credentials.
    Use fetch_metadata_grouped() for the faster parallel version.
    """ 
    conn   = get_connection()
    cursor = conn.cursor()
    metadata: dict = {}

    try:
        cursor.execute(
            f"SELECT table_name "
            f"FROM {catalog}.information_schema.tables "
            f"WHERE table_schema = '{schema}' "
            f"AND table_name != 'silver_taxonomy_lookup'"
        )
        tables = [r[0] for r in cursor.fetchall()]

        for table in tables:
            full_table = f"{catalog}.{schema}.{table}"
            print(f"Processing {full_table}")

            row_count   = get_row_count(cursor, full_table)
            sample_data = get_sample_data(cursor, full_table)

            cursor.execute(f"""
                SELECT column_name, data_type
                FROM {catalog}.information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name   = '{table}'
            """)
            cols_raw = cursor.fetchall()

            columns = []
            for col_name, dtype in cols_raw:
                stats = get_column_stats(cursor, full_table, col_name)
                columns.append({"column": col_name, "type": dtype, "stats": stats})

            columns    = assign_roles(columns, table, row_count)
            table_type = classify_table(columns)

            for col in columns:
                desc = smart_description(col["column"], col["role"], table_type)
                if not desc:
                    desc = llm_description(col["column"], col["type"], table)
                col["description"] = desc

            metadata[full_table] = {
                "table_type":  table_type,
                "row_count":   row_count,
                "sample_data": sample_data,
                "columns":     columns,
            }

        return metadata

    finally:
        cursor.close()
        conn.close()