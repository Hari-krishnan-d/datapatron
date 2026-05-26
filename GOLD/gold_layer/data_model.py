"""
data_model.py — Gold Layer Star-Schema Data Model
==================================================
UPDATED — v12

Key rules enforced
──────────────────
• No extra columns are ever added during data modeling — the model uses ONLY
  columns that exist in the Silver metadata. The LLM prompt explicitly forbids
  inventing columns.

• normalize_model() injects a <entity>_needs_bk placeholder with role
  primary_key_temp when a dimension has no proper _id PK.
  The orchestrator's DM-4 stage detects these and offers the 3-scenario
  business-key workflow.

• Primary keys must end with _id and values follow the pattern:
  <prefix>_0001 (e.g. ord_0001, cust_0001).

• _display_model() uses Rich tables for clean, structured terminal output.
  All debug/verbose prints are suppressed or routed through _quiet_print().

• deploy_to_databricks() uses a single persistent connection (FIX-10).

• _entity_base() uses conservative plural-strip (FIX-11).

• normalize_model FK-remap loop uses explicit shallow copies (FIX-12).

• FIX-13 (v6): Fact tables NEVER have a primary_key column.
  - LLM prompt updated to explicitly forbid PK in fact table.
  - normalize_model() strips any primary_key role from fact columns,
    converts it to foreign_key, and deduplicates by column name.
  - Rewrite system prompt updated with same rule.
  - deploy_to_databricks() no longer adds a PK constraint to the fact table.

• FIX-14 (v7): FK column name remapping now uses substring/prefix matching.
  - _match_fk_to_sk() adds a Pass 2 that catches cases like
    fk_base("transaction_id")="transaction" matching
    dim_base("dim_transaction_type")="transaction_type".
  - _remap_fk_col() replaces the inline FK block in normalize_model():
    when no dim matches AND the LLM-invented column name is not in Silver,
    it searches Silver _id columns by entity base and remaps to the real
    Silver column name, preventing invented names from reaching DDL.
  - normalize_model() now accepts silver_col_names: list[str] = None.

• FIX-16 (v8): Column coverage audit — no Silver column silently dropped.
  - _audit_column_coverage() diffs Silver columns vs placed Gold columns.
  - Date columns excluded using the raw Silver metadata column list, not
    the stripped Gold model (fixes the bug where the Gold model no longer
    contains date cols at audit time).
  - _build_coverage_retry_prompt() gives the LLM explicit per-column
    instructions, distinguishing degenerate dimensions (_id cols with no
    dim table) from measures/descriptive cols.
  - data_modeling_agent() accepts silver_col_names and silver_metadata
    params and runs the coverage audit after every successful LLM response.
  - _is_valid_model() updated to accept "degenerate_dimension" as a valid
    fact table role.
  - _ROLE_STYLE updated with [DD] badge for degenerate_dimension.

• FIX-17 (v10): The sole unmatched _id on the fact table is promoted to
  primary_key automatically — no column name is hardcoded.
  - _find_fact_pk() inspects the fully-built dim_sk_map and collects every
    _id column on the fact table whose name does not appear as a dim PK.
    If exactly one such column exists it is the natural fact-table row
    identifier (transaction_id, order_id, claim_id, booking_id, …) and is
    promoted to role "primary_key". When zero or multiple candidates are
    found the columns are left as degenerate_dimension (safe fallback).
  - normalize_model() calls _find_fact_pk() after deduplication and applies
    the promotion before building the final column list.
  - All primary_key roles in the fact table that DO NOT match the detected
    fact PK are still converted to foreign_key (FIX-13 preserved).
  - _is_valid_model() keeps "primary_key" in VALID_FACT_ROLES.
  - _suggest_placement() and _REWRITE_SYSTEM_PROMPT updated to describe the
    logic-driven rule rather than a hardcoded column name.

• FIX-18 (v11): Dimension PK name collision with the fact-table row identifier
  is now detected and resolved at normalize_model() time.
  - PROBLEM: If DIM_TRANSACTION used "transaction_id" as its primary_key,
    the same name as the fact table's row-level identifier, the result was:
      · _find_fact_pk() saw "transaction_id" in dim_sk_map and excluded it
        from PK promotion — leaving no natural fact PK.
      · The FK from fact → DIM_TRANSACTION was also named "transaction_id",
        identical to the fact PK, causing a DDL self-reference in Databricks.
      · Two tables had a column called "transaction_id" with the PK role,
        making the schema ambiguous and undeployable.
  - FIX: _resolve_dim_pk_collisions() runs inside normalize_model() after
    dim_sk_map is built but before the fact table is processed.

• FIX-18-v2 (v12): Over-application of FIX-18 resolved.
  - PROBLEM (v11 regression): FIX-18 was passing ALL _id columns on the fact
    table (including legitimate FKs like customer_id, loan_id, account_id)
    into fact_id_cols. This caused _resolve_dim_pk_collisions() to wrongly
    rename dim_customer.customer_id → customer_type_id, dim_loan.loan_id →
    loan_type_id, etc. — because those names also appeared on the fact table.
  - ROOT CAUSE 1: fact_id_candidates in normalize_model() included FK columns.
  - ROOT CAUSE 2: _resolve_dim_pk_collisions() lacked a guard to verify the
    dim's entity base actually matches the colliding column's entity base.
  - ROOT CAUSE 3: LLM prompts described FIX-18 too broadly, causing the LLM
    to pre-emptively rename dim PKs that had no actual collision.
  - FIX A: fact_id_candidates now only includes _id columns whose entity base
    has NO matching dimension in the raw model. FK candidates (customer_id →
    dim_customer exists) are excluded from the collision-check set entirely.
  - FIX B: _resolve_dim_pk_collisions() now verifies that the dim's entity
    base matches the colliding column's entity base before renaming. This
    prevents cross-entity false positives even if FIX A misses an edge case.
  - FIX C: LLM prompts (build_model_prompt + _REWRITE_SYSTEM_PROMPT) now
    explicitly state that FIX-18 ONLY applies when the dim entity name is the
    SAME as the fact row identifier entity — not to other dims. Examples of
    correctly unchanged PKs (customer_id, loan_id, location_id) are provided.
  - Result: customer_id, loan_id, account_id and all other Silver _id columns
    that have a matching dim table flow through completely unchanged.
"""

import json
import os
import datetime
import re
import logging
import requests
from openai import OpenAI
from dotenv import load_dotenv
from databricks import sql as dbsql
from rich.console import Console
from rich.table import Table
from rich import box as rich_box

load_dotenv()

MODEL_NAME = os.getenv("MODEL_NAME", "")

console = Console()
logger  = logging.getLogger("MCA.data_model")

# ── Lazy credential store ──────────────────────────────────────────────────────
_client: "OpenAI | None" = None
_db_hostname: str        = ""
_db_warehouse: str       = ""
_db_token: str           = ""

# ── Databricks LLM credentials (may differ from SQL warehouse creds) ───────────
_llm_host:     str = ""
_llm_token:    str = ""
_llm_endpoint: str = ""


# ── Credential helpers ─────────────────────────────────────────────────────────

def _sanitise_hostname(hostname: str) -> str:
    h = hostname.strip()
    for prefix in ("https://", "http://"):
        if h.startswith(prefix):
            h = h[len(prefix):]
    return h.rstrip("/")


def _validate_init_credentials(
    hostname: str,
    sql_warehouse_id: str,
    access_token: str,
    model_endpoint: str,
) -> None:
    errors = []

    if not hostname or hostname.strip() in ("", "YOUR_HOSTNAME"):
        errors.append(
            "hostname is empty or not set. "
            "Expected format: dbc-xxxxxxxx.cloud.databricks.com"
        )

    if not access_token or not access_token.startswith("dapi"):
        errors.append(
            "access_token looks invalid (must start with 'dapi'). "
            "Generate a new token: Databricks → User Settings → Developer → Access Tokens."
        )

    if not sql_warehouse_id or not sql_warehouse_id.startswith("/sql/"):
        errors.append(
            f"sql_warehouse_id '{sql_warehouse_id}' looks wrong. "
            "Expected format: /sql/1.0/warehouses/<warehouse-id>  "
            "(copy from Databricks → SQL Warehouses → Connection Details → HTTP Path)."
        )

    if not model_endpoint or model_endpoint.strip() in ("", "YOUR_MODEL"):
        errors.append(
            "model_endpoint is empty. "
            "Expected: exact serving endpoint name shown in Databricks → Serving "
            "(e.g. 'databricks-meta-llama-3-3-70b-instruct')."
        )

    if errors:
        msg = "\n".join(f"  ✗ {e}" for e in errors)
        raise ValueError(
            f"\n[data_model] init_data_model() credential validation failed:\n{msg}\n"
            "Fix the values above in main.py (or wherever you call init_data_model) and retry."
        )


def init_data_model(
    hostname: str,
    sql_warehouse_id: str,
    access_token: str,
    model_endpoint: str = "",
    llm_host: str = "",
    llm_token: str = "",
) -> None:
    global _client, _db_hostname, _db_warehouse, _db_token
    global _llm_host, _llm_token, _llm_endpoint, MODEL_NAME

    hostname       = _sanitise_hostname(hostname)
    resolved_model = model_endpoint or os.getenv("MODEL_NAME", "")

    resolved_llm_host  = _sanitise_hostname(llm_host) if llm_host else hostname
    resolved_llm_token = llm_token if llm_token else access_token

    _validate_init_credentials(hostname, sql_warehouse_id, access_token, resolved_model)

    _client = OpenAI(
        api_key  = resolved_llm_token,
        base_url = f"https://{resolved_llm_host}/serving-endpoints",
    )
    print(f"[data_model] ✅ Using Databricks LLM — endpoint: {resolved_model}")

    _db_hostname  = hostname
    _db_warehouse = sql_warehouse_id
    _db_token     = access_token
    _llm_host     = resolved_llm_host
    _llm_token    = resolved_llm_token
    _llm_endpoint = resolved_model
    MODEL_NAME    = resolved_model


def _get_client() -> OpenAI:
    if _client is None:
        raise RuntimeError(
            "\n[data_model] _get_client() called before initialisation.\n"
            "Add the following call in main.py BEFORE run_pipeline():\n\n"
            "    from gold_layer.data_model import init_data_model\n"
            "    init_data_model(\n"
            "        hostname         = DATABRICKS_HOST,\n"
            "        sql_warehouse_id = SQL_WAREHOUSE_ID,\n"
            "        access_token     = DATABRICKS_TOKEN,\n"
            "        model_endpoint   = MODEL_NAME,\n"
            "    )\n"
        )
    return _client


# ──────────────────────────────────────────────────────────────────────────────
# Custom JSON Encoder
# ──────────────────────────────────────────────────────────────────────────────
class SafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, datetime.time):
            return obj.isoformat()
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


# ──────────────────────────────────────────────────────────────────────────────
# Metadata → schema
# ──────────────────────────────────────────────────────────────────────────────
def metadata_to_schema(metadata):
    schema = {}
    for table, details in metadata.items():
        schema[table] = []
        for col in details["columns"]:
            schema[table].append({
                "column": col["column"],
                "type":   col["type"],
                "role":   col.get("role", "unknown"),
            })
    return schema


# ──────────────────────────────────────────────────────────────────────────────
# LLM helpers
# ──────────────────────────────────────────────────────────────────────────────
def build_schema_prompt(schema):
    return f"""
You are a data expert.

Classify columns into:
- measures (numbers like sales, amount)
- dimensions (names, category, region)
- primary key

Return ONLY JSON. Do not use markdown.

Schema:
Columns: {schema['columns']}
Data Types: {schema['dtypes']}
Sample: {schema['sample_data']}
"""


def call_llm(prompt):
    try:
        response = _get_client().chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "You are a senior data engineer."},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.2,
            max_tokens=4000,
        )
    except Exception as exc:
        raise RuntimeError(
            f"[data_model] LLM call failed.\n"
            f"  Endpoint : {_llm_host}/serving-endpoints\n"
            f"  Model    : {MODEL_NAME}\n"
            f"  Error    : {exc}"
        ) from exc

    choices = getattr(response, "choices", None)
    if not choices:
        raise RuntimeError(
            f"[data_model] LLM returned no choices. Full response: {response}"
        )

    content = choices[0].message.content

    if isinstance(content, list):
        content = " ".join(
            b.get("text", "") for b in content
            if isinstance(b, dict) and b.get("type") == "text"
        ).strip()

    if not content or not content.strip():
        raise RuntimeError("[data_model] LLM returned an empty response.")

    return content


def clean_llm_output(result: str) -> str:
    result = result.strip()

    fence_start = result.find("```")
    if fence_start != -1:
        after_fence = result[fence_start + 3:]
        newline = after_fence.find("\n")
        if newline != -1:
            after_fence = after_fence[newline + 1:]
        fence_end = after_fence.rfind("```")
        if fence_end != -1:
            after_fence = after_fence[:fence_end]
        candidate = after_fence.strip()
        if candidate.startswith("{") or candidate.startswith("["):
            return candidate

    for start_char in ("{", "["):
        idx = result.find(start_char)
        if idx != -1:
            candidate = result[idx:].strip()
            last_brace = max(candidate.rfind("}"), candidate.rfind("]"))
            if last_brace != -1:
                candidate = candidate[: last_brace + 1].strip()
            return candidate

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Schema understanding agent
# ──────────────────────────────────────────────────────────────────────────────
def analyze_schema(data):
    all_columns  = []
    all_dtypes   = {}
    sample_data  = {}

    for table, details in data.items():
        table_short = table.split(".")[-1]
        for col in details.get("columns", []):
            col_name = col.get("column", "")
            if col_name:
                all_columns.append(col_name)
                all_dtypes[col_name] = col.get("type", "STRING")
        if details.get("sample_data"):
            sample_data[table_short] = details["sample_data"][:2]

    return {
        "columns":     all_columns,
        "dtypes":      all_dtypes,
        "sample_data": sample_data,
    }


def data_understanding_agent(data):
    schema = analyze_schema(data)
    prompt = build_schema_prompt(schema)
    result = call_llm(prompt)
    try:
        cleaned = clean_llm_output(result)
        return json.loads(cleaned)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Data model prompt
# FIX-13: Fact tables NEVER have a primary_key. Explicit rule added.
# FIX-16: Degenerate dimension rule added for _id cols with no dim table.
# FIX-18-v2: Collision rule now scoped ONLY to dims whose entity name matches
#             the fact row identifier entity — not to all dims.
# ──────────────────────────────────────────────────────────────────────────────
def build_model_prompt(schema_output):
    return f"""
        You are a senior data engineer designing a star schema data model.

        Given this schema classification:
        {json.dumps(schema_output, indent=2, cls=SafeJSONEncoder)}

        Your task is to produce a star schema with:
        - 1 Fact table  (transactions / events / performance records)
        - Multiple Dimension tables  (descriptive context)

        CRITICAL COLUMN RULES:
        1. You MUST use ONLY the columns listed in the schema above.
        2. Do NOT invent, add, or create any new columns that are not in the schema.
        3. Every column from the Silver schema MUST appear in exactly one table
        (either the fact table or a dimension table). No column should be left out.
        4. Distribute ALL columns across fact and dimension tables — nothing is dropped.

        STRICT OUTPUT RULES:
        1. Return ONLY valid JSON. No markdown, no explanation.
        2. Use EXACTLY this structure:

        {{
        "fact_table": {{
            "name": "fact_<topic>",
            "columns": [
            {{"column": "<dim1>_id", "type": "STRING", "role": "foreign_key"}},
            {{"column": "<dim2>_id", "type": "STRING", "role": "foreign_key"}},
            {{"column": "<measure>", "type": "DOUBLE",  "role": "measure"}},
            {{"column": "<row_identifier>_id", "type": "STRING", "role": "degenerate_dimension"}}
            ]
        }},
        "dimensions": [...]
        }}

        MODELING RULES:
        1. Fact table MUST contain:
        - NO primary key column. Fact tables NEVER have a primary_key. This is mandatory.
        - At least 1 foreign key per dimension (matching the dim's primary key name exactly)
        - All numeric measure columns (counts, amounts, scores, durations)
        - Every _id column in the fact table MUST have role "foreign_key", never "primary_key"

        2. DEGENERATE DIMENSIONS vs PROPER DIMENSIONS (critical — read carefully):

        An _id column on the fact table should become a FOREIGN KEY to a proper
        dimension IF there are any descriptive/attribute columns in the Silver schema
        that naturally belong to that entity.

        Examples:
            - loan_id + loan_status exist → create dim_loan(loan_id PK, loan_status descriptive)
                                            and place loan_id as foreign_key on the fact table
            - order_id with no order attributes → degenerate_dimension on fact table
            - invoice_id with no invoice attributes → degenerate_dimension on fact table

        RULE: If an _id column has 1 or more related attribute columns (same entity prefix
        or domain relationship), group them into a proper dimension.
        Only use role "degenerate_dimension" when an _id column truly has NO associated
        attributes anywhere in the Silver schema.

        The fact table MUST have EXACTLY ONE row-level identifier which becomes the
        primary_key (detected automatically by the pipeline). All other _id columns
        must either be foreign_keys (pointing to a dim) or degenerate_dimensions
        (truly standalone, no dim possible).

        3. Each Dimension MUST contain:
        - A primary key column ending in _id that EXISTS in the Silver schema
        - Only descriptive/categorical columns AND person-level attributes
          (names, labels, categories, ages, demographics — e.g. age belongs in
          dim_customer, NOT the fact table, even though it is numeric)
        - NO transactional measures, NO foreign keys

        4. DIMENSION PRIMARY KEY NAMING — CRITICAL:
        This rule applies ONLY when a dimension's entity name is the SAME as the
        fact table's row-level identifier entity name.

        The fact row identifier is the single _id column that has NO associated
        dimension table (truly standalone). For example, if "transaction_id" is
        the fact row identifier AND a dim_transaction exists to categorise
        transaction types, those two entities conflict — rename the dim PK.

        Example — "transaction_id" is the fact row identifier AND dim_transaction exists:
            ✗ WRONG:  dim_transaction primary key = "transaction_id"   ← same name!
            ✓ CORRECT: dim_transaction primary key = "transaction_type_id"
                        (use a more specific name that reflects what the dim classifies)

        This rule does NOT affect any other dimensions:
            ✓ dim_customer primary key = "customer_id"    ← correct, keep as-is
            ✓ dim_loan primary key = "loan_id"            ← correct, keep as-is
            ✓ dim_location primary key = "location_id"   ← correct, keep as-is
            ✓ dim_account primary key = "account_id"     ← correct, keep as-is
            ✓ dim_payment primary key = "payment_id"     ← correct, keep as-is

        Only the dimension that shares its entity name with the fact row identifier
        needs a more specific PK name. All other dimensions use their natural _id
        column from the Silver schema unchanged.

        5. Create a separate dimension for EACH of these if columns exist:
        - Person/Customer/Player  → dim_customer / dim_athlete / dim_player
        - Product/Item            → dim_product
        - Event/Activity          → dim_event
        - Location/Country/City   → dim_location
        - Organization/Team       → dim_team

        6. DATE DIMENSION RULES (CRITICAL — read carefully):
        a. If ANY date or timestamp column exists in the Silver schema, you MUST create
            EXACTLY ONE date dimension called "dim_date". No other date dimension is allowed.
        b. dim_date MUST have these columns ONLY (do NOT include any Silver date columns here):
                - date_id   : INT, primary_key   (YYYYMMDD integer, e.g. 20240115)
                - year      : INT, descriptive
                - month     : INT, descriptive
                - month_name: STRING, descriptive
                - quarter   : INT, descriptive
                - week      : INT, descriptive
                - day       : INT, descriptive
                - day_name  : STRING, descriptive
                - is_weekend: BOOLEAN, descriptive
        c. The fact table MUST have "date_id" as a foreign key (INT type) pointing to dim_date.
        d. ALL original date/timestamp columns from Silver MUST be EXCLUDED from both the
            fact table and all other dimension tables. They do NOT appear anywhere in the model.
            date_id in the fact table is the only date reference.

        7. DO NOT leave dimensions as an empty array.
        8. DO NOT return a fact table with only foreign keys and no measures.
        9. Every foreign key in the fact table MUST match a primary key in a dimension.
        10. Every non-date Silver column must appear somewhere in the model. Do not discard any column.

        Schema to model:
        {json.dumps(schema_output, indent=2, cls=SafeJSONEncoder)}
        """


def _build_retry_prompt(schema_output, bad_output):
    return f"""
        Your previous response was INVALID. Here is what you returned:
        {json.dumps(bad_output, indent=2, cls=SafeJSONEncoder)}

        Problems detected:
        - dimensions array is empty OR fact table has no foreign keys OR columns are missing
        - OR fact table incorrectly contains a primary_key column (this is NEVER allowed)
        - OR a dimension primary key has the same name as the fact table's row identifier _id
          (this only applies when the dim entity name matches the fact row identifier entity)

        You MUST fix this. Return a complete star schema for:
        {json.dumps(schema_output, indent=2, cls=SafeJSONEncoder)}

        Rules:
        1. dimensions must NOT be empty
        2. fact table must have foreign keys linking to each dimension
        3. ALL Silver columns must appear in the model (no column left out)
        4. Do NOT add columns that don't exist in the Silver schema
        5. Return ONLY valid JSON, no markdown
        6. FACT TABLE MUST NOT have a primary_key column — every _id in the fact table is
           foreign_key (pointing to a dim) OR degenerate_dimension (no matching dim exists).
           The pipeline auto-promotes the sole unmatched _id to primary_key at runtime.
        7. A dimension's primary key MUST NOT share its name with the fact row identifier
           ONLY when the dim entity name matches the fact row identifier entity.
           All other dimension primary keys (customer_id, loan_id, account_id, location_id,
           etc.) must remain exactly as they appear in the Silver schema.
        """


# ──────────────────────────────────────────────────────────────────────────────
# FIX-16: Column coverage audit helpers
# ──────────────────────────────────────────────────────────────────────────────

def _get_all_modeled_columns(model: dict) -> set:
    """Collect every column name placed in the Gold model (fact + all dims)."""
    placed = set()
    for col in model.get("fact_table", {}).get("columns", []):
        if isinstance(col, dict):
            placed.add(col.get("column", ""))
    for dim in model.get("dimensions", []):
        for col in dim.get("columns", []):
            if isinstance(col, dict):
                placed.add(col.get("column", ""))
    placed.discard("")
    return placed


def _build_silver_date_col_set(silver_col_names: list, silver_metadata: dict = None) -> set:
    """
    Build the set of Silver date/timestamp column names that are intentionally
    excluded from the Gold model (absorbed into dim_date).

    Uses the RAW Silver metadata column type info — NOT the Gold model —
    because by audit time the Gold model has already stripped these columns.

    Falls back to name-pattern matching when metadata is not supplied.
    """
    _DATE_TYPES_RAW = {"date", "timestamp", "timestamp_ntz", "datetime"}

    date_cols: set = set()

    if silver_metadata:
        for _tbl, details in silver_metadata.items():
            for col in details.get("columns", []):
                col_name = col.get("column", "")
                col_type = col.get("type", "").lower().split("(")[0].strip()
                if col_type in _DATE_TYPES_RAW:
                    date_cols.add(col_name)
                elif _DATE_NAME_PATTERN.search(col_name):
                    date_cols.add(col_name)
    else:
        for col_name in (silver_col_names or []):
            if _DATE_NAME_PATTERN.search(col_name):
                date_cols.add(col_name)

    date_cols.discard("")
    return date_cols


def _audit_column_coverage(
    model            : dict,
    silver_col_names : list,
    silver_metadata  : dict = None,
) -> list:
    """
    [FIX-16] Compare every Silver column against what's placed in the Gold model.

    Returns a list of unplaced column dicts:
        [{"column": "transaction_id", "suggestion": "fact_table (primary_key)..."}, ...]

    Intentional exclusions (never flagged as unplaced):
    - Date/timestamp columns → absorbed into dim_date via date_id FK.
      Identified using RAW Silver metadata types (not the stripped Gold model).
    - dim_date's own generated columns (date_id, year, month, …) are NOT in
      silver_col_names so they are never checked.
    """
    if not silver_col_names:
        return []

    placed = _get_all_modeled_columns(model)

    silver_date_cols = _build_silver_date_col_set(silver_col_names, silver_metadata)

    unplaced = []
    for col_name in silver_col_names:
        if col_name in placed:
            continue
        if col_name in silver_date_cols:
            continue
        if col_name == "":
            continue

        suggestion = _suggest_placement(col_name, model, silver_metadata)
        unplaced.append({
            "column":     col_name,
            "suggestion": suggestion,
        })

    return unplaced


def _suggest_placement(
    col_name       : str,
    model          : dict,
    silver_metadata: dict = None,
) -> str:
    col_lower = col_name.lower()

    if col_lower.endswith("_id"):
        entity_prefix = col_lower.replace("_id", "")
        sibling_attrs = []
        if silver_metadata:
            for _tbl, details in silver_metadata.items():
                for col in details.get("columns", []):
                    sib = col.get("column", "").lower()
                    if sib != col_lower and entity_prefix in sib and not sib.endswith("_id"):
                        sibling_attrs.append(col.get("column"))

        if sibling_attrs:
            return (
                f'Create dim_{entity_prefix} with {col_name} as primary_key and '
                f'{sibling_attrs} as descriptive columns. '
                f'Place {col_name} as foreign_key on the fact table.'
            )
        return (
            'fact_table (degenerate_dimension) — row-level identifier with no '
            'associated attributes. normalize_model will auto-promote the sole '
            'unmatched _id to primary_key if it is the only one.'
        )

    measure_hints = (
        "amount", "balance", "score", "count", "total",
        "price", "qty", "quantity", "revenue",
        "duration", "rate", "weight",
    )
    if any(h in col_lower for h in measure_hints):
        return 'fact_table (measure) — numeric metric. Add with role "measure".'

    for dim in model.get("dimensions", []):
        dim_base = _entity_base(dim.get("name", ""))
        if dim_base and dim_base in col_lower:
            return f'{dim["name"]} (descriptive) — add with role "descriptive".'

    return (
        'fact_table (degenerate_dimension) or relevant dimension — '
        'determine placement from business context.'
    )


def _build_coverage_retry_prompt(
    schema_output  : dict,
    bad_output     : dict,
    unplaced_cols  : list,
    silver_metadata: dict = None,
) -> str:
    """
    [FIX-16] Retry prompt when Silver columns are missing from the Gold model.
    """
    id_cols    = [c for c in unplaced_cols if c.endswith("_id")]
    other_cols = [c for c in unplaced_cols if not c.endswith("_id")]

    dim_candidates   = []
    true_degenerates = []

    all_silver_cols = []
    if silver_metadata:
        for _tbl, details in silver_metadata.items():
            all_silver_cols += [c.get("column", "") for c in details.get("columns", [])]

    for col_name in id_cols:
        prefix = col_name.lower().replace("_id", "")
        siblings = [
            c for c in all_silver_cols
            if c.lower() != col_name.lower()
            and prefix in c.lower()
            and not c.lower().endswith("_id")
        ]
        if siblings:
            dim_candidates.append((col_name, siblings))
        else:
            true_degenerates.append(col_name)

    dim_note = ""
    if dim_candidates:
        dim_note = "\nDIM CANDIDATES — CREATE THESE NEW DIMENSIONS:\n"
        for id_col, siblings in dim_candidates:
            entity = id_col.replace("_id", "")
            dim_note += (
                f"  • Create dim_{entity} with:\n"
                f"      primary_key : {id_col}\n"
                f"      descriptive : {siblings}\n"
                f"    Place {id_col} as foreign_key on the fact table.\n"
                f"    Do NOT mark it degenerate_dimension.\n\n"
            )

    degen_note = ""
    if true_degenerates:
        examples = "\n".join(
            f'    {{"column": "{c}", "type": "STRING", "role": "degenerate_dimension"}}'
            for c in true_degenerates
        )
        degen_note = f"""
            TRUE DEGENERATE DIMENSIONS — ADD THESE TO THE FACT TABLE:
            Columns: {json.dumps(true_degenerates)}

            These _id columns have NO associated attribute columns in Silver.
            They are row-level identifiers that live directly on the fact table.
            normalize_model() will automatically promote the sole unmatched _id to
            primary_key at runtime — you do not need to set the role manually.

            Rules:
            - Role MUST be "degenerate_dimension"
            - Do NOT create a new dim table for them
            - Do NOT confuse them with existing FK columns
            - Do NOT omit them

            Add exactly these entries to fact_table.columns:
            {examples}
            """

    other_note = ""
    if other_cols:
        other_note = f"""
        OTHER UNPLACED COLUMNS — DECIDE PLACEMENT:
        Columns: {json.dumps(other_cols)}

        For each column:
        - If numeric (amount, balance, score, count, rate) → fact_table, role "measure"
        - If descriptive of a known entity → add to the relevant dim, role "descriptive"
        """

    return f"""
        Your previous model is INCOMPLETE. The following Silver columns are missing
        entirely — they appear in NEITHER the fact table NOR any dimension:

        UNPLACED: {json.dumps(unplaced_cols)}
        {degen_note}{other_note}
        RULES:
        1. Do NOT remove or rename any existing columns — only ADD the missing ones.
        2. Every Silver column must appear exactly once somewhere in the model.
        3. Return ONLY the corrected full model as valid JSON. No markdown.
        4. All original modeling rules still apply.
        5. A dimension's primary key must NOT share its name with the fact table's
           row-level identifier _id column ONLY when the dim entity name is the SAME
           as the fact row identifier entity. All other dim PKs stay unchanged.
           (e.g. customer_id, loan_id, account_id must remain as-is.)

        Your previous (incomplete) model:
        {json.dumps(bad_output, indent=2, cls=SafeJSONEncoder)}

        Original Silver schema:
        {json.dumps(schema_output, indent=2, cls=SafeJSONEncoder)}
        """


# ──────────────────────────────────────────────────────────────────────────────
# Data modeling agent
# FIX-16: Accepts silver_col_names + silver_metadata; runs coverage audit.
# ──────────────────────────────────────────────────────────────────────────────
def data_modeling_agent(
    schema_output    : dict,
    silver_col_names : list = None,
    silver_metadata  : dict = None,
) -> dict | None:
    """
    Generate a star-schema Gold model from schema_output via LLM.

    Parameters
    ----------
    schema_output    : dict  — output of data_understanding_agent()
    silver_col_names : list  — every column name in the Silver source table(s).
                               When supplied, the coverage audit runs after every
                               successful LLM response and forces a retry if any
                               Silver column is missing from the Gold model.
    silver_metadata  : dict  — raw Silver metadata {table: {columns: [...]}}.
                               Used to identify date columns by type (not just name)
                               so they are correctly excluded from the audit.
                               Optional but strongly recommended.
    """
    prompt      = build_model_prompt(schema_output)
    MAX_RETRIES = 3
    last_error  = "Unknown error"

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"  [data_model] LLM attempt {attempt}/{MAX_RETRIES} ...")
        try:
            result  = call_llm(prompt)
            print(f"  [data_model] Raw response (first 200 chars): {repr(result[:200])}")
            cleaned = clean_llm_output(result)
            print(f"  [data_model] Cleaned (first 200 chars): {repr(cleaned[:200])}")
            output  = json.loads(cleaned)

            if not _is_valid_model(output):
                last_error = f"Model validation failed (attempt {attempt}): empty dimensions or missing FK"
                print(f"  WARNING: {last_error} - retrying ...")
                prompt = _build_retry_prompt(schema_output, output)
                continue

            # ── [FIX-16] Column coverage audit ────────────────────────────────
            if silver_col_names:
                unplaced = _audit_column_coverage(
                    model            = output,
                    silver_col_names = silver_col_names,
                    silver_metadata  = silver_metadata,
                )
                if unplaced:
                    print(
                        f"\n  ⚠  [COVERAGE AUDIT] {len(unplaced)} Silver column(s) "
                        f"missing from Gold model — forcing retry:"
                    )
                    for u in unplaced:
                        print(f"       • {u['column']:<32} → {u['suggestion']}")
                    print()

                    unplaced_names = [u["column"] for u in unplaced]
                    prompt = _build_coverage_retry_prompt(
                        schema_output, output, unplaced_names,
                        silver_metadata=silver_metadata,
                    )
                    last_error = f"Unplaced columns: {unplaced_names}"
                    continue

            return output

        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            print(f"  ERROR on attempt {attempt}: {last_error}")

    print(f"  All {MAX_RETRIES} modeling attempts failed. Last error: {last_error}")
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Model validator
# FIX-16: "degenerate_dimension" added as a valid fact table role.
# FIX-17: "primary_key" added as a valid fact table role.
# ──────────────────────────────────────────────────────────────────────────────
def _is_valid_model(model):
    fact_cols = model.get("fact_table", {}).get("columns", [])
    dims      = model.get("dimensions", [])

    VALID_FACT_ROLES = {"foreign_key", "measure", "degenerate_dimension", "primary_key"}

    has_real_fact   = len(fact_cols) > 1
    has_dimensions  = len(dims) > 0
    has_fk_in_fact  = any(c.get("role") == "foreign_key" for c in fact_cols)
    all_roles_valid = all(
        c.get("role") in VALID_FACT_ROLES
        for c in fact_cols
        if isinstance(c, dict)
    )
    return has_real_fact and has_dimensions and has_fk_in_fact and all_roles_valid


# ──────────────────────────────────────────────────────────────────────────────
# Rich table printer
# ──────────────────────────────────────────────────────────────────────────────
_ROLE_STYLE: dict = {
    "primary_key":          ("[PK]",  "bold green"),
    "primary_key_temp":     ("[PKt]", "bold yellow"),
    "foreign_key":          ("[FK]",  "bold blue"),
    "measure":              ("[M]",   "bold magenta"),
    "descriptive":          ("[D]",   "dim white"),
    "business_key":         ("[BK]",  "bold cyan"),
    "dimension":            ("[DIM]", "cyan"),
    "degenerate_dimension": ("[DD]",  "bold yellow"),
}

def _role_cell(role: str) -> str:
    label, colour = _ROLE_STYLE.get(role, (role, "white"))
    return f"[{colour}]{label}  {role}[/{colour}]"


def _print_table(title: str, columns: list, is_fact: bool = False):
    badge      = "[bold white on dark_green] FACT [/bold white on dark_green]" if is_fact \
                 else "[bold white on blue] DIM  [/bold white on blue]"
    bare_name  = title.split()[-1]
    full_title = f"{badge} [bold]{bare_name}[/bold]"

    t = Table(
        title=full_title,
        title_justify="left",
        box=rich_box.SIMPLE_HEAD,
        show_lines=False,
        expand=False,
        border_style="grey50",
        header_style="bold dim",
        title_style="",
    )
    t.add_column("Column", style="white",    min_width=30)
    t.add_column("Type",   style="cyan",     width=10)
    t.add_column("Role",   no_wrap=True,     width=30)

    for col in columns:
        col_name = col.get("column", col) if isinstance(col, dict) else col
        col_type = col.get("type",   "STRING")  if isinstance(col, dict) else "STRING"
        col_role = col.get("role",   "unknown") if isinstance(col, dict) else "unknown"
        t.add_row(col_name, col_type, _role_cell(col_role))

    console.print(t)


# ──────────────────────────────────────────────────────────────────────────────
# Model display helper
# ──────────────────────────────────────────────────────────────────────────────
def _display_model(model: dict, header: str = "Data Model") -> None:
    console.rule(f"[bold cyan]{header}[/bold cyan]")
    fact = model.get("fact_table", {})
    if fact:
        _print_table(
            title=f"[FACT TABLE]  {fact.get('name', '')}",
            columns=fact.get("columns", []),
            is_fact=True,
        )
    for dim in model.get("dimensions", []):
        _print_table(
            title=f"[DIMENSION]   {dim.get('name', '')}",
            columns=dim.get("columns", []),
            is_fact=False,
        )
    console.rule()


# ──────────────────────────────────────────────────────────────────────────────
# Rewrite chatbot system prompt
# FIX-18-v2: Collision rule scoped to same-entity dims only. Examples of
#             correctly unchanged PKs provided to prevent LLM over-application.
# ──────────────────────────────────────────────────────────────────────────────
_REWRITE_SYSTEM_PROMPT = """\
You are a senior data engineer helping a user refine a star-schema data model.

The user will describe changes they want to make to the current model.
You MUST return ONLY the full updated model as valid JSON — no markdown, no
explanations outside the JSON.

IMPORTANT RULES:
- Do NOT add columns that do not exist in the original Silver schema.
- All existing columns must remain somewhere in the model.
- FACT TABLES NEVER HAVE A PRIMARY KEY assigned by you. Use role
  "degenerate_dimension" for any _id column on the fact table that has no
  associated dimension table (e.g. transaction_id, order_id, claim_id).
  The pipeline will automatically detect and promote the sole unmatched _id
  to role "primary_key" at runtime — you do not need to set this yourself.
- Every other _id column in the fact table that DOES have a dimension must
  have role "foreign_key".
- Every dimension should have exactly 1 primary key ending in _id, UNLESS no
  such column exists in the original schema (in which case, do not invent one).
- Every foreign key in the fact table must match a dimension primary key.
- Primary key values follow the pattern: <prefix>_0001 (e.g. ord_0001).
- No column should appear more than once in the same table.

DIMENSION PRIMARY KEY NAMING — SCOPED RULE:
- A dimension's primary key must NOT share its name with the fact table's
  row-level identifier _id column ONLY when the dimension's entity name is
  the SAME as the fact row identifier's entity name.

- The fact row identifier is the single _id column with NO associated dimension
  table. All other _id columns that DO have a matching dimension are FK columns
  and are NOT affected by this rule.

- Example where the rule APPLIES:
    Fact row identifier = "transaction_id" (no standalone dim for it)
    dim_transaction exists to classify transaction types:
      ✗ WRONG:  dim_transaction.transaction_id   ← same name as fact row id!
      ✓ CORRECT: dim_transaction.transaction_type_id

- Examples where the rule does NOT apply (leave these unchanged):
      ✓ dim_customer.customer_id     ← FK on fact, rule does not apply
      ✓ dim_loan.loan_id             ← FK on fact, rule does not apply
      ✓ dim_account.account_id       ← FK on fact, rule does not apply
      ✓ dim_location.location_id     ← FK on fact, rule does not apply
      ✓ dim_payment.payment_id       ← FK on fact, rule does not apply

  Never rename a dimension PK unless its entity name directly conflicts with
  the fact row identifier entity name. Use the exact Silver column name for all
  other dimension primary keys.

The JSON must always follow this exact structure:
{
  "fact_table": {
    "name": "fact_<topic>",
    "columns": [
      {"column": "<dim1>_id",       "type": "STRING", "role": "foreign_key"},
      {"column": "<row_id>",        "type": "STRING", "role": "degenerate_dimension"},
      {"column": "<measure>",       "type": "DOUBLE",  "role": "measure"}
    ]
  },
  "dimensions": [
    {
      "name": "dim_<n>",
      "columns": [
        {"column": "<n>_id",        "type": "STRING", "role": "primary_key"},
        {"column": "<attribute>",   "type": "STRING", "role": "descriptive"}
      ]
    }
  ]
}

Rules you must never break:
- Use "degenerate_dimension" for any _id on the fact table with no dim.
  The pipeline promotes the sole unmatched one to primary_key automatically.
- Every _id with a matching dim = role "foreign_key".
- Every dimension should have 1 primary key (ending in _id) if it exists in Silver.
  Use the exact Silver column name — do not invent a new name unless this dim
  directly conflicts with the fact row identifier (see scoped rule above).
- Every foreign key in the fact table must match a primary key in a dimension.
- Never return an empty dimensions array.
- Never add markdown or explanatory text — JSON only.
- Never invent new columns not present in the Silver schema (except dim_date standard columns).
- dim_date is the ONLY allowed date dimension. Its columns are fixed:
  date_id (INT, primary_key), year (INT), month (INT), month_name (STRING),
  quarter (INT), week (INT), day (INT), day_name (STRING), is_weekend (BOOLEAN).
- Original Silver date/timestamp columns must NEVER appear in the fact table or any dimension.
- The fact table must reference dim_date via "date_id" (INT, foreign_key) only.
- No column may appear twice in the same table.
- Person-level numeric attributes such as "age", "tenure", "seniority"
  are DESCRIPTIVE and belong in the customer/person dimension, NOT in the
  fact table as measures.
- If an _id column has related attribute columns in the Silver schema, create a
  proper dimension and place the _id as a foreign_key on the fact table.
- Only use "degenerate_dimension" for _id columns with absolutely no associated
  attribute columns anywhere in the Silver schema.
- The fact table must have EXACTLY ONE row-level identifier.
  This is the sole column that normalize_model promotes to primary_key automatically.
"""


def _rewrite_chat_turn(
    conversation: list,
    user_message: str,
    current_model: dict,
) -> tuple:
    conversation.append({"role": "user", "content": user_message})

    response = _get_client().chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": _REWRITE_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    "Here is the current data model you must modify:\n"
                    + json.dumps(current_model, indent=2, cls=SafeJSONEncoder)
                ),
            },
            {"role": "assistant", "content": "Understood. I'll apply your changes and return only the updated JSON."},
        ] + conversation,
        temperature=0.2,
        max_tokens=4000,
    )

    raw = response.choices[0].message.content
    conversation.append({"role": "assistant", "content": raw})

    try:
        cleaned   = clean_llm_output(raw)
        new_model = json.loads(cleaned)
        if _is_valid_model(new_model):
            return new_model, conversation
        else:
            console.print("[yellow]⚠  The model returned is incomplete. Please describe your change again.[/yellow]")
            return None, conversation
    except Exception as e:
        console.print(f"[yellow]⚠  Could not parse LLM response: {e}[/yellow]")
        return None, conversation


def _run_rewrite_chatbot(model: dict) -> dict | None:
    console.print("\n")
    console.rule("[bold green]  REWRITE MODE  [/bold green]")
    console.print(
        "[dim]Describe any change you want — add/remove columns, rename tables,\n"
        "change types, add a dimension, etc. Type [bold]quit[/bold] to exit.\n[/dim]"
    )

    current_model = model
    conversation: list = []

    while True:
        _display_model(current_model, header="Current Model")

        try:
            user_input = input("\n[You] What would you like to change? → ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[yellow]Interrupted — exiting rewrite.[/yellow]")
            return None

        if user_input.lower() in ("quit", "exit", "q"):
            console.print("[yellow]Exiting rewrite. Pipeline stopped.[/yellow]")
            return None

        if not user_input:
            console.print("[dim]No input — please describe a change or type quit.[/dim]")
            continue

        new_model, conversation = _rewrite_chat_turn(conversation, user_input, current_model)

        if new_model is None:
            continue

        _display_model(new_model, header="Updated Model — Review Changes")

        console.print(
            "\n[bold]What would you like to do with this updated model?[/bold]\n"
            "  [cyan]1[/cyan] → Approve and deploy\n"
            "  [cyan]2[/cyan] → Keep making changes\n"
            "  [cyan]3[/cyan] → Quit without deploying\n"
        )
        try:
            sub_choice = input("Your choice (1/2/3): ").strip()
        except (EOFError, KeyboardInterrupt):
            return None

        if sub_choice == "1":
            console.print("[bold green]✓ Model approved. Proceeding to deployment…[/bold green]")
            return new_model
        elif sub_choice == "2":
            current_model = new_model
        elif sub_choice == "3":
            console.print("[yellow]Quitting. Pipeline stopped.[/yellow]")
            return None
        else:
            current_model = new_model


# ──────────────────────────────────────────────────────────────────────────────
# User approval
# ──────────────────────────────────────────────────────────────────────────────
def get_user_approval(model: dict) -> tuple:
    _display_model(model, header="PROPOSED DATA MODEL")

    try:
        choice = input("\n  Do you approve this model? (yes / no): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return "quit", None

    if choice in ("yes", "y"):
        return "approved", model

    console.print("\n[bold yellow]  Model rejected.[/bold yellow]")
    console.print(
        "\n  What would you like to do?\n"
        "  [cyan]1[/cyan] → Quit  (stop the pipeline)\n"
        "  [cyan]2[/cyan] → Rewrite  (open interactive chat to modify the model)\n"
    )

    try:
        action = input("  Your choice (1/2): ").strip()
    except (EOFError, KeyboardInterrupt):
        return "quit", None

    if action == "1":
        return "quit", None
    elif action == "2":
        final_model = _run_rewrite_chatbot(model)
        if final_model is None:
            return "quit", None
        return "approved", final_model
    else:
        return "quit", None


# ──────────────────────────────────────────────────────────────────────────────
# Dim business-key detection
# ──────────────────────────────────────────────────────────────────────────────
def _dim_has_business_key(columns: list, dim_name: str = "") -> bool:
    entity = dim_name.replace("dim_", "").lower()

    for col in columns:
        if not isinstance(col, dict):
            continue
        if col.get("role") != "primary_key":
            continue
        col_name = col.get("column", "").lower()
        if not col_name.endswith("_id"):
            continue
        if entity and entity not in col_name:
            continue
        return True
    return False


# ──────────────────────────────────────────────────────────────────────────────
# FIX-11: safe entity-base extractor
# ──────────────────────────────────────────────────────────────────────────────
def _entity_base(name: str) -> str:
    import re as _re
    n = name.lower()
    n = _re.sub(r"^(dim_|fact_)", "", n)
    n = _re.sub(r"(_sk|_id|_key|_type)$", "", n)
    _no_strip_endings = ("ss", "us", "is", "as", "os")
    if (n.endswith("s")
            and not any(n.endswith(e) for e in _no_strip_endings)
            and len(n) > 4):
        n = n[:-1]
    return n.strip("_")


# ──────────────────────────────────────────────────────────────────────────────
# dim_date helpers
# ──────────────────────────────────────────────────────────────────────────────
_DIM_DATE_COLUMNS = [
    {"column": "date_id",    "type": "INT",     "role": "primary_key"},
    {"column": "year",       "type": "INT",     "role": "descriptive"},
    {"column": "month",      "type": "INT",     "role": "descriptive"},
    {"column": "month_name", "type": "STRING",  "role": "descriptive"},
    {"column": "quarter",    "type": "INT",     "role": "descriptive"},
    {"column": "week",       "type": "INT",     "role": "descriptive"},
    {"column": "day",        "type": "INT",     "role": "descriptive"},
    {"column": "day_name",   "type": "STRING",  "role": "descriptive"},
    {"column": "is_weekend", "type": "BOOLEAN", "role": "descriptive"},
]

_DIM_DATE_FK = {"column": "date_id", "type": "INT", "role": "foreign_key"}

_DATE_TYPES = {"date", "timestamp", "timestamp_ntz", "datetime"}

import re as _re
_DATE_NAME_PATTERN = _re.compile(
    r"(^date$|_date$|_dt$|^dt$|_timestamp$|^timestamp$|_time$|^time$|_day$|^day$)",
    _re.IGNORECASE,
)


def _is_date_column(col: dict) -> bool:
    dtype = col.get("type", "").lower().split("(")[0].strip()
    if dtype in _DATE_TYPES:
        return True
    name = col.get("column", "").lower()
    if _DATE_NAME_PATTERN.search(name):
        return True
    return False


def _collect_silver_date_cols(raw_model: dict) -> set:
    date_cols: set = set()
    for col in raw_model.get("fact_table", {}).get("columns", []):
        if isinstance(col, dict) and _is_date_column(col):
            date_cols.add(col.get("column", ""))
    for dim in raw_model.get("dimensions", []):
        if dim.get("name") == "dim_date":
            continue
        for col in dim.get("columns", []):
            if isinstance(col, dict) and _is_date_column(col):
                date_cols.add(col.get("column", ""))
    date_cols.discard("")
    return date_cols


def _has_any_date_cols(raw_model: dict) -> bool:
    for col in raw_model.get("fact_table", {}).get("columns", []):
        if isinstance(col, dict) and _is_date_column(col):
            return True
    for dim in raw_model.get("dimensions", []):
        if dim.get("name") == "dim_date":
            return True
        for col in dim.get("columns", []):
            if isinstance(col, dict) and _is_date_column(col):
                return True
    return False


# ──────────────────────────────────────────────────────────────────────────────
# FIX-14: FK → SK/ID matcher with substring/prefix fallback
# ──────────────────────────────────────────────────────────────────────────────
def _match_fk_to_sk(fk_col_name: str, dim_sk_map: dict) -> str | None:
    fk_base = _entity_base(fk_col_name)

    # Pass 1 — exact match
    for dim_name, ref_col in dim_sk_map.items():
        dim_clean = dim_name.replace("dim_", "")
        dim_base  = _entity_base(dim_clean)
        if fk_base == dim_base:
            return ref_col

    # Pass 2 — prefix / substring match
    candidates = []
    for dim_name, ref_col in dim_sk_map.items():
        dim_clean = dim_name.replace("dim_", "")
        dim_base  = _entity_base(dim_clean)
        if fk_base in dim_base or dim_base in fk_base:
            candidates.append((len(dim_base), dim_name, ref_col))

    if candidates:
        candidates.sort(key=lambda x: x[0])
        _, matched_dim, ref_col = candidates[0]
        print(f"  [FK-REMAP] '{fk_col_name}' (base='{fk_base}') "
              f"matched '{matched_dim}' via substring → using PK '{ref_col}'")
        return ref_col

    return None


# ──────────────────────────────────────────────────────────────────────────────
# FIX-14: Safe FK column remapper
# ──────────────────────────────────────────────────────────────────────────────
def _remap_fk_col(col: dict, dim_sk_map: dict, silver_col_set: set) -> dict:
    col_name    = col.get("column", "unknown")
    matched_ref = _match_fk_to_sk(col_name, dim_sk_map)

    if matched_ref is None:
        if col_name in silver_col_set:
            return {**col}

        fk_base    = _entity_base(col_name)
        best_match = None
        best_score = -1
        for silver_col in silver_col_set:
            if not silver_col.endswith("_id"):
                continue
            silver_base = _entity_base(silver_col)
            if silver_base == fk_base:
                best_match, best_score = silver_col, 3
                break
            elif fk_base in silver_base and best_score < 2:
                best_match, best_score = silver_col, 2
            elif silver_base in fk_base and best_score < 1:
                best_match, best_score = silver_col, 1

        if best_match:
            print(f"  [FK-REMAP] '{col_name}' not in Silver → "
                  f"remapped to Silver column '{best_match}'")
            return {
                "column": best_match,
                "type":   col.get("type", "STRING"),
                "role":   "foreign_key",
            }

        print(f"  [WARN] FK '{col_name}' not in Silver and no dim matched. "
              f"Keeping as-is — verify this column exists before DDL.")
        return {**col}

    elif matched_ref == col_name:
        return {**col}
    else:
        return {
            "column": matched_ref,
            "type":   "BIGINT" if matched_ref.endswith("_sk") else "STRING",
            "role":   "foreign_key",
        }


# ──────────────────────────────────────────────────────────────────────────────
# FIX-17: Logic-driven fact-table primary key detector
# ──────────────────────────────────────────────────────────────────────────────
def _find_fact_pk(fact_columns: list, dim_sk_map: dict) -> str | None:
    """
    Identify the natural primary key of the fact table.

    The fact PK is the single _id column that:
    - ends with _id
    - has role "degenerate_dimension" or "primary_key" (i.e. not a FK)
    - does NOT appear as a PK in any dimension (dim_sk_map values)

    Returns the column name when exactly one candidate is found.
    Returns None when zero or multiple candidates exist.
    """
    dim_pk_cols = {v for v in dim_sk_map.values() if v is not None}

    candidates = [
        col for col in fact_columns
        if isinstance(col, dict)
        and col.get("column", "").endswith("_id")
        and col.get("column") not in dim_pk_cols
        and col.get("role") in ("degenerate_dimension", "primary_key")
    ]

    if len(candidates) == 1:
        return candidates[0]["column"]

    if len(candidates) > 1:
        print(
            f"  [FIX-17] Multiple unmatched _id columns found "
            f"({[c['column'] for c in candidates]}) — cannot determine fact PK "
            f"automatically. Leaving as degenerate_dimension."
        )
    return None


# ──────────────────────────────────────────────────────────────────────────────
# FIX-18-v2: Dimension PK collision resolver — scoped to same-entity dims only
# ──────────────────────────────────────────────────────────────────────────────
def _resolve_dim_pk_collisions(
    dims_list      : list,
    dim_sk_map     : dict,
    fact_id_cols   : set,
) -> None:
    """
    [FIX-18-v2] Detect and rename dimension primary keys that collide with the
    fact table's natural row-identifier _id column.

    SCOPE (critical — v12 tightening):
    ────────────────────────────────────
    This function only renames a dim PK when BOTH of the following are true:

      1. The dim PK name appears in fact_id_cols (the set of _id columns on the
         fact table that have NO matching dimension — i.e. true degenerate / fact
         row identifier candidates).

      2. The dimension's own entity base matches the entity base of the colliding
         PK column. This means the dim is literally named after the same thing as
         the fact row identifier.

    Example that TRIGGERS a rename (both conditions met):
        fact_id_cols = {"transaction_id"}       ← no dim for this entity
        dim = "dim_transaction"                  ← entity base = "transaction"
        dim PK = "transaction_id"                ← base also = "transaction"  ✓ rename

    Examples that do NOT trigger a rename (condition 2 fails):
        dim = "dim_customer",  PK = "customer_id"
            → entity base "customer" ≠ base of any entry in fact_id_cols ("transaction")
            → NO rename ✓

        dim = "dim_loan",      PK = "loan_id"
            → entity base "loan" ≠ "transaction"  → NO rename ✓

        dim = "dim_account",   PK = "account_id"
            → entity base "account" ≠ "transaction" → NO rename ✓

    This prevents the v11 regression where customer_id / loan_id / account_id
    were wrongly renamed because those column names also appeared on the fact
    table (as FK candidates).

    PARAMETERS
    ──────────
    dims_list    : raw list of dimension dicts being built by normalize_model
    dim_sk_map   : SK map being built by normalize_model (mutated in-place)
    fact_id_cols : set of _id column names that are TRUE fact row-identifier
                   candidates — i.e. columns whose entity base has NO matching
                   dimension. FK columns (customer_id when dim_customer exists)
                   must NOT be in this set.
    """
    for dim in dims_list:
        dim_name  = dim.get("name", "")
        dim_cols  = dim.get("columns", [])

        pk_entry = next(
            (c for c in dim_cols
             if isinstance(c, dict) and c.get("role") == "primary_key"),
            None,
        )
        if pk_entry is None:
            continue

        pk_col_name = pk_entry.get("column", "")
        if not pk_col_name:
            continue

        # Condition 1: the dim PK must appear in the true fact row-id candidates
        if pk_col_name not in fact_id_cols:
            continue

        # Condition 2 (FIX-18-v2): the dim's entity base must match the PK's
        # entity base. If they differ, this is a cross-entity coincidence — skip.
        dim_entity_base = _entity_base(dim_name)
        pk_entity_base  = _entity_base(pk_col_name)
        if dim_entity_base != pk_entity_base:
            print(
                f"  [FIX-18-v2] Skipping rename of {dim_name}.{pk_col_name}: "
                f"dim entity '{dim_entity_base}' ≠ pk entity '{pk_entity_base}' "
                f"— not a true collision."
            )
            continue

        # Genuine collision — rename the dim PK to a more specific name
        new_pk_name = f"{dim_entity_base}_type_id"

        existing_names = {c.get("column", "") for c in dim_cols if isinstance(c, dict)}
        suffix_options = ["_type_id", "_category_id", "_key_id", "_ref_id"]
        for suffix in suffix_options:
            candidate = f"{dim_entity_base}{suffix}"
            if candidate not in existing_names or candidate == pk_col_name:
                new_pk_name = candidate
                break

        print(
            f"  [FIX-18] Collision detected: {dim_name}.{pk_col_name} shares its name "
            f"with fact row-identifier '{pk_col_name}'.\n"
            f"           Renaming dim PK → '{new_pk_name}' to prevent DDL conflict."
        )

        pk_entry["column"] = new_pk_name

        if dim_name in dim_sk_map:
            dim_sk_map[dim_name] = new_pk_name


# ──────────────────────────────────────────────────────────────────────────────
# normalize_model
# ──────────────────────────────────────────────────────────────────────────────
def normalize_model(raw_model, silver_col_names: list = None):
    """
    Normalise a raw LLM-generated star-schema model.

    FIX-17: The single unmatched _id column on the fact table is automatically
    promoted to role "primary_key". Domain-agnostic — no column name is hardcoded.

    FIX-18-v2: _resolve_dim_pk_collisions() is called with a tightened
    fact_id_cols set that contains ONLY true fact row-identifier candidates
    (i.e. _id columns whose entity base has NO matching dimension in the raw
    model). This prevents FK columns like customer_id / loan_id / account_id
    from being incorrectly passed into the collision resolver, which was the
    root cause of the v11 regression.
    """
    silver_col_set: set = set(silver_col_names) if silver_col_names else set()

    new_model = {"fact_table": {}, "dimensions": []}

    silver_date_cols = _collect_silver_date_cols(raw_model)
    needs_dim_date   = _has_any_date_cols(raw_model) or bool(silver_date_cols)

    def normalize_columns(cols):
        normalized = []
        for col in cols:
            if isinstance(col, str):
                normalized.append({"column": col, "type": "STRING", "role": "descriptive"})
            elif isinstance(col, dict):
                if "column" not in col:
                    col = {
                        **col,
                        "column": (col.get("name") or col.get("col")
                                   or col.get("field") or "unknown"),
                    }
                normalized.append(col)
            else:
                normalized.append({"column": str(col), "type": "STRING", "role": "descriptive"})
        return normalized

    # ── STEP 1: Process dimensions ────────────────────────────────────────────
    dim_sk_map: dict = {}

    if needs_dim_date:
        dim_sk_map["dim_date"] = "date_id"

    # ── FIX-18-v2: Build fact_id_candidates as ONLY unmatched _id columns ─────
    # An _id column is a true fact row-identifier candidate if its entity base
    # does NOT match any dimension's entity base in the raw model. FK columns
    # (customer_id when dim_customer exists) are excluded from this set.
    raw_fact_cols = normalize_columns(
        raw_model.get("fact_table", {}).get("columns", [])
    )

    raw_dim_entity_bases: set = {
        _entity_base(d.get("name", ""))
        for d in raw_model.get("dimensions", [])
        if d.get("name") not in ("dim_date", "")
    }

    fact_id_candidates: set = {
        c.get("column", "")
        for c in raw_fact_cols
        if isinstance(c, dict)
        and c.get("column", "").endswith("_id")
        and c.get("column") != "date_id"
        # Only include if the entity base has NO matching dimension —
        # otherwise this is a FK column and must not trigger FIX-18.
        and _entity_base(c.get("column", "")) not in raw_dim_entity_bases
    }
    # ── end FIX-18-v2 prep ───────────────────────────────────────────────────

    for dim in raw_model.get("dimensions", []):
        dim_name = dim.get("name", "dim_unknown")

        if dim_name == "dim_date":
            continue

        clean_name = dim_name.replace("dim_", "")
        columns    = normalize_columns(dim.get("columns", []))

        columns = [c for c in columns if c.get("column") not in silver_date_cols]

        other_dim_names = [
            d.get("name", "").replace("dim_", "")
            for d in raw_model.get("dimensions", [])
            if d.get("name") != dim_name
        ]
        columns = [
            c for c in columns
            if not (
                c.get("column", "").endswith("_id")
                and c.get("role") in ("foreign_key", "primary_key", "descriptive", "unknown")
                and any(
                    c.get("column", "").startswith(other)
                    for other in other_dim_names
                )
            )
        ]

        has_biz_key = _dim_has_business_key(columns, dim_name=dim_name)
        new_columns = []

        if has_biz_key:
            for col in columns:
                new_columns.append({**col})

            biz_key_col = next(
                (col["column"] for col in columns
                 if col.get("role") == "primary_key"
                 and col.get("column", "").endswith("_id")),
                None,
            )
            dim_sk_map[dim_name] = biz_key_col

            # FIX-15: Surrogate dim PK detection
            if silver_col_set and biz_key_col:
                if biz_key_col.lower() not in {c.lower() for c in silver_col_set}:
                    print(
                        f"  [FIX-15]  {dim_name}.{biz_key_col}: not found in Silver "
                        f"→ demoting to primary_key_temp (surrogate BK required)"
                    )
                    for nc in new_columns:
                        if nc.get("column") == biz_key_col and nc.get("role") == "primary_key":
                            nc["role"] = "primary_key_temp"
                            break
                else:
                    print(
                        f"  [FIX-15]  {dim_name}.{biz_key_col}: found in Silver "
                        f"→ keeping as primary_key (real column)"
                    )

        else:
            placeholder_key = f"{clean_name}_id"
            new_columns.append({
                "column": placeholder_key,
                "type":   "STRING",
                "role":   "primary_key_temp",
            })

            LOW_CARDINALITY_HINTS = (
                "country", "type", "status", "flag", "gender",
                "is_", "level", "category", "class", "region"
            )
            HIGH_UNIQUENESS_HINTS = (
                "city", "name", "code", "key", "email",
                "title", "label", "identifier"
            )

            descriptive_cols = [
                c for c in columns
                if c.get("column", "") != placeholder_key
                and c.get("role") != "primary_key"
            ]

            def _uniqueness_score(col: dict) -> int:
                col_name = col.get("column", "").lower()
                if any(h in col_name for h in LOW_CARDINALITY_HINTS):
                    return 0
                if any(h in col_name for h in HIGH_UNIQUENESS_HINTS):
                    return 2
                return 1

            best_natural_key = max(
                descriptive_cols,
                key=_uniqueness_score,
                default=None,
            )

            for col in columns:
                col_name = col.get("column", "unknown")
                if col_name == placeholder_key:
                    continue
                if col.get("role") == "primary_key":
                    col = {**col, "role": "descriptive"}
                elif best_natural_key and col_name == best_natural_key.get("column"):
                    col = {**col, "role": "business_key"}
                else:
                    col = {**col}
                new_columns.append(col)

            dim_sk_map[dim_name] = None

        new_model["dimensions"].append({"name": dim_name, "columns": new_columns})

    # ── [FIX-18-v2] Resolve dim PK collisions with tightened fact_id_cols ─────
    # fact_id_candidates now only contains truly unmatched _id columns —
    # FK columns like customer_id are excluded, preventing wrong renames.
    _resolve_dim_pk_collisions(
        dims_list    = new_model["dimensions"],
        dim_sk_map   = dim_sk_map,
        fact_id_cols = fact_id_candidates,
    )
    # ── end FIX-18-v2 ─────────────────────────────────────────────────────────

    # Inject canonical dim_date
    if needs_dim_date:
        new_model["dimensions"].append({
            "name":    "dim_date",
            "columns": [dict(c) for c in _DIM_DATE_COLUMNS],
        })

    # ── STEP 2: Process fact table ─────────────────────────────────────────────
    raw_fact     = raw_model.get("fact_table", {})
    fact_name    = raw_fact.get("name", "fact_table")
    fact_columns = normalize_columns(raw_fact.get("columns", []))

    # Strip Silver date columns and any existing date_id
    fact_columns = [
        c for c in fact_columns
        if c.get("column") not in silver_date_cols
        and not (c.get("column") == "date_id" and c.get("role") in ("foreign_key", "primary_key"))
    ]

    # FIX-13: Convert any LLM-assigned primary_key → foreign_key in the fact
    # table. The real fact PK is detected and promoted in the FIX-17 step below.
    converted_pks = []
    for col in fact_columns:
        if col.get("role") == "primary_key":
            converted = {**col, "role": "foreign_key"}
            print(f"  [PATCH] '{col.get('column')}' role changed from primary_key "
                  f"to foreign_key in fact table (will be re-evaluated by FIX-17)")
            converted_pks.append(converted)
        else:
            converted_pks.append(col)
    fact_columns = converted_pks

    # Deduplicate fact columns
    seen_fact_cols: set = set()
    deduped_fact_cols = []
    for col in fact_columns:
        col_name = col.get("column", "")
        if col_name not in seen_fact_cols:
            seen_fact_cols.add(col_name)
            deduped_fact_cols.append(col)
        else:
            print(f"  [PATCH] Removed duplicate column '{col_name}' from fact table")
    fact_columns = deduped_fact_cols

    # FIX-17: Detect and promote the natural fact PK
    fact_pk_col = _find_fact_pk(fact_columns, dim_sk_map)
    if fact_pk_col:
        fact_columns = [
            {**c, "role": "primary_key"} if c.get("column") == fact_pk_col else c
            for c in fact_columns
        ]
        print(f"  [FIX-17] '{fact_pk_col}' promoted to primary_key "
              f"(sole unmatched _id — natural fact row identifier)")

    updated_columns = []

    # Add date_id FK first if dim_date is present
    if needs_dim_date:
        updated_columns.append(dict(_DIM_DATE_FK))

    if fact_columns:
        for col in fact_columns:
            role     = col.get("role", "")
            col_name = col.get("column", "unknown")

            if col_name == "date_id":
                continue
            elif role == "foreign_key":
                updated_columns.append(_remap_fk_col(col, dim_sk_map, silver_col_set))
            elif role in ("degenerate_dimension", "primary_key"):
                updated_columns.append({**col})
            else:
                updated_columns.append({**col})
    else:
        for dim_name, ref_col in dim_sk_map.items():
            if dim_name == "dim_date":
                continue
            if ref_col:
                updated_columns.append({
                    "column": ref_col,
                    "type":   "BIGINT" if ref_col.endswith("_sk") else "STRING",
                    "role":   "foreign_key",
                })

    # Final dedup pass
    seen_final: set = set()
    final_columns = []
    for col in updated_columns:
        col_name = col.get("column", "")
        if col_name not in seen_final:
            seen_final.add(col_name)
            final_columns.append(col)
        else:
            print(f"  [PATCH] Removed post-merge duplicate '{col_name}' from fact table")

    new_model["fact_table"] = {"name": fact_name, "columns": final_columns}
    return new_model


# ──────────────────────────────────────────────────────────────────────────────
# Relationship map builder
# ──────────────────────────────────────────────────────────────────────────────
def get_relationship_map(model: dict) -> dict:
    fact_cols = model.get("fact_table", {}).get("columns", [])
    dims      = model.get("dimensions", [])

    fact_fks = {
        col["column"]: col["column"]
        for col in fact_cols
        if isinstance(col, dict) and col.get("role") == "foreign_key"
    }

    relationships = {}
    for dim in dims:
        dim_name = dim.get("name", "")
        dim_cols = dim.get("columns", [])

        dim_pk = next(
            (c["column"] for c in dim_cols
             if isinstance(c, dict) and c.get("role") == "primary_key"),
            None,
        )
        if not dim_pk:
            continue

        matched_fk = None
        if dim_pk in fact_fks:
            matched_fk = dim_pk
        else:
            dim_base = _entity_base(dim_name)
            for fk_col in fact_fks:
                if _entity_base(fk_col) == dim_base:
                    matched_fk = fk_col
                    break

        if matched_fk:
            relationships[dim_name] = {
                "pk":         dim_pk,
                "fk_in_fact": matched_fk,
            }

    return {
        "fact_table": model.get("fact_table", {}).get("name", ""),
        "dimensions": relationships,
    }


# ──────────────────────────────────────────────────────────────────────────────
# dim_date deployer
# ──────────────────────────────────────────────────────────────────────────────
def _deploy_dim_date(
    dim_table_name: str,
    catalog:        str,
    schema:         str,
    fact:           dict,
    model:          dict,
    conn,
    src_catalog:    str = "",
    src_schema:     str = "",
    src_table:      str = "",
) -> None:
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {dim_table_name} ("
        f"date_id INT NOT NULL, year INT, month INT, month_name STRING, quarter INT, "
        f"week INT, day INT, day_name STRING, is_weekend BOOLEAN, "
        f"CONSTRAINT pk_dim_date PRIMARY KEY (date_id) NOT ENFORCED) USING DELTA"
    )
    execute_sql(ddl, conn)
    print(f"    ✓ Table created: {dim_table_name}")


# ──────────────────────────────────────────────────────────────────────────────
# Single-connection SQL execution (FIX-10)
# ──────────────────────────────────────────────────────────────────────────────
def _open_connection():
    if not _db_hostname or not _db_token:
        raise RuntimeError(
            "Databricks connection params not set — "
            "call init_data_model() before deploying."
        )
    return dbsql.connect(
        server_hostname=_db_hostname,
        http_path=_db_warehouse,
        access_token=_db_token,
    )


def execute_sql(sql_query: str, connection=None):
    if connection is not None:
        with connection.cursor() as cursor:
            cursor.execute(sql_query)
        return
    with _open_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)


# ──────────────────────────────────────────────────────────────────────────────
# Type mapper
# ──────────────────────────────────────────────────────────────────────────────
def map_databricks_type(dtype: str) -> str:
    dtype = dtype.upper()
    if dtype in ("STRING", "TEXT"):                return "STRING"
    if dtype in ("INT", "INTEGER"):                return "INT"
    if dtype in ("LONG", "BIGINT"):                return "BIGINT"
    if dtype in ("DOUBLE", "FLOAT", "DECIMAL"):    return "DOUBLE"
    if dtype == "DATE":                            return "DATE"
    if dtype == "TIMESTAMP":                       return "TIMESTAMP"
    if dtype == "BOOLEAN":                         return "BOOLEAN"
    return "STRING"


# ──────────────────────────────────────────────────────────────────────────────
# deploy_to_databricks (FIX-10: single persistent connection)
# ──────────────────────────────────────────────────────────────────────────────
def _get_source_columns(conn, src_catalog: str, src_schema: str, src_table: str) -> set:
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT column_name "
            f"FROM `{src_catalog}`.information_schema.columns "
            f"WHERE table_schema = '{src_schema}' "
        )
        cols = {row[0] for row in cur.fetchall()}
        cur.close()
        return cols
    except Exception as exc:
        print(f"    ⚠  Could not introspect source columns for {src_table}: {exc}")
        return set()


def deploy_to_databricks(
    model:          dict,
    catalog:        str,
    schema:         str,
    source_catalog: str = "",
    source_schema:  str = "",
) -> dict:
    """
    Create Gold-layer tables in catalog.schema.

    DDL only — population is performed by the gold_execute.py notebook.
    """
    eff_src_catalog = source_catalog if source_catalog else catalog
    eff_src_schema  = source_schema  if source_schema  else schema

    fact      = model["fact_table"]
    fact_name = f"{catalog}.{schema}.{fact['name']}"
    dims      = model.get("dimensions", [])

    dim_pk_map: dict = {}
    for dim in dims:
        pk_col = next(
            (c["column"] for c in dim.get("columns", [])
             if isinstance(c, dict) and c.get("role") == "primary_key"),
            None,
        )
        if pk_col:
            dim_pk_map[dim["name"]] = pk_col

    conn = _open_connection()
    try:
        execute_sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`", conn)
        execute_sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}", conn)

        src_tables: set = set()
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT table_name "
                f"FROM `{eff_src_catalog}`.information_schema.tables "
                f"WHERE table_schema = '{eff_src_schema}' "
                f"AND table_name != 'silver_taxonomy_lookup'"
            )
            src_tables = {row[0].lower() for row in cur.fetchall()}
            cur.close()
        except Exception as exc:
            print(f"  ⚠  Could not list source tables: {exc}")

        def _best_src_table(gold_name: str) -> str | None:
            base = gold_name.replace("fact_", "").replace("dim_", "").lower()
            if base in src_tables:
                return base
            for t in src_tables:
                if base in t or t in base:
                    return t
            if len(src_tables) == 1:
                return next(iter(src_tables))
            return None

        # ── Deploy dimension tables ───────────────────────────────────────────
        for dim in dims:
            dim_table_name = f"{catalog}.{schema}.{dim['name']}"
            pk_col         = dim_pk_map.get(dim["name"])

            if dim["name"] == "dim_date":
                date_src_tbl = _best_src_table(fact["name"])
                _deploy_dim_date(
                    dim_table_name,
                    eff_src_catalog,
                    eff_src_schema,
                    fact,
                    model,
                    conn,
                    src_table=date_src_tbl or "",
                )
                continue

            dim_col_defs = []
            for col in dim["columns"]:
                if isinstance(col, dict):
                    col_name = col.get("column", "unknown")
                    col_type = map_databricks_type(col.get("type", "STRING"))
                else:
                    col_name, col_type = str(col), "STRING"
                dim_col_defs.append(f"  {col_name} {col_type}")

            constraints = []
            if pk_col:
                constraints.append(
                    f"  CONSTRAINT pk_{dim['name']} "
                    f"PRIMARY KEY ({pk_col}) NOT ENFORCED"
                )

            _body   = ",\n".join(dim_col_defs + constraints)
            dim_sql = (
                f"CREATE OR REPLACE TABLE {dim_table_name} (\n"
                f"{_body}\n) USING DELTA"
            )
            execute_sql(dim_sql, conn)

            if pk_col:
                try:
                    execute_sql(
                        f"ALTER TABLE {dim_table_name} "
                        f"DROP CONSTRAINT IF EXISTS pk_{dim['name']}", conn
                    )
                    execute_sql(
                        f"ALTER TABLE {dim_table_name} "
                        f"ADD CONSTRAINT pk_{dim['name']} "
                        f"PRIMARY KEY ({pk_col}) NOT ENFORCED", conn
                    )
                except Exception:
                    pass

        # ── Deploy fact table ─────────────────────────────────────────────────
        def _fk_base(n: str) -> str:
            return _entity_base(n)

        fk_constraint_lines = []
        for col in fact["columns"]:
            if not isinstance(col, dict) or col.get("role") != "foreign_key":
                continue
            fk_col  = col.get("column", "")
            fk_base = _fk_base(fk_col)
            for dim_name, pk_col in dim_pk_map.items():
                dim_base = _fk_base(dim_name.replace("dim_", ""))
                if fk_base == dim_base:
                    ref_table = f"{catalog}.{schema}.{dim_name}"
                    fk_constraint_lines.append(
                        f"  CONSTRAINT fk_{fact['name']}_{fk_col} "
                        f"FOREIGN KEY ({fk_col}) REFERENCES {ref_table}({pk_col}) "
                        f"NOT ENFORCED"
                    )
                    break

        fact_col_defs = []
        for col in fact["columns"]:
            if isinstance(col, dict):
                col_name = col.get("column", "unknown")
                col_type = map_databricks_type(col.get("type", "STRING"))
            else:
                col_name, col_type = str(col), "STRING"
            fact_col_defs.append(f"  {col_name} {col_type}")

        _body    = ",\n".join(fact_col_defs + fk_constraint_lines)
        fact_sql = (
            f"CREATE OR REPLACE TABLE {fact_name} (\n"
            f"{_body}\n) USING DELTA"
        )
        execute_sql(fact_sql, conn)

        for col in fact["columns"]:
            if not isinstance(col, dict) or col.get("role") != "foreign_key":
                continue
            fk_col  = col.get("column", "")
            fk_base = _fk_base(fk_col)
            for dim_name, pk_col in dim_pk_map.items():
                dim_base = _fk_base(dim_name.replace("dim_", ""))
                if fk_base == dim_base:
                    ref_table       = f"{catalog}.{schema}.{dim_name}"
                    constraint_name = f"fk_{fact['name']}_{fk_col}"
                    try:
                        execute_sql(
                            f"ALTER TABLE {fact_name} "
                            f"DROP CONSTRAINT IF EXISTS {constraint_name}", conn
                        )
                        execute_sql(
                            f"ALTER TABLE {fact_name} "
                            f"ADD CONSTRAINT {constraint_name} "
                            f"FOREIGN KEY ({fk_col}) REFERENCES {ref_table}({pk_col}) "
                            f"NOT ENFORCED", conn
                        )
                    except Exception:
                        pass
                    break

    finally:
        conn.close()

    rel_map = get_relationship_map(model)
    print(f"\n  --- Deployment completed ---.")
    return rel_map