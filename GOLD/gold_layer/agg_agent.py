"""
agg_agent.py — Aggregate Table / View Generation
=================================================
FIX: Removed module-level import of DATABRICKS_ACCESS_TOKEN / DATABRICKS_HOSTNAME /
     SQL_WAREHOUSE_ID from metadata.py (those constants no longer exist there).
     Credentials are now injected once via init_agg_agent() called from main.py,
     matching the same lazy-init pattern used by data_model.py.
"""

import json
import os
import re
from openai import OpenAI
from dotenv import load_dotenv
from databricks import sql as dbsql

load_dotenv()

# ── Module-level credential store (populated by init_agg_agent) ───────────────
_HOSTNAME:     str = ""
_ACCESS_TOKEN: str = ""
_WAREHOUSE_ID: str = ""
_MODEL_NAME:   str = os.getenv("MODEL_NAME", "")

_client: "OpenAI | None" = None


def init_agg_agent(
    hostname: str,
    sql_warehouse_id: str,
    access_token: str,
    model_endpoint: str = "",
) -> None:
    """
    Inject Databricks / LLM credentials into this module.
    Call this once from main.py immediately after stage_collect_credentials().
    """
    global _HOSTNAME, _ACCESS_TOKEN, _WAREHOUSE_ID, _MODEL_NAME, _client

    # Strip https:// so dbsql.connect() gets bare hostname
    _HOSTNAME     = hostname.replace("https://", "").replace("http://", "").rstrip("/")
    _ACCESS_TOKEN = access_token

    # Ensure warehouse ID always has full HTTP path format
    _WAREHOUSE_ID = sql_warehouse_id if sql_warehouse_id.startswith("/sql/1.0/warehouses/") \
                    else f"/sql/1.0/warehouses/{sql_warehouse_id}"

    if model_endpoint:
        _MODEL_NAME = model_endpoint

    # Use clean hostname (without https://) for OpenAI base_url
    _client = OpenAI(
        api_key  = access_token,
        base_url = f"https://{_HOSTNAME}/serving-endpoints",
    )


def _get_client() -> OpenAI:
    if _client is None:
        raise RuntimeError(
            "agg_agent has not been initialised — "
            "call init_agg_agent(hostname, sql_warehouse_id, access_token) first."
        )
    return _client


# ============================================================
# Databricks SQL connection
# ============================================================
def get_db_connection():
    if not _HOSTNAME or not _ACCESS_TOKEN:
        raise RuntimeError(
            "agg_agent credentials not set — call init_agg_agent() first."
        )
    return dbsql.connect(
        server_hostname=_HOSTNAME,
        http_path=_WAREHOUSE_ID,
        access_token=_ACCESS_TOKEN,
    )


# ============================================================
# Utility: Extract JSON safely
# ============================================================
def extract_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
        raise ValueError("No valid JSON found in LLM response")


# ============================================================
# Utility: Get bare table name from a fully-qualified key
# e.g. "final_testing.gold_sales.fact_ecommerce_orders" → "fact_ecommerce_orders"
# ============================================================
def _bare(table_key: str) -> str:
    return table_key.split(".")[-1]


# ============================================================
# Utility: Ensure every aggregate expression has an AS alias
#
# FIX: The original regex blindly added AS alias after every
# aggregate function it found, which broke arithmetic expressions
# like:
#   SUM(fs.order_amount) AS total / COUNT(fs.order_id) AS count
# because the alias was injected in the MIDDLE of the expression.
#
# The correct approach is:
#   1. Split the SELECT clause into comma-separated expressions
#      (respecting nested parentheses so we never split inside a
#      function call).
#   2. For each expression, check whether it ALREADY has a top-
#      level AS alias.
#   3. Only add AS alias to expressions that contain at least one
#      aggregate AND have no top-level AS alias yet.
#   4. Derive the alias from the aggregate functions present.
# ============================================================

_AGG_FUNCTIONS = re.compile(
    r'\b(SUM|AVG|COUNT|MIN|MAX|COLLECT_LIST|COLLECT_SET|APPROX_COUNT_DISTINCT)\s*\(',
    re.IGNORECASE,
)

_AGG_FN_PREFIX = {
    "sum":                   "total",
    "avg":                   "avg",
    "count":                 "count",
    "min":                   "min",
    "max":                   "max",
    "collect_list":          "list",
    "collect_set":           "set",
    "approx_count_distinct": "approx",
}

# Matches a single AGG(optional_table.col) to extract column name for alias
_SINGLE_AGG_RE = re.compile(
    r'\b(SUM|AVG|COUNT|MIN|MAX|COLLECT_LIST|COLLECT_SET|APPROX_COUNT_DISTINCT)\s*'
    r'\(\s*(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*\)',
    re.IGNORECASE,
)


def _split_select_expressions(select_clause: str) -> list:
    """
    Split a SELECT clause (without FROM) into individual column expressions,
    respecting nested parentheses so we never split inside a function call.
    """
    exprs = []
    depth = 0
    start = 0

    for i, ch in enumerate(select_clause):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif ch == ',' and depth == 0:
            exprs.append(select_clause[start:i].strip())
            start = i + 1

    tail = select_clause[start:].strip()
    if tail:
        exprs.append(tail)

    return exprs


def _make_agg_alias(expr: str) -> str:
    """
    Derive a reasonable alias for an aggregate expression.
    Single aggregate  → prefix_colname  (e.g. total_order_amount)
    Compound (SUM/COUNT) → avg_colname
    """
    matches = list(_SINGLE_AGG_RE.finditer(expr))

    if not matches:
        return "value"

    if len(matches) == 1:
        fn_name  = matches[0].group(1).lower()
        col_name = matches[0].group(2).lower()
        return f"{_AGG_FN_PREFIX.get(fn_name, fn_name)}_{col_name}"

    # Compound arithmetic expression (e.g. SUM / COUNT)
    fn1 = _AGG_FN_PREFIX.get(matches[0].group(1).lower(), matches[0].group(1).lower())
    fn2 = _AGG_FN_PREFIX.get(matches[1].group(1).lower(), matches[1].group(1).lower())
    col = matches[0].group(2).lower()

    if fn1 == "total" and fn2 == "count":
        return f"avg_{col}"
    return f"{fn1}_{fn2}_{col}"


def _alias_aggregates(select_sql: str) -> str:
    """
    Ensure every SELECT expression that contains an aggregate function
    has an AS alias at the TOP LEVEL of that expression.

    Handles:
      Simple:    SUM(fs.amount)            -> SUM(fs.amount) AS total_amount
      Compound:  SUM(fs.a) / COUNT(fs.b)   -> SUM(fs.a) / COUNT(fs.b) AS avg_a
      Aliased:   SUM(fs.a) AS my_alias     -> unchanged
      Plain col: fs.region                 -> unchanged
    """
    from_match = re.search(r'\bFROM\b', select_sql, re.IGNORECASE)
    if not from_match:
        return select_sql

    select_part = select_sql[: from_match.start()]
    rest_part   = select_sql[from_match.start() :]

    exprs = _split_select_expressions(select_part)

    fixed_exprs = []
    for expr in exprs:
        # No aggregate at all — leave as-is
        if not _AGG_FUNCTIONS.search(expr):
            fixed_exprs.append(expr)
            continue

        # Already has a top-level AS alias — check with a simple heuristic:
        # strip whitespace and look for  \bAS\s+\w+  near the end, outside parens.
        # We rebuild depth to check that the final AS is at depth 0.
        depth = 0
        last_as_pos = -1
        i = 0
        tokens = re.split(r'(\(|\)|\bAS\b)', expr, flags=re.IGNORECASE)
        pos = 0
        has_top_alias = False
        for tok in tokens:
            if tok == '(':
                depth += 1
            elif tok == ')':
                depth -= 1
            elif tok.upper() == 'AS' and depth == 0:
                has_top_alias = True

        if has_top_alias:
            fixed_exprs.append(expr)
            continue

        # No alias yet — add one
        alias = _make_agg_alias(expr)
        fixed_exprs.append(f"{expr} AS {alias}")

    return ", ".join(fixed_exprs) + "\n" + rest_part


# ============================================================
# Utility: Rewrite CAST(x AS <numeric>) → TRY_CAST(x AS <numeric>)
# Prevents CAST_INVALID_INPUT when the source column contains
# string 'null' or other non-numeric strings (e.g. value columns
# in IoT / time-series silver tables).
# ============================================================
_NUMERIC_TYPES = r'(?:DOUBLE|FLOAT|DECIMAL(?:\s*\(\s*\d+\s*,\s*\d+\s*\))?|BIGINT|INT(?:EGER)?|SMALLINT|TINYINT|REAL)'

_CAST_RE = re.compile(
    r'\bCAST\s*\(([^)]+)\bAS\b\s*(' + _NUMERIC_TYPES + r')\s*\)',
    re.IGNORECASE,
)

def _safe_cast(sql: str) -> str:
    """Replace CAST(x AS <numeric_type>) with TRY_CAST(x AS <numeric_type>)."""
    return _CAST_RE.sub(lambda m: f"TRY_CAST({m.group(1)}AS {m.group(2)})", sql)


# ============================================================
# FIX: Fetch LIVE column names from Databricks for a table.
# ============================================================
def get_live_columns(catalog: str, schema: str, table: str) -> list:
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT column_name
                FROM {catalog}.information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name   = '{table}'
                ORDER BY ordinal_position
            """)
            rows = cur.fetchall()
        conn.close()
        return [r[0] for r in rows]
    except Exception as e:
        print(f"    ⚠ Could not fetch live columns for {catalog}.{schema}.{table}: {e}")
        return []


# ============================================================
# Utility: Extract fact table schema from metadata
# ============================================================
def _extract_fact_schema(metadata: dict) -> dict:
    for table_key, details in metadata.items():
        bare = _bare(table_key)

        if not bare.startswith("fact_"):
            continue

        cols = details.get("columns", [])

        all_columns = [
            c.get("column", c) if isinstance(c, dict) else c
            for c in cols
        ]

        pk_col = next(
            (c.get("column", "") if isinstance(c, dict) else c
             for c in cols
             if isinstance(c, dict) and c.get("role") == "primary_key"),
            None,
        )

        fk_columns = []
        for c in cols:
            col_name = c.get("column", c) if isinstance(c, dict) else c
            role     = c.get("role", "")  if isinstance(c, dict) else ""

            if col_name == pk_col:
                continue

            if role == "foreign_key":
                fk_columns.append(col_name)
                continue

            if (col_name.endswith("_sk") or col_name.endswith("_id")) \
                    and role not in ("measure", "primary_key"):
                fk_columns.append(col_name)

        measure_cols = [
            c.get("column", c) if isinstance(c, dict) else c
            for c in cols
            if isinstance(c, dict) and c.get("role") == "measure"
        ]

        measure_keywords = [
            "amount", "sales", "price", "qty", "total",
            "revenue", "cost", "profit", "count", "score",
            "fee", "discount", "value", "rate",
        ]
        for c in cols:
            col_name = c.get("column", c) if isinstance(c, dict) else c
            role     = c.get("role", "")  if isinstance(c, dict) else ""
            if col_name not in measure_cols and role not in ("primary_key", "foreign_key"):
                if any(kw in col_name.lower() for kw in measure_keywords):
                    measure_cols.append(col_name)

        return {
            "table_key":       table_key,
            "table_name":      bare,
            "alias":           "fs",
            "all_columns":     all_columns,
            "fk_columns":      fk_columns,
            "measure_columns": measure_cols,
        }

    # ── Fallback: no fact_ table found — pick the best available table ──────────
    # This handles schemas where there is only one table (e.g. a flat sales table)
    # or where the table was classified as a dimension by the silver layer analysis.
    if not metadata:
        return {}

    # Prefer the table with the most measure-like columns; otherwise just the first one.
    best_key     = None
    best_measure = -1

    measure_keywords = [
        "amount", "sales", "price", "qty", "total",
        "revenue", "cost", "profit", "count", "score",
        "fee", "discount", "value", "rate",
    ]

    for table_key, details in metadata.items():
        cols = details.get("columns", [])
        score = sum(
            1 for c in cols
            if any(kw in (c.get("column", c) if isinstance(c, dict) else c).lower()
                   for kw in measure_keywords)
        )
        if score > best_measure:
            best_measure = score
            best_key     = table_key

    if best_key is None:
        best_key = next(iter(metadata))

    bare    = _bare(best_key)
    details = metadata[best_key]
    cols    = details.get("columns", [])

    all_columns = [
        c.get("column", c) if isinstance(c, dict) else c
        for c in cols
    ]

    pk_col = next(
        (c.get("column", "") if isinstance(c, dict) else c
         for c in cols
         if isinstance(c, dict) and c.get("role") == "primary_key"),
        None,
    )

    fk_columns = [
        c.get("column", c) if isinstance(c, dict) else c
        for c in cols
        if isinstance(c, dict) and c.get("role") == "foreign_key"
    ]

    measure_cols = [
        c.get("column", c) if isinstance(c, dict) else c
        for c in cols
        if isinstance(c, dict) and c.get("role") == "measure"
    ]
    for c in cols:
        col_name = c.get("column", c) if isinstance(c, dict) else c
        role     = c.get("role", "")  if isinstance(c, dict) else ""
        if col_name not in measure_cols and role not in ("primary_key", "foreign_key"):
            if any(kw in col_name.lower() for kw in measure_keywords):
                measure_cols.append(col_name)

    print(f"   ⚠ No fact_ table found — using '{bare}' as the source table for KPI SQL.")

    return {
        "table_key":       best_key,
        "table_name":      bare,
        "alias":           "fs",
        "all_columns":     all_columns,
        "fk_columns":      fk_columns,
        "measure_columns": measure_cols,
    }


# ============================================================
# Utility: Build dim → FK mapping from metadata
# ============================================================
def _build_dim_fk_map(fact_schema: dict, metadata: dict) -> list:
    fk_cols   = set(fact_schema.get("fk_columns", []))
    join_info = []

    alias_map  = {}
    alias_used = {"fs"}

    for table_key in metadata:
        bare = _bare(table_key)
        if not bare.startswith("dim_"):
            continue
        clean = bare.replace("dim_", "")
        alias = clean[:2].lower()
        if alias in alias_used:
            alias = clean[:3].lower()
        if alias in alias_used:
            alias = clean[:4].lower()
        if alias in alias_used:
            alias = clean[:5].lower()
        alias_used.add(alias)
        alias_map[table_key] = alias

    for table_key, details in metadata.items():
        bare = _bare(table_key)
        if not bare.startswith("dim_"):
            continue

        dim_alias     = alias_map.get(table_key, bare[:2])
        dim_cols      = details.get("columns", [])
        dim_col_names = [
            c.get("column", c) if isinstance(c, dict) else c
            for c in dim_cols
        ]

        dim_pk = next(
            (c.get("column", c) if isinstance(c, dict) else c
             for c in dim_cols
             if isinstance(c, dict) and c.get("role") == "primary_key"),
            None,
        )
        if not dim_pk:
            dim_pk = next(
                (n for n in dim_col_names if n.endswith("_sk") or n.endswith("_id")),
                None,
            )
        if not dim_pk:
            continue

        matched_fk = None

        if dim_pk in fk_cols:
            matched_fk = dim_pk
        else:
            def _base(n):
                for sfx in ["_sk", "_id", "_key"]:
                    if n.endswith(sfx):
                        n = n[:-len(sfx)]
                        break
                if n.endswith("s"):
                    n = n[:-1]
                return n.lower()

            dim_base = _base(bare.replace("dim_", ""))
            for fk in fk_cols:
                if _base(fk) == dim_base:
                    matched_fk = fk
                    break

        if matched_fk:
            join_info.append({
                "dim_table_key": table_key,
                "dim_table":     bare,
                "dim_alias":     dim_alias,
                "fact_fk_col":   matched_fk,
                "dim_pk_col":    dim_pk,
            })

    return join_info


# ============================================================
# Build a verified join map using LIVE column names.
# ============================================================
def _build_verified_join_reference(
    fact_schema: dict,
    metadata: dict,
    catalog: str,
    source_schema: str,
) -> tuple:
    fact_name    = fact_schema.get("table_name", "")
    fk_cols      = fact_schema.get("fk_columns", [])
    measure_cols = fact_schema.get("measure_columns", [])

    live_fact_cols = set(get_live_columns(catalog, source_schema, fact_name))

    verified_fk_cols      = [c for c in fk_cols      if c in live_fact_cols]
    verified_measure_cols = [c for c in measure_cols  if c in live_fact_cols]

    pk_col = next(
        (c.get("column", "") if isinstance(c, dict) else c
         for c in fact_schema.get("all_columns", [])
         if isinstance(c, dict) and c.get("role") == "primary_key"),
        None,
    )
    for col in live_fact_cols:
        if col not in verified_fk_cols and col != pk_col:
            if col.endswith("_sk") or col.endswith("_id"):
                verified_fk_cols.append(col)

    fk_set     = set(verified_fk_cols)
    join_lines = []

    alias_map  = {}
    alias_used = {"fs"}

    for table_key in metadata:
        bare = _bare(table_key)
        if not bare.startswith("dim_"):
            continue
        clean = bare.replace("dim_", "")
        alias = clean[:2].lower()
        if alias in alias_used:
            alias = clean[:3].lower()
        if alias in alias_used:
            alias = clean[:4].lower()
        if alias in alias_used:
            alias = clean[:5].lower()
        alias_used.add(alias)
        alias_map[table_key] = alias

    for table_key, details in metadata.items():
        bare = _bare(table_key)
        if not bare.startswith("dim_"):
            continue

        dim_alias = alias_map.get(table_key, bare[:2])

        live_dim_cols = get_live_columns(catalog, source_schema, bare)
        live_dim_set  = set(live_dim_cols)

        dim_cols      = details.get("columns", [])
        dim_col_names = [c.get("column", c) if isinstance(c, dict) else c for c in dim_cols]

        dim_pk = next(
            (c.get("column", c) if isinstance(c, dict) else c
             for c in dim_cols
             if isinstance(c, dict) and c.get("role") == "primary_key"),
            None,
        )
        if not dim_pk:
            dim_pk = next(
                (n for n in dim_col_names if n.endswith("_sk") or n.endswith("_id")),
                None,
            )

        if dim_pk and dim_pk not in live_dim_set:
            fallback = next(
                (c for c in live_dim_cols if c.endswith("_sk")),
                next((c for c in live_dim_cols if c.endswith("_id")), None),
            )
            if fallback:
                print(f"    ⚠ Dim PK '{dim_pk}' not in live {bare} — using '{fallback}' instead")
                dim_pk = fallback

        if not dim_pk:
            continue

        def _base(n):
            for sfx in ["_sk", "_id", "_key"]:
                if n.endswith(sfx):
                    n = n[:-len(sfx)]
                    break
            if n.endswith("s"):
                n = n[:-1]
            return n.lower()

        matched_fk = None
        if dim_pk in fk_set:
            matched_fk = dim_pk
        else:
            dim_base = _base(bare.replace("dim_", ""))
            for fk in fk_set:
                if _base(fk) == dim_base:
                    matched_fk = fk
                    break

        if matched_fk:
            join_lines.append(
                f"  {bare} (alias {dim_alias})  →  "
                f"LEFT JOIN {catalog}.{source_schema}.{bare} {dim_alias} "
                f"ON fs.{matched_fk} = {dim_alias}.{dim_pk}"
            )

    join_reference = "\n".join(join_lines) if join_lines \
        else "  (no matching dim FK columns found — do not add any JOINs)"

    return join_reference, verified_fk_cols, verified_measure_cols


# ============================================================
# Step 1: Generate aggregate SQL
# ============================================================
def generate_agg_sqls(kpis, catalog, source_schema, metadata):

    fact_schema = _extract_fact_schema(metadata)
    fact_name   = fact_schema.get("table_name") or next(
        (_bare(k) for k in metadata), None
    )
    if not fact_name:
        raise RuntimeError("No tables found in metadata — cannot generate aggregate SQL.")

    join_reference, verified_fk_cols, verified_measure_cols = \
        _build_verified_join_reference(fact_schema, metadata, catalog, source_schema)

    live_fact_all = get_live_columns(catalog, source_schema, fact_name)

    prompt = f"""
You are a senior Databricks SQL engineer. Generate one aggregate SELECT query per KPI.

CRITICAL — FACT TABLE INFORMATION:
Fact table (use this exact path): {catalog}.{source_schema}.{fact_name}
Fact table alias: fs

ACTUAL FK columns that exist in the fact table right now (verified from live schema):
{json.dumps(verified_fk_cols, indent=2)}

ACTUAL measure/numeric columns in the fact table (verified from live schema):
{json.dumps(verified_measure_cols, indent=2)}

ALL columns that exist in the fact table (do NOT reference any column outside this list):
{json.dumps(live_fact_all, indent=2)}

AVAILABLE JOINS — copy these JOIN clauses EXACTLY as written (column names are verified):
{join_reference}

STRICT RULES:
1. NEVER invent or guess column names.
   Only use columns from "ALL columns that exist in the fact table" list above.
   If a column is not in that list, it does NOT exist — do not use it.

2. Copy JOIN clauses EXACTLY from AVAILABLE JOINS above.
   Do NOT change the ON condition column names under any circumstances.

3. Use SHORT aliases in SELECT and GROUP BY (fs for fact, dim aliases from JOIN list).
   NEVER use fully-qualified names like {catalog}.{source_schema}.dim_product.product_name.
   CORRECT:  SELECT dp.product_name, SUM(fs.total_net_order_value) AS total_sales
   WRONG:    SELECT {catalog}.{source_schema}.dim_product.product_name ...

4. Only JOIN dims that are needed for the specific KPI.

5. Standard Spark SQL / Databricks Unity Catalog syntax.

6. When casting a STRING column to a numeric type (DOUBLE, FLOAT, DECIMAL, INT, BIGINT etc.)
   ALWAYS use TRY_CAST instead of CAST.
   Reason: source columns may contain the literal string 'null' or other non-numeric values.
   CORRECT: AVG(TRY_CAST(fs.value AS DOUBLE))
   WRONG:   AVG(CAST(fs.value AS DOUBLE))

7. Every aggregate (SUM, AVG, COUNT, MIN, MAX) MUST have an AS alias:
   CORRECT: SUM(fs.total_net_order_value) AS total_sales_revenue
   WRONG:   SUM(fs.total_net_order_value)

7. Output ONLY valid JSON — no markdown, no explanation.

KPIs:
{json.dumps(kpis, indent=2, default=str)}

Catalog       : {catalog}
Source schema : {source_schema}

Full metadata (for context — but column existence is governed by the verified lists above):
{json.dumps(metadata, indent=2, default=str)}

Output format:
{{
    "aggregates": [
        {{
            "kpi_name": "",
            "agg_table_name": "agg_<snake_case_name>",
            "description": "",
            "select_sql": "SELECT ... FROM {catalog}.{source_schema}.{fact_name} fs LEFT JOIN ... GROUP BY ..."
        }}
    ]
}}
"""
    response = _get_client().chat.completions.create(
        model=_MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        max_tokens=4000,
    )
    content = response.choices[0].message.content
    return extract_json(content)


# ============================================================
# Step 2: Validate SQL — dry run with EXPLAIN
# ============================================================
def validate_sql(aggregates, catalog, target_schema, object_type):
    valid   = []
    invalid = []

    print("\n   Validating SQL queries...")
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            for agg in aggregates:
                select_sql = _safe_cast(agg["select_sql"].strip().rstrip(";"))
                try:
                    cur.execute(f"EXPLAIN {select_sql}")
                    valid.append(agg)
                    print(f"   ✅ Valid  : {agg['agg_table_name']}")
                except Exception as e:
                    err_msg = str(e)
                    print(f"   ⚠ Invalid: {agg['agg_table_name']}")
                    print(f"     Error  : {err_msg[:200]}")
                    agg["validation_error"] = err_msg
                    invalid.append(agg)
    finally:
        conn.close()

    return valid, invalid


# ============================================================
# Step 3: Auto-fix failed SQLs using live schema + error message
# ============================================================
def fix_failed_sqls(invalid_aggs, catalog, source_schema, metadata):
    if not invalid_aggs:
        return []

    fact_schema = _extract_fact_schema(metadata)
    fact_name   = fact_schema.get("table_name") or next(
        (_bare(k) for k in metadata), None
    )
    if not fact_name:
        return []

    join_reference, verified_fk_cols, verified_measure_cols = \
        _build_verified_join_reference(fact_schema, metadata, catalog, source_schema)

    live_fact_all = get_live_columns(catalog, source_schema, fact_name)

    print(f"\n🔧 Auto-fixing {len(invalid_aggs)} failed SQL(s)...")

    fixed = []
    for agg in invalid_aggs:
        prompt = f"""
You are a Databricks SQL expert. Fix the SQL query below.

Error message (read carefully — it tells you exactly which column name is wrong):
{agg.get("validation_error", "Unknown error")}

Original SQL:
{agg["select_sql"]}

The error is caused by using a column name that does NOT exist in the table.
Replace the wrong column name with the correct one from the verified lists below.

Fact table: {catalog}.{source_schema}.{fact_name}  (alias: fs)

FK columns that ACTUALLY exist in the fact table (verified from live Databricks schema):
{json.dumps(verified_fk_cols, indent=2)}

Measure/numeric columns that ACTUALLY exist in the fact table (verified):
{json.dumps(verified_measure_cols, indent=2)}

ALL columns in the fact table — only reference these, nothing else:
{json.dumps(live_fact_all, indent=2)}

AVAILABLE JOINS — copy these EXACTLY, do not change any column name in the ON clause:
{join_reference}

RULES:
1. Replace every wrong column name with the correct one from the verified lists.
2. Copy JOIN clauses exactly from AVAILABLE JOINS — the ON conditions are verified correct.
3. Use short aliases (fs for fact, dim aliases from JOIN list) everywhere.
4. Never reference a column that is not in "ALL columns in the fact table" list.
5. Every aggregate (SUM, AVG, COUNT, MIN, MAX) must have an AS alias.
6. When casting STRING columns to numeric types always use TRY_CAST, never CAST.
   Example: TRY_CAST(fs.value AS DOUBLE) — this tolerates string 'null' values.
7. CRITICAL: The error message tells you EXACTLY which alias is wrong.
   Read the suggestion in the error carefully:
   e.g. "Did you mean [`lo`.`location_id`]" → use `lo`.`location_id` not `cu`.`location_id`
   Always follow the suggestion in the error message exactly.

Catalog       : {catalog}
Source schema : {source_schema}

Metadata (for context only):
{json.dumps(metadata, indent=2, default=str)}

Return ONLY a JSON object — no markdown:
{{
    "kpi_name": "{agg['kpi_name']}",
    "agg_table_name": "{agg['agg_table_name']}",
    "description": "{agg.get('description', '')}",
    "select_sql": "<corrected SELECT statement>"
}}
"""
        try:
            response = _get_client().chat.completions.create(
                model=_MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=2000,
            )
            content   = response.choices[0].message.content
            fixed_agg = extract_json(content)
            print(f"   ✅ Fixed  : {agg['agg_table_name']}")
            fixed.append(fixed_agg)
        except Exception as e:
            print(f"   ❌ Could not fix {agg['agg_table_name']}: {e}")
            fixed.append(agg)

    return fixed


# ============================================================
# Step 4: Ask user — same schema or new schema?
# ============================================================
def get_target_schema(source_schema: str, source_catalog: str) -> tuple:
    """
    Ask the user where to store the aggregate tables/views.

    Catalog : press Enter → reuse source_catalog
              type a name → use that catalog (created if it doesn't exist)
    Schema  : press Enter → reuse source_schema  (is_new = False)
              type a name → new schema            (is_new = True)

    Returns (target_catalog, target_schema, is_new)
    """
    print("\n==== Aggregate Table Storage ====")
    print(f"  Current catalog : {source_catalog}")
    print(f"  Current schema  : {source_schema}")

    # ── Catalog ───────────────────────────────────────────────────────────────
    catalog_input = input(
        f"\n  Target catalog  (press Enter to use '{source_catalog}'): "
    ).strip()
    target_catalog = catalog_input if catalog_input else source_catalog

    # ── Schema ────────────────────────────────────────────────────────────────
    schema_input = input(
        f"  Target schema   (press Enter to use '{source_schema}'): "
    ).strip()

    if not schema_input:
        return target_catalog, source_schema, False

    return target_catalog, schema_input, True


# ============================================================
# Step 5: TABLE or VIEW?
# ============================================================
def get_object_type() -> str:
    print("\n  Save aggregates as:")
    print("  1. Table  (materialised, faster reads)")
    print("  2. View   (always reflects latest data)")

    while True:
        choice = input("Choose option (1/2): ").strip()
        if choice == "1":
            return "TABLE"
        elif choice == "2":
            return "VIEW"
        else:
            print("⚠ Please enter 1 or 2.")


# ============================================================
# Step 6: Single connection — create schema + deploy all
# ============================================================
def create_and_deploy(aggregates, catalog, target_schema, object_type, is_new):
    deployed = []

    print("\n   Opening Databricks connection...")
    conn = get_db_connection()
    print("   Connected.\n")

    try:
        with conn.cursor() as cur:

            if is_new:
                print(f"🔧 Creating catalog: {catalog} ...")
                cur.execute(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
                print(f"   ✅ Catalog ready: {catalog}")

                print(f"🔧 Creating schema: {catalog}.{target_schema} ...")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{target_schema}`")
                print(f"   ✅ Schema ready: {catalog}.{target_schema}\n")

            for agg in aggregates:
                full_name  = f"`{catalog}`.`{target_schema}`.`{agg['agg_table_name']}`"
                select_sql = _safe_cast(
                    _alias_aggregates(agg["select_sql"].strip().rstrip(";"))
                )

                if object_type == "TABLE":
                    ddl = (
                        f"CREATE OR REPLACE TABLE {full_name}\n"
                        f"USING DELTA\n"
                        f"AS\n{select_sql}"
                    )
                else:
                    ddl = f"CREATE OR REPLACE VIEW {full_name} AS\n{select_sql}"

                print(f"🔄 Deploying {object_type}: {full_name}")
                print(f"   SQL preview: {select_sql[:150]}{'...' if len(select_sql) > 150 else ''}")

                try:
                    cur.execute(ddl)
                    print("   ✅ Deployed successfully")
                    deployed.append({
                        "kpi_name":         agg["kpi_name"],
                        "agg_table_name":   agg["agg_table_name"],
                        "full_object_name": f"{catalog}.{target_schema}.{agg['agg_table_name']}",
                        "object_type":      object_type,
                        "description":      agg.get("description", ""),
                        "select_sql":       select_sql,
                        "status":           "deployed",
                    })
                except Exception as e:
                    print(f"   ❌ Failed: {e}")
                    deployed.append({
                        "kpi_name":         agg["kpi_name"],
                        "agg_table_name":   agg["agg_table_name"],
                        "full_object_name": f"{catalog}.{target_schema}.{agg['agg_table_name']}",
                        "object_type":      object_type,
                        "description":      agg.get("description", ""),
                        "select_sql":       select_sql,
                        "status":           f"failed: {e}",
                    })
    finally:
        conn.close()
        print("\n   Connection closed.")

    return deployed


# ============================================================
# Step 7: Save deployment report locally
# ============================================================
def save_agg_report(deployed, catalog, target_schema):
    path = os.getenv("AGG_OUTPUT_DIR") or os.path.join(
        "C:", os.sep, "Users", "Admin", "Documents",
        "Gen AI", "gold_layer_project", "outputs", "Aggregates",
    )
    os.makedirs(path, exist_ok=True)

    safe_catalog = catalog.replace(" ", "_")
    safe_schema  = target_schema.replace(" ", "_")
    file_path    = os.path.join(path, f"{safe_catalog}_{safe_schema}_aggregates.json")

    with open(file_path, "w") as f:
        json.dump({"aggregates": deployed}, f, indent=2, default=str)

    print(f"💾 Aggregate report saved: {file_path}")
    return file_path


def filter_valid_kpis(kpis, metadata):
    # print("\n🧹 KPI Filtering:")
    # print("   ✅ Passing all KPIs to SQL generation (no filtering applied)")
    return kpis


# ============================================================
# Main Aggregate Pipeline
# ============================================================
def agg_pipeline_flow(final_kpis, catalog, source_schema, metadata,
                      deploy_type="", target_catalog=None, target_schema=None):
    kpi_list = final_kpis.get("kpis", [])
    kpi_list = filter_valid_kpis(kpi_list, metadata)

    if not kpi_list:
        print("\n⚠ No KPIs found — skipping aggregate generation.")
        return

    fact_schema = _extract_fact_schema(metadata)

    join_reference, verified_fk_cols, verified_measure_cols = \
        _build_verified_join_reference(fact_schema, metadata, catalog, source_schema)

    print(f"\n Fact table detected    : {fact_schema.get('table_name') or '⚠ NOT FOUND — using fallback'}")
    print(f"   FK columns (verified)   : {verified_fk_cols}")
    print(f"   Measures (verified)     : {verified_measure_cols}")
    print(f"   Dim JOINs available     :\n{join_reference}")

    # 1. Generate SQL
    print("\n Generating aggregate SQL for all KPIs...")
    agg_result = generate_agg_sqls(kpi_list, catalog, source_schema, metadata)
    aggregates = agg_result.get("aggregates", [])

    if not aggregates:
        print("❌ No aggregate definitions generated. Exiting.")
        return

    print(f"\n---- Generated {len(aggregates)} Aggregate Object(s) ----")
    for i, agg in enumerate(aggregates, 1):
        print(f"  {i}. {agg['agg_table_name']}  ->  {agg.get('description', '')}")

    # 2. Target catalog/schema — use values from orchestrator if provided, else ask user
    if target_catalog and target_schema:
        final_catalog = target_catalog
        final_schema  = target_schema
        is_new        = True
        print(f"\n  ✅ Deploying aggregates to: {final_catalog}.{final_schema}")
    else:
        final_catalog, final_schema, is_new = get_target_schema(source_schema, catalog)

    # 3. Object type — use deploy_type from orchestrator if provided, else ask user
    if deploy_type and deploy_type.lower() in ("table", "view"):
        object_type = deploy_type.upper()
        print(f"  ✅ Using deployment type from orchestrator: {object_type}")
    else:
        object_type = get_object_type()

    # 4. Validate
    valid, invalid = validate_sql(aggregates, final_catalog, final_schema, object_type)

    # 5. Auto-fix
    if invalid:
        fixed = fix_failed_sqls(invalid, catalog, source_schema, metadata)

        if fixed:
            print(f"\n   Re-validating {len(fixed)} fixed SQL(s)...")
            re_valid, still_invalid = validate_sql(fixed, final_catalog, final_schema, object_type)
            valid.extend(re_valid)

            if still_invalid:
                print(f"\n   ⚠ {len(still_invalid)} SQL(s) could not be auto-fixed and will be skipped:")
                for a in still_invalid:
                    print(f"     - {a['agg_table_name']}")

    if not valid:
        print("\n❌ No valid SQLs to deploy after validation. Exiting.")
        return

    print(f"\n---- {len(valid)} valid aggregate(s) ready to deploy ----")

    # 6. Deploy
    print(f"\n---- Deploying to {final_catalog}.{final_schema} as {object_type}(S) ----")
    deployed = create_and_deploy(valid, final_catalog, final_schema, object_type, is_new)

    # 7. Summary
    success = [d for d in deployed if d["status"] == "deployed"]
    failed  = [d for d in deployed if d["status"] != "deployed"]

    print(f"\n==== Deployment Summary ====")
    print(f"  ✅ Deployed : {len(success)}")
    print(f"  ❌ Failed   : {len(failed)}")

    if failed:
        print("\n  Failed objects:")
        for d in failed:
            print(f"    - {d['agg_table_name']} : {d['status']}")

    # 8. Save report
    save_agg_report(deployed, final_catalog, final_schema)

    return deployed