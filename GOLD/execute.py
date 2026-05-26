"""
gold_execute.py — Gold Layer Execution (Databricks Notebook)
=============================================================
Upload this file to Databricks and run as a notebook.
 
FIXES IN THIS VERSION:
  FIX-1  CONSTRAINT name must NOT be backtick-quoted.
         CONSTRAINT pk_dim_department (correct) vs CONSTRAINT pk_`dim_department` (wrong)
 
  FIX-2  dim_date Silver date detection now introspects Silver information_schema
         directly — model columns have date cols stripped by normalize_model().
 
  FIX-3  dim_date TRUNCATE before INSERT prevents duplicate rows on re-runs.
 
  FIX-4  date_id in fact table is now derived from the most important Silver
         date column (CAST to YYYYMMDD INT via DATE_FORMAT) since date_id does
         not exist in Silver — _populate_table previously skipped it entirely.
 
  FIX-5  FK constraints are added AFTER data is loaded (Databricks requires
         referenced dim tables to be populated before FK constraints can be
         applied). All FK constraints are wired at the end of the pipeline.
"""
 
import json
import datetime
 
# =============================================================================
# CONFIG — update these four values before running
# =============================================================================
 
GOLD_MODEL_PATH = "/Volumes/gold/gold1/plan/plan.json.txt"
 
# =============================================================================
# LOAD GOLD MODEL PLAN
# =============================================================================
 
with open(GOLD_MODEL_PATH) as f:
    model = json.load(f)
 
# Read catalog/schema directly from plan.json (written by orchestrator)
SILVER_CATALOG = model.get("silver_catalog", "")
SILVER_SCHEMA  = model.get("silver_schema",  "")
GOLD_CATALOG   = model.get("gold_catalog",   "")
GOLD_SCHEMA    = model.get("gold_schema",    "")
 
fact_table = model.get("fact_table", {})
dimensions = model.get("dimensions", [])
 
# Validate — fail fast if any are missing
_missing = [k for k, v in {
    "silver_catalog": SILVER_CATALOG,
    "silver_schema" : SILVER_SCHEMA,
    "gold_catalog"  : GOLD_CATALOG,
    "gold_schema"   : GOLD_SCHEMA,
}.items() if not v]
 
if _missing:
    raise ValueError(
        f"plan.json is missing required fields: {_missing}\n"
        f"Re-run the orchestrator to regenerate plan.json with these fields."
    )
 
print("=" * 62)
print("  GOLD LAYER EXECUTION — Silver → Gold Migration")
print("=" * 62)
print(f"  Silver source : {SILVER_CATALOG}.{SILVER_SCHEMA}")
print(f"  Gold target   : {GOLD_CATALOG}.{GOLD_SCHEMA}")
print(f"  Fact table    : {fact_table.get('name', 'N/A')}")
print(f"  Dimensions    : {len(dimensions)}")
print("=" * 62)
 
 
# =============================================================================
# CONSTANTS
# =============================================================================
 
DATE_TYPES     = {"date", "timestamp", "timestamp_ntz", "datetime"}
DATE_NAME_HINT = ("date", "_dt", "timestamp", "_time")
 
# Priority keywords — higher index = more transactional = higher priority
DATE_PRIORITY_KEYWORDS = [
    "time", "dt", "date",
    "end_date", "close_date",
    "delivery_date", "ship_date",
    "review_date", "eval_date",
    "join_date", "joining_date",
    "hire_date", "hired_date",
    "order_date", "sale_date",
    "transaction_date", "txn_date",
    "event_date", "created_date",
    "created_at", "occurred_at",
]
 
_MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
_DAY_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday",
]
 
 
# =============================================================================
# HELPERS
# =============================================================================
 
def _map_to_spark_sql_type(dtype: str) -> str:
    dtype = dtype.upper().strip()
    mapping = {
        "STRING":    "STRING",
        "TEXT":      "STRING",
        "INT":       "INT",
        "INTEGER":   "INT",
        "BIGINT":    "BIGINT",
        "LONG":      "BIGINT",
        "DOUBLE":    "DOUBLE",
        "FLOAT":     "FLOAT",
        "DECIMAL":   "DECIMAL(18,2)",
        "DATE":      "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "BOOLEAN":   "BOOLEAN",
    }
    return mapping.get(dtype, "STRING")
 
 
def _get_source_tables(catalog: str, schema: str) -> set:
    try:
        rows = spark.sql(
            f"SELECT table_name FROM `{catalog}`.information_schema.tables "
            f"WHERE table_schema = '{schema}'"
        ).collect()
        return {r[0].lower() for r in rows}
    except Exception as exc:
        print(f"  ⚠  Could not list source tables in {catalog}.{schema}: {exc}")
        return set()
 
 
def _get_source_columns_with_types(catalog: str, schema: str, table: str) -> dict:
    """Return {column_name: data_type} for all columns in a Silver table."""
    try:
        rows = spark.sql(
            f"SELECT column_name, data_type "
            f"FROM `{catalog}`.information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        ).collect()
        return {r[0]: r[1].lower() for r in rows}
    except Exception as exc:
        print(f"  ⚠  Could not introspect columns for {table}: {exc}")
        return {}
 
 
def _get_source_columns(catalog: str, schema: str, table: str) -> set:
    return set(_get_source_columns_with_types(catalog, schema, table).keys())
 
 
def _best_src_table(gold_name: str, src_tables: set) -> str | None:
    """
    Map a Gold table name back to the best-matching Silver source table.
    4-level: exact → substring → word overlap → only-table fallback.
    """
    base = gold_name.replace("fact_", "").replace("dim_", "").lower()
 
    if base in src_tables:
        return base
    for t in src_tables:
        if base in t or t in base:
            return t
    base_words = set(base.split("_"))
    for t in src_tables:
        if base_words & set(t.split("_")):
            return t
    if src_tables:
        fallback = next(iter(src_tables))
        print(f"  ⚠  No name match for '{gold_name}' → using '{fallback}' as fallback")
        return fallback
    return None
 
 
def _pick_best_date_column(col_type_map: dict) -> str | None:
    """
    FIX-4: Pick the most important date/timestamp column from Silver.
 
    Strategy:
      1. Filter to only date/timestamp columns (by type or name hint).
      2. Score each column by its position in DATE_PRIORITY_KEYWORDS —
         higher index = more transactional = higher priority.
      3. Return the highest-scoring column.
      4. If no keyword matches, return the first date column found.
    """
    date_cols = {
        col: dtype for col, dtype in col_type_map.items()
        if dtype in DATE_TYPES
        or any(h in col.lower() for h in DATE_NAME_HINT)
    }
 
    if not date_cols:
        return None
 
    best_col   = None
    best_score = -1
 
    for col in date_cols:
        col_lower = col.lower()
        score = 0
        for idx, keyword in enumerate(DATE_PRIORITY_KEYWORDS):
            if keyword in col_lower:
                score = max(score, idx + 1)
        if score > best_score:
            best_score = score
            best_col   = col
 
    return best_col or next(iter(date_cols))
 
 
def _build_ddl_no_fk(table_fqn: str, columns: list) -> str:
    """
    Build CREATE OR REPLACE TABLE DDL with PK constraint only.
    FK constraints are added separately AFTER data is loaded (FIX-5).
 
    FIX-1: Constraint name is a plain SQL identifier — NO backticks.
    """
    col_defs = []
    pk_col   = None
 
    for col in columns:
        if not isinstance(col, dict):
            continue
        col_name = col.get("column", "unknown")
        col_type = _map_to_spark_sql_type(col.get("type", "STRING"))
        role     = col.get("role", "")
 
        if role == "primary_key_temp":
            continue
 
        suffix = " NOT NULL" if role == "primary_key" else ""
        col_defs.append(f"  `{col_name}` {col_type}{suffix}")
 
        if role == "primary_key":
            pk_col = col_name
 
    # FIX-1: strip backticks from short table name for constraint identifier
    tbl_short   = table_fqn.split(".")[-1].strip("`")
    constraints = []
    if pk_col:
        constraints.append(
            f"  CONSTRAINT pk_{tbl_short} PRIMARY KEY (`{pk_col}`) NOT ENFORCED"
        )
 
    body = ",\n".join(col_defs + constraints)
    return f"CREATE OR REPLACE TABLE {table_fqn} (\n{body}\n) USING DELTA"
 
 
def _populate_table(
    gold_fqn:           str,
    model_cols:         list,
    src_catalog:        str,
    src_schema:         str,
    src_table:          str,
    distinct_on_pk:     bool = True,
    date_id_source_col: str | None = None,
) -> int:
    """
    Populate a Gold table by reading matching columns from Silver.
 
    FIX-4: If date_id_source_col is provided, derives date_id as
           CAST(DATE_FORMAT(CAST(<col> AS DATE), 'yyyyMMdd') AS INT)
           so the fact table gets correct YYYYMMDD integer values.
    """
    source_col_map = _get_source_columns_with_types(src_catalog, src_schema, src_table)
    source_cols    = set(source_col_map.keys())
 
    if not source_cols:
        print(f"  ⚠  Source {src_catalog}.{src_schema}.{src_table} has no columns — skipping.")
        return 0
 
    src_fqn = f"`{src_catalog}`.`{src_schema}`.`{src_table}`"
 
    pk_col = next(
        (c["column"] for c in model_cols
         if isinstance(c, dict) and c.get("role") == "primary_key"
         and c["column"] in source_cols),
        None,
    )
 
    select_exprs = []
    insert_cols  = []
 
    for col in model_cols:
        if not isinstance(col, dict):
            continue
        col_name = col.get("column", "")
        role     = col.get("role", "")
 
        if not col_name or role == "primary_key_temp":
            continue
 
        # FIX-4: derive date_id from Silver date column as YYYYMMDD INT
        if col_name == "date_id" and date_id_source_col:
            if date_id_source_col in source_cols:
                select_exprs.append(
                    f"CAST(DATE_FORMAT(CAST(`{date_id_source_col}` AS DATE), "
                    f"'yyyyMMdd') AS INT) AS `date_id`"
                )
                insert_cols.append("`date_id`")
                print(f"  date_id   : derived from Silver col '{date_id_source_col}' "
                      f"→ YYYYMMDD INT")
            else:
                print(f"  ⚠  Silver date col '{date_id_source_col}' not found — "
                      f"date_id will be NULL")
            continue
 
        # Regular column — only include if it exists in Silver
        if col_name in source_cols:
            select_exprs.append(f"`{col_name}`")
            insert_cols.append(f"`{col_name}`")
 
    if not select_exprs:
        print(f"  ⚠  No column overlap between model and source for {gold_fqn} — skipping.")
        return 0
 
    col_list   = ", ".join(insert_cols)
    select_str = ", ".join(select_exprs)
 
    if pk_col and distinct_on_pk:
        select_sql = f"SELECT DISTINCT {select_str} FROM {src_fqn}"
    else:
        select_sql = f"SELECT {select_str} FROM {src_fqn}"
 
    insert_sql = f"INSERT INTO {gold_fqn} ({col_list})\n{select_sql}"
 
    print(f"  Populating {gold_fqn}")
    print(f"  Source    : {src_fqn}  ({len(insert_cols)} column(s))")
    if pk_col and distinct_on_pk:
        print(f"  Mode      : DISTINCT on primary key ({pk_col})")
 
    try:
        spark.sql(insert_sql)
        count = spark.table(gold_fqn).count()
        print(f"  ✅ Written : {count:,} rows → {gold_fqn}")
        return count
    except Exception as exc:
        print(f"  ❌ Population failed for {gold_fqn}: {exc}")
        return 0
 
 
def _deploy_dim_date(dim_fqn, fact, src_catalog, src_schema, src_tables):
    print(f"\n  Building dim_date …")
 
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dim_fqn} (
        date_id INT NOT NULL, year INT, month INT, month_name STRING,
        quarter INT, week INT, day INT, day_name STRING, is_weekend BOOLEAN,
        CONSTRAINT pk_dim_date PRIMARY KEY (date_id) NOT ENFORCED
        ) USING DELTA
        """.strip())
 
    spark.sql(f"TRUNCATE TABLE {dim_fqn}")
 
    # Detect date range (same as before)
    min_date, max_date, used_date_col = None, None, None
    src_tbl = _best_src_table(fact.get("name", ""), src_tables)
    if src_tbl:
        col_type_map  = _get_source_columns_with_types(src_catalog, src_schema, src_tbl)
        best_date_col = _pick_best_date_column(col_type_map)
        if best_date_col:
            try:
                row = spark.sql(
                    f"SELECT MIN(CAST(`{best_date_col}` AS DATE)), "
                    f"MAX(CAST(`{best_date_col}` AS DATE)) "
                    f"FROM `{src_catalog}`.`{src_schema}`.`{src_tbl}`"
                ).collect()[0]
                if row[0] and row[1]:
                    min_date      = str(row[0])
                    max_date      = str(row[1])
                    used_date_col = best_date_col
                    print(f"  Date range: {min_date} → {max_date}")
            except Exception as e:
                print(f"  ⚠  Date range query failed: {e}")
 
    if not min_date or not max_date:
        import datetime
        today    = datetime.date.today()
        min_date = f"{today.year - 2}-01-01"
        max_date = f"{today.year + 1}-12-31"
        print(f"  ⚠  Using fallback date range: {min_date} → {max_date}")
 
    # ✅ Single SQL — no Python loop, no chunks
    spark.sql(f"""
            INSERT INTO {dim_fqn}
            SELECT
                CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT)   AS date_id,
                YEAR(d)                                    AS year,
                MONTH(d)                                   AS month,
                DATE_FORMAT(d, 'MMMM')                     AS month_name,
                QUARTER(d)                                 AS quarter,
                WEEKOFYEAR(d)                              AS week,
                DAY(d)                                     AS day,
                DATE_FORMAT(d, 'EEEE')                     AS day_name,
                DAYOFWEEK(d) IN (1, 7)                     AS is_weekend
            FROM (
                SELECT EXPLODE(SEQUENCE(
                    TO_DATE('{min_date}'),
                    TO_DATE('{max_date}'),
                    INTERVAL 1 DAY
                )) AS d
            )
            """)
 
    count = spark.sql(f"SELECT COUNT(1) FROM {dim_fqn}").collect()[0][0]
    print(f"  ✅ Written : {count:,} rows → {dim_fqn}")
    return used_date_col
 
 
def _apply_fk_constraints(
    fact_fqn:   str,
    fact:       dict,
    dimensions: list,
    catalog:    str,
    schema:     str,
) -> None:
    """
    FIX-5: Add FK constraints to the fact table AFTER all data is loaded.
 
    Why after: Databricks registers FK constraints as metadata (NOT ENFORCED)
    but still requires the referenced table to exist and be populated first.
    Adding FKs in the DDL before data load causes errors on re-runs and
    violates the dim-first, fact-second deployment order.
 
    Each foreign_key column in the fact table gets:
      FOREIGN KEY (<fk_col>) REFERENCES <dim_table>(<pk_col>) NOT ENFORCED
    """
    print(f"\n  ── Applying FK Constraints to {fact.get('name', '')} ──")
 
    # Build dim_name → pk_col lookup
    dim_pk_map = {}
    for dim in dimensions:
        dim_name = dim.get("name", "")
        pk_col   = next(
            (c["column"] for c in dim.get("columns", [])
             if isinstance(c, dict) and c.get("role") == "primary_key"),
            None,
        )
        if pk_col:
            dim_pk_map[dim_name] = pk_col
 
    fact_name = fact.get("name", "")
 
    for col in fact.get("columns", []):
        if not isinstance(col, dict) or col.get("role") != "foreign_key":
            continue
 
        fk_col  = col.get("column", "")
        fk_base = fk_col.replace("_id", "").replace("_sk", "").lower()
 
        # Match FK column to a dimension by comparing entity base names
        matched_dim = None
        matched_pk  = None
        for dim_name, pk_col in dim_pk_map.items():
            dim_base = dim_name.replace("dim_", "").lower()
            if fk_base == dim_base or fk_col == pk_col:
                matched_dim = dim_name
                matched_pk  = pk_col
                break
 
        if not matched_dim:
            print(f"  ⚠  No matching dim found for FK '{fk_col}' — skipping")
            continue
 
        ref_table       = f"`{catalog}`.`{schema}`.`{matched_dim}`"
        constraint_name = f"fk_{fact_name}_{fk_col}"
 
        try:
            # Drop first for idempotent re-runs
            spark.sql(
                f"ALTER TABLE {fact_fqn} "
                f"DROP CONSTRAINT IF EXISTS {constraint_name}"
            )
            # Add FK — NOT ENFORCED (Databricks standard for star schemas)
            spark.sql(
                f"ALTER TABLE {fact_fqn} "
                f"ADD CONSTRAINT {constraint_name} "
                f"FOREIGN KEY (`{fk_col}`) "
                f"REFERENCES {ref_table}(`{matched_pk}`) "
                f"NOT ENFORCED"
            )
            print(f"  ✅ FK: {fact_name}.{fk_col} → {matched_dim}.{matched_pk}")
        except Exception as exc:
            print(f"  ⚠  Could not add FK '{constraint_name}': {exc}")
 
 
# =============================================================================
# STEP 2 — CREATE GOLD SCHEMA
# =============================================================================
 
print(f"\n  ── Step 2: Create Gold Schema ──")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{GOLD_CATALOG}`.`{GOLD_SCHEMA}`")
print(f"  Schema    : {GOLD_CATALOG}.{GOLD_SCHEMA} ready")
 
 
# =============================================================================
# STEP 3 — DISCOVER SILVER SOURCE TABLES
# =============================================================================
 
src_tables = _get_source_tables(SILVER_CATALOG, SILVER_SCHEMA)
print(f"\n  Silver tables found: {sorted(src_tables)}")
 
# ✅ After STEP 3 — cache ALL column info in one query
print(f"\n  ── Caching Silver column metadata ──")
 
_col_cache: dict[str, dict] = {}   # {table_name: {col_name: dtype}}
 
try:
    rows = spark.sql(
        f"SELECT table_name, column_name, data_type "
        f"FROM `{SILVER_CATALOG}`.information_schema.columns "
        f"WHERE table_schema = '{SILVER_SCHEMA}'"
    ).collect()
    for r in rows:
        _col_cache.setdefault(r[0].lower(), {})[r[1]] = r[2].lower()
    print(f"  Cached metadata for {len(_col_cache)} table(s)")
except Exception as e:
    print(f"  ⚠  Could not cache column metadata: {e}")
 
 
# Update helpers to use cache
def _get_source_columns_with_types(catalog, schema, table):
    if table.lower() in _col_cache:
        return _col_cache[table.lower()]
    # fallback to original query
    try:
        rows = spark.sql(
            f"SELECT column_name, data_type FROM `{catalog}`.information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        ).collect()
        return {r[0]: r[1].lower() for r in rows}
    except Exception as exc:
        print(f"  ⚠  Could not introspect columns for {table}: {exc}")
        return {}
 
def _get_source_columns(catalog, schema, table):
    return set(_get_source_columns_with_types(catalog, schema, table).keys())
 
# =============================================================================
# STEP 4 — DEPLOY DIMENSION TABLES
# =============================================================================
 
print(f"\n{'=' * 62}")
print(f"  ── Step 3/4: Dimension Tables ──")
print(f"{'=' * 62}")
 
dim_results     = {}
silver_date_col = None   # captured from dim_date, reused for fact date_id (FIX-4)
 
for dim in dimensions:
    dim_name = dim.get("name", "")
    dim_cols = dim.get("columns", [])
    dim_fqn  = f"`{GOLD_CATALOG}`.`{GOLD_SCHEMA}`.`{dim_name}`"
 
    print(f"\n  PIPELINE : {dim_name}")
    print(f"  TARGET   : {GOLD_CATALOG}.{GOLD_SCHEMA}.{dim_name}")
    print(f"  COLUMNS  : {len(dim_cols)}")
    print("-" * 62)
 
    try:
        # ── dim_date: special generator ───────────────────────────────────────
        if dim_name == "dim_date":
            used_col = _deploy_dim_date(
                dim_fqn     = dim_fqn,
                fact        = fact_table,
                src_catalog = SILVER_CATALOG,
                src_schema  = SILVER_SCHEMA,
                src_tables  = src_tables,
            )
            # FIX-4: store for fact date_id derivation
            if used_col:
                silver_date_col = used_col
                print(f"  ℹ   Silver date col captured for fact date_id: '{silver_date_col}'")
            dim_results[dim_name] = spark.table(dim_fqn).count()
            continue
 
        # ── Step 3a: Create dim table (PK only — FK added after) ──────────────
        print(f"\n  ── Step 3a: Create DDL ──")
        ddl = _build_ddl_no_fk(dim_fqn, dim_cols)
        spark.sql(ddl)
        print(f"  Table     : {dim_fqn} created (empty)")
 
        # ── Step 3b: Populate from Silver ─────────────────────────────────────
        print(f"\n  ── Step 3b: Populate from Silver ──")
        src_tbl = _best_src_table(dim_name, src_tables)
 
        if src_tbl:
            rows_written = _populate_table(
                gold_fqn       = dim_fqn,
                model_cols     = dim_cols,
                src_catalog    = SILVER_CATALOG,
                src_schema     = SILVER_SCHEMA,
                src_table      = src_tbl,
                distinct_on_pk = True,
            )
            dim_results[dim_name] = rows_written
        else:
            print(f"  ⚠  No Silver source table found for {dim_name} — table left empty")
            dim_results[dim_name] = 0
 
    except Exception as e:
        print(f"\n  ❌ FAILED  : {dim_name}")
        print(f"     ERROR   : {e}")
        import traceback
        traceback.print_exc()
        dim_results[dim_name] = -1
 
 
# =============================================================================
# STEP 5 — DEPLOY FACT TABLE
# =============================================================================
 
print(f"\n{'=' * 62}")
print(f"  ── Step 5: Fact Table ──")
print(f"{'=' * 62}")
 
fact_name   = fact_table.get("name", "")
fact_cols   = fact_table.get("columns", [])
fact_fqn    = f"`{GOLD_CATALOG}`.`{GOLD_SCHEMA}`.`{fact_name}`"
fact_result = 0
 
print(f"\n  PIPELINE : {fact_name}")
print(f"  TARGET   : {GOLD_CATALOG}.{GOLD_SCHEMA}.{fact_name}")
print(f"  COLUMNS  : {len(fact_cols)}")
print("-" * 62)
 
try:
    # Step 5a: Create fact table (PK only — FKs applied after population)
    print(f"\n  ── Step 5a: Create DDL ──")
    ddl = _build_ddl_no_fk(fact_fqn, fact_cols)
    spark.sql(ddl)
    print(f"  Table     : {fact_fqn} created (empty)")
 
    # Step 5b: Populate from Silver
    print(f"\n  ── Step 5b: Populate from Silver ──")
    src_tbl = _best_src_table(fact_name, src_tables)
 
    # FIX-4: if dim_date didn't set silver_date_col, detect it now
    if not silver_date_col and src_tbl:
        col_type_map    = _get_source_columns_with_types(SILVER_CATALOG, SILVER_SCHEMA, src_tbl)
        silver_date_col = _pick_best_date_column(col_type_map)
        if silver_date_col:
            print(f"  ℹ   Detected Silver date col for date_id: '{silver_date_col}'")
 
    if src_tbl:
        fact_result = _populate_table(
            gold_fqn            = fact_fqn,
            model_cols          = fact_cols,
            src_catalog         = SILVER_CATALOG,
            src_schema          = SILVER_SCHEMA,
            src_table           = src_tbl,
            distinct_on_pk      = False,
            date_id_source_col  = silver_date_col,   # FIX-4
        )
    else:
        print(f"  ⚠  No Silver source table found for {fact_name} — table left empty")
 
except Exception as e:
    print(f"\n  ❌ FAILED  : {fact_name}")
    print(f"     ERROR   : {e}")
    import traceback
    traceback.print_exc()
 
 
# =============================================================================
# STEP 5c — APPLY FK CONSTRAINTS (after all data is loaded)
# FIX-5: Must happen after dims AND fact are populated.
# =============================================================================
 
print(f"\n{'=' * 62}")
print(f"  ── Step 5c: Apply FK Constraints ──")
print(f"{'=' * 62}")
 
try:
    _apply_fk_constraints(
        fact_fqn   = fact_fqn,
        fact       = fact_table,
        dimensions = dimensions,
        catalog    = GOLD_CATALOG,
        schema     = GOLD_SCHEMA,
    )
except Exception as e:
    print(f"  ⚠  FK constraint step encountered an error: {e}")
    import traceback
    traceback.print_exc()
 
 
# =============================================================================
# STEP 6 — VERIFICATION: ROW COUNT SUMMARY
# =============================================================================
 
print(f"\n{'=' * 62}")
print(f"  ── Step 6: Verification — Row Count Summary ──")
print(f"{'=' * 62}")
 
all_ok = True
 
for dim_name, rows in dim_results.items():
    status = "✅" if rows > 0 else ("⚠ " if rows == 0 else "❌")
    if rows <= 0:
        all_ok = False
    print(f"  {status}  {dim_name:<40} {rows:>10,} rows")
 
fact_status = "✅" if fact_result > 0 else "⚠ "
if fact_result <= 0:
    all_ok = False
print(f"  {fact_status}  {fact_name:<40} {fact_result:>10,} rows")
 
print("=" * 62)
if all_ok:
    print("  ✅  GOLD MIGRATION COMPLETE — all tables populated")
else:
    print("  ⚠   GOLD MIGRATION DONE — some tables may be empty (check ⚠  above)")
print("=" * 62)