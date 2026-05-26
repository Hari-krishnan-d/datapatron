"""
silver_agent/execute.py — Silver Execution (Databricks Notebook)
=================================================================
Upload this file to Databricks and run as a notebook.

PLAN FILE:
  silver_migration_plan.json — routing (bronze → silver) + column_map
  with null_handling, target_type, cast_recommended per column.

EXECUTION ORDER PER PIPELINE:
  Step 1  — Read from Bronze
  Step 1b — Pre-compute aggregate fill values (mean/median/min/max)
  Step 2  — Replace NULL placeholders  (N/A, DK, -, null → actual NULL)
             ↳ ALL columns cleaned regardless of spec match (case-insensitive)
             ↳ Whitespace-only values also nullified
  Step 3  — Null handling              (fill_mean/constant/forward_fill/drop_rows…)
  Step 3b — Numeric NULL fill          (mean/median/min/max — typed, from mapping)
             ↳ Also catches whitespace survivors missed by Step 2
  Step 4  — Deduplication              (full / key)
  Step 5  — String rules               (trim, casing, strip special)
  Step 6  — Date rules                 (source format → ISO 8601)
  Step 7  — Advanced rules             (phone, email, currency, country, gender, boolean, address)
  Step 8  — Value Standardization      (pure CASE WHEN from silver_taxonomy_lookup)
  Step 9  — Rename columns             (source_column → target_column)
  Step 10 — Type casting               (DATE/TIMESTAMP/BIGINT/DECIMAL — null-safe)
             ↳ Post-cast fill: re-applies fill_value after cast to catch try_cast NULLs
  Step 11 — Write to Silver            (Delta, overwriteSchema=true)

NULL HANDLING FIXES:
  Problem 1 — Step 2 skipped columns with no spec match (e.g. 'age' vs 'Age').
              Fix: ALL STRING columns are now cleaned regardless of spec presence.
              Spec is only used to determine source_type; missing spec = STRING.

  Problem 2 — Step 2 missed whitespace-only values like ' ', '  ' etc.
              Fix: added trim(col) = '' check on every STRING column.

  Problem 3 — Step 3 writes fill_value as string literal. Step 10 try_cast
              then produces NULL if the value is unparseable.
              Fix: Step 10 re-applies fill_value after each cast operation so
              any NULL introduced by try_cast is immediately filled back.

  Problem 4 — Step 3b only checked isNull(). Whitespace survivors that slipped
              past Step 2 are not NULL so they passed through unfilled, then
              became NULL after Step 10 casting.
              Fix: Step 3b now treats trim(col)=='' as NULL before filling.

STEP 8 DESIGN:
  silver_mapping.py runs first (once, interactively) and commits EVERY
  known variant — including typos like "DEbIT", "ubpi", "creldit" — to
  silver_taxonomy_lookup as explicit raw_value → standardized_value rows.

  Step 8 here is a pure CASE WHEN exact match:
    col == raw_value  →  standardized_value

  raw_value in taxonomy is stored exactly as it appears in source (with
  all original spaces, casing etc). The CASE WHEN does NOT normalize —
  it matches the source value character-for-character against taxonomy.

  There is NO runtime fuzzy logic in this file. If new unseen variants
  appear in a future load, re-run silver_mapping.py, approve, then re-run
  this file. All knowledge lives in the taxonomy table.
"""

import json
import re
from pyspark.sql import functions as F, SparkSession
from pyspark.sql import Window
from pyspark.sql.types import (
    StringType, LongType, DoubleType, BooleanType,
    TimestampType, DateType, IntegerType, FloatType
)

# =============================================================================
# CONFIG
# =============================================================================
MIGRATION_PLAN_PATH = "/Volumes/healthcare/silver/pl/plan.json"

with open(MIGRATION_PLAN_PATH) as f:
    cfg = json.load(f)

if "spark" not in locals():
    spark = SparkSession.builder.appName("SilverExecution").getOrCreate()

pipelines = cfg["migration_plan"]["pipelines"]


# =============================================================================
# DATE / TIMESTAMP FORMAT LISTS
# =============================================================================
DATE_FORMATS_SQL = [
    # Numeric
    "yyyy-MM-dd", "dd-MM-yyyy", "MM/dd/yyyy", "dd/MM/yyyy", "yyyy/MM/dd",
    "MM-dd-yyyy", "yyyyMMdd", "d-MM-yyyy", "d/MM/yyyy", "MM/d/yyyy",
    "dd-MM-yy", "MM/dd/yy", "dd/MM/yy", "yy-MM-dd",
    # Named month
    "d MMM yyyy", "dd MMM yyyy", "MMM d yyyy", "MMM dd yyyy",
    "d MMMM yyyy", "dd MMMM yyyy", "MMMM d yyyy", "MMMM dd yyyy",
    "MMM d, yyyy", "MMM dd, yyyy", "MMMM d, yyyy", "MMMM dd, yyyy",
    "d-MMM-yyyy", "dd-MMM-yyyy", "d-MMMM-yyyy", "dd-MMMM-yyyy",
    "yyyy MMM dd", "yyyy-MMM-dd",
    # With time
    "yyyy-MM-dd HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "MM/dd/yyyy HH:mm:ss",
    "dd-MM-yyyy HH:mm", "dd/MM/yyyy HH:mm",
    # Duration
    "mm:ss.S",
]

TIMESTAMP_FORMATS_SQL = [
    # ISO (T/Z stripped at value level — no T/Z literals needed in format strings)
    "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss.SS",
    "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SSSSSS", "yyyy-MM-dd HH:mm",
    # dd-MM-yyyy
    "dd-MM-yyyy HH:mm:ss", "dd-MM-yyyy HH:mm",
    # dd/MM/yyyy
    "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm",
    # MM/dd/yyyy
    "MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy HH:mm",
    # yyyy/MM/dd
    "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm",
    # Named month
    "dd MMM yyyy HH:mm:ss", "dd MMM yyyy HH:mm",
    "dd-MMM-yyyy HH:mm:ss", "dd-MMM-yyyy HH:mm",
    "MMM dd yyyy HH:mm:ss", "MMM dd, yyyy HH:mm:ss", "MMM dd, yyyy HH:mm",
    # Compact + two-digit year
    "yyyyMMdd HH:mm:ss", "yyyyMMdd HH:mm",
    "dd-MM-yy HH:mm:ss", "MM/dd/yy HH:mm:ss", "dd/MM/yy HH:mm",
    # Duration
    "mm:ss.S", "mm:ss.SS", "mm:ss.SSS",
]

NULL_PLACEHOLDERS = [
    "N/A", "NA", "n/a", "na", "None", "none", "NONE",
    "NULL", "null", "Null", "NaN", "nan", "NIL", "nil",
    "DK", "dk", "TBD", "tbd", "-", "--", "---",
    "?", "unknown", "Unknown", "UNKNOWN", "",
]


# =============================================================================
# HELPERS
# =============================================================================

def resolve_column_map(pipeline: dict) -> list:
    for key in ("column_map", "columns"):
        result = pipeline.get(key, [])
        if result:
            return result
    return pipeline.get("transformations", {}).get("columns", [])


def _order_col(df):
    for f in df.schema.fields:
        if isinstance(f.dataType, (TimestampType, DateType)):
            return f.name
    for f in df.schema.fields:
        if isinstance(f.dataType, (LongType, IntegerType)):
            return f.name
    return None


def _safe_date_cast(col_name: str) -> F.Column:
    # Build a pure SQL expression string — no Column objects in f-strings.
    # Pre-strip T separator so T-literal format strings are never needed.
    cleaned_sql = (
        f"regexp_replace(`{col_name}`, "
        r"'(\d{4}-\d{2}-\d{2})[T](\d{2}:\d{2})', '$1 $2')"
    )
    return F.coalesce(*[
        F.expr(f"try_to_date({cleaned_sql}, '{fmt}')")
        for fmt in DATE_FORMATS_SQL
    ])


def _safe_timestamp_cast(col_name: str) -> F.Column:
    # Build a pure SQL expression string — no Column objects in f-strings.
    # Pre-strip timezone suffix then T separator so no T/Z literals needed.
    tz_stripped = (
        f"regexp_replace(`{col_name}`, "
        r"'([+-]\d{2}:\d{2}|Z)$', '')"
    )
    cleaned_sql = (
        f"regexp_replace({tz_stripped}, "
        r"'(\d{4}-\d{2}-\d{2})[T](\d{2}:\d{2})', '$1 $2')"
    )
    return F.coalesce(*[
        F.expr(f"try_to_timestamp({cleaned_sql}, '{fmt}')")
        for fmt in TIMESTAMP_FORMATS_SQL
    ])


def _clean_to_numeric_expr(col_name: str) -> str:
    c = f"`{col_name}`"
    c = f"regexp_replace({c}, '[£$€¥₹\\s]', '')"
    c = f"regexp_replace({c}, '\\.(\\d{{3}})', '$1')"
    c = f"regexp_replace({c}, ',(?=\\d+$)', '.')"
    c = f"regexp_replace({c}, ',', '')"
    return f"try_cast({c} AS DOUBLE)"


# =============================================================================
# STEP 1 — READ FROM BRONZE
# =============================================================================

def read_bronze(pipeline: dict):
    bronze_table = pipeline["source"]["bronze_table"]
    print(f"  READ   : {bronze_table}")
    df   = spark.table(bronze_table)
    rows = df.count()
    print(f"  Rows   : {rows:,}")
    return df, rows


# =============================================================================
# STEP 1b — PRE-COMPUTE AGGREGATE FILL VALUES
# =============================================================================

def _compute_agg_fill_values(df, column_map: list) -> list:
    df_cols_ci = {c.lower(): c for c in df.columns}
    for spec in column_map:
        handling = spec.get("null_handling", {})
        strategy = handling.get("strategy", "keep")
        if strategy not in ("fill_mean", "fill_median", "fill_min", "fill_max"):
            continue
        src    = spec.get("source_column", "")
        actual = df_cols_ci.get(src.lower()) or df_cols_ci.get(
                 spec.get("target_column", "").lower())
        if not actual:
            print(f"  ⚠  Agg : '{src}' not found — skipping {strategy}")
            continue
        try:
            num_expr = _clean_to_numeric_expr(actual)
            tmp = (df.selectExpr(f"{num_expr} AS _num_")
                     .filter(F.col("_num_").isNotNull()))
            if strategy == "fill_mean":
                val = tmp.select(F.mean("_num_")).collect()[0][0]
            elif strategy == "fill_median":
                r   = tmp.approxQuantile("_num_", [0.5], 0.01)
                val = r[0] if r else None
            elif strategy == "fill_min":
                val = tmp.select(F.min("_num_")).collect()[0][0]
            elif strategy == "fill_max":
                val = tmp.select(F.max("_num_")).collect()[0][0]
            else:
                val = None
            if val is not None:
                val = round(float(val), 4)
                spec["null_handling"]["fill_value"] = val
                print(f"  Agg    : {actual} {strategy} = {round(val, 2)}")
            else:
                print(f"  ⚠  Agg : {actual} {strategy} returned NULL")
        except Exception as exc:
            print(f"  ⚠  Agg : {actual} {strategy} failed: {exc}")
    return column_map


# =============================================================================
# STEP 2 — REPLACE NULL PLACEHOLDERS
# =============================================================================

def replace_null_placeholders(df, column_map: list):
    # FIX: clean ALL STRING columns regardless of spec presence.
    # Spec is only used to determine source_type; missing spec = treated as STRING.
    # Whitespace-only values (' ', '  ') are also nullified on every STRING column.
    ph_sql   = ", ".join(f"'{p.replace(chr(39), chr(39)+chr(39))}'"
                         for p in NULL_PLACEHOLDERS if p)
    spec_map = {s.get("source_column", "").lower(): s for s in column_map}
    exprs    = []
    replaced = 0

    for col in df.columns:
        spec     = spec_map.get(col.lower())
        src_type = (spec.get("source_type", "STRING") if spec else "STRING").upper()

        # Clean ALL STRING columns — with or without a spec entry
        if src_type == "STRING":
            exprs.append(
                f"CASE WHEN `{col}` IS NULL "
                f"     OR trim(`{col}`) = '' "
                f"     OR trim(`{col}`) IN ({ph_sql}) "
                f"THEN NULL ELSE `{col}` END AS `{col}`"
            )
            replaced += 1
        else:
            exprs.append(f"`{col}`")

    df = df.selectExpr(*exprs)
    print(f"  Nullify: {replaced} STRING column(s) cleaned")
    return df


# =============================================================================
# STEP 3 — NULL HANDLING
# =============================================================================

def apply_null_strategies(df, column_map: list):
    df_cols_ci = {c.lower(): c for c in df.columns}
    handled    = 0

    # Pass 1: drop_rows
    for spec in column_map:
        if spec.get("null_handling", {}).get("strategy") != "drop_rows":
            continue
        src    = spec.get("source_column", "")
        actual = df_cols_ci.get(src.lower()) or df_cols_ci.get(
                 spec.get("target_column", "").lower())
        if not actual:
            continue
        before  = df.count()
        df      = df.filter(F.col(f"`{actual}`").isNotNull())
        removed = before - df.count()
        print(f"  Null   : {actual} → drop rows ({removed:,} removed)")
        handled += 1
        df_cols_ci = {c.lower(): c for c in df.columns}

    order_col = _order_col(df)
    need_row_order = (order_col is None) and any(
        spec.get("null_handling", {}).get("strategy") in ("forward_fill", "backward_fill")
        for spec in column_map
    )
    if need_row_order:
        df        = df.withColumn("_row_order_", F.monotonically_increasing_id())
        order_col = "_row_order_"
        print(f"  Null   : using row insertion order for fill")

    # Pass 2: selectExpr for all other strategies
    exprs = []
    for col in df.columns:
        if col == "_row_order_":
            continue
        spec = next(
            (s for s in column_map
             if s.get("source_column", "").lower() == col.lower()
             or s.get("target_column", "").lower() == col.lower()), None
        )
        if not spec:
            exprs.append(f"`{col}`")
            continue
        handling = spec.get("null_handling", {})
        strategy = handling.get("strategy", "keep")

        if strategy in ("keep", "drop_rows"):
            exprs.append(f"`{col}`")
        elif strategy in ("fill_constant", "fill_mean", "fill_median",
                          "fill_min", "fill_max"):
            fv = handling.get("fill_value")
            if fv is None:
                exprs.append(f"`{col}`")
                print(f"  ⚠  Null : {col} {strategy} — no fill_value, keeping NULLs")
            else:
                fv_str = str(fv).replace("'", "''")
                exprs.append(
                    f"CASE WHEN `{col}` IS NULL "
                    f"THEN '{fv_str}' "
                    f"ELSE `{col}` END AS `{col}`"
                )
                label = "fill constant" if strategy == "fill_constant" else strategy
                print(f"  Null   : {col} → {label} ('{fv}')")
                handled += 1
        elif strategy == "forward_fill":
            exprs.append(
                f"LAST(`{col}`, TRUE) OVER ("
                f"ORDER BY `{order_col}` "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                f") AS `{col}`"
            )
            print(f"  Null   : {col} → forward fill")
            handled += 1
        elif strategy == "backward_fill":
            exprs.append(
                f"FIRST(`{col}`, TRUE) OVER ("
                f"ORDER BY `{order_col}` "
                f"ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"
                f") AS `{col}`"
            )
            print(f"  Null   : {col} → backward fill")
            handled += 1
        else:
            exprs.append(f"`{col}`")

    df = df.selectExpr(*exprs)
    if "_row_order_" in df.columns:
        df = df.drop("_row_order_")
    print(f"  Null   : {handled} column(s) processed"
          if handled else "  Null   : no strategies applied")
    return df


# =============================================================================
# STEP 3b — NUMERIC NULL FILL (mean / median / min / max)
# =============================================================================

def apply_numeric_null_fill(df, column_map: list):
    """
    Fills NULL values for columns whose null_handling strategy is
    fill_mean / fill_median / fill_min / fill_max using the fill_value
    already computed and stored in column_map by _compute_agg_fill_values.

    Runs after apply_null_strategies to ensure numeric precision —
    Step 3 coerces fill_value to string (works for STRING columns),
    this step applies a properly typed F.lit() cast to the column's
    actual Spark type for numeric columns.

    Matches source_column first, target_column as fallback.
    """
    NUMERIC_STRATEGIES = {"fill_mean", "fill_median", "fill_min", "fill_max"}
    NUMERIC_TYPES      = (DoubleType, FloatType, IntegerType, LongType)

    df_cols_ci = {c.lower(): c for c in df.columns}
    filled     = 0

    for spec in column_map:
        strategy = spec.get("null_handling", {}).get("strategy", "keep")
        if strategy not in NUMERIC_STRATEGIES:
            continue

        fill_value = spec.get("null_handling", {}).get("fill_value")
        if fill_value is None:
            print(f"  ⚠  NumFill: no fill_value for "
                  f"'{spec.get('source_column', '')}' — skipping")
            continue

        # Resolve actual column name (source first, target as fallback)
        src    = spec.get("source_column", "")
        tgt    = spec.get("target_column", src)
        actual = df_cols_ci.get(src.lower()) or df_cols_ci.get(tgt.lower())
        if not actual:
            print(f"  ⚠  NumFill: column '{src}' not found in DataFrame — skipping")
            continue

        # Determine fill expression — cast to column's actual Spark type
        field = next((f for f in df.schema.fields if f.name == actual), None)
        if field and isinstance(field.dataType, NUMERIC_TYPES):
            fill_expr = F.lit(float(fill_value)).cast(field.dataType)
        else:
            # Column is still STRING at this stage (cast happens in Step 10)
            fill_expr = F.lit(str(fill_value))

        # Also treat whitespace-only values as missing — catches survivors
        # that slipped past Step 2 before try_cast turns them into NULL in Step 10
        is_missing = (
            F.col(f"`{actual}`").isNull() |
            (F.trim(F.col(f"`{actual}`").cast(StringType())) == "")
        )

        df = df.withColumn(
            actual,
            F.when(is_missing, fill_expr)
             .otherwise(F.col(f"`{actual}`"))
        )
        print(f"  NumFill: {actual:<30} → {strategy} = {fill_value}")
        filled += 1

    if filled == 0:
        print("  NumFill: no numeric fill strategies applied")
    return df


# =============================================================================
# STEP 8 — VALUE STANDARDIZATION (pure CASE WHEN, exact match)
# =============================================================================

def _resolve_taxonomy_table(pipeline: dict) -> str:
    """
    Resolve the silver_taxonomy_lookup table name.
    Tries three locations:
      1. pipeline["operations"]["taxonomy_table_name"]
      2. pipeline["transformations"]["taxonomy_table_name"]
      3. Derived from silver target: catalog.schema.silver_taxonomy_lookup
    """
    name = pipeline.get("operations", {}).get("taxonomy_table_name", "")
    if name:
        return name
    name = pipeline.get("transformations", {}).get("taxonomy_table_name", "")
    if name:
        return name
    target = pipeline.get("target", {}).get("table", "")
    if target:
        parts = target.replace("`", "").split(".")
        if len(parts) >= 2:
            return f"{parts[0]}.{parts[1]}.silver_taxonomy_lookup"
    return ""


def _read_taxonomy_mappings(taxonomy_table: str, bronze_table: str) -> dict:
    """
    Read silver_taxonomy_lookup and return:
      {source_column_lower: {raw_value_exact: standardized_value, ...}, ...}

    NOTE: raw_value is stored AS-IS (with original spaces, casing) from
    silver_mapping.py. We do NOT normalize here — exact match is used in
    the CASE WHEN so the key must match the source value character-for-character.

    Filter: prefer rows matching the bare bronze table name.
    Fallback: load all rows when 0 match (column name disambiguates).
    """
    try:
        if not spark.catalog.tableExists(taxonomy_table):
            print(f"  Taxonomy: '{taxonomy_table}' not found — skipping Step 8")
            return {}

        bare_table = (
            bronze_table.replace("`", "").rsplit(".", 1)[-1].strip().lower()
            if bronze_table else ""
        )

        tax_df = (
            spark.table(taxonomy_table)
            .filter(F.col("standardized_value").isNotNull())
            .filter(F.col("raw_value").isNotNull())
        )

        total_rows = tax_df.count()
        print(f"  Taxonomy: {total_rows} total row(s) in {taxonomy_table}")

        if not total_rows:
            print(f"  Taxonomy: empty — skipping Step 8")
            return {}

        if bare_table:
            filtered = tax_df.filter(
                F.lower(F.trim(F.col("source_table"))) == F.lit(bare_table)
            )
            n = filtered.count()
            print(f"  Taxonomy: {n} row(s) match source_table='{bare_table}'")
            if n > 0:
                tax_df = filtered
            else:
                print(f"  Taxonomy: no table-level match — loading all "
                      f"{total_rows} rows (column name disambiguates)")

        rows = tax_df.select("source_column", "raw_value",
                             "standardized_value").collect()
        mappings: dict = {}
        for row in rows:
            col_name = (row["source_column"] or "").strip().lower()
            # NOTE: raw_value kept exactly as stored — no normalization
            raw      = (row["raw_value"] or "")
            std      = (row["standardized_value"] or "").strip()
            if col_name and raw:
                mappings.setdefault(col_name, {})[raw] = std

        total = sum(len(v) for v in mappings.values())
        print(f"  Taxonomy: {total} mapping(s) loaded for "
              f"{len(mappings)} column(s)")
        return mappings

    except Exception as e:
        print(f"  ⚠  Taxonomy read error: {e}")
        return {}


def _fuzzy_extend_mappings(
    df, actual_col: str, existing_mappings: dict,
    canonical_values: list,
    taxonomy_table: str, source_schema: str, source_table: str,
    source_column: str, workspace_url: str = "", token: str = "",
    warehouse_id: str = "",
) -> dict:
    """
    Fast fuzzy extension — runs on DISTINCT unmapped values only, not all rows.

    Why this is fast:
      A 5M-row categorical column has ~200-500 distinct values.
      Fuzzy-matching 300 distinct values against 83 canonicals = 24,900
      comparisons in Python — under 1 second.
      The result is added to the CASE WHEN chain as SQL literals,
      which runs at JVM speed on all 5M rows. No Python UDF overhead.

    Returns extended mappings dict (existing + fuzzy-discovered).
    Also commits new fuzzy mappings to taxonomy so next run uses CASE WHEN.
    """
    import difflib as _dl

    THRESHOLD = 0.72

    def _norm(s):
        if not s: return ""
        s = s.strip().lower()
        s = re.sub(r"[-_]", " ", s)
        s = re.sub(r"\s+", " ", s)
        return s.strip()

    def _combined(a_n, b_n):
        lev = _dl.SequenceMatcher(None, a_n, b_n).ratio()
        sa  = "".join(sorted(a_n.replace(" ", "")))
        sb  = "".join(sorted(b_n.replace(" ", "")))
        srt = _dl.SequenceMatcher(None, sa, sb).ratio()
        return (lev * 0.6) + (srt * 0.4)

    canon_pairs = [(c, _norm(c)) for c in canonical_values]

    # Step 1: collect distinct values from this column that are NOT
    # already in existing_mappings (exact key match)
    try:
        distinct_rows = (
            df.select(F.col(f"`{actual_col}`").alias("_v"))
              .filter(F.col("_v").isNotNull())
              .filter(F.trim(F.col("_v")) != "")
              .distinct()
              .collect()
        )
    except Exception as e:
        print(f"  ⚠  Fuzzy: could not collect distinct values for {actual_col}: {e}")
        return existing_mappings

    unmapped_vals = []
    new_mappings  = {}
    for row in distinct_rows:
        val = str(row["_v"])   # preserve exact value including spaces
        if not val.strip():
            continue
        # Already handled by exact CASE WHEN lookup → skip
        if val in existing_mappings:
            continue
        # Value is exactly a canonical → map directly
        matched_canon = next(
            (c for c in canonical_values if val == c), None
        )
        if matched_canon:
            new_mappings[val] = matched_canon
            continue
        unmapped_vals.append(val)

    if not unmapped_vals and not new_mappings:
        print(f"  Fuzzy  : {actual_col:<30} → no unmapped values")
        return existing_mappings

    # Step 2: fuzzy-match each unmapped distinct value against canonicals
    for val in unmapped_vals:
        vn = _norm(val)   # normalize only for similarity scoring
        best_score, best_canon = 0.0, None
        for canon, canon_n in canon_pairs:
            score = _combined(vn, canon_n)
            if score > best_score:
                best_score, best_canon = score, canon
        if best_score >= THRESHOLD and best_canon:
            new_mappings[val] = best_canon   # store exact raw value as key

    if not new_mappings:
        print(f"  Fuzzy  : {actual_col:<30} → {len(unmapped_vals)} unmapped, none above threshold")
        return existing_mappings

    print(f"  Fuzzy  : {actual_col:<30} → {len(new_mappings)}/{len(unmapped_vals)} "
          f"unmapped values fuzzy-mapped")

    # Step 3: commit new fuzzy mappings to taxonomy (exact raw value stored)
    if taxonomy_table and new_mappings:
        try:
            from datetime import datetime, timezone
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            def esc(s): return str(s).replace("'", "''")
            rows = [
                f"('{esc(source_schema)}','{esc(source_table)}',"
                f"'{esc(source_column)}','{esc(raw)}',"
                f"'{esc(canon)}','STRING','high',TO_TIMESTAMP('{now_str}'))"
                for raw, canon in new_mappings.items()
            ]
            merge_sql = f"""MERGE INTO {taxonomy_table} AS target
USING (
    SELECT col0 AS source_schema, col1 AS source_table,
           col2 AS source_column, col3 AS raw_value,
           col4 AS standardized_value, col5 AS data_type,
           col6 AS confidence,         col7 AS last_updated
    FROM (VALUES {','.join(rows)})
    AS t(col0,col1,col2,col3,col4,col5,col6,col7)
) AS source
ON  target.source_schema = source.source_schema
AND target.source_table  = source.source_table
AND target.source_column = source.source_column
AND target.raw_value     = source.raw_value
WHEN MATCHED THEN UPDATE SET
    standardized_value = source.standardized_value,
    last_updated       = source.last_updated
WHEN NOT MATCHED THEN INSERT
    (source_schema,source_table,source_column,raw_value,
     standardized_value,data_type,confidence,last_updated)
VALUES
    (source.source_schema,source.source_table,source.source_column,
     source.raw_value,source.standardized_value,source.data_type,
     source.confidence,source.last_updated)"""
            spark.sql(merge_sql)
            print(f"  Fuzzy  : {len(rows)} new mapping(s) committed to taxonomy")
        except Exception as e:
            print(f"  ⚠  Fuzzy: taxonomy commit failed (non-fatal): {e}")

    # Return merged mappings for CASE WHEN
    merged = dict(existing_mappings)
    merged.update(new_mappings)
    return merged


def apply_value_standardization(df, column_map: list, pipeline: dict):
    """
    Step 8 — pure CASE WHEN exact match against silver_taxonomy_lookup.

    silver_mapping.py stores raw_value exactly as it appears in source
    (including all spaces, original casing). The CASE WHEN here matches
    the source column value character-for-character against taxonomy
    raw_value — NO normalization on either side.

    If a value in the data has no exact row in taxonomy, it passes through
    unchanged. To fix it: re-run silver_mapping.py → approve → re-run this.
    """
    taxonomy_table = _resolve_taxonomy_table(pipeline)
    if not taxonomy_table:
        print("  Standardize: could not determine taxonomy table — skipping")
        return df

    print(f"  Taxonomy table : {taxonomy_table}")
    bronze_table      = pipeline.get("source", {}).get("bronze_table", "")
    taxonomy_mappings = _read_taxonomy_mappings(taxonomy_table, bronze_table)

    if not taxonomy_mappings:
        print("  Standardize: no mappings — skipping")
        return df

    df_cols_ci = {c.lower(): c for c in df.columns}

    # Build candidate columns from taxonomy keys
    candidate_cols: dict = {}
    for col_lower in taxonomy_mappings:
        actual = df_cols_ci.get(col_lower)
        if actual:
            candidate_cols[col_lower] = actual
        else:
            print(f"  ⚠  Taxonomy col '{col_lower}' not in DataFrame — skipping")

    # Also honour inline mappings from column_map (override taxonomy)
    for col_spec in column_map:
        src_col = col_spec.get("source_column", "").strip()
        std_cfg = col_spec.get("value_standardization", {})
        if std_cfg.get("enabled", False) and std_cfg.get("mappings"):
            col_lower = src_col.lower()
            actual    = df_cols_ci.get(col_lower)
            if actual and col_lower not in candidate_cols:
                candidate_cols[col_lower] = actual

    if not candidate_cols:
        print("  Standardize: no matching columns in DataFrame — skipping")
        return df

    standardized = 0
    full_table    = pipeline.get("source", {}).get("bronze_table", "")
    for col_lower, actual_col in candidate_cols.items():

        tax_for_col = taxonomy_mappings.get(col_lower, {})

        # Inline overrides
        inline = {}
        col_spec = next(
            (s for s in column_map
             if s.get("source_column", "").strip().lower() == col_lower), None
        )
        if col_spec:
            std_cfg = col_spec.get("value_standardization", {})
            if std_cfg.get("enabled", False):
                inline = {
                    k: v
                    for k, v in std_cfg.get("mappings", {}).items()
                }

        mappings: dict = {}
        mappings.update(tax_for_col)
        mappings.update(inline)   # inline overrides taxonomy

        if not mappings:
            continue

        # ── Fuzzy extension FIRST (before CASE WHEN) ─────────────────────
        canonical_from_mappings = set(mappings.values())
        canonical_from_taxonomy = set()
        if taxonomy_table:
            try:
                tax_canon_rows = spark.table(taxonomy_table).filter(
                    F.lower(F.col("source_column")) == F.lit(col_lower)
                ).select("standardized_value").distinct().collect()
                canonical_from_taxonomy = {
                    r["standardized_value"].strip()
                    for r in tax_canon_rows
                    if r["standardized_value"]
                }
            except Exception:
                pass

        canonical_values = sorted(canonical_from_mappings | canonical_from_taxonomy)

        if len(canonical_values) >= 1:
            parts_resolved = full_table.replace("`","").split(".")
            src_schema_fz  = parts_resolved[1] if len(parts_resolved) >= 3 else "bronze"
            src_table_fz   = parts_resolved[-1]
            mappings = _fuzzy_extend_mappings(
                df                = df,
                actual_col        = actual_col,
                existing_mappings = mappings,
                canonical_values  = canonical_values,
                taxonomy_table    = taxonomy_table,
                source_schema     = src_schema_fz,
                source_table      = src_table_fz,
                source_column     = actual_col,
            )

        # ── CASE WHEN — exact match, no normalization ─────────────────────
        # raw_value in taxonomy is stored exactly as it appears in source.
        # Source column value is compared character-for-character.
        # No trim/lower/regexp on either side.
        # Built as SQL string to avoid Python recursion depth on large mappings.
        def _esc(s):
            return str(s).replace("'", "''")

        when_clauses = " ".join(
            f"WHEN `{actual_col}` = '{_esc(raw_val)}' THEN '{_esc(std_val)}'"
            for raw_val, std_val in mappings.items()
        )
        sql_case = f"CASE {when_clauses} ELSE `{actual_col}` END"
        df = df.withColumn(actual_col, F.expr(sql_case))

        # Lowercase the standardized value — normalize output to small letters
        df = df.withColumn(actual_col, F.lower(F.col(f"`{actual_col}`")))

        parts = []
        if tax_for_col: parts.append(f"{len(tax_for_col)} taxonomy")
        if inline:      parts.append(f"{len(inline)} inline")
        print(f"  Standardize: {actual_col:<30} ← "
              f"{' + '.join(parts)}  ({len(mappings)} total mappings)")
        standardized += 1

    if standardized == 0:
        print("  Standardize: no columns standardized")
    else:
        print(f"  Standardize: {standardized} column(s) standardized")
    return df


# =============================================================================
# STEP 5 — STRING RULES
# =============================================================================

def apply_string_rules(df, column_map: list):
    for entry in column_map:
        src   = entry.get("source_column", "")
        rules = entry.get("string_rules", {})
        if not rules or rules.get("skip") or src not in df.columns:
            continue
        expr = F.col(f"`{src}`")
        if rules.get("trim"):
            expr = F.trim(expr)
        casing = rules.get("casing", "")
        if casing == "lower":   expr = F.lower(expr)
        elif casing == "upper": expr = F.upper(expr)
        elif casing == "title": expr = F.initcap(expr)
        if rules.get("strip_special"):
            expr = F.trim(F.regexp_replace(
                F.regexp_replace(expr, r"[^a-zA-Z0-9\s_]", ""), r"\s+", " "))
        df = df.withColumn(src, expr)
    return df


# =============================================================================
# STEP 6 — DATE RULES
# =============================================================================

def apply_date_rules(df, column_map: list):
    for entry in column_map:
        src   = entry.get("source_column", "")
        rules = entry.get("date_rules", {})
        if not rules or rules.get("skip") or src not in df.columns:
            continue
        src_fmt = rules.get("source_format", "")
        tgt_fmt = rules.get("target_format", "yyyy-MM-dd")
        if not src_fmt:
            continue
        src_fmt_sql = src_fmt.replace("'T'", " ").replace('"T"', " ")
        if "HH" in src_fmt:
            df = df.withColumn(src,
                F.regexp_replace(F.col(f"`{src}`"),
                    r"(\d{4}-\d{2}-\d{2})[T](\d{2}:\d{2})", "$1 $2"))
            df = df.withColumn(src, F.date_format(
                F.expr(f"try_to_timestamp(`{src}`, '{src_fmt_sql}')"), tgt_fmt))
        else:
            df = df.withColumn(src, F.date_format(
                F.expr(f"try_to_date(`{src}`, '{src_fmt_sql}')"), tgt_fmt))
    return df


# =============================================================================
# STEP 7 — ADVANCED RULES
# =============================================================================

BOOL_TRUE  = {"true","yes","1","y","t","on","active","enabled"}
BOOL_FALSE = {"false","no","0","n","f","off","inactive","disabled"}
COUNTRY_A2 = {
    "united states":"US","usa":"US","us":"US","united kingdom":"GB","uk":"GB",
    "india":"IN","germany":"DE","france":"FR","canada":"CA","australia":"AU",
    "japan":"JP","china":"CN","brazil":"BR","mexico":"MX",
}
A2_TO_A3 = {
    "US":"USA","GB":"GBR","IN":"IND","DE":"DEU","FR":"FRA",
    "CA":"CAN","AU":"AUS","JP":"JPN","CN":"CHN","BR":"BRA","MX":"MEX",
}


def apply_advanced_rules(df, column_map: list):
    for entry in column_map:
        src = entry.get("source_column", "")
        if not src or src not in df.columns:
            continue

        phone = entry.get("phone_rules", {})
        if phone:
            cc = re.sub(r"[^\d]", "", phone.get("country_code", "+1"))
            @F.udf(StringType())
            def _phone(val, c=cc):
                if val is None: return None
                d = re.sub(r"[^\d]", "", str(val))
                if not d: return None
                return f"+{d}" if (len(d) > 10 and d.startswith(c)) else f"+{c}{d}"
            df = df.withColumn(src, _phone(F.col(f"`{src}`")))

        email = entry.get("email_rules", {})
        if email and not email.get("skip") and email.get("normalize"):
            df = df.withColumn(src, F.lower(F.trim(F.col(f"`{src}`"))))

        curr = entry.get("currency_rules", {})
        if curr:
            df = df.withColumn(src,
                F.regexp_replace(F.col(f"`{src}`").cast("string"),
                    r"[^\d.\-]", "").cast(DoubleType()))

        country = entry.get("country_rules", {})
        if country and not country.get("skip"):
            fmt = country.get("format", "alpha-2")
            a2, a3 = COUNTRY_A2, A2_TO_A3
            @F.udf(StringType())
            def _country(val, f=fmt):
                if val is None: return None
                v  = val.strip().lower()
                c2 = a2.get(v, v.upper()[:2])
                return a3.get(c2, c2) if f == "alpha-3" else c2
            df = df.withColumn(src, _country(F.col(f"`{src}`")))

        gender = entry.get("gender_rules", {})
        if gender and not gender.get("skip"):
            vocab  = gender.get("vocab", "M/F/O")
            m_vals = {"m","male","man","1","mr"}
            f_vals = {"f","female","woman","2","ms","mrs"}
            m_o = {"M/F/O":"M","male/female/other":"male","1/2/0":"1"}[vocab]
            f_o = {"M/F/O":"F","male/female/other":"female","1/2/0":"2"}[vocab]
            o_o = {"M/F/O":"O","male/female/other":"other","1/2/0":"0"}[vocab]
            @F.udf(StringType())
            def _gender(val, m=m_o, fv=f_o, o=o_o):
                if val is None: return None
                v = val.strip().lower()
                if v in m_vals: return m
                if v in f_vals: return fv
                return o
            df = df.withColumn(src, _gender(F.col(f"`{src}`")))

        boolean = entry.get("boolean_rules", {})
        if boolean and not boolean.get("skip"):
            fmt = boolean.get("format", "true/false")
            t_  = {"true/false":"true","1/0":"1","yes/no":"yes"}[fmt]
            f_  = {"true/false":"false","1/0":"0","yes/no":"no"}[fmt]
            @F.udf(StringType())
            def _bool(val, tv=t_, fv=f_):
                if val is None: return None
                v = str(val).strip().lower()
                if v in BOOL_TRUE:  return tv
                if v in BOOL_FALSE: return fv
                return None
            df = df.withColumn(src, _bool(F.col(f"`{src}`")))

        address = entry.get("address_rules", {})
        if address and not address.get("skip"):
            if address.get("mode") == "parse":
                for idx, part in enumerate(["street","city","state_zip","country"]):
                    df = df.withColumn(f"{src}_{part}",
                        F.trim(F.split(F.col(f"`{src}`"), ",")[idx]))
            else:
                df = df.withColumn(src, F.initcap(F.trim(
                    F.regexp_replace(F.col(f"`{src}`"), r"\s+", " "))))
    return df


# =============================================================================
# STEP 4 — DEDUPLICATION
# =============================================================================

def deduplicate(df, ops: dict):
    strategy = ops.get("dedup_strategy", {})
    dtype    = strategy.get("type", "full") if strategy else "full"
    if not ops.get("drop_duplicates", True) or dtype == "skip":
        print(f"  Dedup  : disabled")
        return df
    before = df.count()
    if dtype == "key":
        keys   = strategy.get("keys", [])
        keep   = strategy.get("keep", "first")
        ts_col = strategy.get("timestamp_col", "")
        if not keys:
            df = df.dropDuplicates()
        elif keep == "latest" and ts_col and ts_col in df.columns:
            window = Window.partitionBy(*keys).orderBy(F.col(ts_col).desc())
            df     = (df.withColumn("_rn", F.row_number().over(window))
                        .filter(F.col("_rn") == 1).drop("_rn"))
        else:
            df = df.dropDuplicates(keys)
    else:
        df = df.dropDuplicates()
    removed = before - df.count()
    if removed > 0:
        print(f"  Dedup  : {removed:,} duplicate rows removed ({dtype})")
    else:
        print(f"  Dedup  : no duplicates found")
    return df


# =============================================================================
# STEP 9 — RENAME COLUMNS
# =============================================================================

def apply_renames(df, column_map: list):
    df_lower = {c.lower(): c for c in df.columns}
    renamed, dropped = [], []
    for spec in column_map:
        src    = spec.get("source_column", "")
        tgt    = spec.get("target_column", src)
        rule   = spec.get("transformation", "keep").lower()
        actual = df_lower.get(src.lower())
        if actual is None:
            continue
        if rule == "drop":
            df       = df.drop(actual)
            df_lower = {c.lower(): c for c in df.columns}
            dropped.append(src)
        elif actual != tgt:
            df       = df.withColumnRenamed(actual, tgt)
            df_lower = {c.lower(): c for c in df.columns}
            renamed.append(f"{actual} → {tgt}")
    if renamed:
        print(f"  Rename : {len(renamed)} column(s)  "
              f"({', '.join(renamed[:5])}{'…' if len(renamed)>5 else ''})")
    if dropped:
        print(f"  Dropped: {', '.join(dropped)}")
    if not renamed and not dropped:
        print(f"  Rename : no renames needed")
    return df


# =============================================================================
# STEP 10 — TYPE CASTING
# =============================================================================

def apply_type_casts(df, column_map: list):
    # FIX: after each cast, re-apply fill_value for any column that has a
    # fill strategy — try_cast silently produces NULL for unparseable values;
    # the post-cast fill catches those.
    NUMERIC_STRATEGIES = {"fill_mean", "fill_median", "fill_min", "fill_max",
                          "fill_constant"}
    NUMERIC_TYPES      = (DoubleType, FloatType, IntegerType, LongType)

    # Build lookup: target_column → fill_value
    fill_lookup = {}
    for spec in column_map:
        strategy   = spec.get("null_handling", {}).get("strategy", "keep")
        fill_value = spec.get("null_handling", {}).get("fill_value")
        tgt        = spec.get("target_column", spec.get("source_column", ""))
        if strategy in NUMERIC_STRATEGIES and fill_value is not None and tgt:
            fill_lookup[tgt] = fill_value

    casted = 0
    for spec in column_map:
        tgt_col     = spec.get("target_column", "")
        target_type = spec.get("target_type", "").upper().strip()
        source_type = spec.get("source_type", "STRING").upper().strip()
        cast_applied = (spec.get("cast_recommended", False)
                        or spec.get("cast_applied", False))
        if not target_type or target_type == "STRING":
            continue
        if target_type == source_type and not cast_applied:
            continue
        if not tgt_col or tgt_col not in df.columns:
            continue
        try:
            if target_type == "DATE":
                df = df.withColumn(tgt_col, _safe_date_cast(tgt_col))
                print(f"  Cast   : {tgt_col:<30} → DATE")
                casted += 1
            elif target_type == "TIMESTAMP":
                # T separator and timezone suffix stripped inside _safe_timestamp_cast
                df = df.withColumn(tgt_col, _safe_timestamp_cast(tgt_col))
                print(f"  Cast   : {tgt_col:<30} → TIMESTAMP")
                casted += 1
            elif target_type in ("BIGINT", "LONG"):
                sample = [r[tgt_col] for r in
                          df.select(tgt_col).filter(
                              F.col(f"`{tgt_col}`").isNotNull()
                          ).limit(5).collect()]
                if any(re.search(r"[a-zA-Z_\-]", str(v))
                       for v in sample if v is not None):
                    print(f"  ⚠  Cast : {tgt_col} BIGINT skipped "
                          f"— values contain letters — keeping STRING")
                else:
                    df = df.withColumn(tgt_col,
                        F.expr(f"try_cast(`{tgt_col}` AS BIGINT)"))
                    print(f"  Cast   : {tgt_col:<30} → BIGINT")
                    casted += 1
            elif target_type in ("INT", "INTEGER"):
                sample = [r[tgt_col] for r in
                          df.select(tgt_col).filter(
                              F.col(f"`{tgt_col}`").isNotNull()
                          ).limit(5).collect()]
                if any(re.search(r"[a-zA-Z_\-]", str(v))
                       for v in sample if v is not None):
                    print(f"  ⚠  Cast : {tgt_col} INT skipped — keeping STRING")
                else:
                    df = df.withColumn(tgt_col,
                        F.expr(f"try_cast(`{tgt_col}` AS INT)"))
                    print(f"  Cast   : {tgt_col:<30} → INT")
                    casted += 1
            elif target_type == "DOUBLE":
                clean = _clean_to_numeric_expr(tgt_col)
                df = df.selectExpr(
                    *[f"{clean} AS `{tgt_col}`" if c == tgt_col
                      else f"`{c}`" for c in df.columns])
                print(f"  Cast   : {tgt_col:<30} → DOUBLE")
                casted += 1
            elif target_type == "FLOAT":
                inner = _clean_to_numeric_expr(tgt_col).replace(
                    "try_cast(","",1).rsplit(" AS DOUBLE)",1)[0]
                df = df.selectExpr(
                    *[f"try_cast({inner} AS FLOAT) AS `{tgt_col}`"
                      if c == tgt_col else f"`{c}`" for c in df.columns])
                print(f"  Cast   : {tgt_col:<30} → FLOAT")
                casted += 1
            elif target_type.startswith("DECIMAL"):
                inner = _clean_to_numeric_expr(tgt_col).replace(
                    "try_cast(","",1).rsplit(" AS DOUBLE)",1)[0]
                df = df.selectExpr(
                    *[f"try_cast({inner} AS {target_type}) AS `{tgt_col}`"
                      if c == tgt_col else f"`{c}`" for c in df.columns])
                print(f"  Cast   : {tgt_col:<30} → {target_type}")
                casted += 1
            elif target_type == "BOOLEAN":
                df = df.withColumn(tgt_col,
                    F.expr(f"try_cast(`{tgt_col}` AS BOOLEAN)"))
                print(f"  Cast   : {tgt_col:<30} → BOOLEAN")
                casted += 1
            # Post-cast fill — re-apply fill_value for NULLs introduced by try_cast
            if tgt_col in fill_lookup and tgt_col in df.columns:
                fv    = fill_lookup[tgt_col]
                field = next((f for f in df.schema.fields
                              if f.name == tgt_col), None)
                if field and isinstance(field.dataType, NUMERIC_TYPES):
                    fill_expr = F.lit(float(fv)).cast(field.dataType)
                else:
                    fill_expr = F.lit(str(fv))
                df = df.withColumn(
                    tgt_col,
                    F.when(F.col(f"`{tgt_col}`").isNull(), fill_expr)
                     .otherwise(F.col(f"`{tgt_col}`"))
                )
                print(f"  PostFill: {tgt_col:<29} → refilled after cast "
                      f"(fill_value={fv})")

        except Exception as exc:
            print(f"  ⚠  Cast failed: {tgt_col} → {target_type}: {exc}")

    if casted == 0:
        print(f"  Cast   : no type changes required")
    return df


    # =============================================================================
# STEP 10b — FINAL STRING NORMALISATION (trim + lowercase all STRING columns)
# =============================================================================

def apply_final_string_normalisation(df):
    """
    After all transformations, trim whitespace and lowercase every STRING column.
    Runs on the final DataFrame before write — catches any survivors from any step.
    Non-STRING columns (DATE, TIMESTAMP, DOUBLE, INT etc.) are untouched.
    NULL values stay NULL — not converted to the string 'null'.
    """
    normalised = 0
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(
                    F.col(f"`{field.name}`").isNotNull(),
                    F.lower(F.trim(F.col(f"`{field.name}`")))
                ).otherwise(F.lit(None).cast(StringType()))
            )
            normalised += 1
    print(f"  FinalNorm: {normalised} STRING column(s) trimmed and lowercased")
    return df


# =============================================================================
# STEP 11 — WRITE TO SILVER
# =============================================================================

def write_silver(df, pipeline: dict):
    silver_table   = pipeline["target"]["table"]
    write_strategy = pipeline.get("transformations", {}).get(
        "write_strategy", "overwrite")
    row_count = df.count()
    print(f"  WRITE  : {silver_table}")
    print(f"  Mode   : {write_strategy}  |  Rows: {row_count:,}")
    try:
        (df.write.format("delta")
           .mode(write_strategy)
           .option("overwriteSchema", "true")
           .saveAsTable(silver_table))
    except Exception as e:
        if "schema" in str(e).lower() or "ACL" in str(e):
            print(f"  ⚠  Schema conflict — dropping and recreating")
            spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
            (df.write.format("delta")
               .mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable(silver_table))
        else:
            raise
    print(f"  ✅ Done : {silver_table}  ({row_count:,} rows written)")


# =============================================================================
# MAIN EXECUTION LOOP
# =============================================================================

for pipe in pipelines:
    pipeline_id  = pipe["pipeline_id"]
    ops          = pipe.get("operations", pipe.get("transformations", {}))
    bronze_table = pipe["source"]["bronze_table"]
    column_map   = resolve_column_map(pipe)

    print("\n" + "=" * 62)
    print(f"  PIPELINE : {pipeline_id}")
    print(f"  SOURCE   : {bronze_table}")
    print(f"  TARGET   : {pipe['target']['table']}")
    print(f"  COLUMNS  : {len(column_map)}")
    null_cols = [c for c in column_map
                 if c.get("null_handling", {}).get("strategy", "keep") != "keep"]
    print(f"  NULL STRATEGIES : {len(null_cols)} column(s) configured")
    print("=" * 62)

    try:
        print(f"\n  ── Step 1: Read ──")
        df, total_rows = read_bronze(pipe)

        print(f"\n  ── Step 1b: Pre-compute Aggregate Fill Values ──")
        column_map = _compute_agg_fill_values(df, column_map)
        try:
            pipe["column_map"] = column_map
            with open(MIGRATION_PLAN_PATH, "w") as _f:
                json.dump(cfg, _f, indent=2, ensure_ascii=False)
            print(f"  Plan   : fill values saved → {MIGRATION_PLAN_PATH}")
        except Exception as _e:
            print(f"  ⚠  Plan : could not save fill values: {_e}")

        print(f"\n  ── Step 2: Null Placeholder Cleanup ──")
        df = replace_null_placeholders(df, column_map)

        print(f"\n  ── Step 3: Null Handling ──")
        df = apply_null_strategies(df, column_map)

        print(f"\n  ── Step 3b: Numeric Null Fill ──")
        df = apply_numeric_null_fill(df, column_map)

        print(f"\n  ── Step 4: Deduplication ──")
        df = deduplicate(df, ops)

        print(f"\n  ── Step 5: String Rules ──")
        df = apply_string_rules(df, column_map)

        print(f"\n  ── Step 6: Date Rules ──")
        df = apply_date_rules(df, column_map)

        print(f"\n  ── Step 7: Advanced Rules ──")
        df = apply_advanced_rules(df, column_map)

        print(f"\n  ── Step 8: Value Standardization (CASE WHEN) ──")
        df = apply_value_standardization(df, column_map, pipe)

        print(f"\n  ── Step 9: Rename Columns ──")
        df = apply_renames(df, column_map)

        print(f"\n  ── Step 10: Type Casting ──")
        df = apply_type_casts(df, column_map)

        print(f"\n  ── Step 10b: Final String Normalisation ──")
        df = apply_final_string_normalisation(df)

        print(f"\n  ── Step 11: Write to Silver ──")
        write_silver(df, pipe)

    except Exception as e:
        print(f"\n  ❌ FAILED  : {pipeline_id}")
        print(f"     ERROR   : {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 62)
print("  ✅  SILVER INGESTION COMPLETE")
print("=" * 62)