"""
executor.py — Pipeline Executor (Databricks Notebook)
=======================================================
Step 9 of the orchestration pipeline.

Reads migration_plan.json and executes data movement into
Databricks Delta (Unity Catalog) Bronze layer.

Supported sources:
    s3         → spark, full or incremental (watermark filter on df)
    azure_sql  → spark, full or incremental (WHERE pushdown to SQL)
    postgresql → spark, full or incremental (WHERE pushdown to SQL)
    rest_api   → python (requests), always incremental via
                 bronze_watermark_control table

Watermark pattern (REST API):
    1. Read last_watermark_value  from bronze_watermark_control
    2. Broad API call             → (watermark - buffer) to (now + buffer)
    3. Precise in-memory filter   → watermark < ts <= now - lag_minutes
    4. MERGE into target Delta table
    5. Update bronze_watermark_control → only after successful write

Watermark pattern (S3 / SQL):
    1. Read last_watermark_value  from bronze_watermark_control
    2. Filter df / push WHERE to source
    3. Append / MERGE into target Delta table
    4. Update bronze_watermark_control → only after successful write
"""

import json
import requests
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F


# ======================================================================
# WATERMARK CONTROL TABLE
# ======================================================================

def ensure_watermark_table(watermark_control_table: str, pipelines: list):
    """
    Create bronze_watermark_control if it does not exist.
    Seeds one Pending row per REST API pipeline using the UNIQUE
    source_name (e.g. api_result_pipeline_001) — not the generic
    table_name from strategy.json.

    WHERE NOT EXISTS guard makes this safe to re-run without
    overwriting real watermarks from previous runs.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {watermark_control_table} (
            source_name          STRING,
            entity_name          STRING,
            watermark_column     STRING,
            last_watermark_value TIMESTAMP,
            api_endpoint         STRING,
            pipeline_name        STRING,
            last_run_time        TIMESTAMP,
            run_status           STRING,
            created_at           TIMESTAMP,
            updated_at           TIMESTAMP
        )
        USING DELTA
    """)
    print(f"  ✓ Watermark control table ready: {watermark_control_table}")

    for pipe in pipelines:
        if pipe["source"]["source_type"] != "rest_api":
            continue

        pipeline_id = pipe["pipeline_id"]
        endpoint    = pipe["source"]["connection"].get("endpoint", "")

        for table in pipe.get("tables", []):
            wm_cfg     = table.get("watermark", {})
            source_name  = wm_cfg.get("source_name") or table.get("source_name") or pipeline_id
            table_name   = table.get("source_name", pipeline_id)
            wm_col       = wm_cfg.get("watermark_column", "")
            default_wm   = wm_cfg.get("default_watermark", "1900-01-01 00:00:00")

            spark.sql(f"""
                INSERT INTO {watermark_control_table}
                    (source_name, entity_name, watermark_column,
                     last_watermark_value, api_endpoint, pipeline_name,
                     last_run_time, run_status, created_at, updated_at)
                SELECT
                    '{source_name}',
                    '{table_name}',
                    '{wm_col}',
                    CAST('{default_wm}' AS TIMESTAMP),
                    '{endpoint}',
                    '{pipeline_id}',
                    current_timestamp(),
                    'Pending',
                    current_timestamp(),
                    current_timestamp()
                WHERE NOT EXISTS (
                    SELECT 1 FROM {watermark_control_table}
                    WHERE source_name = '{source_name}'
                    AND   entity_name = '{table_name}'
                )
            """)
            print(f"  ✓ Watermark row ready: {source_name} | {table_name} | {default_wm}")


def get_last_watermark(
    watermark_control_table: str,
    source_name: str,
    entity_name: str,
    default: str,
) -> str:
    """
    Read last successful watermark for this source+entity pair.
    Falls back to default if no Success row exists yet.
    """
    try:
        row = (
            spark.table(watermark_control_table)
            .filter(
                (F.col("source_name") == source_name) &
                (F.col("entity_name") == entity_name) &
                (F.col("run_status")  == "Success")
            )
            .orderBy(F.col("last_run_time").desc())
            .select(F.col("last_watermark_value").cast("string"))
            .first()
        )
        if row and row[0]:
            val = row[0].split("+")[0].split(".")[0].strip()
            print(f"  → Watermark from table : {val}")
            return val
        print(f"  → No prior watermark found — using seed: {default}")
        return default
    except Exception as e:
        print(f"  ⚠ Watermark read error ({e}) — using seed: {default}")
        return default


def update_watermark(
    watermark_control_table: str,
    source_name:      str,
    entity_name:      str,
    watermark_column: str,
    new_watermark:    str,
    pipeline_name:    str,
    api_endpoint:     str = "",
    run_status:       str = "Success",
):
    """
    INSERT a new history row — never overwrites existing rows.
    Full audit trail: every run appends a new row.
    get_last_watermark() always picks the latest Success row.
    Called ONLY after successful data write.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    spark.sql(f"""
        INSERT INTO {watermark_control_table}
            (source_name, entity_name, watermark_column,
             last_watermark_value, api_endpoint, pipeline_name,
             last_run_time, run_status, created_at, updated_at)
        VALUES (
            '{source_name}',
            '{entity_name}',
            '{watermark_column}',
            CAST('{new_watermark}' AS TIMESTAMP),
            '{api_endpoint}',
            '{pipeline_name}',
            CAST('{now}' AS TIMESTAMP),
            '{run_status}',
            CAST('{now}' AS TIMESTAMP),
            CAST('{now}' AS TIMESTAMP)
        )
    """)
    print(f"  ✓ Watermark recorded → {source_name} | {new_watermark} | {run_status}")


def mark_failed(
    watermark_control_table: str,
    source_name: str,
    entity_name: str,
    pipeline_name: str,
):
    """
    INSERT a Failed row. last_watermark_value is NULL so
    get_last_watermark() ignores it and retries from last Success.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        spark.sql(f"""
            INSERT INTO {watermark_control_table}
                (source_name, entity_name, pipeline_name,
                 run_status, last_run_time, updated_at)
            VALUES (
                '{source_name}',
                '{entity_name}',
                '{pipeline_name}',
                'Failed',
                CAST('{now}' AS TIMESTAMP),
                CAST('{now}' AS TIMESTAMP)
            )
        """)
        print(f"  ✗ Failure recorded → {source_name} | Failed")
    except Exception as e:
        print(f"  ⚠ Could not record failure: {e}")


# ======================================================================
# S3 READER
# ======================================================================

def read_s3(source: dict, table: dict):
    conn     = source.get("connection", {})
    key      = conn.get("access_key", "")
    secret   = conn.get("secret_key", "")
    bucket   = source["location"]["bucket"]
    base     = source["location"].get("base_path", "").strip("/")
    filename = table["source_name"]

    path = f"s3a://{bucket}/{base}/{filename}" if base else f"s3a://{bucket}/{filename}"
    print(f"  READ S3: {path}")

    reader = (
        spark.read
        .format(source.get("file_format", "csv"))
        .option("header", True)
        .option("inferSchema", True)
    )

    if key and secret:
        reader = (
            reader
            .option(f"fs.s3a.bucket.{bucket}.access.key", key)
            .option(f"fs.s3a.bucket.{bucket}.secret.key", secret)
            .option(
                f"fs.s3a.bucket.{bucket}.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )
        print(f"  ✓ AWS credentials loaded for bucket '{bucket}'")
    else:
        print("  ⚠ No AWS credentials — attempting with instance profile")

    return reader.load(path)


# ======================================================================
# AZURE SQL READER
# ======================================================================

def read_azure_sql(source: dict, table: dict, watermark_filter: str = None):
    conn = source["connection"]
    jdbc = f"jdbc:sqlserver://{conn['host']}:1433;database={conn['database']}"
    src_name = table["source_name"]

    if watermark_filter:
        wm_col  = table["watermark_column"]
        dbtable = (
            f"(SELECT * FROM {src_name} "
            f"WHERE {wm_col} > '{watermark_filter}') AS incremental_query"
        )
        print(f"  READ AZURE SQL (incremental): {src_name} WHERE {wm_col} > {watermark_filter}")
    else:
        dbtable = src_name
        print(f"  READ AZURE SQL (full): {src_name}")

    return (
        spark.read.format("jdbc")
        .option("url",      jdbc)
        .option("dbtable",  dbtable)
        .option("user",     conn["user"])
        .option("password", conn["password"])
        .option("driver",   "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("numPartitions", table.get("parallelism", 4))
        .load()
    )


# ======================================================================
# POSTGRESQL READER
# ======================================================================

def read_postgresql(source: dict, table: dict, watermark_filter: str = None):
    conn = source["connection"]
    jdbc = (
        f"jdbc:postgresql://{conn['host']}:{conn.get('port', 5432)}"
        f"/{conn['database']}"
    )
    src_name = table["source_name"]

    if watermark_filter:
        wm_col  = table["watermark_column"]
        dbtable = (
            f"(SELECT * FROM {src_name} "
            f"WHERE {wm_col} > '{watermark_filter}') AS incremental_query"
        )
        print(f"  READ POSTGRESQL (incremental): {src_name} WHERE {wm_col} > {watermark_filter}")
    else:
        dbtable = src_name
        print(f"  READ POSTGRESQL (full): {src_name}")

    return (
        spark.read.format("jdbc")
        .option("url",      jdbc)
        .option("dbtable",  dbtable)
        .option("user",     conn["user"])
        .option("password", conn["password"])
        .option("driver",   "org.postgresql.Driver")
        .option("numPartitions", table.get("parallelism", 4))
        .load()
    )


# ======================================================================
# REST API READER + PRECISE FILTER
# ======================================================================

def _parse_ts(ts_str: str) -> datetime:
    """
    Parse a timestamp string into a UTC-aware datetime.

    Handles all common formats including DD-MM-YYYY (used in Indian locale)
    and ISO 8601 variants. Falls back to epoch-1900 sentinel if unparseable.
    """
    clean = ts_str.strip()
    clean = clean.split("+")[0].split(".")[0].rstrip("Z").strip()

    FORMATS = [
        # ISO / standard (YYYY first)
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        # DD-MM-YYYY (Indian locale, used in this project's API data)
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%YT%H:%M:%S",
        "%d-%m-%Y",
        # MM-DD-YYYY (US locale)
        "%m-%d-%Y %H:%M:%S",
        "%m-%d-%Y",
        # DD/MM/YYYY variants
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        # MM/DD/YYYY variants
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
    ]

    for fmt in FORMATS:
        try:
            return datetime.strptime(clean, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    print(f"  ⚠ _parse_ts: could not parse '{ts_str}' — treating as 1900-01-01")
    return datetime(1900, 1, 1, tzinfo=timezone.utc)


def fetch_rest_api(source: dict, table: dict, last_watermark: str):
    """
    Call the REST API with a broad date window, then apply precise filter.
    Returns (filtered_rows, new_watermark).

    First run  (watermark == seed):  keep ALL rows, new_watermark = MAX(ts)
    Subsequent (watermark > seed):   keep rows where wm < ts <= now-lag
    """
    conn             = source["connection"]
    wm_cfg           = table["watermark"]
    watermark_col    = wm_cfg.get("watermark_column")
    lag_minutes      = wm_cfg.get("lag_minutes",      5)
    api_buffer_days  = wm_cfg.get("api_buffer_days",  1)
    precision_filter = wm_cfg.get("precision_filter", True)
    default_wm       = wm_cfg.get("default_watermark", "1900-01-01 00:00:00")

    now           = datetime.now(timezone.utc)
    trailing_edge = now - timedelta(minutes=lag_minutes)
    last_ts       = _parse_ts(last_watermark)

    is_first_run  = (last_watermark.strip()[:19] == default_wm.strip()[:19])

    api_from      = (last_ts - timedelta(days=api_buffer_days)).strftime("%Y-%m-%d %H:%M:%S")
    api_to        = (now     + timedelta(days=api_buffer_days)).strftime("%Y-%m-%d %H:%M:%S")
    filter_before = trailing_edge.strftime("%Y-%m-%d %H:%M:%S")

    endpoint    = conn.get("endpoint", "")
    auth_type   = conn.get("auth_type", "none")
    api_key     = conn.get("api_key", "")
    header_name = conn.get("header_name", "api_key")

    headers = {}
    params  = {"from": api_from, "to": api_to}

    if auth_type == "api_key":
        key_name          = header_name or "api_key"
        params[key_name]  = api_key
        headers[key_name] = api_key
    elif auth_type == "bearer":
        headers["Authorization"] = f"Bearer {api_key}"
    elif auth_type == "basic":
        headers["Authorization"] = f"Basic {api_key}"

    if is_first_run:
        print(f"  → FIRST RUN — loading all data, no upper bound filter")
    print(f"  → Calling API : {endpoint}")
    print(f"    Broad window: {api_from}  →  {api_to}")

    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=30)
    except requests.exceptions.ConnectionError as exc:
        raise RuntimeError(f"Cannot reach API: {exc}")
    except requests.exceptions.Timeout:
        raise RuntimeError("API request timed out after 30s")

    if response.status_code >= 400:
        raise RuntimeError(f"API returned HTTP {response.status_code}: {response.text[:300]}")

    content_type = response.headers.get("Content-Type", "")
    if content_type and "application/json" not in content_type:
        raise RuntimeError(
            f"API did not return JSON (Content-Type: {content_type}). "
            f"Preview: {response.text[:300]}"
        )

    data = response.json()
    if isinstance(data, list):
        raw_rows = data
    elif isinstance(data, dict):
        raw_rows = next(
            (data[k] for k in ("data", "results", "records", "items")
             if isinstance(data.get(k), list)),
            [data],
        )
    else:
        raw_rows = []

    print(f"  → API returned {len(raw_rows)} raw rows")

    # ── First run: keep everything, watermark = MAX(ts) ──────────────
    if is_first_run:
        new_watermark = filter_before   # fallback
        if watermark_col and raw_rows:
            ts_values = []
            for row in raw_rows:
                raw_ts = row.get(watermark_col)
                if raw_ts:
                    try:
                        ts_values.append(_parse_ts(str(raw_ts)))
                    except Exception:
                        pass
            if ts_values:
                new_watermark = max(ts_values).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  → First run: keeping all {len(raw_rows)} rows")
        print(f"  → New watermark (MAX from data): {new_watermark}")
        return raw_rows, new_watermark

    # ── Subsequent runs: precise window filter ────────────────────────
    new_watermark = filter_before

    if precision_filter and watermark_col:
        lower = _parse_ts(last_watermark)
        upper = _parse_ts(filter_before)
        kept  = []
        for row in raw_rows:
            raw_ts = row.get(watermark_col)
            if raw_ts is None:
                continue
            try:
                ts = _parse_ts(str(raw_ts))
                if lower < ts <= upper:
                    kept.append(row)
            except Exception:
                continue
        print(
            f"  → Precise filter [{last_watermark}  →  {filter_before}]: "
            f"{len(raw_rows)} in | {len(kept)} kept"
        )
        return kept, new_watermark

    print(f"  → Precision filter skipped — keeping all {len(raw_rows)} rows")
    return raw_rows, new_watermark


# ======================================================================
# SCHEMA ALIGNMENT
# ======================================================================

def align_schema(df, target: str):
    """Cast incoming df columns to match target table schema."""
    try:
        tgt = spark.table(target)
        for c in tgt.schema:
            matches = [x for x in df.columns if x.lower() == c.name.lower()]
            if matches:
                df = df.withColumnRenamed(matches[0], c.name)
                df = df.withColumn(c.name, df[c.name].cast(c.dataType))
        return df
    except Exception:
        return df


# ======================================================================
# WRITERS
# ======================================================================

def write_append(df, target: str, partition_by=None):
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(target)
    print(f"  ✓ Appended → {target}")


def write_merge(df, target: str, watermark_col: str, merge_keys: list = None):
    """
    MERGE (upsert) df into Delta target table.

    merge_keys: explicit list of columns for the MERGE ON key.
                If not provided, auto-detected by finding ID/key columns
                in THIS table's schema — avoids cross-schema MERGE errors
                where columns from a different API bleed into the ON clause.

    Deduplication:
        Delta MERGE requires each source row to match AT MOST ONE target row.
        If the source batch contains multiple rows with the same merge key
        (e.g. customer placed multiple orders → same Customer_ID appears twice),
        Delta throws DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE.

        Fix: deduplicate source df on merge key before MERGE, keeping the row
        with the latest watermark_col value (most recent event wins).
        If no watermark_col exists, just drop exact duplicates on merge key.
    """
    if not spark.catalog.tableExists(target):
        df.write.format("delta").mode("append").saveAsTable(target)
        print(f"  ✓ Created + inserted → {target}")
        return

    # ── Resolve merge key columns ─────────────────────────────────────
    if merge_keys:
        key_cols = [k for k in merge_keys if k in df.columns]
    else:
        key_cols = []

    if not key_cols:
        id_cols = [
            c for c in df.columns
            if c != watermark_col and (
                c.lower().endswith("_id") or
                c.lower().endswith("_key") or
                c.lower() in ("id", "key", "uuid", "guid")
            )
        ]
        key_cols = id_cols[:1] if id_cols else [
            c for c in df.columns if c != watermark_col
        ][:1]

    if not key_cols:
        key_cols = list(df.columns)[:1]

    # ── Deduplicate source on merge key (fix MULTIPLE_SOURCE_ROW error) ──
    pre_dedup    = df.count()
    wm_col_in_df = watermark_col if watermark_col and watermark_col in df.columns else None

    if wm_col_in_df:
        from pyspark.sql.window import Window
        w = Window.partitionBy(*key_cols).orderBy(F.col(wm_col_in_df).desc())
        df = (
            df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    else:
        df = df.dropDuplicates(key_cols)

    post_dedup = df.count()
    if pre_dedup != post_dedup:
        print(f"  ⚠ Deduplicated source: {pre_dedup} rows → {post_dedup} rows "
              f"(kept latest per {key_cols})")

    temp_view = f"_inc_{target.replace('.', '_').replace('-', '_')}"
    df.createOrReplaceTempView(temp_view)

    on_clause = " AND ".join(f"t.`{c}` = s.`{c}`" for c in key_cols)
    print(f"  → MERGE key: {key_cols}")

    spark.sql(f"""
        MERGE INTO {target} AS t
        USING {temp_view} AS s
        ON {on_clause}
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  ✓ Merged → {target}")


# ======================================================================
# TABLE EXECUTOR  (one table from one pipeline)
# ======================================================================

def execute_table(pipe: dict, table: dict, watermark_control_table: str):

    pipeline_id = pipe["pipeline_id"]
    source      = pipe["source"]
    source_type = source["source_type"]
    target      = table["target_table"]
    load_type   = table.get("load_type", "full")
    wm_col      = table.get("watermark_column")
    write_strat = table.get("write_strategy", "append")
    partition_by= table.get("partition_by")

    # ── REST API ──────────────────────────────────────────────────────
    if source_type == "rest_api":
        wm_cfg      = table.get("watermark", {})
        default_wm  = wm_cfg.get("default_watermark", "1900-01-01 00:00:00")
        wm_col      = wm_cfg.get("watermark_column")
        endpoint    = source["connection"].get("endpoint", "")

        source_name = wm_cfg.get("source_name") or table.get("source_name") or pipeline_id
        table_name  = table.get("source_name", pipeline_id)

        last_wm = get_last_watermark(watermark_control_table, source_name, table_name, default_wm)
        print(f"  last_watermark : {last_wm}")
        print(f"  target_table   : {target}")

        try:
            filtered_rows, new_wm = fetch_rest_api(source, table, last_wm)
        except RuntimeError as exc:
            print(f"  ✗ API fetch failed: {exc}")
            mark_failed(watermark_control_table, source_name, table_name, pipeline_id)
            raise

        if not filtered_rows:
            print("  ⚠ No new rows after filter — skipping write.")
            return

        def _flatten_row(row: dict) -> dict:
            import json as _json
            return {
                k: _json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                for k, v in row.items()
            }

        flat_rows = [_flatten_row(r) for r in filtered_rows if isinstance(r, dict)]
        if not flat_rows:
            print("  ⚠ No valid rows after flattening — skipping write.")
            return

        try:
            df = spark.createDataFrame(flat_rows)
        except Exception as schema_err:
            print(f"  ⚠ Schema inference failed ({schema_err}) — retrying with all-string cast")
            str_rows = [{k: str(v) if v is not None else None for k, v in r.items()} for r in flat_rows]
            df = spark.createDataFrame(str_rows)

        df = align_schema(df, target)

        merge_keys = wm_cfg.get("merge_keys") or table.get("merge_keys") or None

        if wm_col:
            write_merge(df, target, wm_col, merge_keys=merge_keys)
        else:
            print("  ⚠ No watermark_column configured — using append instead of merge")
            write_append(df, target, partition_by)

        update_watermark(
            watermark_control_table = watermark_control_table,
            source_name      = source_name,
            entity_name      = table_name,
            watermark_column = wm_col or "",
            new_watermark    = new_wm,
            pipeline_name    = pipeline_id,
            api_endpoint     = endpoint,
            run_status       = "Success",
        )
        return

    # ── S3 / Azure SQL / PostgreSQL ───────────────────────────────────

    last_wm = None
    if load_type == "incremental" and wm_col:
        last_wm = get_last_watermark(
            watermark_control_table, pipeline_id, table["source_name"], "1900-01-01 00:00:00"
        )
        print(f"  last_watermark : {last_wm}")

    if source_type == "s3":
        df = read_s3(source, table)
        if load_type == "incremental" and wm_col and last_wm:
            df = df.filter(F.col(wm_col) > last_wm)
            print(f"  Incremental filter: {wm_col} > {last_wm}")

    elif source_type == "azure_sql":
        wm_filter = last_wm if (load_type == "incremental" and last_wm) else None
        df = read_azure_sql(source, table, watermark_filter=wm_filter)

    elif source_type == "postgresql":
        wm_filter = last_wm if (load_type == "incremental" and last_wm) else None
        df = read_postgresql(source, table, watermark_filter=wm_filter)

    else:
        raise Exception(f"Unsupported source type: {source_type}")

    if df.rdd.isEmpty():
        print("  ⚠ No data returned — skipping write.")
        return

    print(f"  → {df.count()} rows fetched")
    df = align_schema(df, target)

    merge_keys = table.get("merge_keys") or None
    if write_strat == "merge" and wm_col:
        write_merge(df, target, wm_col, merge_keys=merge_keys)
    elif write_strat == "merge" and not wm_col:
        print(f"  ⚠ write_strategy=merge but no watermark_column — falling back to append")
        write_append(df, target, partition_by)
    else:
        write_append(df, target, partition_by)

    if load_type == "incremental" and wm_col:
        max_wm = df.agg(F.max(wm_col)).first()[0]
        if max_wm:
            update_watermark(
                watermark_control_table = watermark_control_table,
                source_name      = pipeline_id,
                entity_name      = table["source_name"],
                watermark_column = wm_col,
                new_watermark    = str(max_wm),
                pipeline_name    = pipeline_id,
                run_status       = "Success",
            )


# ======================================================================
# MAIN RUN FUNCTION  — every call is fully independent of previous runs
# ======================================================================

def run_ingestion(config_path: str = "/Volumes/2data/bronze/new/migration_plan.json"):
    """
    Entry point for one ingestion run.

    ALL state is local to this function:
      - config file is re-read fresh every call
      - pipeline variables are derived from the fresh config
      - session counters start at zero
    Re-running the notebook cell NEVER accumulates state from prior runs.
    """
    run_start = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # ── Load config fresh on every run ───────────────────────────────
    with open(config_path) as f:
        cfg = json.load(f)

    plan      = cfg["migration_plan"]
    pipelines = plan["pipelines"]

    _target  = plan.get("target", {})
    _catalog = _target.get("catalog", "bronze_catalog")
    _schema  = _target.get("schema",  "raw_ingestion")

    watermark_control_table = (
        plan.get("watermark_control_table")
        or f"{_catalog}.{_schema}.bronze_watermark_control"
    )

    # ── Session header ────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  BRONZE INGESTION EXECUTOR")
    print(f"  Run started : {run_start}")
    print(f"  Config      : {config_path}")
    print(f"  WM table    : {watermark_control_table}")
    print("=" * 60)

    ensure_watermark_table(watermark_control_table, pipelines)

    # Fresh counters — isolated to THIS run, not inherited from any prior run
    total_tables  = 0
    total_success = 0
    total_failed  = 0
    failed_tables = []

    for pipe in pipelines:

        source_type = pipe["source"]["source_type"]

        print(f"\n{'─' * 60}")
        print(f"  PIPELINE : {pipe['pipeline_id']}")
        print(f"  SOURCE   : {source_type}")
        print(f"{'─' * 60}")

        for table in pipe["tables"]:
            total_tables += 1
            tgt = table.get("target_table", "?")
            src = table.get("source_name", "?")
            print(f"\n  Table: {src} → {tgt}")

            try:
                execute_table(pipe, table, watermark_control_table)
                total_success += 1

            except Exception as e:
                total_failed += 1
                failed_tables.append(f"{pipe['pipeline_id']}.{src}")
                print(f"  ✗ FAILED: {src}")
                print(f"    ERROR : {str(e)}")
                # Note: mark_failed is called inside execute_table before the
                # exception is re-raised — do NOT call it again here or it
                # records a duplicate Failed row with the wrong source_name.

    # ── Session summary ───────────────────────────────────────────────
    run_end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("\n" + "=" * 60)
    print("  INGESTION COMPLETE")
    print(f"  Run started : {run_start}")
    print(f"  Run ended   : {run_end}")
    print(f"  Total       : {total_tables}")
    print(f"  Success     : {total_success}")
    print(f"  Failed      : {total_failed}")
    if failed_tables:
        print("  Failed tables:")
        for ft in failed_tables:
            print(f"    ✗ {ft}")
    print("=" * 60)

    return {
        "run_start":     run_start,
        "run_end":       run_end,
        "total_tables":  total_tables,
        "total_success": total_success,
        "total_failed":  total_failed,
        "failed_tables": failed_tables,
    }


# ── Invoke ────────────────────────────────────────────────────────────
run_result = run_ingestion()