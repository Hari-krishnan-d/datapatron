"""
silver_agent/silver_schema.py — Silver Schema Agent
=====================================================
Step S6 of the silver pipeline. Called by orchestrator.py.

Reads transform_plan.json produced by silver_transform.py and generates:
  1. silver_schema.json  — structured schema definition per table
  2. DDL statements      — CREATE TABLE ... USING DELTA for each silver table

KEY FIX — column types come from transform_plan columns[i]["target_type"]
           NOT from ["type"] or ["source_type"].

  source_type  = what the column was in bronze (often STRING for everything)
  target_type  = what the user approved in Step 2 of silver_transform.py
                 (BIGINT, DATE, DECIMAL(18,2), TIMESTAMP, etc.)

The DDL must reflect target_type so the silver table has the correct schema.

Output: silver_schema.json
{
  "session_id": "...",
  "tables": [
    {
      "table_name":    "finance_silver",
      "pipeline_id":   "...",
      "source_table":  "testbro.bronze.finance",
      "write_strategy":"overwrite",
      "primary_key":   "customer_id",
      "columns": [
        {
          "name":          "customer_id",
          "type":          "BIGINT",        ← target_type from transform_plan
          "source_column": "cUStoMeR_iD",
          "source_type":   "string",        ← original bronze type for reference
          "transformation":"rename + cast",
          "cast_applied":  true
        },
        ...
      ],
      "ddl": "CREATE TABLE IF NOT EXISTS {catalog}.{schema}.finance_silver (\n    customer_id BIGINT,\n    ..."
    }
  ]
}
"""

import json
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger("Silver.Schema")

# Spark SQL type normalisation — lowercase input → uppercase canonical form
_TYPE_NORMALISE = {
    "string"   : "STRING",
    "str"      : "STRING",
    "bigint"   : "BIGINT",
    "long"     : "BIGINT",
    "int"      : "INT",
    "integer"  : "INT",
    "double"   : "DOUBLE",
    "float"    : "FLOAT",
    "boolean"  : "BOOLEAN",
    "bool"     : "BOOLEAN",
    "date"     : "DATE",
    "timestamp": "TIMESTAMP",
    "binary"   : "BINARY",
}


def _normalise_type(raw_type: str) -> str:
    """
    Normalise a Spark SQL type to uppercase canonical form.
    Preserves DECIMAL(p,s) precision. Falls back to STRING.
    """
    if not raw_type:
        return "STRING"
    t = raw_type.strip()
    # DECIMAL(18,2) etc. — keep as-is, just uppercase
    if t.upper().startswith("DECIMAL"):
        return t.upper()
    normalised = _TYPE_NORMALISE.get(t.lower())
    if normalised:
        return normalised
    logger.warning("Unrecognised type '%s' — keeping as-is uppercased", t)
    return t.upper()


def _resolve_target_type(col: dict) -> str:
    """
    Get the correct silver column type from a transform_plan column entry.

    Priority:
      1. target_type  — set by silver_transform.py Step 2 (user-approved cast)
      2. type         — fallback for legacy plans that don't have target_type
      3. STRING       — safe default

    THE BUG WAS HERE: old silver_schema.py read col["type"] which is the
    bronze source type stored as STRING for untyped tables.
    """
    # Priority 1: target_type (written by silver_transform.py after user approval)
    target = col.get("target_type", "").strip()
    if target:
        return _normalise_type(target)

    # Priority 2: plain "type" field (legacy plans)
    legacy = col.get("type", "").strip()
    if legacy:
        return _normalise_type(legacy)

    return "STRING"


def _build_table_schema(table_transform: dict, pipeline_id: str) -> dict:
    """
    Build the silver schema entry for one table from transform_plan data.
    Reads target_type for every column to generate correct-typed DDL.
    """
    source_table      = table_transform.get("full_table", table_transform.get("table", ""))
    raw_table_name    = table_transform.get("table", "unknown")
    silver_table_name = f"{raw_table_name}_silver"
    columns_raw       = table_transform.get("columns", [])

    # ── Build column list with correct types from target_type ─────────────────
    schema_columns = []
    for col in columns_raw:
        silver_col   = col.get("target_column") or col.get("name", "")
        source_col   = col.get("source_column", "")
        source_type  = _normalise_type(col.get("source_type") or col.get("type", "STRING"))
        target_type  = _resolve_target_type(col)    # ← reads target_type first
        transform    = col.get("transformation", "standardize")
        cast_applied = col.get("cast_recommended", False)

        schema_columns.append({
            "name"          : silver_col,
            "type"          : target_type,   # ← this is what DDL and downstream use
            "source_column" : source_col,
            "source_type"   : source_type,   # ← kept for reference / lineage
            "transformation": transform,
            "cast_applied"  : cast_applied,
        })

    # ── Detect primary key ────────────────────────────────────────────────────
    ops         = table_transform.get("operations", {})
    primary_key = ops.get("primary_key", "")
    if not primary_key and schema_columns:
        for col in schema_columns:
            if col["name"].endswith("_id"):
                primary_key = col["name"]
                break

    # ── Generate DDL with correct types ───────────────────────────────────────
    col_defs = ",\n    ".join(
        f"{col['name']} {col['type']}"
        for col in schema_columns
    )
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {{catalog}}.{{schema}}.{silver_table_name} (\n"
        f"    {col_defs}\n"
        f") USING DELTA"
    )

    return {
        "table_name"    : silver_table_name,
        "pipeline_id"   : pipeline_id,
        "source_table"  : source_table,
        "write_strategy": ops.get("write_strategy", "overwrite"),
        "primary_key"   : primary_key,
        "columns"       : schema_columns,
        "ddl"           : ddl,
    }


def _print_schema_summary(table_schema: dict) -> None:
    """Print a readable summary of the generated schema, highlighting casts."""
    cols  = table_schema["columns"]
    casts = [c for c in cols if c.get("cast_applied")]

    print(f"\n  {'─' * 65}")
    print(f"  {table_schema['table_name']}")
    print(f"  Source: {table_schema['source_table']}")
    print(f"  {'─' * 65}")
    print(f"  {'SILVER COLUMN':<30} {'SILVER TYPE':<18} BRONZE TYPE")
    print(f"  {'─' * 65}")

    for col in cols:
        cast_marker  = " ✦" if col.get("cast_applied") else "  "
        type_changed = col["type"] != col["source_type"]
        bronze_note  = f"← was {col['source_type']}" if type_changed else ""
        print(
            f"  {col['name']:<30} {col['type']:<18} {bronze_note}{cast_marker}"
        )

    print(f"  {'─' * 65}")
    print(f"  Columns   : {len(cols)}")
    print(f"  Casts (✦) : {len(casts)}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_silver_schema(
    transform_plan_path: str = "",
    output_dir:          str = ".",
    metadata_path:       str = "",   # alias — orchestrator may pass this instead
) -> tuple:
    """
    Generate silver_schema.json from transform_plan.json.

    Args:
        transform_plan_path : Path to transform_plan.json
        output_dir          : Where to save silver_schema.json

    Returns:
        (schema_dict, schema_path)
    """
    logger.info("Silver Schema Agent started.")

    # Accept either parameter name from the orchestrator
    resolved_path = transform_plan_path or metadata_path
    if not resolved_path:
        raise ValueError(
            "run_silver_schema() requires transform_plan_path or metadata_path"
        )

    with open(resolved_path, "r", encoding="utf-8") as f:
        transform_plan = json.load(f)

    session_id = transform_plan.get("session_id", "unknown")
    tables_in  = transform_plan.get("tables", [])

    print("\n" + "=" * 65)
    print("  SILVER — SCHEMA GENERATION")
    print(f"  Session : {session_id}")
    print(f"  Tables  : {len(tables_in)}")
    print("=" * 65)
    print("\n  Reading target_type (user-approved) from transform plan...")
    print("  Building DDL with correct Spark SQL types.\n")

    schema_tables = []

    for table_transform in tables_in:
        pipeline_id = table_transform.get("pipeline_id", "")
        full_name   = table_transform.get("full_table", table_transform.get("table", ""))
        print(f"  Processing: {full_name}")

        try:
            table_schema = _build_table_schema(table_transform, pipeline_id)
            schema_tables.append(table_schema)
            _print_schema_summary(table_schema)
        except Exception as exc:
            logger.error("Schema generation failed for %s: %s", full_name, exc)
            print(f"  ❌ Failed for {full_name}: {exc}")

    schema = {
        "session_id" : session_id,
        "created_at" : datetime.now(timezone.utc).isoformat(),
        "tables"     : schema_tables,
    }

    os.makedirs(output_dir, exist_ok=True)
    schema_path = os.path.join(output_dir, "silver_schema.json")
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)

    logger.info("Silver schema → %s", schema_path)

    total_cols  = sum(len(t["columns"]) for t in schema_tables)
    total_casts = sum(
        sum(1 for c in t["columns"] if c.get("cast_applied"))
        for t in schema_tables
    )

    print(f"\n  {'─' * 55}")
    print(f"  SILVER SCHEMA COMPLETE")
    print(f"  Tables    : {len(schema_tables)}")
    print(f"  Columns   : {total_cols}")
    print(f"  Casts     : {total_casts}")
    print(f"  Saved  → {schema_path}")
    print(f"  {'─' * 55}\n")

    return schema, schema_path