"""
silver_agent/silver_final_gen.py — Silver Migration Plan Generator
===================================================================
Step S8 of the silver pipeline. Called by orchestrator.py.

Reads:
  silver_credentials.json          → bronze source connection
  silver_table_creation_status.json → silver target tables
  transform_plan.json               → column map + transformation rules
  silver_schema_output.json         → silver columln definitions

Produces:
  silver_migration_plan.json

This is what silver execution.py reads to:
  1. Connect to Databricks bronze tables
  2. Apply all transformations (rename, cast, null removal, dedup)
  3. Write transformed data into silver Delta tables

Structure:
{
  "session_id": "...",
  "created_at": "...",
  "migration_plan": {
    "pipelines": [
      {
        "pipeline_id":   "...",
        "source": {
          "source_type":    "databricks_bronze",
          "connection": {
            "workspace_url": "...",
            "warehouse_id":  "...",
            "token":         "..."
          },
          "bronze_table":   "test.bronze.inspection_data"
        },
        "target": {
          "catalog":      "test",
          "schema":       "silver",
          "table":        "test.silver.inspection_data_silver"
        },
        "transformations": {
          "remove_nulls":      true,
          "drop_duplicates":   true,
          "standardize_names": true,
          "cast_types":        true
        },
        "column_map": [
          {
            "source_column":  "OperatorID",
            "target_column":  "operator_id",
            "source_type":    "STRING",
            "target_type":    "STRING",
            "transformation": "rename"
          },
          ...
        ]
      }
    ]
  }
}
"""

import json
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger("Silver.FinalGen")


def run_silver_final_gen(
    credentials_path:    str,
    table_creation_path: str,
    transform_plan_path: str,
    schema_path:         str,
    mapping_path:        str | None = None,
    output_dir:          str = ".",
) -> tuple:
    """
    Generate silver_migration_plan.json.

    Args:
        credentials_path    : silver_credentials.json
        table_creation_path : silver_table_creation_status.json
        transform_plan_path : transform_plan.json
        schema_path         : silver_schema_output.json
    mapping_path        : mapping_table.json
        output_dir          : Where to save silver_migration_plan.json

    Returns:
        (plan: dict, plan_path: str)
    """
    logger.info("Silver Final Gen started.")

    # ── Load inputs ───────────────────────────────────────────────────────────
    with open(credentials_path, "r", encoding="utf-8") as f:
        creds = json.load(f)

    with open(table_creation_path, "r", encoding="utf-8") as f:
        table_status = json.load(f)

    with open(transform_plan_path, "r", encoding="utf-8") as f:
        transform_plan = json.load(f)

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_data = json.load(f)

    session_id     = creds.get("session_id", "unknown")
    silver_catalog = table_status.get("catalog", "")
    silver_schema  = table_status.get("schema",  "")
    
    taxonomy_table = f"{silver_catalog}.{silver_schema}.silver_taxonomy_lookup"

    # Build lookup: table_name → created table full path
    created_lookup = {
        r["original_table_name"].split(".")[-1]: r["table_name"]
        for r in table_status.get("tables", [])
        if r.get("status") == "created"
    }

    # Build lookup: table_name → schema entry
    schema_lookup = {
        t["table_name"]: t
        for t in schema_data.get("tables", [])
    }

    pipelines = []

    for pipeline in creds.get("pipelines", []):
        pipeline_id = pipeline["pipeline_id"]
        conn        = pipeline.get("connection_template", {})

        source_conn = {
            "workspace_url": conn.get("workspace_url", ""),
            "warehouse_id" : conn.get("warehouse_id",  ""),
            "token"        : conn.get("token",         ""),
        }
        bronze_catalog = conn.get("catalog", "")
        bronze_schema  = conn.get("schema",  "")
        bronze_table   = conn.get("table",   "")

        # Match transform tables for this pipeline
        pipe_tables = [
            t for t in transform_plan.get("tables", [])
            if t.get("pipeline_id") == pipeline_id
        ]

        for tbl in pipe_tables:
            source_table  = tbl.get("full_table", "")
            bronze_bare   = tbl.get("table", "")

            # Find the silver table name (may have been renamed by user)
            silver_bare   = f"{bronze_bare}_silver"
            silver_full   = created_lookup.get(
                silver_bare,
                f"{silver_catalog}.{silver_schema}.{silver_bare}",
            )

            operations = tbl.get("operations", {})
            col_map    = tbl.get("columns", [])
            
            # Ensure standardization points to the taxonomy table
            if "taxonomy_table_name" not in operations:
                operations["taxonomy_table_name"] = taxonomy_table

            pipelines.append({
                "pipeline_id": pipeline_id,
                "source": {
                    "source_type" : "databricks_bronze",
                    "connection"  : source_conn,
                    "bronze_table": source_table or f"{bronze_catalog}.{bronze_schema}.{bronze_table}",
                },
                "target": {
                    "catalog": silver_catalog,
                    "schema" : silver_schema,
                    "table"  : silver_full,
                },
                "transformations": {
                    "remove_nulls"     : operations.get("remove_nulls",      True),
                    "drop_duplicates"  : operations.get("drop_duplicates",   True),
                    "standardize_names": operations.get("standardize_names", True),
                    "cast_types"       : operations.get("cast_types",        True),
                },
                "column_map": col_map,
            })

    plan = {
        "session_id" : session_id,
        "created_at" : datetime.now(timezone.utc).isoformat(),
        "migration_plan": {
            "pipelines": pipelines,
        },
    }

    # ── Save ──────────────────────────────────────────────────────────────────
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "silver_migration_plan.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)

    logger.info("Silver migration plan → %s", out_path)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("  ✅  SILVER MIGRATION PLAN GENERATED")
    print("=" * 62)
    print(f"\n  Saved to : {out_path}")
    print(f"  Session  : {session_id}")
    print(f"  Target   : {silver_catalog}.{silver_schema}")
    print(f"  Pipelines: {len(pipelines)}")
    for p in pipelines:
        print(
            f"    → {p['pipeline_id']}  "
            f"{p['source']['bronze_table']}  →  {p['target']['table']}"
        )
    print()
    print("  NEXT: Upload silver_migration_plan.json to Databricks")
    print("        and run silver_execution.py\n")

    return plan, out_path