

# # # """
# # # migration_plan.py — Migration Plan Generator
# # # =============================================
# # # Step 8 of the orchestration pipeline. Called by orchestrator.py.

# # # Combines:
# # #     credentials.json           (Step 2 — connection details)
# # #     table_creation_status.json (Step 7 — catalog / schema / created tables)
# # #     strategy.json              (Step 5 — ingestion mode, watermark, parallelism)
# # #     schema_output.json         (Step 6 — unique table names per pipeline)  ← NEW

# # # Produces:
# # #     migration_plan.json        (consumed by Executor — Step 9)

# # # Supported source types:
# # #     s3         → spark engine
# # #     azure_sql  → spark engine
# # #     postgresql → spark engine
# # #     rest_api   → python engine, carries full watermark window config

# # # Key fix vs previous version:
# # #     REST API target_table now comes from schema_output.json (which has unique
# # #     names like api_result_pipeline_001) instead of strategy.json (which has
# # #     the generic "api_result" for every pipeline). Without this fix, all 7 APIs
# # #     wrote to the same table.
# # # """

# # # import json
# # # import os


# # # # ======================================================================
# # # # PROGRAMMATIC ENTRY  (called by orchestrator)
# # # # ======================================================================

# # # def run_migration_plan(
# # #     credentials_path:    str,
# # #     table_creation_path: str,
# # #     strategy_path:       str,
# # #     output_dir:          str = ".",
# # #     schema_path:         str | None = None,   # schema_output.json (Step 6)
# # # ) -> tuple:
# # #     """
# # #     Generate migration_plan.json from upstream artefacts.

# # #     Args:
# # #         credentials_path:    Path to credentials.json
# # #         table_creation_path: Path to table_creation_status.json
# # #         strategy_path:       Path to strategy.json
# # #         output_dir:          Directory to save migration_plan.json
# # #         schema_path:         Path to schema_output.json (optional but recommended)
# # #                              Used to resolve unique target_table names for REST API.

# # #     Returns:
# # #         Tuple of (migration_plan_dict, migration_plan_filepath)
# # #     """
# # #     os.makedirs(output_dir, exist_ok=True)
# # #     output_path = os.path.join(output_dir, "migration_plan.json")

# # #     cred           = _load_json(credentials_path)
# # #     table_creation = _load_json(table_creation_path)
# # #     strategy       = _load_json(strategy_path)
# # #     schema_output  = _load_json(schema_path) if schema_path else {}

# # #     result = _generate_migration_plan(cred, table_creation, strategy, schema_output)

# # #     with open(output_path, "w", encoding="utf-8") as f:
# # #         json.dump(result, f, indent=2)

# # #     print("\n✅ Migration plan generated successfully")
# # #     print(f"   Saved to : {output_path}")
# # #     _print_summary(result)

# # #     return result, output_path


# # # # ======================================================================
# # # # CORE GENERATOR
# # # # ======================================================================

# # # def _generate_migration_plan(
# # #     cred:           dict,
# # #     table_creation: dict,
# # #     strategy:       dict,
# # #     schema_output:  dict,
# # # ) -> dict:

# # #     session_id = cred.get("session_id", "unknown")
# # #     catalog    = table_creation.get("catalog", "")
# # #     schema     = table_creation.get("schema",  "")

# # #     # Quick lookup: pipeline_id → strategy pipeline
# # #     strategy_map = {
# # #         p["pipeline_id"]: p
# # #         for p in strategy.get("pipelines", [])
# # #     }

# # #     # Build rename overrides from table_creation_status.json
# # #     # (captures user renames from Table.py approval step)
# # #     rename_overrides = _build_rename_overrides(table_creation)

# # #     # Build schema lookup: pipeline_id → unique table name from schema_output
# # #     # Rename overrides are applied here so user renames flow into migration_plan.
# # #     schema_table_map = _build_schema_table_map(schema_output, rename_overrides)

# # #     # Watermark control table (executor creates it on first run)
# # #     watermark_control_table = f"{catalog}.{schema}.bronze_watermark_control"

# # #     final_pipelines = []
# # #     skipped         = []

# # #     for cred_pipeline in cred.get("pipelines", []):

# # #         pipeline_id = cred_pipeline["pipeline_id"]
# # #         source_type = cred_pipeline["source_type"].lower()

# # #         # Match to strategy
# # #         strategy_pipeline = strategy_map.get(pipeline_id)
# # #         if not strategy_pipeline:
# # #             print(f"  ⚠ Strategy missing for pipeline {pipeline_id} — skipping.")
# # #             skipped.append(pipeline_id)
# # #             continue

# # #         # Skip pipelines that failed credential collection
# # #         if cred_pipeline.get("status") == "credentials_failed":
# # #             print(f"  ⚠ Credentials failed for {pipeline_id} — skipping.")
# # #             skipped.append(pipeline_id)
# # #             continue

# # #         # Support both connection_template (new) and connection (legacy)
# # #         ct = (
# # #             cred_pipeline.get("connection_template")
# # #             or cred_pipeline.get("connection")
# # #             or {}
# # #         )

# # #         # ── Build source block ────────────────────────────────────────
# # #         try:
# # #             source_block = _build_source_block(source_type, cred_pipeline, ct)
# # #         except ValueError as exc:
# # #             print(f"  ⚠ {exc} — skipping {pipeline_id}.")
# # #             skipped.append(pipeline_id)
# # #             continue

# # #         # ── Build tables block ────────────────────────────────────────
# # #         if source_type == "rest_api":
# # #             tables_block = _build_rest_api_tables(
# # #                 strategy_pipeline,
# # #                 catalog,
# # #                 schema,
# # #                 watermark_control_table,
# # #                 schema_table_map,   # ← unique names from schema_output
# # #             )
# # #         else:
# # #             tables_block = _build_db_tables(
# # #                 strategy_pipeline, source_type, catalog, schema
# # #             )

# # #         final_pipelines.append({
# # #             "pipeline_id": pipeline_id,
# # #             "source":      source_block,
# # #             "tables":      tables_block,
# # #         })

# # #     if skipped:
# # #         print(f"\n  ⚠ Skipped pipelines: {', '.join(skipped)}")

# # #     return {
# # #         "session_id": session_id,
# # #         "migration_plan": {
# # #             "target": {
# # #                 "catalog": catalog,
# # #                 "schema":  schema,
# # #             },
# # #             "watermark_control_table": watermark_control_table,
# # #             "pipelines":               final_pipelines,
# # #         },
# # #     }


# # # # ======================================================================
# # # # SCHEMA OUTPUT LOOKUP
# # # # ======================================================================

# # # def _build_schema_table_map(schema_output: dict, rename_overrides: dict = None) -> dict:
# # #     """
# # #     Build a lookup: pipeline_id → {"table_name": ..., "primary_key": ...}

# # #     schema_output["tables"] entries for REST API look like:
# # #         {
# # #             "table_name":  "api_result_pipeline_001",   ← unique (or user-renamed)
# # #             "pipeline_id": "pipeline_001",
# # #             "source_type": "rest_api"
# # #         }

# # #     rename_overrides: dict of {original_bare_name → approved_bare_name}
# # #         Built from table_creation_status.json when user renamed a table
# # #         in the Table Creation Agent approval step.
# # #         Applied here so migration_plan.json uses the user-chosen name.
# # #     """
# # #     rename_overrides = rename_overrides or {}

# # #     mapping = {}
# # #     for t in schema_output.get("tables", []):
# # #         pid  = t.get("pipeline_id", "")
# # #         name = t.get("table_name", "")
# # #         src  = t.get("source_type", "")
# # #         pk   = t.get("primary_key")

# # #         if pid and name and src == "rest_api":
# # #             # Apply rename override if user changed the name in Table.py
# # #             resolved_name = rename_overrides.get(name, name)
# # #             if resolved_name != name:
# # #                 print(f"  → Rename applied: '{name}' → '{resolved_name}' (pipeline: {pid})")
# # #             mapping[pid] = {"table_name": resolved_name, "primary_key": pk}

# # #     return mapping


# # # # ======================================================================
# # # # RENAME OVERRIDES  (Fix 2: commit user renames from Table.py)
# # # # ======================================================================

# # # def _build_rename_overrides(table_creation: dict) -> dict:
# # #     """
# # #     Build rename override map from table_creation_status.json.

# # #     When a user chooses option [2] in the Table Creation Agent approval step
# # #     and renames e.g. "api_result_pipeline_001" → "store_transactions",
# # #     Table.py writes:
# # #         {
# # #             "table_name":          "catalog.schema.store_transactions",   ← approved
# # #             "original_table_name": "catalog.schema.api_result_pipeline_001",
# # #             "status": "created"
# # #         }

# # #     This function extracts those pairs as bare names (no catalog/schema prefix):
# # #         {"api_result_pipeline_001": "store_transactions"}

# # #     final_gen.py passes this to _build_schema_table_map() so the rename
# # #     is reflected in migration_plan.json → target_table uses user's name.

# # #     Returns empty dict if no renames exist (user accepted all defaults).
# # #     """
# # #     overrides = {}
# # #     for t in table_creation.get("tables", []):
# # #         orig     = t.get("original_table_name", "")
# # #         approved = t.get("table_name", "")
# # #         status   = t.get("status", "")

# # #         if status != "created" or not orig or not approved:
# # #             continue

# # #         # Strip catalog.schema prefix → bare table name
# # #         orig_bare     = orig.split(".")[-1]
# # #         approved_bare = approved.split(".")[-1]

# # #         if orig_bare != approved_bare:
# # #             overrides[orig_bare] = approved_bare
# # #             print(f"  → User rename detected: '{orig_bare}' → '{approved_bare}'")

# # #     if overrides:
# # #         print(f"  ✓ {len(overrides)} user rename(s) will be applied to migration_plan.json")
# # #     return overrides


# # # # ======================================================================
# # # # SOURCE BLOCK DISPATCHER
# # # # ======================================================================

# # # def _build_source_block(source_type: str, pipeline: dict, ct: dict) -> dict:
# # #     if source_type == "s3":
# # #         return _build_s3_source(pipeline, ct)
# # #     elif source_type == "azure_sql":
# # #         return _build_azure_sql_source(pipeline, ct)
# # #     elif source_type == "postgresql":
# # #         return _build_postgresql_source(pipeline, ct)
# # #     elif source_type == "rest_api":
# # #         return _build_rest_api_source(pipeline, ct)
# # #     else:
# # #         raise ValueError(f"Unsupported source type '{source_type}'")


# # # # ======================================================================
# # # # SOURCE BLOCK BUILDERS
# # # # ======================================================================

# # # def _build_s3_source(pipeline: dict, ct: dict) -> dict:
# # #     return {
# # #         "source_type": "s3",
# # #         "engine":      pipeline.get("execution_engine", "spark"),
# # #         "connection": {
# # #             "access_key": (
# # #                 ct.get("aws_access_key_id")
# # #                 or ct.get("access_key")
# # #                 or ""
# # #             ),
# # #             "secret_key": (
# # #                 ct.get("aws_secret_access_key")
# # #                 or ct.get("secret_key")
# # #                 or ""
# # #             ),
# # #             "region": ct.get("region", "us-east-1"),
# # #         },
# # #         "location": {
# # #             "bucket": ct.get("bucket", ""),
# # #             "base_path": (
# # #                 ct.get("prefix")
# # #                 or ct.get("base_path")
# # #                 or ""
# # #             ),
# # #         },
# # #         "file_format": ct.get("file_format", "csv"),
# # #     }


# # # def _build_azure_sql_source(pipeline: dict, ct: dict) -> dict:
# # #     return {
# # #         "source_type": "azure_sql",
# # #         "engine":      pipeline.get("execution_engine", "spark"),
# # #         "connection": {
# # #             "host":     ct.get("server", ""),
# # #             "database": ct.get("database", ""),
# # #             "user":     ct.get("username", ""),
# # #             "password": ct.get("password", ""),
# # #         },
# # #         "schema": ct.get("schema", "dbo"),
# # #     }


# # # def _build_postgresql_source(pipeline: dict, ct: dict) -> dict:
# # #     return {
# # #         "source_type": "postgresql",
# # #         "engine":      pipeline.get("execution_engine", "spark"),
# # #         "connection": {
# # #             "host":     ct.get("host", ""),
# # #             "port":     int(ct.get("port", 5432)),
# # #             "database": ct.get("database", ""),
# # #             "user":     ct.get("username", ""),
# # #             "password": ct.get("password", ""),
# # #         },
# # #         "schema": ct.get("schema", "public"),
# # #     }


# # # def _build_rest_api_source(pipeline: dict, ct: dict) -> dict:
# # #     return {
# # #         "source_type": "rest_api",
# # #         "engine":      "python",
# # #         "connection": {
# # #             "endpoint":    ct.get("endpoint", ""),
# # #             "auth_type":   ct.get("auth_type", "none"),
# # #             "api_key":     ct.get("api_key", ""),
# # #             "header_name": ct.get("header_name", ""),
# # #         },
# # #     }


# # # # ======================================================================
# # # # TABLE BLOCK BUILDERS
# # # # ======================================================================

# # # def _build_db_tables(
# # #     strategy_pipeline: dict,
# # #     source_type:       str,
# # #     catalog:           str,
# # #     schema:            str,
# # # ) -> list:
# # #     """
# # #     Build table entries for S3, Azure SQL, PostgreSQL.
# # #     Table names come from source file/table name — already unique.
# # #     """
# # #     tables_block = []

# # #     for table in strategy_pipeline.get("tables", []):
# # #         src      = table["table_name"]
# # #         tgt_name = _format_table_name(src, source_type)
# # #         target   = f"{catalog}.{schema}.{tgt_name}"

# # #         tables_block.append({
# # #             "source_name":      src,
# # #             "target_table":     target,
# # #             "write_strategy":   table.get("write_strategy", "append"),
# # #             "load_type":        "incremental" if table.get("watermark_column") else "full",
# # #             "watermark_column": table.get("watermark_column"),
# # #             "partition_by":     table.get("partition_by"),
# # #             "parallelism":      table.get("parallelism", 1),
# # #         })

# # #     return tables_block


# # # def _build_rest_api_tables(
# # #     strategy_pipeline:       dict,
# # #     catalog:                 str,
# # #     schema:                  str,
# # #     watermark_control_table: str | None,
# # #     schema_table_map:        dict,         # pipeline_id → unique table name
# # # ) -> list:
# # #     """
# # #     Build table entries for REST API pipelines.

# # #     target_table is resolved from schema_table_map (schema_output.json) so
# # #     each API pipeline gets its own unique physical table:
# # #         pipeline_001 → catalog.schema.api_result_pipeline_001
# # #         pipeline_002 → catalog.schema.api_result_pipeline_002
# # #         ...
# # #         pipeline_007 → catalog.schema.api_result_pipeline_007

# # #     Falls back to strategy table_name if schema_output not available,
# # #     but this will cause all APIs to share one table — always pass schema_path.
# # #     """
# # #     pipeline_id  = strategy_pipeline["pipeline_id"]
# # #     tables_block = []

# # #     for table in strategy_pipeline.get("tables", []):
# # #         generic_name = table["table_name"]   # e.g. "api_result" — same for all APIs
# # #         api_id       = table.get("api_id") or pipeline_id

# # #         # ── Resolve unique table name + primary key from schema_output ─
# # #         schema_entry = schema_table_map.get(pipeline_id, {})
# # #         unique_name  = schema_entry.get("table_name", generic_name) if isinstance(schema_entry, dict) else schema_entry
# # #         primary_key  = schema_entry.get("primary_key") if isinstance(schema_entry, dict) else None
# # #         target       = f"{catalog}.{schema}.{unique_name}"

# # #         if unique_name == generic_name:
# # #             print(
# # #                 f"  ⚠ schema_output.json not found or missing entry for {pipeline_id}. "
# # #                 f"Using generic name '{generic_name}' — multiple APIs will share one table. "
# # #                 f"Pass schema_path to run_migration_plan() to fix this."
# # #             )

# # #         # Build merge_keys list from primary_key so write_merge uses the right key
# # #         merge_keys = [primary_key] if primary_key else None

# # #         tables_block.append({
# # #             "source_name":    unique_name,
# # #             "target_table":   target,
# # #             "write_strategy": "merge",
# # #             "load_type":      "incremental",
# # #             "merge_keys":     merge_keys,   # ← explicit MERGE ON key per API

# # #             # ── Watermark window config ───────────────────────────────
# # #             "watermark": {
# # #                 "control_table":     watermark_control_table,
# # #                 "source_name":       unique_name,   # ← watermark row key = destination table name
# # #                 "api_id":            api_id,
# # #                 "watermark_column":  table.get("watermark_column"),
# # #                 "default_watermark": table.get("default_watermark", "1900-01-01 00:00:00"),
# # #                 "lag_minutes":       table.get("lag_minutes",       5),
# # #                 "api_buffer_days":   table.get("api_buffer_days",   1),
# # #                 "precision_filter":  table.get("precision_filter",  True),
# # #                 "merge_keys":        merge_keys,
# # #             },

# # #             "partition_by": table.get("partition_by"),
# # #             "parallelism":  table.get("parallelism", 2),
# # #         })

# # #     return tables_block


# # # # ======================================================================
# # # # TABLE NAME FORMATTER  (S3 / SQL only)
# # # # ======================================================================

# # # def _format_table_name(name: str, source_type: str) -> str:
# # #     """
# # #     Normalise a source table/file name into a clean Delta table name.

# # #     S3:         folder/subfolder/sales_data.csv → sales_data
# # #     Azure SQL:  dbo.orders                      → orders
# # #     PostgreSQL: public.users                    → users
# # #     """
# # #     name = name.strip().lower()

# # #     if source_type == "s3":
# # #         name = name.split("/")[-1]
# # #         for ext in (".csv", ".parquet", ".json", ".orc", ".avro"):
# # #             if name.endswith(ext):
# # #                 name = name[: -len(ext)]
# # #                 break
# # #     else:
# # #         name = name.split(".")[-1]

# # #     return name.replace(" ", "_").replace("-", "_")


# # # # ======================================================================
# # # # SUMMARY PRINTER
# # # # ======================================================================

# # # def _print_summary(result: dict):
# # #     plan      = result.get("migration_plan", {})
# # #     pipelines = plan.get("pipelines", [])
# # #     wm_table  = plan.get("watermark_control_table")

# # #     print(f"\n  Target   : {plan['target']['catalog']}.{plan['target']['schema']}")
# # #     print(f"  Pipelines: {len(pipelines)}")

# # #     for p in pipelines:
# # #         src_type = p["source"]["source_type"]
# # #         for t in p.get("tables", []):
# # #             print(f"    → {p['pipeline_id']}  [{src_type}]  {t['target_table']}")

# # #     if wm_table:
# # #         print(f"\n  Watermark control table : {wm_table}")


# # # # ======================================================================
# # # # UTILITIES
# # # # ======================================================================

# # # def _load_json(path: str) -> dict:
# # #     if not path:
# # #         return {}
# # #     if not os.path.exists(path):
# # #         raise FileNotFoundError(f"File not found: {path}")
# # #     with open(path, "r", encoding="utf-8") as f:
# # #         return json.load(f)


# # # # ======================================================================
# # # # STANDALONE CLI
# # # # ======================================================================

# # # if __name__ == "__main__":

# # #     print("\n=== Migration Plan Generator ===\n")

# # #     cred_path     = input("Credentials JSON path          : ").strip()
# # #     table_path    = input("Table Creation JSON path       : ").strip()
# # #     strategy_path = input("Strategy JSON path             : ").strip()
# # #     schema_path   = input("Schema Output JSON path        : ").strip() or None
# # #     output_dir    = input("Output directory [.]           : ").strip() or "."

# # #     try:
# # #         result, path = run_migration_plan(
# # #             credentials_path    = cred_path,
# # #             table_creation_path = table_path,
# # #             strategy_path       = strategy_path,
# # #             output_dir          = output_dir,
# # #             schema_path         = schema_path,
# # #         )
# # #         print(json.dumps(result, indent=2))

# # #     except FileNotFoundError as exc:
# # #         print(f"\n❌ {exc}")
# # #     except Exception as exc:
# # #         print(f"\n❌ Unexpected error: {exc}")
# # #         raise



# # """
# # migration_plan.py — Migration Plan Generator
# # =============================================
# # Step 8 of the orchestration pipeline. Called by orchestrator.py.

# # Combines:
# #     credentials.json           (Step 2 — connection details)
# #     table_creation_status.json (Step 7 — catalog / schema / created tables)
# #     strategy.json              (Step 5 — ingestion mode, watermark, parallelism)
# #     schema_output.json         (Step 6 — unique table names per pipeline)

# # Produces:
# #     migration_plan.json        (consumed by Executor — Step 9)

# # Supported source types:
# #     s3         → spark engine
# #     azure_sql  → spark engine
# #     postgresql → spark engine
# #     rest_api   → python engine, carries full watermark window config

# # Key fix vs previous version:
# #     REST API target_table now comes from schema_output.json (which has unique
# #     names like api_result_pipeline_001) instead of strategy.json (which has
# #     the generic "api_result" for every pipeline). Without this fix, all 7 APIs
# #     wrote to the same table.

# # S3 filename fix:
# #     S3 files with dots in the filename (e.g. "dbt_delta_dev01.aderis.acuity.csv")
# #     previously produced multi-part table names like
# #     "catalog.schema.dbt_delta_dev01.aderis.acuity" — causing
# #     REQUIRES_SINGLE_PART_NAMESPACE. Fixed by replacing dots with underscores
# #     after stripping the file extension.
# # """

# # import json
# # import os
# # import re


# # # ======================================================================
# # # PROGRAMMATIC ENTRY  (called by orchestrator)
# # # ======================================================================

# # def run_migration_plan(
# #     credentials_path:    str,
# #     table_creation_path: str,
# #     strategy_path:       str,
# #     output_dir:          str = ".",
# #     schema_path:         str | None = None,
# # ) -> tuple:
# #     os.makedirs(output_dir, exist_ok=True)
# #     output_path = os.path.join(output_dir, "migration_plan.json")

# #     cred           = _load_json(credentials_path)
# #     table_creation = _load_json(table_creation_path)
# #     strategy       = _load_json(strategy_path)
# #     schema_output  = _load_json(schema_path) if schema_path else {}

# #     result = _generate_migration_plan(cred, table_creation, strategy, schema_output)

# #     with open(output_path, "w", encoding="utf-8") as f:
# #         json.dump(result, f, indent=2)

# #     print("\n✅ Migration plan generated successfully")
# #     print(f"   Saved to : {output_path}")
# #     _print_summary(result)

# #     return result, output_path


# # # ======================================================================
# # # CORE GENERATOR
# # # ======================================================================

# # def _generate_migration_plan(
# #     cred:           dict,
# #     table_creation: dict,
# #     strategy:       dict,
# #     schema_output:  dict,
# # ) -> dict:

# #     session_id = cred.get("session_id", "unknown")
# #     catalog    = table_creation.get("catalog", "")
# #     schema     = table_creation.get("schema",  "")

# #     strategy_map = {
# #         p["pipeline_id"]: p
# #         for p in strategy.get("pipelines", [])
# #     }

# #     rename_overrides = _build_rename_overrides(table_creation)
# #     schema_table_map = _build_schema_table_map(schema_output, rename_overrides)
# #     watermark_control_table = f"{catalog}.{schema}.bronze_watermark_control"

# #     final_pipelines = []
# #     skipped         = []

# #     for cred_pipeline in cred.get("pipelines", []):

# #         pipeline_id = cred_pipeline["pipeline_id"]
# #         source_type = cred_pipeline["source_type"].lower()

# #         strategy_pipeline = strategy_map.get(pipeline_id)
# #         if not strategy_pipeline:
# #             print(f"  ⚠ Strategy missing for pipeline {pipeline_id} — skipping.")
# #             skipped.append(pipeline_id)
# #             continue

# #         if cred_pipeline.get("status") == "credentials_failed":
# #             print(f"  ⚠ Credentials failed for {pipeline_id} — skipping.")
# #             skipped.append(pipeline_id)
# #             continue

# #         ct = (
# #             cred_pipeline.get("connection_template")
# #             or cred_pipeline.get("connection")
# #             or {}
# #         )

# #         try:
# #             source_block = _build_source_block(source_type, cred_pipeline, ct)
# #         except ValueError as exc:
# #             print(f"  ⚠ {exc} — skipping {pipeline_id}.")
# #             skipped.append(pipeline_id)
# #             continue

# #         if source_type == "rest_api":
# #             tables_block = _build_rest_api_tables(
# #                 strategy_pipeline,
# #                 catalog,
# #                 schema,
# #                 watermark_control_table,
# #                 schema_table_map,
# #             )
# #         else:
# #             tables_block = _build_db_tables(
# #                 strategy_pipeline, source_type, catalog, schema
# #             )

# #         final_pipelines.append({
# #             "pipeline_id": pipeline_id,
# #             "source":      source_block,
# #             "tables":      tables_block,
# #         })

# #     if skipped:
# #         print(f"\n  ⚠ Skipped pipelines: {', '.join(skipped)}")

# #     return {
# #         "session_id": session_id,
# #         "migration_plan": {
# #             "target": {
# #                 "catalog": catalog,
# #                 "schema":  schema,
# #             },
# #             "watermark_control_table": watermark_control_table,
# #             "pipelines":               final_pipelines,
# #         },
# #     }


# # # ======================================================================
# # # SCHEMA OUTPUT LOOKUP
# # # ======================================================================

# # def _build_schema_table_map(schema_output: dict, rename_overrides: dict = None) -> dict:
# #     rename_overrides = rename_overrides or {}
# #     mapping = {}
# #     for t in schema_output.get("tables", []):
# #         pid  = t.get("pipeline_id", "")
# #         name = t.get("table_name", "")
# #         src  = t.get("source_type", "")
# #         pk   = t.get("primary_key")

# #         if pid and name and src == "rest_api":
# #             resolved_name = rename_overrides.get(name, name)
# #             if resolved_name != name:
# #                 print(f"  → Rename applied: '{name}' → '{resolved_name}' (pipeline: {pid})")
# #             mapping[pid] = {"table_name": resolved_name, "primary_key": pk}

# #     return mapping


# # # ======================================================================
# # # RENAME OVERRIDES
# # # ======================================================================

# # def _build_rename_overrides(table_creation: dict) -> dict:
# #     overrides = {}
# #     for t in table_creation.get("tables", []):
# #         orig     = t.get("original_table_name", "")
# #         approved = t.get("table_name", "")
# #         status   = t.get("status", "")

# #         if status != "created" or not orig or not approved:
# #             continue

# #         orig_bare     = orig.split(".")[-1]
# #         approved_bare = approved.split(".")[-1]

# #         if orig_bare != approved_bare:
# #             overrides[orig_bare] = approved_bare
# #             print(f"  → User rename detected: '{orig_bare}' → '{approved_bare}'")

# #     if overrides:
# #         print(f"  ✓ {len(overrides)} user rename(s) will be applied to migration_plan.json")
# #     return overrides


# # # ======================================================================
# # # SOURCE BLOCK DISPATCHER
# # # ======================================================================

# # def _build_source_block(source_type: str, pipeline: dict, ct: dict) -> dict:
# #     if source_type == "s3":
# #         return _build_s3_source(pipeline, ct)
# #     elif source_type == "azure_sql":
# #         return _build_azure_sql_source(pipeline, ct)
# #     elif source_type == "postgresql":
# #         return _build_postgresql_source(pipeline, ct)
# #     elif source_type == "rest_api":
# #         return _build_rest_api_source(pipeline, ct)
# #     else:
# #         raise ValueError(f"Unsupported source type '{source_type}'")


# # # ======================================================================
# # # SOURCE BLOCK BUILDERS
# # # ======================================================================

# # def _build_s3_source(pipeline: dict, ct: dict) -> dict:
# #     return {
# #         "source_type": "s3",
# #         "engine":      pipeline.get("execution_engine", "spark"),
# #         "connection": {
# #             "access_key": (
# #                 ct.get("aws_access_key_id")
# #                 or ct.get("access_key")
# #                 or ""
# #             ),
# #             "secret_key": (
# #                 ct.get("aws_secret_access_key")
# #                 or ct.get("secret_key")
# #                 or ""
# #             ),
# #             "region": ct.get("region", "us-east-1"),
# #         },
# #         "location": {
# #             "bucket":    ct.get("bucket", ""),
# #             "base_path": (
# #                 ct.get("prefix")
# #                 or ct.get("base_path")
# #                 or ""
# #             ),
# #         },
# #         "file_format": ct.get("file_format", "csv"),
# #     }


# # def _build_azure_sql_source(pipeline: dict, ct: dict) -> dict:
# #     return {
# #         "source_type": "azure_sql",
# #         "engine":      pipeline.get("execution_engine", "spark"),
# #         "connection": {
# #             "host":     ct.get("server", ""),
# #             "database": ct.get("database", ""),
# #             "user":     ct.get("username", ""),
# #             "password": ct.get("password", ""),
# #         },
# #         "schema": ct.get("schema", "dbo"),
# #     }


# # def _build_postgresql_source(pipeline: dict, ct: dict) -> dict:
# #     return {
# #         "source_type": "postgresql",
# #         "engine":      pipeline.get("execution_engine", "spark"),
# #         "connection": {
# #             "host":     ct.get("host", ""),
# #             "port":     int(ct.get("port", 5432)),
# #             "database": ct.get("database", ""),
# #             "user":     ct.get("username", ""),
# #             "password": ct.get("password", ""),
# #         },
# #         "schema": ct.get("schema", "public"),
# #     }


# # def _build_rest_api_source(pipeline: dict, ct: dict) -> dict:
# #     return {
# #         "source_type": "rest_api",
# #         "engine":      "python",
# #         "connection": {
# #             "endpoint":    ct.get("endpoint", ""),
# #             "auth_type":   ct.get("auth_type", "none"),
# #             "api_key":     ct.get("api_key", ""),
# #             "header_name": ct.get("header_name", ""),
# #         },
# #     }


# # # ======================================================================
# # # TABLE BLOCK BUILDERS
# # # ======================================================================

# # def _build_db_tables(
# #     strategy_pipeline: dict,
# #     source_type:       str,
# #     catalog:           str,
# #     schema:            str,
# # ) -> list:
# #     tables_block = []

# #     for table in strategy_pipeline.get("tables", []):
# #         src      = table["table_name"]
# #         tgt_name = _format_table_name(src, source_type)
# #         target   = f"{catalog}.{schema}.{tgt_name}"

# #         tables_block.append({
# #             "source_name":      src,
# #             "target_table":     target,
# #             "write_strategy":   table.get("write_strategy", "append"),
# #             "load_type":        "incremental" if table.get("watermark_column") else "full",
# #             "watermark_column": table.get("watermark_column"),
# #             "partition_by":     table.get("partition_by"),
# #             "parallelism":      table.get("parallelism", 1),
# #         })

# #     return tables_block


# # def _build_rest_api_tables(
# #     strategy_pipeline:       dict,
# #     catalog:                 str,
# #     schema:                  str,
# #     watermark_control_table: str | None,
# #     schema_table_map:        dict,
# # ) -> list:
# #     pipeline_id  = strategy_pipeline["pipeline_id"]
# #     tables_block = []

# #     for table in strategy_pipeline.get("tables", []):
# #         generic_name = table["table_name"]
# #         api_id       = table.get("api_id") or pipeline_id

# #         schema_entry = schema_table_map.get(pipeline_id, {})
# #         unique_name  = schema_entry.get("table_name", generic_name) if isinstance(schema_entry, dict) else schema_entry
# #         primary_key  = schema_entry.get("primary_key") if isinstance(schema_entry, dict) else None
# #         target       = f"{catalog}.{schema}.{unique_name}"

# #         if unique_name == generic_name:
# #             print(
# #                 f"  ⚠ schema_output.json missing entry for {pipeline_id}. "
# #                 f"Using generic name '{generic_name}'."
# #             )

# #         merge_keys = [primary_key] if primary_key else None

# #         tables_block.append({
# #             "source_name":    unique_name,
# #             "target_table":   target,
# #             "write_strategy": "merge",
# #             "load_type":      "incremental",
# #             "merge_keys":     merge_keys,
# #             "watermark": {
# #                 "control_table":     watermark_control_table,
# #                 "source_name":       unique_name,
# #                 "api_id":            api_id,
# #                 "watermark_column":  table.get("watermark_column"),
# #                 "default_watermark": table.get("default_watermark", "1900-01-01 00:00:00"),
# #                 "lag_minutes":       table.get("lag_minutes",       5),
# #                 "api_buffer_days":   table.get("api_buffer_days",   1),
# #                 "precision_filter":  table.get("precision_filter",  True),
# #                 "merge_keys":        merge_keys,
# #             },
# #             "partition_by": table.get("partition_by"),
# #             "parallelism":  table.get("parallelism", 2),
# #         })

# #     return tables_block


# # # ======================================================================
# # # TABLE NAME FORMATTER
# # # ======================================================================

# # def _format_table_name(name: str, source_type: str) -> str:
# #     """
# #     Normalise a source table/file name into a clean single-part Delta table name.

# #     BUG FIXED: S3 files with dots in the filename (e.g.
# #     "dbt_delta_dev01.aderis.aderis_acuity_raw.csv") previously produced
# #     multi-part table names like:
# #         catalog.schema.dbt_delta_dev01.aderis.aderis_acuity_raw
# #     Spark rejected this with REQUIRES_SINGLE_PART_NAMESPACE because it
# #     interpreted it as a 5-part identifier.

# #     Fix: after stripping the file extension, ALL remaining dots are replaced
# #     with underscores so the result is always a valid single-part name.

# #     Examples:
# #         S3:   folder/sub/sales_data.csv                  → sales_data
# #         S3:   dbt_delta_dev01.aderis.acuity_raw.csv      → dbt_delta_dev01_aderis_acuity_raw
# #         S3:   my-report 2024.parquet                     → my_report_2024
# #         SQL:  dbo.orders                                 → orders
# #         SQL:  public.users                               → users
# #     """
# #     name = name.strip().lower()

# #     if source_type == "s3":
# #         # Take only the filename (drop path prefix)
# #         name = name.split("/")[-1]
# #         # Strip known file extensions
# #         for ext in (".csv", ".parquet", ".json", ".orc", ".avro", ".tsv", ".txt"):
# #             if name.endswith(ext):
# #                 name = name[: -len(ext)]
# #                 break
# #     else:
# #         # SQL sources: take only the part after the last dot (schema.table → table)
# #         name = name.split(".")[-1]

# #     # Replace ALL dots, spaces, dashes with underscores
# #     # Dots in S3 filenames must become underscores — this is the core fix
# #     name = name.replace(".", "_").replace(" ", "_").replace("-", "_")

# #     # Collapse consecutive underscores and strip leading/trailing ones
# #     name = re.sub(r"_+", "_", name).strip("_")

# #     return name


# # # ======================================================================
# # # SUMMARY PRINTER
# # # ======================================================================

# # def _print_summary(result: dict):
# #     plan      = result.get("migration_plan", {})
# #     pipelines = plan.get("pipelines", [])
# #     wm_table  = plan.get("watermark_control_table")

# #     print(f"\n  Target   : {plan['target']['catalog']}.{plan['target']['schema']}")
# #     print(f"  Pipelines: {len(pipelines)}")

# #     for p in pipelines:
# #         src_type = p["source"]["source_type"]
# #         for t in p.get("tables", []):
# #             print(f"    → {p['pipeline_id']}  [{src_type}]  {t['target_table']}")

# #     if wm_table:
# #         print(f"\n  Watermark control table : {wm_table}")


# # # ======================================================================
# # # UTILITIES
# # # ======================================================================

# # def _load_json(path: str) -> dict:
# #     if not path:
# #         return {}
# #     if not os.path.exists(path):
# #         raise FileNotFoundError(f"File not found: {path}")
# #     with open(path, "r", encoding="utf-8") as f:
# #         return json.load(f)


# # # ======================================================================
# # # STANDALONE CLI
# # # ======================================================================

# # if __name__ == "__main__":

# #     print("\n=== Migration Plan Generator ===\n")

# #     cred_path     = input("Credentials JSON path          : ").strip()
# #     table_path    = input("Table Creation JSON path       : ").strip()
# #     strategy_path = input("Strategy JSON path             : ").strip()
# #     schema_path   = input("Schema Output JSON path        : ").strip() or None
# #     output_dir    = input("Output directory [.]           : ").strip() or "."

# #     try:
# #         result, path = run_migration_plan(
# #             credentials_path    = cred_path,
# #             table_creation_path = table_path,
# #             strategy_path       = strategy_path,
# #             output_dir          = output_dir,
# #             schema_path         = schema_path,
# #         )
# #         print(json.dumps(result, indent=2))

# #     except FileNotFoundError as exc:
# #         print(f"\n❌ {exc}")
# #     except Exception as exc:
# #         print(f"\n❌ Unexpected error: {exc}")
# #         raise



# """
# migration_plan.py — Migration Plan Generator
# =============================================
# Step 8 of the orchestration pipeline. Called by orchestrator.py.

# Combines:
#     credentials.json           (Step 2 — connection details)
#     table_creation_status.json (Step 7 — catalog / schema / created tables)
#     strategy.json              (Step 5 — ingestion mode, watermark, parallelism)
#     schema_output.json         (Step 6 — unique table names per pipeline)

# Produces:
#     migration_plan.json        (consumed by Executor — Step 9)

# Supported source types:
#     s3         → spark engine
#     azure_sql  → spark engine
#     postgresql → spark engine
#     rest_api   → python engine, carries full watermark window config

# Key fix vs previous version:
#     REST API target_table now comes from schema_output.json (which has unique
#     names like api_result_pipeline_001) instead of strategy.json (which has
#     the generic "api_result" for every pipeline). Without this fix, all 7 APIs
#     wrote to the same table.

# S3 filename fix:
#     S3 files with dots in the filename (e.g. "dbt_delta_dev01.aderis.acuity.csv")
#     previously produced multi-part table names like
#     "catalog.schema.dbt_delta_dev01.aderis.acuity" — causing
#     REQUIRES_SINGLE_PART_NAMESPACE. Fixed by replacing dots with underscores
#     after stripping the file extension.
# """

# import json
# import os
# import re


# # ======================================================================
# # PROGRAMMATIC ENTRY  (called by orchestrator)
# # ======================================================================

# def run_migration_plan(
#     credentials_path:    str,
#     table_creation_path: str,
#     strategy_path:       str,
#     output_dir:          str = ".",
#     schema_path:         str | None = None,
# ) -> tuple:
#     os.makedirs(output_dir, exist_ok=True)
#     output_path = os.path.join(output_dir, "migration_plan.json")

#     cred           = _load_json(credentials_path)
#     table_creation = _load_json(table_creation_path)
#     strategy       = _load_json(strategy_path)
#     schema_output  = _load_json(schema_path) if schema_path else {}

#     result = _generate_migration_plan(cred, table_creation, strategy, schema_output)

#     with open(output_path, "w", encoding="utf-8") as f:
#         json.dump(result, f, indent=2)

#     print("\n✅ Migration plan generated successfully")
#     print(f"   Saved to : {output_path}")
#     _print_summary(result)

#     return result, output_path


# # ======================================================================
# # CORE GENERATOR
# # ======================================================================

# def _generate_migration_plan(
#     cred:           dict,
#     table_creation: dict,
#     strategy:       dict,
#     schema_output:  dict,
# ) -> dict:

#     session_id = cred.get("session_id", "unknown")
#     catalog    = table_creation.get("catalog", "")
#     schema     = table_creation.get("schema",  "")

#     strategy_map = {
#         p["pipeline_id"]: p
#         for p in strategy.get("pipelines", [])
#     }

#     rename_overrides = _build_rename_overrides(table_creation)
#     schema_table_map = _build_schema_table_map(schema_output, rename_overrides)
#     watermark_control_table = f"{catalog}.{schema}.bronze_watermark_control"

#     final_pipelines = []
#     skipped         = []

#     for cred_pipeline in cred.get("pipelines", []):

#         pipeline_id = cred_pipeline["pipeline_id"]
#         source_type = cred_pipeline["source_type"].lower()

#         strategy_pipeline = strategy_map.get(pipeline_id)
#         if not strategy_pipeline:
#             print(f"  ⚠ Strategy missing for pipeline {pipeline_id} — skipping.")
#             skipped.append(pipeline_id)
#             continue

#         if cred_pipeline.get("status") == "credentials_failed":
#             print(f"  ⚠ Credentials failed for {pipeline_id} — skipping.")
#             skipped.append(pipeline_id)
#             continue

#         ct = (
#             cred_pipeline.get("connection_template")
#             or cred_pipeline.get("connection")
#             or {}
#         )

#         try:
#             source_block = _build_source_block(source_type, cred_pipeline, ct)
#         except ValueError as exc:
#             print(f"  ⚠ {exc} — skipping {pipeline_id}.")
#             skipped.append(pipeline_id)
#             continue

#         if source_type == "rest_api":
#             tables_block = _build_rest_api_tables(
#                 strategy_pipeline,
#                 catalog,
#                 schema,
#                 watermark_control_table,
#                 schema_table_map,
#             )
#         else:
#             tables_block = _build_db_tables(
#                 strategy_pipeline, source_type, catalog, schema,
#                 schema_output,   # ← pass schema_output so we use schema agent's table names
#             )

#         final_pipelines.append({
#             "pipeline_id": pipeline_id,
#             "source":      source_block,
#             "tables":      tables_block,
#         })

#     if skipped:
#         print(f"\n  ⚠ Skipped pipelines: {', '.join(skipped)}")

#     return {
#         "session_id": session_id,
#         "migration_plan": {
#             "target": {
#                 "catalog": catalog,
#                 "schema":  schema,
#             },
#             "watermark_control_table": watermark_control_table,
#             "pipelines":               final_pipelines,
#         },
#     }


# # ======================================================================
# # SCHEMA OUTPUT LOOKUP
# # ======================================================================

# def _build_schema_table_map(schema_output: dict, rename_overrides: dict = None) -> dict:
#     rename_overrides = rename_overrides or {}
#     mapping = {}
#     for t in schema_output.get("tables", []):
#         pid  = t.get("pipeline_id", "")
#         name = t.get("table_name", "")
#         src  = t.get("source_type", "")
#         pk   = t.get("primary_key")

#         if pid and name and src == "rest_api":
#             resolved_name = rename_overrides.get(name, name)
#             if resolved_name != name:
#                 print(f"  → Rename applied: '{name}' → '{resolved_name}' (pipeline: {pid})")
#             mapping[pid] = {"table_name": resolved_name, "primary_key": pk}

#     return mapping


# # ======================================================================
# # RENAME OVERRIDES
# # ======================================================================

# def _build_rename_overrides(table_creation: dict) -> dict:
#     overrides = {}
#     for t in table_creation.get("tables", []):
#         orig     = t.get("original_table_name", "")
#         approved = t.get("table_name", "")
#         status   = t.get("status", "")

#         if status != "created" or not orig or not approved:
#             continue

#         orig_bare     = orig.split(".")[-1]
#         approved_bare = approved.split(".")[-1]

#         if orig_bare != approved_bare:
#             overrides[orig_bare] = approved_bare
#             print(f"  → User rename detected: '{orig_bare}' → '{approved_bare}'")

#     if overrides:
#         print(f"  ✓ {len(overrides)} user rename(s) will be applied to migration_plan.json")
#     return overrides


# # ======================================================================
# # SOURCE BLOCK DISPATCHER
# # ======================================================================

# def _build_source_block(source_type: str, pipeline: dict, ct: dict) -> dict:
#     if source_type == "s3":
#         return _build_s3_source(pipeline, ct)
#     elif source_type == "azure_sql":
#         return _build_azure_sql_source(pipeline, ct)
#     elif source_type == "postgresql":
#         return _build_postgresql_source(pipeline, ct)
#     elif source_type == "rest_api":
#         return _build_rest_api_source(pipeline, ct)
#     else:
#         raise ValueError(f"Unsupported source type '{source_type}'")


# # ======================================================================
# # SOURCE BLOCK BUILDERS
# # ======================================================================

# def _build_s3_source(pipeline: dict, ct: dict) -> dict:
#     return {
#         "source_type": "s3",
#         "engine":      pipeline.get("execution_engine", "spark"),
#         "connection": {
#             "access_key": (
#                 ct.get("aws_access_key_id")
#                 or ct.get("access_key")
#                 or ""
#             ),
#             "secret_key": (
#                 ct.get("aws_secret_access_key")
#                 or ct.get("secret_key")
#                 or ""
#             ),
#             "region": ct.get("region", "us-east-1"),
#         },
#         "location": {
#             "bucket":    ct.get("bucket", ""),
#             "base_path": (
#                 ct.get("prefix")
#                 or ct.get("base_path")
#                 or ""
#             ),
#         },
#         "file_format": ct.get("file_format", "csv"),
#     }


# def _build_azure_sql_source(pipeline: dict, ct: dict) -> dict:
#     return {
#         "source_type": "azure_sql",
#         "engine":      pipeline.get("execution_engine", "spark"),
#         "connection": {
#             "host":     ct.get("server", ""),
#             "database": ct.get("database", ""),
#             "user":     ct.get("username", ""),
#             "password": ct.get("password", ""),
#         },
#         "schema": ct.get("schema", "dbo"),
#     }


# def _build_postgresql_source(pipeline: dict, ct: dict) -> dict:
#     return {
#         "source_type": "postgresql",
#         "engine":      pipeline.get("execution_engine", "spark"),
#         "connection": {
#             "host":     ct.get("host", ""),
#             "port":     int(ct.get("port", 5432)),
#             "database": ct.get("database", ""),
#             "user":     ct.get("username", ""),
#             "password": ct.get("password", ""),
#         },
#         "schema": ct.get("schema", "public"),
#     }


# def _build_rest_api_source(pipeline: dict, ct: dict) -> dict:
#     return {
#         "source_type": "rest_api",
#         "engine":      "python",
#         "connection": {
#             "endpoint":    ct.get("endpoint", ""),
#             "auth_type":   ct.get("auth_type", "none"),
#             "api_key":     ct.get("api_key", ""),
#             "header_name": ct.get("header_name", ""),
#         },
#     }


# # ======================================================================
# # TABLE BLOCK BUILDERS
# # ======================================================================

# def _build_db_tables(
#     strategy_pipeline: dict,
#     source_type:       str,
#     catalog:           str,
#     schema:            str,
#     schema_output:     dict = None,
# ) -> list:
#     """
#     Build table entries for S3, Azure SQL, PostgreSQL.

#     Target table name comes from schema_output["tables"][i]["table_name"]
#     (the clean short name the schema agent decided, e.g. "aderis_acuity_raw").

#     This is the SAME name Table.py creates in Databricks.

#     Previously used _format_table_name(source_filename) which produced a long
#     name like "dbt_delta_dev01_aderis_aderis_acuity_raw" — different from the
#     short name Table.py created — causing duplicate tables and write failures.
#     """
#     # Build lookup: source_name → schema table_name
#     # schema_output["tables"] entries look like:
#     #   {"table_name": "aderis_acuity_raw", "source_name": "dbt_delta_dev01.aderis.aderis_acuity_raw.csv"}
#     schema_name_lookup = {}
#     if schema_output:
#         for t in schema_output.get("tables", []):
#             src  = t.get("source_name", "")
#             name = t.get("table_name",  "") or t.get("approved_table_name", "")
#             if src and name:
#                 schema_name_lookup[src] = name

#     tables_block = []

#     for table in strategy_pipeline.get("tables", []):
#         src_name = table["table_name"]   # raw source filename from strategy.json

#         # Use schema_output table_name if available (matches what Table.py created)
#         # Fall back to sanitizing source filename only if schema_output has no entry
#         if src_name in schema_name_lookup:
#             tgt_name = schema_name_lookup[src_name]
#         else:
#             tgt_name = _format_table_name(src_name, source_type)

#         target = f"{catalog}.{schema}.{tgt_name}"

#         tables_block.append({
#             "source_name":      src_name,
#             "target_table":     target,
#             "write_strategy":   table.get("write_strategy", "append"),
#             "load_type":        "incremental" if table.get("watermark_column") else "full",
#             "watermark_column": table.get("watermark_column"),
#             "partition_by":     table.get("partition_by"),
#             "parallelism":      table.get("parallelism", 1),
#         })

#     return tables_block


# def _build_rest_api_tables(
#     strategy_pipeline:       dict,
#     catalog:                 str,
#     schema:                  str,
#     watermark_control_table: str | None,
#     schema_table_map:        dict,
# ) -> list:
#     pipeline_id  = strategy_pipeline["pipeline_id"]
#     tables_block = []

#     for table in strategy_pipeline.get("tables", []):
#         generic_name = table["table_name"]
#         api_id       = table.get("api_id") or pipeline_id

#         schema_entry = schema_table_map.get(pipeline_id, {})
#         unique_name  = schema_entry.get("table_name", generic_name) if isinstance(schema_entry, dict) else schema_entry
#         primary_key  = schema_entry.get("primary_key") if isinstance(schema_entry, dict) else None
#         target       = f"{catalog}.{schema}.{unique_name}"

#         if unique_name == generic_name:
#             print(
#                 f"  ⚠ schema_output.json missing entry for {pipeline_id}. "
#                 f"Using generic name '{generic_name}'."
#             )

#         merge_keys = [primary_key] if primary_key else None

#         tables_block.append({
#             "source_name":    unique_name,
#             "target_table":   target,
#             "write_strategy": "merge",
#             "load_type":      "incremental",
#             "merge_keys":     merge_keys,
#             "watermark": {
#                 "control_table":     watermark_control_table,
#                 "source_name":       unique_name,
#                 "api_id":            api_id,
#                 "watermark_column":  table.get("watermark_column"),
#                 "default_watermark": table.get("default_watermark", "1900-01-01 00:00:00"),
#                 "lag_minutes":       table.get("lag_minutes",       5),
#                 "api_buffer_days":   table.get("api_buffer_days",   1),
#                 "precision_filter":  table.get("precision_filter",  True),
#                 "merge_keys":        merge_keys,
#             },
#             "partition_by": table.get("partition_by"),
#             "parallelism":  table.get("parallelism", 2),
#         })

#     return tables_block


# # ======================================================================
# # TABLE NAME FORMATTER
# # ======================================================================

# def _format_table_name(name: str, source_type: str) -> str:
#     """
#     Normalise a source table/file name into a clean single-part Delta table name.

#     BUG FIXED: S3 files with dots in the filename (e.g.
#     "dbt_delta_dev01.aderis.aderis_acuity_raw.csv") previously produced
#     multi-part table names like:
#         catalog.schema.dbt_delta_dev01.aderis.aderis_acuity_raw
#     Spark rejected this with REQUIRES_SINGLE_PART_NAMESPACE because it
#     interpreted it as a 5-part identifier.

#     Fix: after stripping the file extension, ALL remaining dots are replaced
#     with underscores so the result is always a valid single-part name.

#     Examples:
#         S3:   folder/sub/sales_data.csv                  → sales_data
#         S3:   dbt_delta_dev01.aderis.acuity_raw.csv      → dbt_delta_dev01_aderis_acuity_raw
#         S3:   my-report 2024.parquet                     → my_report_2024
#         SQL:  dbo.orders                                 → orders
#         SQL:  public.users                               → users
#     """
#     name = name.strip().lower()

#     if source_type == "s3":
#         # Take only the filename (drop path prefix)
#         name = name.split("/")[-1]
#         # Strip known file extensions
#         for ext in (".csv", ".parquet", ".json", ".orc", ".avro", ".tsv", ".txt"):
#             if name.endswith(ext):
#                 name = name[: -len(ext)]
#                 break
#     else:
#         # SQL sources: take only the part after the last dot (schema.table → table)
#         name = name.split(".")[-1]

#     # Replace ALL dots, spaces, dashes with underscores
#     # Dots in S3 filenames must become underscores — this is the core fix
#     name = name.replace(".", "_").replace(" ", "_").replace("-", "_")

#     # Collapse consecutive underscores and strip leading/trailing ones
#     name = re.sub(r"_+", "_", name).strip("_")

#     return name


# # ======================================================================
# # SUMMARY PRINTER
# # ======================================================================

# def _print_summary(result: dict):
#     plan      = result.get("migration_plan", {})
#     pipelines = plan.get("pipelines", [])
#     wm_table  = plan.get("watermark_control_table")

#     print(f"\n  Target   : {plan['target']['catalog']}.{plan['target']['schema']}")
#     print(f"  Pipelines: {len(pipelines)}")

#     for p in pipelines:
#         src_type = p["source"]["source_type"]
#         for t in p.get("tables", []):
#             print(f"    → {p['pipeline_id']}  [{src_type}]  {t['target_table']}")

#     if wm_table:
#         print(f"\n  Watermark control table : {wm_table}")


# # ======================================================================
# # UTILITIES
# # ======================================================================

# def _load_json(path: str) -> dict:
#     if not path:
#         return {}
#     if not os.path.exists(path):
#         raise FileNotFoundError(f"File not found: {path}")
#     with open(path, "r", encoding="utf-8") as f:
#         return json.load(f)


# # ======================================================================
# # STANDALONE CLI
# # ======================================================================

# if __name__ == "__main__":

#     print("\n=== Migration Plan Generator ===\n")

#     cred_path     = input("Credentials JSON path          : ").strip()
#     table_path    = input("Table Creation JSON path       : ").strip()
#     strategy_path = input("Strategy JSON path             : ").strip()
#     schema_path   = input("Schema Output JSON path        : ").strip() or None
#     output_dir    = input("Output directory [.]           : ").strip() or "."

#     try:
#         result, path = run_migration_plan(
#             credentials_path    = cred_path,
#             table_creation_path = table_path,
#             strategy_path       = strategy_path,
#             output_dir          = output_dir,
#             schema_path         = schema_path,
#         )
#         print(json.dumps(result, indent=2))

#     except FileNotFoundError as exc:
#         print(f"\n❌ {exc}")
#     except Exception as exc:
#         print(f"\n❌ Unexpected error: {exc}")
#         raise


"""
migration_plan.py — Migration Plan Generator
=============================================
Step 8 of the orchestration pipeline. Called by orchestrator.py.

Combines:
    credentials.json           (Step 2 — connection details)
    table_creation_status.json (Step 7 — catalog / schema / created tables)
    strategy.json              (Step 5 — ingestion mode, watermark, parallelism)
    schema_output.json         (Step 6 — unique table names per pipeline)

Produces:
    migration_plan.json        (consumed by Executor — Step 9)

Supported source types:
    s3         → spark engine
    azure_sql  → spark engine
    postgresql → spark engine
    rest_api   → python engine, carries full watermark window config

Key fix vs previous version:
    REST API target_table now comes from schema_output.json (which has unique
    names like api_result_pipeline_001) instead of strategy.json (which has
    the generic "api_result" for every pipeline). Without this fix, all 7 APIs
    wrote to the same table.

S3 filename fix:
    S3 files with dots in the filename (e.g. "dbt_delta_dev01.aderis.acuity.csv")
    previously produced multi-part table names like
    "catalog.schema.dbt_delta_dev01.aderis.acuity" — causing
    REQUIRES_SINGLE_PART_NAMESPACE. Fixed by replacing dots with underscores
    after stripping the file extension.
"""

import json
import os
import re


# ======================================================================
# PROGRAMMATIC ENTRY  (called by orchestrator)
# ======================================================================

def run_migration_plan(
    credentials_path:    str,
    table_creation_path: str,
    strategy_path:       str,
    output_dir:          str = ".",
    schema_path:         str | None = None,
) -> tuple:
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "migration_plan.json")

    cred           = _load_json(credentials_path)
    table_creation = _load_json(table_creation_path)
    strategy       = _load_json(strategy_path)
    schema_output  = _load_json(schema_path) if schema_path else {}

    result = _generate_migration_plan(cred, table_creation, strategy, schema_output)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print("\n✅ Migration plan generated successfully")
    print(f"   Saved to : {output_path}")
    _print_summary(result)

    return result, output_path


# ======================================================================
# CORE GENERATOR
# ======================================================================

def _generate_migration_plan(
    cred:           dict,
    table_creation: dict,
    strategy:       dict,
    schema_output:  dict,
) -> dict:

    session_id = cred.get("session_id", "unknown")
    catalog    = table_creation.get("catalog", "")
    schema     = table_creation.get("schema",  "")

    strategy_map = {
        p["pipeline_id"]: p
        for p in strategy.get("pipelines", [])
    }

    rename_overrides     = _build_rename_overrides(table_creation)
    schema_table_map     = _build_schema_table_map(schema_output, rename_overrides)
    # Build lookup: source_name → exact full table name that Table.py physically created
    created_table_lookup = _build_created_table_lookup(table_creation)
    watermark_control_table = f"{catalog}.{schema}.bronze_watermark_control"

    final_pipelines = []
    skipped         = []

    for cred_pipeline in cred.get("pipelines", []):

        pipeline_id = cred_pipeline["pipeline_id"]
        source_type = cred_pipeline["source_type"].lower()

        strategy_pipeline = strategy_map.get(pipeline_id)
        if not strategy_pipeline:
            print(f"  ⚠ Strategy missing for pipeline {pipeline_id} — skipping.")
            skipped.append(pipeline_id)
            continue

        if cred_pipeline.get("status") == "credentials_failed":
            print(f"  ⚠ Credentials failed for {pipeline_id} — skipping.")
            skipped.append(pipeline_id)
            continue

        ct = (
            cred_pipeline.get("connection_template")
            or cred_pipeline.get("connection")
            or {}
        )

        try:
            source_block = _build_source_block(source_type, cred_pipeline, ct)
        except ValueError as exc:
            print(f"  ⚠ {exc} — skipping {pipeline_id}.")
            skipped.append(pipeline_id)
            continue

        if source_type == "rest_api":
            tables_block = _build_rest_api_tables(
                strategy_pipeline,
                catalog,
                schema,
                watermark_control_table,
                schema_table_map,
            )
        else:
            tables_block = _build_db_tables(
                strategy_pipeline, source_type, catalog, schema,
                schema_output,
                created_table_lookup,  # ← exact names Table.py physically created
            )

        final_pipelines.append({
            "pipeline_id": pipeline_id,
            "source":      source_block,
            "tables":      tables_block,
        })

    if skipped:
        print(f"\n  ⚠ Skipped pipelines: {', '.join(skipped)}")

    return {
        "session_id": session_id,
        "migration_plan": {
            "target": {
                "catalog": catalog,
                "schema":  schema,
            },
            "watermark_control_table": watermark_control_table,
            "pipelines":               final_pipelines,
        },
    }


# ======================================================================
# SCHEMA OUTPUT LOOKUP
# ======================================================================

def _build_schema_table_map(schema_output: dict, rename_overrides: dict = None) -> dict:
    rename_overrides = rename_overrides or {}
    mapping = {}
    for t in schema_output.get("tables", []):
        pid  = t.get("pipeline_id", "")
        name = t.get("table_name", "")
        src  = t.get("source_type", "")
        pk   = t.get("primary_key")

        if pid and name and src == "rest_api":
            resolved_name = rename_overrides.get(name, name)
            if resolved_name != name:
                print(f"  → Rename applied: '{name}' → '{resolved_name}' (pipeline: {pid})")
            mapping[pid] = {"table_name": resolved_name, "primary_key": pk}

    return mapping


# ======================================================================
# RENAME OVERRIDES
# ======================================================================

def _build_rename_overrides(table_creation: dict) -> dict:
    overrides = {}
    for t in table_creation.get("tables", []):
        orig     = t.get("original_table_name", "")
        approved = t.get("table_name", "")
        status   = t.get("status", "")

        if status != "created" or not orig or not approved:
            continue

        orig_bare     = orig.split(".")[-1]
        approved_bare = approved.split(".")[-1]

        if orig_bare != approved_bare:
            overrides[orig_bare] = approved_bare
            print(f"  → User rename detected: '{orig_bare}' → '{approved_bare}'")

    if overrides:
        print(f"  ✓ {len(overrides)} user rename(s) will be applied to migration_plan.json")
    return overrides


def _build_created_table_lookup(table_creation: dict) -> dict:
    """
    Build lookup: bare_table_name → full created table name (catalog.schema.table).

    Reads table_creation_status.json which contains the EXACT table name
    that Table.py physically created in Databricks after user approval.

    This is the single source of truth for target_table in migration_plan.json.
    Both original and approved names are indexed so any rename is captured.

    Example:
        table_creation_status.json has:
            {"table_name": "pgr.silver.aderis_acuity_raw",
             "original_table_name": "pgr.silver.dbt_delta_dev01_aderis_aderis_acuity_raw",
             "status": "created"}

        Lookup produced:
            {
                "aderis_acuity_raw":                   "pgr.silver.aderis_acuity_raw",
                "dbt_delta_dev01_aderis_aderis_acuity_raw": "pgr.silver.aderis_acuity_raw",
            }

    So no matter what name strategy.json uses for the source table,
    migration_plan always points the executor at the real created table.
    """
    lookup = {}
    for t in table_creation.get("tables", []):
        if t.get("status") != "created":
            continue

        full_name  = t.get("table_name", "")          # e.g. pgr.silver.aderis_acuity_raw
        orig_name  = t.get("original_table_name", "")  # e.g. pgr.silver.dbt_delta_dev01_...
        src_name   = t.get("source_name", "")          # e.g. dbt_delta_dev01.standard.standardized.csv

        if not full_name:
            continue

        if src_name:
            lookup[src_name] = full_name

        bare      = full_name.split(".")[-1]
        orig_bare = orig_name.split(".")[-1] if orig_name else ""

        # Index by final (approved) bare name
        lookup[bare] = full_name
        # Also index by original bare name so source_name from strategy matches too
        if orig_bare and orig_bare != bare:
            lookup[orig_bare] = full_name

    return lookup


# ======================================================================
# SOURCE BLOCK DISPATCHER
# ======================================================================

def _build_source_block(source_type: str, pipeline: dict, ct: dict) -> dict:
    if source_type == "s3":
        return _build_s3_source(pipeline, ct)
    elif source_type == "azure_sql":
        return _build_azure_sql_source(pipeline, ct)
    elif source_type == "postgresql":
        return _build_postgresql_source(pipeline, ct)
    elif source_type == "rest_api":
        return _build_rest_api_source(pipeline, ct)
    else:
        raise ValueError(f"Unsupported source type '{source_type}'")


# ======================================================================
# SOURCE BLOCK BUILDERS
# ======================================================================

def _build_s3_source(pipeline: dict, ct: dict) -> dict:
    return {
        "source_type": "s3",
        "engine":      pipeline.get("execution_engine", "spark"),
        "connection": {
            "access_key": (
                ct.get("aws_access_key_id")
                or ct.get("access_key")
                or ""
            ),
            "secret_key": (
                ct.get("aws_secret_access_key")
                or ct.get("secret_key")
                or ""
            ),
            "region": ct.get("region", "us-east-1"),
        },
        "location": {
            "bucket":    ct.get("bucket", ""),
            "base_path": (
                ct.get("prefix")
                or ct.get("base_path")
                or ""
            ),
        },
        "file_format": ct.get("file_format", "csv"),
    }


def _build_azure_sql_source(pipeline: dict, ct: dict) -> dict:
    return {
        "source_type": "azure_sql",
        "engine":      pipeline.get("execution_engine", "spark"),
        "connection": {
            "host":     ct.get("server", ""),
            "database": ct.get("database", ""),
            "user":     ct.get("username", ""),
            "password": ct.get("password", ""),
        },
        "schema": ct.get("schema", "dbo"),
    }


def _build_postgresql_source(pipeline: dict, ct: dict) -> dict:
    return {
        "source_type": "postgresql",
        "engine":      pipeline.get("execution_engine", "spark"),
        "connection": {
            "host":     ct.get("host", ""),
            "port":     int(ct.get("port", 5432)),
            "database": ct.get("database", ""),
            "user":     ct.get("username", ""),
            "password": ct.get("password", ""),
        },
        "schema": ct.get("schema", "public"),
    }


def _build_rest_api_source(pipeline: dict, ct: dict) -> dict:
    return {
        "source_type": "rest_api",
        "engine":      "python",
        "connection": {
            "endpoint":    ct.get("endpoint", ""),
            "auth_type":   ct.get("auth_type", "none"),
            "api_key":     ct.get("api_key", ""),
            "header_name": ct.get("header_name", ""),
        },
    }


# ======================================================================
# TABLE BLOCK BUILDERS
# ======================================================================

def _build_db_tables(
    strategy_pipeline:    dict,
    source_type:          str,
    catalog:              str,
    schema:               str,
    schema_output:        dict = None,
    created_table_lookup: dict = None,
) -> list:
    """
    Build table entries for S3, Azure SQL, PostgreSQL.

    target_table resolution — priority order:
      1. created_table_lookup  — exact full name Table.py physically created
                                 (read from table_creation_status.json)
                                 This is the ground truth. If the table exists
                                 here, use it — no derivation needed.

      2. schema_output lookup  — clean short name schema agent decided
                                 (e.g. "aderis_acuity_raw" from schema_output.json)

      3. _format_table_name()  — last resort, sanitize source filename
                                 (only used if neither file has an entry)
    """
    schema_name_lookup    = {}
    created_table_lookup  = created_table_lookup or {}

    if schema_output:
        for t in schema_output.get("tables", []):
            src  = t.get("source_name", "")
            name = t.get("table_name", "") or t.get("approved_table_name", "")
            if src and name:
                schema_name_lookup[src] = name

    tables_block = []

    for table in strategy_pipeline.get("tables", []):
        src_name = table["table_name"]   # raw source filename from strategy.json
        sanitized = _format_table_name(src_name, source_type)

        # Priority 1: exact full table name from table_creation_status.json
        if sanitized in created_table_lookup:
            target = created_table_lookup[sanitized]
        elif src_name in created_table_lookup:
            target = created_table_lookup[src_name]

        # Priority 2: schema agent's clean name + catalog.schema prefix
        elif src_name in schema_name_lookup:
            target = f"{catalog}.{schema}.{schema_name_lookup[src_name]}"

        # Priority 3: sanitize source filename (last resort)
        else:
            target = f"{catalog}.{schema}.{sanitized}"
            print(f"  ⚠  No created table entry for '{src_name}' — using derived name '{target}'")

        tables_block.append({
            "source_name":      src_name,
            "target_table":     target,
            "write_strategy":   table.get("write_strategy", "append"),
            "load_type":        "incremental" if table.get("watermark_column") else "full",
            "watermark_column": table.get("watermark_column"),
            "partition_by":     table.get("partition_by"),
            "parallelism":      table.get("parallelism", 1),
        })

    return tables_block


def _build_rest_api_tables(
    strategy_pipeline:       dict,
    catalog:                 str,
    schema:                  str,
    watermark_control_table: str | None,
    schema_table_map:        dict,
) -> list:
    pipeline_id  = strategy_pipeline["pipeline_id"]
    tables_block = []

    for table in strategy_pipeline.get("tables", []):
        generic_name = table["table_name"]
        api_id       = table.get("api_id") or pipeline_id

        schema_entry = schema_table_map.get(pipeline_id, {})
        unique_name  = schema_entry.get("table_name", generic_name) if isinstance(schema_entry, dict) else schema_entry
        primary_key  = schema_entry.get("primary_key") if isinstance(schema_entry, dict) else None
        target       = f"{catalog}.{schema}.{unique_name}"

        if unique_name == generic_name:
            print(
                f"  ⚠ schema_output.json missing entry for {pipeline_id}. "
                f"Using generic name '{generic_name}'."
            )

        merge_keys = [primary_key] if primary_key else None

        tables_block.append({
            "source_name":    unique_name,
            "target_table":   target,
            "write_strategy": "merge",
            "load_type":      "incremental",
            "merge_keys":     merge_keys,
            "watermark": {
                "control_table":     watermark_control_table,
                "source_name":       unique_name,
                "api_id":            api_id,
                "watermark_column":  table.get("watermark_column"),
                "default_watermark": table.get("default_watermark", "1900-01-01 00:00:00"),
                "lag_minutes":       table.get("lag_minutes",       5),
                "api_buffer_days":   table.get("api_buffer_days",   1),
                "precision_filter":  table.get("precision_filter",  True),
                "merge_keys":        merge_keys,
            },
            "partition_by": table.get("partition_by"),
            "parallelism":  table.get("parallelism", 2),
        })

    return tables_block


# ======================================================================
# TABLE NAME FORMATTER
# ======================================================================

def _format_table_name(name: str, source_type: str) -> str:
    """
    Normalise a source table/file name into a clean single-part Delta table name.

    BUG FIXED: S3 files with dots in the filename (e.g.
    "dbt_delta_dev01.aderis.aderis_acuity_raw.csv") previously produced
    multi-part table names like:
        catalog.schema.dbt_delta_dev01.aderis.aderis_acuity_raw
    Spark rejected this with REQUIRES_SINGLE_PART_NAMESPACE because it
    interpreted it as a 5-part identifier.

    Fix: after stripping the file extension, ALL remaining dots are replaced
    with underscores so the result is always a valid single-part name.

    Examples:
        S3:   folder/sub/sales_data.csv                  → sales_data
        S3:   dbt_delta_dev01.aderis.acuity_raw.csv      → dbt_delta_dev01_aderis_acuity_raw
        S3:   my-report 2024.parquet                     → my_report_2024
        SQL:  dbo.orders                                 → orders
        SQL:  public.users                               → users
    """
    name = name.strip().lower()

    if source_type == "s3":
        # Take only the filename (drop path prefix)
        name = name.split("/")[-1]
        # Strip known file extensions
        for ext in (".csv", ".parquet", ".json", ".orc", ".avro", ".tsv", ".txt"):
            if name.endswith(ext):
                name = name[: -len(ext)]
                break
    else:
        # SQL sources: take only the part after the last dot (schema.table → table)
        name = name.split(".")[-1]

    # Replace ALL dots, spaces, dashes with underscores
    # Dots in S3 filenames must become underscores — this is the core fix
    name = name.replace(".", "_").replace(" ", "_").replace("-", "_")

    # Collapse consecutive underscores and strip leading/trailing ones
    name = re.sub(r"_+", "_", name).strip("_")

    return name


# ======================================================================
# SUMMARY PRINTER
# ======================================================================

def _print_summary(result: dict):
    plan      = result.get("migration_plan", {})
    pipelines = plan.get("pipelines", [])
    wm_table  = plan.get("watermark_control_table")

    print(f"\n  Target   : {plan['target']['catalog']}.{plan['target']['schema']}")
    print(f"  Pipelines: {len(pipelines)}")

    for p in pipelines:
        src_type = p["source"]["source_type"]
        for t in p.get("tables", []):
            print(f"    → {p['pipeline_id']}  [{src_type}]  {t['target_table']}")

    if wm_table:
        print(f"\n  Watermark control table : {wm_table}")


# ======================================================================
# UTILITIES
# ======================================================================

def _load_json(path: str) -> dict:
    if not path:
        return {}
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ======================================================================
# STANDALONE CLI
# ======================================================================

if __name__ == "__main__":

    print("\n=== Migration Plan Generator ===\n")

    cred_path     = input("Credentials JSON path          : ").strip()
    table_path    = input("Table Creation JSON path       : ").strip()
    strategy_path = input("Strategy JSON path             : ").strip()
    schema_path   = input("Schema Output JSON path        : ").strip() or None
    output_dir    = input("Output directory [.]           : ").strip() or "."

    try:
        result, path = run_migration_plan(
            credentials_path    = cred_path,
            table_creation_path = table_path,
            strategy_path       = strategy_path,
            output_dir          = output_dir,
            schema_path         = schema_path,
        )
        print(json.dumps(result, indent=2))

    except FileNotFoundError as exc:
        print(f"\n❌ {exc}")
    except Exception as exc:
        print(f"\n❌ Unexpected error: {exc}")
        raise