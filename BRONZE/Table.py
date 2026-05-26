
# # """
# # TABLE/Table.py — Table Creation Agent
# # =======================================
# # Step 7 of the orchestration pipeline.

# # FIX: Column names and table names are now preserved EXACTLY as they
# #      appear in schema_output.json — no lowercasing, no cleaning.
# #      DROP TABLE before CREATE so stale schemas never persist.
# # """

# # import json
# # import os
# # import requests
# # import time


# # # =============================================================================
# # # USER APPROVAL LAYER
# # # =============================================================================

# # def _approve_table(full_table_name: str, ddl: str) -> tuple:
# #     parts  = full_table_name.rsplit(".", 1)
# #     prefix = parts[0] if len(parts) == 2 else ""

# #     print("\n" + "═" * 65)
# #     print("  TABLE NAME REVIEW")
# #     print("═" * 65)
# #     print(f"  Extracted name : {full_table_name}")
# #     print("─" * 65)
# #     print("\n  DDL Preview:")
# #     for line in ddl.splitlines():
# #         print(f"    {line}")
# #     print("═" * 65)

# #     while True:
# #         print("\n  Options:")
# #         print(f"    [1] Use extracted name  → {full_table_name}")
# #         print( "    [2] Enter a custom name")

# #         choice = input("\n  Your choice [1 / 2]: ").strip().upper()

# #         if choice == "1":
# #             print(f"  ✅ Using extracted name: {full_table_name}")
# #             return full_table_name, ddl

# #         elif choice == "2":
# #             custom = input(
# #                 "  Enter your table name (table part only): "
# #             ).strip().lower().replace(" ", "_")
# #             if not custom:
# #                 print("  ⚠  Name cannot be empty. Try again.")
# #                 continue
# #             custom_full = f"{prefix}.{custom}" if prefix else custom
# #             new_ddl     = ddl.replace(full_table_name, custom_full, 1)
# #             print(f"  ✅ Using custom name: {custom_full}")
# #             return custom_full, new_ddl

# #         else:
# #             print("  ⚠  Invalid choice. Enter 1 or 2.")


# # # =============================================================================
# # # TABLE CREATION AGENT
# # # =============================================================================

# # class TableCreationAgent:

# #     def __init__(self, schema_file: str, output_file: str = "table_creation_status.json"):
# #         self.schema_file = schema_file
# #         self.output_file = output_file

# #     def execute(
# #         self,
# #         workspace_url: str | None = None,
# #         warehouse_id:  str | None = None,
# #         token:         str | None = None,
# #         catalog:       str | None = None,
# #         schema_name:   str | None = None,
# #         auto_approve:  bool       = False,
# #     ):
# #         schema_data = self._load_json(self.schema_file)

# #         if not workspace_url:
# #             print("\n=== DESTINATION DETAILS ===")
# #             workspace_url = input("  Databricks Workspace URL        : ").strip().rstrip("/")
# #             warehouse_id  = input("  Databricks Warehouse ID         : ").strip()
# #             token         = input("  Databricks Personal Access Token: ").strip()
# #             catalog       = input("  Catalog name                    : ").strip()
# #             schema_name   = input("  Schema name                     : ").strip()

# #         headers = {
# #             "Authorization": f"Bearer {token}",
# #             "Content-Type":  "application/json",
# #         }

# #         results        = []
# #         created_tables = set()

# #         try:
# #             # Create catalog
# #             print(f"\n  Creating catalog : {catalog}")
# #             self._execute_sql(
# #                 workspace_url, warehouse_id, headers,
# #                 f"CREATE CATALOG IF NOT EXISTS {catalog}",
# #             )

# #             # Create schema
# #             print(f"  Creating schema  : {catalog}.{schema_name}")
# #             self._execute_sql(
# #                 workspace_url, warehouse_id, headers,
# #                 f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}",
# #             )

# #             all_tables  = schema_data.get("tables", [])
# #             dest_tables = [t for t in all_tables if not t.get("is_watermark_table", False)]

# #             print(f"\n  {len(dest_tables)} destination table(s) to process.\n")

# #             for idx, table in enumerate(dest_tables, 1):
# #                 # ── Table name: preserve exactly from schema_output ────────────
# #                 # _normalize_table_name in schema_mapping already cleaned the
# #                 # table name (finance.csv → finance). Use it directly.
# #                 raw_name        = table.get("table_name", f"table_{idx}")
# #                 columns         = table.get("columns", [])
# #                 partition_by    = table.get("partition_by")
# #                 full_table_name = f"{catalog}.{schema_name}.{raw_name}"

# #                 if full_table_name in created_tables:
# #                     print(f"  ⚠  Duplicate skipped: {full_table_name}")
# #                     continue

# #                 # ── Build DDL with EXACT column names from schema_output ───────
# #                 try:
# #                     ddl = self._build_ddl(full_table_name, columns, partition_by)
# #                 except Exception as exc:
# #                     results.append({"table_name": full_table_name, "status": "failed", "error": str(exc)})
# #                     continue

# #                 # ── Approval ──────────────────────────────────────────────────
# #                 if auto_approve:
# #                     approved_name = full_table_name
# #                     approved_ddl  = ddl
# #                 else:
# #                     approved_name, approved_ddl = _approve_table(full_table_name, ddl)

# #                 # ── DROP existing table then CREATE fresh ──────────────────────
# #                 # DROP ensures stale schemas (old cleaned column names) are
# #                 # always replaced with the correct original column names.
# #                 # CREATE TABLE IF NOT EXISTS alone would silently skip if the
# #                 # table already exists — wrong column names would persist forever.
# #                 try:
# #                     print(f"  Dropping : {approved_name} (if exists)")
# #                     self._execute_sql(
# #                         workspace_url, warehouse_id, headers,
# #                         f"DROP TABLE IF EXISTS {approved_name}",
# #                     )

# #                     # Remove IF NOT EXISTS since we just dropped it
# #                     create_ddl = approved_ddl.replace(
# #                         "CREATE TABLE IF NOT EXISTS",
# #                         "CREATE TABLE"
# #                     )
# #                     self._execute_sql(workspace_url, warehouse_id, headers, create_ddl)

# #                     results.append({
# #                         "table_name"         : approved_name,
# #                         "original_table_name": full_table_name,
# #                         "status"             : "created",
# #                     })
# #                     created_tables.add(approved_name)
# #                     print(f"  ✓ Created: {approved_name}")

# #                     if approved_name != full_table_name:
# #                         self._update_schema_name(full_table_name, approved_name)

# #                 except Exception as exc:
# #                     results.append({"table_name": approved_name, "status": "failed", "error": str(exc)})
# #                     print(f"  ❌ Failed: {approved_name} — {exc}")

# #         except Exception as exc:
# #             print(f"\n  ❌ Fatal error: {exc}")

# #         # Save status
# #         final_output = {"catalog": catalog, "schema": schema_name, "tables": results}
# #         self._save_json(final_output)

# #         created = sum(1 for r in results if r["status"] == "created")
# #         failed  = sum(1 for r in results if r["status"] == "failed")

# #         print("\n" + "=" * 65)
# #         print("  TABLE CREATION COMPLETE")
# #         print(f"  Created : {created}")
# #         print(f"  Failed  : {failed}")
# #         print("=" * 65)

# #     # =========================================================================
# #     # DDL BUILDER
# #     # Column names used EXACTLY as given — no lowercasing, no cleaning.
# #     # schema_mapping.py already stores exact original names from the source.
# #     # Backtick-quoted so mixed-case names work in Databricks.
# #     # =========================================================================
# #     def _build_ddl(self, full_table_name: str, columns: list, partition_by) -> str:
# #         if not columns:
# #             raise Exception(f"No columns defined for {full_table_name}")

# #         col_defs = ",\n    ".join(
# #             f"`{c['name']}` {c.get('type', 'STRING').upper()}"
# #             #  ↑ backtick-quoted exact name     ↑ type from LLM decision
# #             for c in columns
# #         )

# #         ddl = (
# #             f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n"
# #             f"    {col_defs}\n"
# #             f")\nUSING DELTA"
# #         )

# #         if partition_by:
# #             parts = ", ".join(f"`{p}`" for p in partition_by)
# #             ddl  += f"\nPARTITIONED BY ({parts})"

# #         return ddl

# #     # =========================================================================
# #     # SCHEMA OUTPUT SYNC
# #     # =========================================================================
# #     def _update_schema_name(self, original_full: str, approved_full: str):
# #         original_bare = original_full.split(".")[-1]
# #         approved_bare = approved_full.split(".")[-1]

# #         schema_data = self._load_json(self.schema_file)
# #         changed     = False

# #         for t in schema_data.get("tables", []):
# #             if t.get("table_name") == original_bare:
# #                 t["table_name"]          = approved_bare
# #                 t["approved_table_name"] = approved_bare
# #                 changed = True
# #                 break

# #         if changed:
# #             with open(self.schema_file, "w") as f:
# #                 json.dump(schema_data, f, indent=2)
# #             print(f"  📝 schema_output updated: '{original_bare}' → '{approved_bare}'")

# #     # =========================================================================
# #     # HELPERS
# #     # =========================================================================
# #     def _execute_sql(self, workspace_url, warehouse_id, headers, sql):
# #         response = requests.post(
# #             f"{workspace_url}/api/2.0/sql/statements",
# #             headers=headers,
# #             json={"statement": sql, "warehouse_id": warehouse_id},
# #         )
# #         if response.status_code != 200:
# #             raise Exception(f"SQL Submit Failed: {response.text}")

# #         stmt_id    = response.json()["statement_id"]
# #         status_url = f"{workspace_url}/api/2.0/sql/statements/{stmt_id}"
# #         start      = time.time()

# #         while True:
# #             sr    = requests.get(status_url, headers=headers)
# #             state = sr.json()["status"]["state"]
# #             if state == "SUCCEEDED":
# #                 print("  ✔ SQL executed successfully")
# #                 return
# #             elif state in ["FAILED", "CANCELED"]:
# #                 raise Exception(f"SQL Failed: {sr.json()['status'].get('error', {})}")
# #             elif time.time() - start > 300:
# #                 raise Exception("SQL Execution Timed Out")
# #             time.sleep(2)

# #     def _load_json(self, path: str) -> dict:
# #         if not path or not os.path.exists(path):
# #             return {}
# #         with open(path, "r") as f:
# #             return json.load(f)

# #     def _save_json(self, data: dict) -> None:
# #         with open(self.output_file, "w") as f:
# #             json.dump(data, f, indent=2)


# # # =============================================================================
# # # ENTRY POINT
# # # =============================================================================
# # def run_table_creation(
# #     schema_path:   str,
# #     workspace_url: str,
# #     warehouse_id:  str,
# #     token:         str,
# #     catalog:       str,
# #     schema_name:   str,
# #     output_dir:    str  = ".",
# #     auto_approve:  bool = False,
# # ) -> tuple:
# #     os.makedirs(output_dir, exist_ok=True)
# #     status_path = os.path.join(output_dir, "table_creation_status.json")

# #     agent = TableCreationAgent(schema_path, output_file=status_path)
# #     agent.execute(
# #         workspace_url=workspace_url,
# #         warehouse_id=warehouse_id,
# #         token=token,
# #         catalog=catalog,
# #         schema_name=schema_name,
# #         auto_approve=auto_approve,
# #     )

# #     with open(status_path, "r") as f:
# #         status = json.load(f)

# #     return status, status_path


# # # =============================================================================
# # # CLI
# # # =============================================================================
# # if __name__ == "__main__":
# #     schema_path = input("Enter Schema Output JSON path: ").strip()
# #     agent = TableCreationAgent(schema_path)
# #     agent.execute()


# """
# TABLE/Table.py — Table Creation Agent
# =======================================
# Step 7 of the orchestration pipeline.

# FIX: Table name shown in approval and created in Databricks now uses the
#      EXACT same sanitization as migration_plan.py so executor always
#      writes to the same table that was created here.

#      Root cause of mismatch:
#        Table.py showed "standardized" (from schema_output table_name field)
#        migration_plan.py derived "dbt_delta_dev01_aderis_aderis_acuity_raw"
#        (from sanitizing the source filename)
#        → executor wrote to a table that didn't exist

#      Fix: _sanitize_table_part() now matches _format_table_name() in
#      migration_plan.py exactly — strips extension, replaces ALL dots/spaces/
#      dashes with underscores, collapses consecutive underscores.
# """

# import json
# import os
# import re
# import requests
# import time


# # =============================================================================
# # TABLE NAME SANITIZATION
# # Must match _format_table_name() in migration_plan.py exactly.
# # Both functions must produce the same name from the same input.
# # =============================================================================

# def _sanitize_table_part(name: str) -> str:
#     """
#     Produce a clean single-part Delta table name from a source filename or
#     table name. Matches migration_plan._format_table_name() exactly.

#     Examples:
#         "dbt_delta_dev01.aderis.aderis_acuity_raw.csv" → "dbt_delta_dev01_aderis_aderis_acuity_raw"
#         "sales_data.csv"                               → "sales_data"
#         "my-report 2024.parquet"                       → "my_report_2024"
#         "dbo.orders"                                   → "dbo_orders"  (SQL source)
#         "standardized"                                 → "standardized"
#     """
#     # Strip any known file extension
#     for ext in (".csv", ".parquet", ".json", ".orc", ".avro", ".tsv", ".txt"):
#         if name.lower().endswith(ext):
#             name = name[: -len(ext)]
#             break

#     # Replace dots, spaces, dashes with underscores
#     name = name.replace(".", "_").replace(" ", "_").replace("-", "_")

#     # Collapse consecutive underscores and strip leading/trailing ones
#     name = re.sub(r"_+", "_", name).strip("_")

#     return name.lower()


# def _derive_table_name(table: dict) -> str:
#     """
#     Derive the physical table name that migration_plan.py will also produce.

#     Priority:
#       1. source_name  — the raw source filename/table name stored by schema agent
#                         (same value migration_plan.py sanitizes)
#       2. table_name   — fallback from schema_output

#     Both go through _sanitize_table_part() so the result is always consistent.
#     """
#     # Prefer source_name — this is what migration_plan.py uses
#     raw = (
#         table.get("source_name")
#         or table.get("table_name", "")
#     )
#     return _sanitize_table_part(raw) if raw else "unknown_table"


# # =============================================================================
# # USER APPROVAL LAYER
# # =============================================================================

# def _approve_table(full_table_name: str, ddl: str) -> tuple:
#     parts  = full_table_name.rsplit(".", 1)
#     prefix = parts[0] if len(parts) == 2 else ""

#     print("\n" + "═" * 65)
#     print("  TABLE NAME REVIEW")
#     print("═" * 65)
#     print(f"  Table name : {full_table_name}")
#     print("─" * 65)
#     print("\n  DDL Preview:")
#     for line in ddl.splitlines():
#         print(f"    {line}")
#     print("═" * 65)

#     while True:
#         print("\n  Options:")
#         print(f"    [1] Use this name  → {full_table_name}")
#         print( "    [2] Enter a custom name")

#         choice = input("\n  Your choice [1 / 2]: ").strip().upper()

#         if choice == "1":
#             print(f"  ✅ Using: {full_table_name}")
#             return full_table_name, ddl

#         elif choice == "2":
#             custom = input(
#                 "  Enter your table name (table part only): "
#             ).strip().lower().replace(" ", "_")
#             if not custom:
#                 print("  ⚠  Name cannot be empty. Try again.")
#                 continue
#             custom_full = f"{prefix}.{custom}" if prefix else custom
#             new_ddl     = ddl.replace(full_table_name, custom_full, 1)
#             print(f"  ✅ Using: {custom_full}")
#             return custom_full, new_ddl

#         else:
#             print("  ⚠  Invalid choice. Enter 1 or 2.")


# # =============================================================================
# # TABLE CREATION AGENT
# # =============================================================================

# class TableCreationAgent:

#     def __init__(self, schema_file: str, output_file: str = "table_creation_status.json"):
#         self.schema_file = schema_file
#         self.output_file = output_file

#     def execute(
#         self,
#         workspace_url: str | None = None,
#         warehouse_id:  str | None = None,
#         token:         str | None = None,
#         catalog:       str | None = None,
#         schema_name:   str | None = None,
#         auto_approve:  bool       = False,
#     ):
#         schema_data = self._load_json(self.schema_file)

#         if not workspace_url:
#             print("\n=== DESTINATION DETAILS ===")
#             workspace_url = input("  Databricks Workspace URL        : ").strip().rstrip("/")
#             warehouse_id  = input("  Databricks Warehouse ID         : ").strip()
#             token         = input("  Databricks Personal Access Token: ").strip()
#             catalog       = input("  Catalog name                    : ").strip()
#             schema_name   = input("  Schema name                     : ").strip()

#         headers = {
#             "Authorization": f"Bearer {token}",
#             "Content-Type":  "application/json",
#         }

#         results        = []
#         created_tables = set()

#         try:
#             # Create catalog
#             print(f"\n  Creating catalog : {catalog}")
#             self._execute_sql(
#                 workspace_url, warehouse_id, headers,
#                 f"CREATE CATALOG IF NOT EXISTS {catalog}",
#             )

#             # Create schema
#             print(f"  Creating schema  : {catalog}.{schema_name}")
#             self._execute_sql(
#                 workspace_url, warehouse_id, headers,
#                 f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}",
#             )

#             all_tables  = schema_data.get("tables", [])
#             dest_tables = [t for t in all_tables if not t.get("is_watermark_table", False)]

#             print(f"\n  {len(dest_tables)} destination table(s) to process.\n")

#             for idx, table in enumerate(dest_tables, 1):

#                 # ── Derive table name the same way migration_plan.py does ──────
#                 # This guarantees executor writes to the same table we create.
#                 raw_name        = _derive_table_name(table)
#                 columns         = table.get("columns", [])
#                 partition_by    = table.get("partition_by")
#                 full_table_name = f"{catalog}.{schema_name}.{raw_name}"

#                 # Store the resolved name back so migration_plan.py can read it
#                 table["_resolved_table_name"] = raw_name

#                 if full_table_name in created_tables:
#                     print(f"  ⚠  Duplicate skipped: {full_table_name}")
#                     continue

#                 # ── Build DDL ─────────────────────────────────────────────────
#                 try:
#                     ddl = self._build_ddl(full_table_name, columns, partition_by)
#                 except Exception as exc:
#                     results.append({
#                         "table_name": full_table_name,
#                         "status": "failed",
#                         "error": str(exc),
#                     })
#                     continue

#                 # ── Approval ──────────────────────────────────────────────────
#                 if auto_approve:
#                     approved_name = full_table_name
#                     approved_ddl  = ddl
#                 else:
#                     approved_name, approved_ddl = _approve_table(full_table_name, ddl)

#                 # ── DROP + CREATE (ensures stale schemas never persist) ────────
#                 try:
#                     print(f"  Dropping : {approved_name} (if exists)")
#                     self._execute_sql(
#                         workspace_url, warehouse_id, headers,
#                         f"DROP TABLE IF EXISTS {approved_name}",
#                     )

#                     create_ddl = approved_ddl.replace(
#                         "CREATE TABLE IF NOT EXISTS",
#                         "CREATE TABLE"
#                     )
#                     self._execute_sql(workspace_url, warehouse_id, headers, create_ddl)

#                     results.append({
#                         "table_name"         : approved_name,
#                         "original_table_name": full_table_name,
#                         "status"             : "created",
#                     })
#                     created_tables.add(approved_name)
#                     print(f"  ✓ Created: {approved_name}")

#                     if approved_name != full_table_name:
#                         self._update_schema_name(full_table_name, approved_name)

#                 except Exception as exc:
#                     results.append({
#                         "table_name": approved_name,
#                         "status"    : "failed",
#                         "error"     : str(exc),
#                     })
#                     print(f"  ❌ Failed: {approved_name} — {exc}")

#         except Exception as exc:
#             print(f"\n  ❌ Fatal error: {exc}")

#         final_output = {"catalog": catalog, "schema": schema_name, "tables": results}
#         self._save_json(final_output)

#         created = sum(1 for r in results if r["status"] == "created")
#         failed  = sum(1 for r in results if r["status"] == "failed")

#         print("\n" + "=" * 65)
#         print("  TABLE CREATION COMPLETE")
#         print(f"  Created : {created}")
#         print(f"  Failed  : {failed}")
#         print("=" * 65)

#     # =========================================================================
#     # DDL BUILDER
#     # Column names used EXACTLY as given — backtick-quoted for Databricks.
#     # =========================================================================
#     def _build_ddl(self, full_table_name: str, columns: list, partition_by) -> str:
#         if not columns:
#             raise Exception(f"No columns defined for {full_table_name}")

#         col_defs = ",\n    ".join(
#             f"`{c['name']}` {c.get('type', 'STRING').upper()}"
#             for c in columns
#         )

#         ddl = (
#             f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n"
#             f"    {col_defs}\n"
#             f")\nUSING DELTA"
#         )

#         if partition_by:
#             parts = ", ".join(f"`{p}`" for p in partition_by)
#             ddl  += f"\nPARTITIONED BY ({parts})"

#         return ddl

#     # =========================================================================
#     # SCHEMA OUTPUT SYNC — write resolved name back so migration_plan reads it
#     # =========================================================================
#     def _update_schema_name(self, original_full: str, approved_full: str):
#         original_bare = original_full.split(".")[-1]
#         approved_bare = approved_full.split(".")[-1]

#         schema_data = self._load_json(self.schema_file)
#         changed     = False

#         for t in schema_data.get("tables", []):
#             if _sanitize_table_part(t.get("table_name", "")) == original_bare:
#                 t["table_name"]          = approved_bare
#                 t["approved_table_name"] = approved_bare
#                 changed = True
#                 break

#         if changed:
#             with open(self.schema_file, "w") as f:
#                 json.dump(schema_data, f, indent=2)
#             print(f"  📝 schema_output updated: '{original_bare}' → '{approved_bare}'")

#     # =========================================================================
#     # HELPERS
#     # =========================================================================
#     def _execute_sql(self, workspace_url, warehouse_id, headers, sql):
#         response = requests.post(
#             f"{workspace_url}/api/2.0/sql/statements",
#             headers=headers,
#             json={
#                 "statement"    : sql,
#                 "warehouse_id" : warehouse_id,
#                 "wait_timeout" : "30s",
#             },
#         )
#         if response.status_code != 200:
#             raise Exception(f"SQL Submit Failed: {response.text}")

#         result = response.json()
#         state  = result.get("status", {}).get("state", "")

#         # Already done within wait_timeout
#         if state == "SUCCEEDED":
#             print("  ✔ SQL executed successfully")
#             return
#         if state in ("FAILED", "CANCELED"):
#             raise Exception(f"SQL Failed immediately: {result.get('status', {}).get('error', {})}")

#         # Still running — poll
#         stmt_id    = result["statement_id"]
#         status_url = f"{workspace_url}/api/2.0/sql/statements/{stmt_id}"
#         start      = time.time()

#         while True:
#             sr    = requests.get(status_url, headers=headers, timeout=15)
#             state = sr.json().get("status", {}).get("state", "")
#             if state == "SUCCEEDED":
#                 print("  ✔ SQL executed successfully")
#                 return
#             if state in ("FAILED", "CANCELED"):
#                 raise Exception(f"SQL Failed: {sr.json().get('status', {}).get('error', {})}")
#             if time.time() - start > 300:
#                 raise Exception("SQL Execution Timed Out")
#             time.sleep(2)

#     def _load_json(self, path: str) -> dict:
#         if not path or not os.path.exists(path):
#             return {}
#         with open(path, "r") as f:
#             return json.load(f)

#     def _save_json(self, data: dict) -> None:
#         with open(self.output_file, "w") as f:
#             json.dump(data, f, indent=2)


# # =============================================================================
# # ENTRY POINT
# # =============================================================================
# def run_table_creation(
#     schema_path:   str,
#     workspace_url: str,
#     warehouse_id:  str,
#     token:         str,
#     catalog:       str,
#     schema_name:   str,
#     output_dir:    str  = ".",
#     auto_approve:  bool = False,
# ) -> tuple:
#     os.makedirs(output_dir, exist_ok=True)
#     status_path = os.path.join(output_dir, "table_creation_status.json")

#     agent = TableCreationAgent(schema_path, output_file=status_path)
#     agent.execute(
#         workspace_url=workspace_url,
#         warehouse_id=warehouse_id,
#         token=token,
#         catalog=catalog,
#         schema_name=schema_name,
#         auto_approve=auto_approve,
#     )

#     with open(status_path, "r") as f:
#         status = json.load(f)

#     return status, status_path


# # =============================================================================
# # CLI
# # =============================================================================
# if __name__ == "__main__":
#     schema_path = input("Enter Schema Output JSON path: ").strip()
#     agent = TableCreationAgent(schema_path)
#     agent.execute()


"""
TABLE/Table.py — Table Creation Agent
=======================================
Step 7 of the orchestration pipeline.

FIX: Table name shown in approval and created in Databricks now uses the
     EXACT same sanitization as migration_plan.py so executor always
     writes to the same table that was created here.

     Root cause of mismatch:
       Table.py showed "standardized" (from schema_output table_name field)
       migration_plan.py derived "dbt_delta_dev01_aderis_aderis_acuity_raw"
       (from sanitizing the source filename)
       → executor wrote to a table that didn't exist

     Fix: _sanitize_table_part() now matches _format_table_name() in
     migration_plan.py exactly — strips extension, replaces ALL dots/spaces/
     dashes with underscores, collapses consecutive underscores.
"""

import json
import os
import re
import requests
import time


# =============================================================================
# TABLE NAME SANITIZATION
# Must match _format_table_name() in migration_plan.py exactly.
# Both functions must produce the same name from the same input.
# =============================================================================

def _sanitize_table_part(name: str) -> str:
    """
    Produce a clean single-part Delta table name from a source filename or
    table name. Matches migration_plan._format_table_name() exactly.

    Examples:
        "dbt_delta_dev01.aderis.aderis_acuity_raw.csv" → "dbt_delta_dev01_aderis_aderis_acuity_raw"
        "sales_data.csv"                               → "sales_data"
        "my-report 2024.parquet"                       → "my_report_2024"
        "dbo.orders"                                   → "dbo_orders"  (SQL source)
        "standardized"                                 → "standardized"
    """
    # Strip any known file extension
    for ext in (".csv", ".parquet", ".json", ".orc", ".avro", ".tsv", ".txt"):
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break

    # Replace dots, spaces, dashes with underscores
    name = name.replace(".", "_").replace(" ", "_").replace("-", "_")

    # Collapse consecutive underscores and strip leading/trailing ones
    name = re.sub(r"_+", "_", name).strip("_")

    return name.lower()


def _derive_table_name(table: dict) -> str:
    """
    Use the table_name from schema_output.json as the single source of truth.

    The schema agent already decided the clean short name (e.g. "aderis_acuity_raw").
    Table.py must use that exact name — NOT re-derive from source_name/filename.

    Re-deriving from source_name ("dbt_delta_dev01.aderis.aderis_acuity_raw.csv")
    produces a different long name, causing a duplicate table to be created
    alongside the short-name table the schema agent intended.

    Only fall back to sanitizing source_name if table_name is genuinely absent.
    """
    # Always prefer table_name — this is what the schema agent decided
    table_name = table.get("table_name", "").strip()
    if table_name:
        # Still sanitize in case it has spaces/dashes, but don't re-expand dots
        return re.sub(r"[^a-zA-Z0-9_]", "_", table_name).lower().strip("_")

    # Fallback only if table_name is missing
    source_name = table.get("source_name", "")
    return _sanitize_table_part(source_name) if source_name else "unknown_table"


# =============================================================================
# USER APPROVAL LAYER
# =============================================================================

def _approve_table(full_table_name: str, ddl: str) -> tuple:
    parts  = full_table_name.rsplit(".", 1)
    prefix = parts[0] if len(parts) == 2 else ""

    print("\n" + "═" * 65)
    print("  TABLE NAME REVIEW")
    print("═" * 65)
    print(f"  Table name : {full_table_name}")
    print("─" * 65)
    print("\n  DDL Preview:")
    for line in ddl.splitlines():
        print(f"    {line}")
    print("═" * 65)

    while True:
        print("\n  Options:")
        print(f"    [1] Use this name  → {full_table_name}")
        print( "    [2] Enter a custom name")

        choice = input("\n  Your choice [1 / 2]: ").strip().upper()

        if choice == "1":
            print(f"  ✅ Using: {full_table_name}")
            return full_table_name, ddl

        elif choice == "2":
            custom = input(
                "  Enter your table name (table part only): "
            ).strip().lower().replace(" ", "_")
            if not custom:
                print("  ⚠  Name cannot be empty. Try again.")
                continue
            custom_full = f"{prefix}.{custom}" if prefix else custom
            new_ddl     = ddl.replace(full_table_name, custom_full, 1)
            print(f"  ✅ Using: {custom_full}")
            return custom_full, new_ddl

        else:
            print("  ⚠  Invalid choice. Enter 1 or 2.")


# =============================================================================
# TABLE CREATION AGENT
# =============================================================================

class TableCreationAgent:

    def __init__(self, schema_file: str, output_file: str = "table_creation_status.json"):
        self.schema_file = schema_file
        self.output_file = output_file

    def execute(
        self,
        workspace_url: str | None = None,
        warehouse_id:  str | None = None,
        token:         str | None = None,
        catalog:       str | None = None,
        schema_name:   str | None = None,
        auto_approve:  bool       = False,
    ):
        schema_data = self._load_json(self.schema_file)

        if not workspace_url:
            print("\n=== DESTINATION DETAILS ===")
            workspace_url = input("  Databricks Workspace URL        : ").strip().rstrip("/")
            warehouse_id  = input("  Databricks Warehouse ID         : ").strip()
            token         = input("  Databricks Personal Access Token: ").strip()
            catalog       = input("  Catalog name                    : ").strip()
            schema_name   = input("  Schema name                     : ").strip()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }

        results        = []
        created_tables = set()

        try:
            # Create catalog
            print(f"\n  Creating catalog : {catalog}")
            self._execute_sql(
                workspace_url, warehouse_id, headers,
                f"CREATE CATALOG IF NOT EXISTS {catalog}",
            )

            # Create schema
            print(f"  Creating schema  : {catalog}.{schema_name}")
            self._execute_sql(
                workspace_url, warehouse_id, headers,
                f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}",
            )

            all_tables  = schema_data.get("tables", [])
            dest_tables = [t for t in all_tables if not t.get("is_watermark_table", False)]

            print(f"\n  {len(dest_tables)} destination table(s) to process.\n")

            for idx, table in enumerate(dest_tables, 1):

                # ── Derive table name the same way migration_plan.py does ──────
                # This guarantees executor writes to the same table we create.
                raw_name        = _derive_table_name(table)
                columns         = table.get("columns", [])
                partition_by    = table.get("partition_by")
                full_table_name = f"{catalog}.{schema_name}.{raw_name}"

                # Store the resolved name back so migration_plan.py can read it
                table["_resolved_table_name"] = raw_name

                if full_table_name in created_tables:
                    print(f"  ⚠  Duplicate skipped: {full_table_name}")
                    continue

                # ── Build DDL ─────────────────────────────────────────────────
                try:
                    ddl = self._build_ddl(full_table_name, columns, partition_by)
                except Exception as exc:
                    results.append({
                        "table_name": full_table_name,
                        "status": "failed",
                        "error": str(exc),
                    })
                    continue

                # ── Approval ──────────────────────────────────────────────────
                if auto_approve:
                    approved_name = full_table_name
                    approved_ddl  = ddl
                else:
                    approved_name, approved_ddl = _approve_table(full_table_name, ddl)

                # ── DROP + CREATE (ensures stale schemas never persist) ────────
                try:
                    print(f"  Dropping : {approved_name} (if exists)")
                    self._execute_sql(
                        workspace_url, warehouse_id, headers,
                        f"DROP TABLE IF EXISTS {approved_name}",
                    )

                    create_ddl = approved_ddl.replace(
                        "CREATE TABLE IF NOT EXISTS",
                        "CREATE TABLE"
                    )
                    self._execute_sql(workspace_url, warehouse_id, headers, create_ddl)

                    results.append({
                        "table_name"         : approved_name,
                        "original_table_name": full_table_name,
                        "source_name"        : table.get("source_name"),
                        "status"             : "created",
                    })
                    created_tables.add(approved_name)
                    print(f"  ✓ Created: {approved_name}")

                    if approved_name != full_table_name:
                        self._update_schema_name(full_table_name, approved_name)

                except Exception as exc:
                    results.append({
                        "table_name": approved_name,
                        "status"    : "failed",
                        "error"     : str(exc),
                    })
                    print(f"  ❌ Failed: {approved_name} — {exc}")

        except Exception as exc:
            print(f"\n  ❌ Fatal error: {exc}")

        final_output = {"catalog": catalog, "schema": schema_name, "tables": results}
        self._save_json(final_output)

        created = sum(1 for r in results if r["status"] == "created")
        failed  = sum(1 for r in results if r["status"] == "failed")

        print("\n" + "=" * 65)
        print("  TABLE CREATION COMPLETE")
        print(f"  Created : {created}")
        print(f"  Failed  : {failed}")
        print("=" * 65)

    # =========================================================================
    # DDL BUILDER
    # Column names used EXACTLY as given — backtick-quoted for Databricks.
    # =========================================================================
    def _build_ddl(self, full_table_name: str, columns: list, partition_by) -> str:
        if not columns:
            raise Exception(f"No columns defined for {full_table_name}")

        col_defs = ",\n    ".join(
            f"`{c['name']}` {c.get('type', 'STRING').upper()}"
            for c in columns
        )

        ddl = (
            f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n"
            f"    {col_defs}\n"
            f")\nUSING DELTA"
        )

        if partition_by:
            parts = ", ".join(f"`{p}`" for p in partition_by)
            ddl  += f"\nPARTITIONED BY ({parts})"

        return ddl

    # =========================================================================
    # SCHEMA OUTPUT SYNC — write resolved name back so migration_plan reads it
    # =========================================================================
    def _update_schema_name(self, original_full: str, approved_full: str):
        original_bare = original_full.split(".")[-1]
        approved_bare = approved_full.split(".")[-1]

        schema_data = self._load_json(self.schema_file)
        changed     = False

        for t in schema_data.get("tables", []):
            if _sanitize_table_part(t.get("table_name", "")) == original_bare:
                t["table_name"]          = approved_bare
                t["approved_table_name"] = approved_bare
                changed = True
                break

        if changed:
            with open(self.schema_file, "w") as f:
                json.dump(schema_data, f, indent=2)
            print(f"  📝 schema_output updated: '{original_bare}' → '{approved_bare}'")

    # =========================================================================
    # HELPERS
    # =========================================================================
    def _execute_sql(self, workspace_url, warehouse_id, headers, sql):
        response = requests.post(
            f"{workspace_url}/api/2.0/sql/statements",
            headers=headers,
            json={
                "statement"    : sql,
                "warehouse_id" : warehouse_id,
                "wait_timeout" : "30s",
            },
        )
        if response.status_code != 200:
            raise Exception(f"SQL Submit Failed: {response.text}")

        result = response.json()
        state  = result.get("status", {}).get("state", "")

        # Already done within wait_timeout
        if state == "SUCCEEDED":
            print("  ✔ SQL executed successfully")
            return
        if state in ("FAILED", "CANCELED"):
            raise Exception(f"SQL Failed immediately: {result.get('status', {}).get('error', {})}")

        # Still running — poll
        stmt_id    = result["statement_id"]
        status_url = f"{workspace_url}/api/2.0/sql/statements/{stmt_id}"
        start      = time.time()

        while True:
            sr    = requests.get(status_url, headers=headers, timeout=15)
            state = sr.json().get("status", {}).get("state", "")
            if state == "SUCCEEDED":
                print("  ✔ SQL executed successfully")
                return
            if state in ("FAILED", "CANCELED"):
                raise Exception(f"SQL Failed: {sr.json().get('status', {}).get('error', {})}")
            if time.time() - start > 300:
                raise Exception("SQL Execution Timed Out")
            time.sleep(2)

    def _load_json(self, path: str) -> dict:
        if not path or not os.path.exists(path):
            return {}
        with open(path, "r") as f:
            return json.load(f)

    def _save_json(self, data: dict) -> None:
        with open(self.output_file, "w") as f:
            json.dump(data, f, indent=2)


# =============================================================================
# ENTRY POINT
# =============================================================================
def run_table_creation(
    schema_path:   str,
    workspace_url: str,
    warehouse_id:  str,
    token:         str,
    catalog:       str,
    schema_name:   str,
    output_dir:    str  = ".",
    auto_approve:  bool = False,
) -> tuple:
    os.makedirs(output_dir, exist_ok=True)
    status_path = os.path.join(output_dir, "table_creation_status.json")

    agent = TableCreationAgent(schema_path, output_file=status_path)
    agent.execute(
        workspace_url=workspace_url,
        warehouse_id=warehouse_id,
        token=token,
        catalog=catalog,
        schema_name=schema_name,
        auto_approve=auto_approve,
    )

    with open(status_path, "r") as f:
        status = json.load(f)

    return status, status_path


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    schema_path = input("Enter Schema Output JSON path: ").strip()
    agent = TableCreationAgent(schema_path)
    agent.execute()