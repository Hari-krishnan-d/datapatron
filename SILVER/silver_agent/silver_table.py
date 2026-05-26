"""
silver_agent/silver_table.py — Silver Table Creation Agent
===========================================================
Step S7 of the silver pipeline. Called by orchestrator.py.

Reads silver_schema.json → executes CREATE TABLE DDL in Databricks.

Credential logic:
  1. Reads workspace_url / warehouse_id / token from silver_credentials.json
     (the same file silver_credentials.py wrote — no re-prompting)
  2. Asks: silver catalog + schema name (these are the TARGET, not bronze)

User sees DDL preview per table and can approve or rename before creation.

Output: silver_table_creation_status.json
  {
    "catalog": "testbro",
    "schema":  "silver2",
    "tables":  [
      {"table_name": "testbro.silver2.finance_silver",
       "source_table": "testbro.bronze.finance",
       "status": "created"},
      ...
    ]
  }
"""

import json
import os
import time
import logging
import requests

logger = logging.getLogger("Silver.Table")


# =============================================================================
# HELPERS
# =============================================================================

def _ask(prompt: str, default: str = "", required: bool = True) -> str:
    display = f" [{default}]" if default else ""
    while True:
        value = input(f"  {prompt}{display}: ").strip()
        if not value and default:
            return default
        if value:
            return value
        if not required:
            return ""
        print("  ⚠  Required field.")


def _ask_token(prompt: str, default: str = "") -> str:
    """Collect token via plain input() — getpass fails in many terminal environments."""
    hint = " [keep existing — press Enter]" if default else ""
    while True:
        value = input(f"  {prompt}{hint}: ").strip()
        if not value and default:
            print(f"  ✅ Keeping existing token  : ********")
            return default
        if value:
            masked = value[:4] + "*" * (len(value) - 4) if len(value) > 4 else "****"
            print(f"  ✅ Token received          : {masked}")
            return value
        print("  ⚠  Token is required.")


# =============================================================================
# CREDENTIAL RESOLUTION
# Reads from silver_credentials.json first (written by silver_credentials.py).
# Falls back to session_config.json, then prompts fresh if neither exists.
# =============================================================================

def _resolve_databricks_config(
    silver_credentials_path: str | None = None,
    session_config_path:     str | None = None,
) -> dict:
    """
    Resolve Databricks connection details for silver table creation.

    Priority:
      1. silver_credentials.json  — written by silver_credentials.py this run
         Keys: workspace_url, warehouse_id, token, silver_catalog, silver_schema
      2. session_config.json      — Phase 1 bronze credentials (reuse connection,
                                    ask for silver catalog + schema)
      3. Fresh prompts            — if neither file exists
    """
    # ── Try silver_credentials.json first ─────────────────────────────────────
    if silver_credentials_path and os.path.exists(silver_credentials_path):
        with open(silver_credentials_path, "r", encoding="utf-8") as f:
            sc = json.load(f)

        # silver_credentials.json stores pipelines list
        pipelines = sc.get("pipelines", [])
        if pipelines:
            conn = pipelines[0].get("connection_template", {})
            workspace_url  = conn.get("workspace_url", "").rstrip("/")
            warehouse_id   = conn.get("warehouse_id", "")
            token          = conn.get("token", "")
            # Silver destination is stored separately in the credentials file
            dest = pipelines[0].get("destination", {})
            silver_catalog = dest.get("catalog", "") or _ask("Silver catalog name")
            silver_schema  = dest.get("schema",  "") or _ask("Silver schema name", default="silver")


            print(f"\n  ℹ️  Reusing silver credentials:")
            print(f"     Workspace  : {workspace_url}")
            print(f"     Warehouse  : {warehouse_id}")
            print(f"     Token      : ********")
            print(f"     Catalog    : {silver_catalog}")
            print(f"     Schema     : {silver_schema}\n")

            return {
                "workspace_url": workspace_url,
                "warehouse_id" : warehouse_id,
                "token"        : token,
                "catalog"      : silver_catalog,
                "schema"       : silver_schema,
            }

    # ── Try session_config.json (bronze creds) ────────────────────────────────
    sc = {}
    if session_config_path and os.path.exists(session_config_path):
        with open(session_config_path, "r", encoding="utf-8") as f:
            sc = json.load(f)

    if sc:
        workspace_url  = sc.get("databricks_workspace_url", "").rstrip("/")
        warehouse_id   = sc.get("databricks_warehouse_id", "")
        token          = sc.get("databricks_token", "")
        bronze_catalog = sc.get("catalog", "")

        print(f"\n  ℹ️  Reusing Phase 1 Databricks connection:")
        print(f"     Workspace : {workspace_url}")
        print(f"     Warehouse : {warehouse_id}")
        print(f"     Token     : ********\n")

        print(f"  Bronze catalog is : {bronze_catalog}")
        use_same = input("  Use the same catalog for silver? [Y / N]: ").strip().upper()
        catalog  = bronze_catalog if use_same == "Y" else _ask("Silver catalog name")
        schema   = _ask("Silver schema name", default="silver")

    else:
        # ── No config files — collect fresh ───────────────────────────────────
        print("\n  No saved credentials found — please enter Databricks details:\n")
        workspace_url = _ask("Workspace URL  (e.g. https://dbc-xxxx.cloud.databricks.com)")
        warehouse_id  = _ask("SQL Warehouse ID")
        token         = _ask_token("Personal Access Token")
        catalog       = _ask("Silver catalog name")
        schema        = _ask("Silver schema name", default="silver")

    return {
        "workspace_url": workspace_url,
        "warehouse_id" : warehouse_id,
        "token"        : token,
        "catalog"      : catalog,
        "schema"       : schema,
    }


# =============================================================================
# SQL EXECUTION
# =============================================================================

def _execute_sql(workspace_url: str, warehouse_id: str, token: str, sql: str) -> None:
    """
    Submit SQL to Databricks SQL Statements API and wait for completion.

    Fixes vs original:
      - Passes wait_timeout=30s so short statements return immediately
      - Checks for SUCCEEDED in the submit response (no poll needed)
      - Handles FAILED state in submit response before entering poll loop
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type" : "application/json",
    }
    resp = requests.post(
        f"{workspace_url}/api/2.0/sql/statements",
        headers=headers,
        json={
            "statement"    : sql,
            "warehouse_id" : warehouse_id,
            "wait_timeout" : "30s",     # ← wait up to 30s before returning
        },
        timeout=60,
    )
    if resp.status_code != 200:
        raise Exception(f"SQL submit failed ({resp.status_code}): {resp.text[:300]}")

    result = resp.json()
    state  = result.get("status", {}).get("state", "")

    # Already done (returned within wait_timeout)
    if state == "SUCCEEDED":
        return
    if state in ("FAILED", "CANCELED"):
        err = result.get("status", {}).get("error", {})
        raise Exception(f"SQL failed: {err}")

    # Still running — poll until done
    stmt_id    = result["statement_id"]
    status_url = f"{workspace_url}/api/2.0/sql/statements/{stmt_id}"
    start      = time.time()

    while True:
        sr    = requests.get(status_url, headers=headers, timeout=15)
        state = sr.json().get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELED"):
            err = sr.json().get("status", {}).get("error", {})
            raise Exception(f"SQL failed: {err}")
        if time.time() - start > 300:
            raise Exception("SQL timed out after 300s")
        time.sleep(2)


# =============================================================================
# TABLE APPROVAL
# =============================================================================

def _approve_table(full_table_name: str, ddl: str) -> tuple:
    """Show table name + DDL preview, let user confirm or rename."""
    parts  = full_table_name.rsplit(".", 1)
    prefix = parts[0] if len(parts) == 2 else ""

    print("\n" + "═" * 65)
    print("  SILVER TABLE REVIEW")
    print("═" * 65)
    print(f"  Proposed name : {full_table_name}")
    print("─" * 65)
    print("  DDL Preview:")
    for line in ddl.splitlines():
        print(f"    {line}")
    print("═" * 65)
    print("\n  [1] Use proposed name")
    print("  [2] Enter a custom name")

    while True:
        choice = input("\n  Your choice [1 / 2]: ").strip()
        if choice == "1":
            print(f"  ✅ Using: {full_table_name}")
            return full_table_name, ddl
        if choice == "2":
            custom = input(
                "  Enter table name (table part only): "
            ).strip().lower().replace(" ", "_")
            if not custom:
                print("  ⚠  Name cannot be empty.")
                continue
            custom_full = f"{prefix}.{custom}" if prefix else custom
            new_ddl     = ddl.replace(full_table_name, custom_full, 1)
            print(f"  ✅ Using: {custom_full}")
            return custom_full, new_ddl
        print("  ⚠  Enter 1 or 2.")


# =============================================================================
# DDL BUILDER
# Reads column["type"] which silver_schema.py now correctly sets to target_type.
# =============================================================================

def _build_ddl(full_name: str, columns: list, stored_ddl: str,
               catalog: str, schema_name: str) -> str:
    """
    Return the DDL to execute.

    Preference order:
      1. DDL from silver_schema.json with placeholders resolved  ← best
      2. Rebuild from columns list using col["type"]             ← fallback
    """
    # Resolve {catalog} and {schema} placeholders
    resolved = stored_ddl.replace("{catalog}", catalog).replace("{schema}", schema_name)

    # If placeholder resolution worked and no raw placeholders remain, use it
    if resolved and "{catalog}" not in resolved and "{schema}" not in resolved:
        return resolved

    # Fallback: rebuild from column list
    # col["type"] is now set to target_type by the fixed silver_schema.py
    col_defs = ",\n    ".join(
        f"{c['name']} {c.get('type', 'STRING').upper()}"
        for c in columns
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {full_name} (\n"
        f"    {col_defs}\n"
        f") USING DELTA"
    )


# =============================================================================
# SILVER TABLE CREATION AGENT
# =============================================================================

def run_silver_table_creation(
    schema_path:             str,
    silver_credentials_path: str | None = None,
    session_config_path:     str | None = None,
    output_dir:              str        = ".",
    auto_approve:            bool       = False,
) -> tuple:
    """
    Create silver tables in Databricks from silver_schema.json.

    Args:
        schema_path              : Path to silver_schema.json
        silver_credentials_path  : Path to silver_credentials.json (preferred)
        session_config_path      : Path to session_config.json (fallback)
        output_dir               : Where to save silver_table_creation_status.json
        auto_approve             : Skip table name review prompt

    Returns:
        (status: dict, status_path: str)
    """
    logger.info("Silver Table Creation Agent started.")

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_data = json.load(f)

    print("\n" + "=" * 62)
    print("  SILVER — TABLE CREATION")
    print("=" * 62)

    db            = _resolve_databricks_config(silver_credentials_path, session_config_path)
    workspace_url = db["workspace_url"]
    warehouse_id  = db["warehouse_id"]
    token         = db["token"]
    catalog       = db["catalog"]
    schema_name   = db["schema"]

    results        = []
    created_tables = set()

    try:
        # ── Ensure catalog and schema exist ───────────────────────────────────
        print(f"\n  Creating catalog : {catalog}")
        _execute_sql(workspace_url, warehouse_id, token,
                     f"CREATE CATALOG IF NOT EXISTS {catalog}")
        print(f"  ✔ Catalog ready")

        print(f"  Creating schema  : {catalog}.{schema_name}")
        _execute_sql(workspace_url, warehouse_id, token,
                     f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
        print(f"  ✔ Schema ready")

        tables = schema_data.get("tables", [])
        print(f"\n  {len(tables)} silver table(s) to create.\n")

        for table in tables:
            raw_name   = table.get("table_name", "unnamed")
            columns    = table.get("columns", [])
            source_tbl = table.get("source_table", "")
            full_name  = f"{catalog}.{schema_name}.{raw_name}"

            if full_name in created_tables:
                print(f"  ⚠  Duplicate skipped: {full_name}")
                continue

            # Build DDL with correct types
            ddl = _build_ddl(
                full_name  = full_name,
                columns    = columns,
                stored_ddl = table.get("ddl", ""),
                catalog    = catalog,
                schema_name= schema_name,
            )

            # Approval step
            if auto_approve:
                approved_name = full_name
                approved_ddl  = ddl
            else:
                approved_name, approved_ddl = _approve_table(full_name, ddl)

            # Execute DDL
            try:
                print(f"  Creating: {approved_name} ...", end=" ", flush=True)
                _execute_sql(workspace_url, warehouse_id, token, approved_ddl)
                print("✅")
                results.append({
                    "table_name"         : approved_name,
                    "source_table"       : source_tbl,
                    "original_table_name": full_name,
                    "status"             : "created",
                })
                created_tables.add(approved_name)

                if approved_name != full_name:
                    _update_schema_name(schema_path, raw_name, approved_name.split(".")[-1])

            except Exception as exc:
                print(f"❌")
                results.append({
                    "table_name": approved_name,
                    "status"    : "failed",
                    "error"     : str(exc),
                })
                logger.error("Failed to create %s: %s", approved_name, exc)
                print(f"  ❌ Failed: {approved_name} — {exc}")

    except Exception as exc:
        print(f"\n  ❌ Fatal error: {exc}")
        logger.error("Fatal error in silver table creation: %s", exc)

    # ── Save status ───────────────────────────────────────────────────────────
    status = {
        "catalog": catalog,
        "schema" : schema_name,
        "tables" : results,
    }
    os.makedirs(output_dir, exist_ok=True)
    status_path = os.path.join(output_dir, "silver_table_creation_status.json")
    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    created = sum(1 for r in results if r["status"] == "created")
    failed  = sum(1 for r in results if r["status"] == "failed")

    print(f"\n  {'=' * 62}")
    print(f"  SILVER TABLE CREATION COMPLETE")
    print(f"  Created : {created}   Failed : {failed}")
    if created:
        for r in results:
            if r["status"] == "created":
                print(f"  ✅ {r['table_name']}")
    print(f"  {'=' * 62}\n")

    logger.info("Silver table creation → %s", status_path)
    return status, status_path


def _update_schema_name(schema_path: str, original_bare: str, approved_bare: str) -> None:
    """Write renamed table back into silver_schema.json."""
    try:
        with open(schema_path, "r") as f:
            data = json.load(f)
        for t in data.get("tables", []):
            if t.get("table_name") == original_bare:
                t["table_name"]          = approved_bare
                t["approved_table_name"] = approved_bare
                break
        with open(schema_path, "w") as f:
            json.dump(data, f, indent=2)
        logger.info("Schema updated: '%s' → '%s'", original_bare, approved_bare)
    except Exception as exc:
        logger.warning("Could not update schema name: %s", exc)