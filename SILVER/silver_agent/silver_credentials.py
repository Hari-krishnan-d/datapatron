"""
SILVER/credentials/databricks.py — Silver Credentials Agent
=============================================================
Collects Databricks credentials for silver pipelines.

TWO MODES:

  REUSE (Flow D / E — reuse_creds=True)
  ──────────────────────────────────────
  Phase 1 already collected and saved Databricks credentials
  into session_config.json. This agent reads them automatically.

  workspace_url  ← session_config["databricks_workspace_url"]
  warehouse_id   ← session_config["databricks_warehouse_id"]
  token          ← session_config["databricks_token"]
  catalog        ← session_config["catalog"]          (bronze catalog)
  schema         ← session_config["schema"]           (bronze schema)

  Only asks the user ONE thing: which bronze table to read from
  (or leave blank to read all tables in that schema).

  FRESH (Flow C — reuse_creds=False)
  ────────────────────────────────────
  No Phase 1 exists. User provides all Databricks details manually.

Output: silver_credentials.json
"""

import json
import os
import copy
import logging
from datetime import datetime, timezone

logger = logging.getLogger("Silver.Credentials")

SENSITIVE_FIELDS = {"token"}
REQUIRED_FIELDS  = ["workspace_url", "warehouse_id", "token", "catalog", "schema"]


# =============================================================================
# HELPERS
# =============================================================================

def _mask(conn: dict) -> dict:
    return {
        k: ("*" * 8 if k in SENSITIVE_FIELDS and v else v)
        for k, v in conn.items()
    }


def _ask(prompt: str, default: str = "", required: bool = True) -> str:
    display = f" [{default}]" if default else ""
    while True:
        value = input(f"    {prompt}{display}: ").strip()
        if not value and default:
            return default
        if value:
            return value
        if not required:
            return ""
        print("    ⚠  Required field — please enter a value.")


def _ask_token(prompt: str, default: str = "") -> str:
    """
    Collect a token/secret via plain input().
    getpass is avoided because it fails silently in Databricks notebooks,
    Jupyter, and many CI/terminal environments.

    After entry the token is echoed back as masked stars so the user
    knows something was captured without exposing the value.
    """
    hint = " [keep existing — press Enter]" if default else ""
    while True:
        value = input(f"    {prompt}{hint}: ").strip()

        if not value and default:
            print(f"    ✅ Keeping existing token  : ********")
            return default

        if value:
            # Show masked confirmation so user knows it was received
            masked = value[:4] + "*" * (len(value) - 4) if len(value) > 4 else "****"
            print(f"    ✅ Token received          : {masked}")
            return value

        print("    ⚠  Token is required — please paste your Personal Access Token.")


def _load_session_config(path: str) -> dict:
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# =============================================================================
# SINGLE PIPELINE CREDENTIAL COLLECTOR
# =============================================================================

def _collect_pipeline(
    pipeline:           dict,
    session_config:     dict,
    reuse_bronze_creds: bool,
    global_dest:        dict = None,
    global_bronze_table: str = "", # Use globally collected bronze connection for Flow C
    global_bronze_conn: dict = None,
) -> dict:
    pipeline_id = pipeline["pipeline_id"]
    conn        = copy.deepcopy(pipeline.get("connection_template", {}))

    print(f"\n  {'─' * 55}")
    print(f"  Source   : Databricks Bronze  →  Silver")
    print(f"  {'─' * 55}")

    if reuse_bronze_creds and session_config:
        conn["workspace_url"] = session_config.get("databricks_workspace_url", "")
        conn["warehouse_id"]  = session_config.get("databricks_warehouse_id", "")
        conn["token"]         = session_config.get("databricks_token", "")
        conn["catalog"]       = session_config.get("catalog", "")
        conn["schema"]        = session_config.get("schema", "")
    elif global_bronze_conn: # Use globally collected bronze connection for Flow C
        conn.update(global_bronze_conn)
        print(f"  ✅ Using global bronze connection details.")

    # Apply global bronze table name or ask per pipeline if not global
    if global_bronze_table:
        conn["table"] = global_bronze_table
        print(f"  ✅ Using global bronze table: '{global_bronze_table}'")
    else:
        print(f"\n  Which bronze table do you want to transform for pipeline {pipeline_id}?")
        bronze_ref  = pipeline.get("bronze_source", {})
        default_tbl = bronze_ref.get("table", "")
        conn["table"] = _ask(
            "Bronze table name  (leave blank = all tables in schema)",
            default=conn.get("table", ""),
            required=False,
        )

    # ── Silver destination ────────────────────────────────────────────────────
    if global_dest:
        pipeline["destination"] = {
            "catalog": global_dest["catalog"],
            "schema" : global_dest["schema"],
        }
        print(f"  ✅ Using global destination: {global_dest['catalog']}.{global_dest['schema']}")
    else:
        print(f"\n  [Silver Destination] Where to write the transformed data?\n")
        dest              = pipeline.get("destination", {})
        default_s_catalog = dest.get("catalog", conn.get("catalog", "silver_catalog"))
        default_s_schema  = dest.get("schema",  "silver")

        pipeline["destination"] = {
            "catalog": _ask("Silver catalog name", default=default_s_catalog),
            "schema" : _ask("Silver schema name",  default=default_s_schema),
        }

    # ── Validate ──────────────────────────────────────────────────────────────
    missing = [f for f in REQUIRED_FIELDS if not conn.get(f)]
    if missing:
        raise ValueError(
            f"Pipeline '{pipeline_id}' missing required fields: {missing}"
        )

    logger.info("Silver creds OK for %s: %s", pipeline_id, _mask(conn))

    pipeline["connection_template"]      = conn
    pipeline["status"]                   = "credentials_collected"
    pipeline["credentials_collected_at"] = datetime.now(timezone.utc).isoformat()
    return pipeline


# =============================================================================
# SILVER CREDENTIALS AGENT
# =============================================================================

class SilverCredentialsAgent:

    def execute(
        self,
        silver_plan:         dict,
        session_config_path: str | None = None,
        output_dir:          str        = ".",
    ) -> tuple:
        """
        Args:
            silver_plan:          Plan dict with silver pipelines.
            session_config_path:  Path to session_config.json from Phase 1.
                                  Pass None for Flow C (fresh collection).
            output_dir:           Where to write silver_credentials.json.

        Returns:
            (updated_plan, credentials_path)
        """
        logger.info("Silver Credentials Agent started.")

        reuse = bool(session_config_path)
        sc    = _load_session_config(session_config_path) if reuse else {}
        plan  = copy.deepcopy(silver_plan)
        total = len(plan.get("pipelines", []))

        print("\n" + "=" * 57)
        print("  SILVER — CREDENTIAL COLLECTION")
        print(f"  Session   : {plan.get('session_id', 'unknown')}")
        print(f"  Pipelines : {total}")
        print(f"  Mode      : {'Reusing Phase 1 credentials' if reuse else 'Fresh collection'}")
        print("=" * 57)
        
        global_bronze_conn = {}
        global_bronze_table = ""
        global_dest = {}

        # 1. Collect/Reuse global Databricks Bronze connection details
        if reuse and sc:
            # Scenario 1: Reusing Phase 1 credentials (Flow D/E)
            print(f"\n  ✅ Reusing Phase 1 Databricks connection for all pipelines:")
            print(f"     Workspace : {sc.get('databricks_workspace_url')}")
            print(f"     Warehouse : {sc.get('databricks_warehouse_id')}")
            print(f"     Catalog   : {sc.get('catalog')} (Bronze)")
            print(f"     Schema    : {sc.get('schema')} (Bronze)")
            print(f"     Token     : ********")
            global_bronze_conn = {
                "workspace_url": sc.get("databricks_workspace_url", ""),
                "warehouse_id": sc.get("databricks_warehouse_id", ""),
                "token": sc.get("databricks_token", ""),
                "catalog": sc.get("catalog", ""),
                "schema": sc.get("schema", ""),
            }
        else:
            # Scenario 2: Fresh collection for silver (Flow C)
            print(f"\n  [Databricks Bronze Source] Enter common connection details for all silver pipelines\n")
            print(f"  ℹ  Your token will be shown masked after entry for confirmation.\n")
            global_bronze_conn["workspace_url"] = _ask(
                "Workspace URL  (e.g. https://dbc-xxxx.cloud.databricks.com)",
                default="",
            )
            global_bronze_conn["warehouse_id"] = _ask(
                "SQL Warehouse ID",
                default="",
            )
            global_bronze_conn["token"] = _ask_token(
                "Personal Access Token",
                default="",
            )
            global_bronze_conn["catalog"] = _ask(
                "Bronze catalog name",
                default="",
            )
            global_bronze_conn["schema"] = _ask(
                "Bronze schema name",
                default="",
            )
            print(f"  ✅ Global bronze connection set.")

        # 2. Collect global bronze table name (if multiple pipelines or single Flow C)
        if total > 1 or (total == 1 and not reuse):
            print(f"\n  [Global Bronze Source Table] Set source table for ALL pipelines in this phase")
            global_bronze_table = _ask(
                "Bronze table name (leave blank = all tables in schema)",
                default="",
                required=False,
            )
            if global_bronze_table:
                print(f"  ✅ Global bronze source table set: '{global_bronze_table}'")
        if total > 1 or reuse:
            print(f"\n  [Global Silver Destination] Set target for ALL pipelines in this phase")
            # The _ask function ensures a non-empty value is returned due to required=True
            # If the user enters nothing, it defaults to "silver_catalog" and "silver"
            # This ensures global_dest will not be empty if this block is executed.
            global_dest["catalog"] = _ask("Silver catalog name", default="silver_catalog")
            global_dest["schema"]  = _ask("Silver schema name",  default="silver")
            print(f"  ✅ Global destination set: {global_dest['catalog']}.{global_dest['schema']}")
        elif total == 1:
            print(f"\n  [Silver Destination] Only one pipeline, asking for destination directly.")

        failed = []
        for pipeline in plan.get("pipelines", []):
            try:
                _collect_pipeline(pipeline, sc, reuse_bronze_creds=reuse, global_dest=global_dest, global_bronze_table=global_bronze_table, global_bronze_conn=global_bronze_conn)
            except Exception as exc:
                logger.error("Credential error — %s: %s", pipeline["pipeline_id"], exc)
                pipeline["status"] = "credentials_failed"
                pipeline["error"]  = str(exc)
                failed.append(pipeline["pipeline_id"])

        plan["stage"]                    = "credentials_collected"
        plan["credentials_collected_at"] = datetime.now(timezone.utc).isoformat()
        if failed:
            plan["credentials_warnings"] = failed

        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, "silver_credentials.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(plan, f, indent=2, ensure_ascii=False)
        logger.info("Silver credentials → %s", out_path)

        print("\n" + "=" * 57)
        print("  SILVER CREDENTIAL COLLECTION COMPLETE")
        print(f"  Total   : {total}")
        print(f"  Success : {total - len(failed)}")
        print(f"  Failed  : {len(failed)}")
        if failed:
            print(f"  Failed  : {', '.join(failed)}")
        print("=" * 57 + "\n")

        return plan, out_path