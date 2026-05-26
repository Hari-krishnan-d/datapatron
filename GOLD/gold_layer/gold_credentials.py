"""
gold_credentials.py — Credentials Collection & Module Initialisation
=====================================================================
Responsible for:
  1. Prompting the user once for all Databricks / LLM credentials.
  2. Injecting those credentials into every sub-module that needs them.

This keeps credential logic in ONE place so gold_orchestrator.py and
gold_main.py stay free of raw input() calls and os.getenv() lookups.

Usage
-----
    from GOLD.gold_layer.gold_credentials import collect_and_init_credentials

    creds = collect_and_init_credentials()   # call once at startup
"""

import os
from dotenv import load_dotenv

# ── Sub-module init functions ────────────────────────────────────────────────
from GOLD.gold_layer.data_model    import init_data_model
from GOLD.gold_layer.gold_metadata import init_gold_metadata
from GOLD.gold_layer.kpi_agent     import init_kpi_agent
from GOLD.gold_layer.agg_agent     import init_agg_agent

load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def collect_and_init_credentials() -> dict:
    """
    Prompt the user once for Databricks connection details, then inject the
    credentials into every gold-layer sub-module.

    Returns
    -------
    dict with keys:
        hostname         : Databricks workspace hostname
        sql_warehouse_id : SQL Warehouse HTTP path (e.g. /sql/1.0/warehouses/…)
        access_token     : Databricks personal access token
        model_endpoint   : LLM serving-endpoint name
    """
    print("\n" + "-" * 60)
    print(" STEP 1 → DATABRICKS CREDENTIALS COLLECTION")
    print("-" * 60)

    hostname         = input("\n  HOST           : ").strip()
    sql_warehouse_id = input("  SQL ID         : ").strip()
    access_token     = input("  TOKEN          : ").strip()

    creds: dict = {
        "hostname"        : hostname.replace("https://", "").replace("http://", "").rstrip("/"),
        "sql_warehouse_id": sql_warehouse_id if sql_warehouse_id.startswith("/sql/1.0/warehouses/")
                            else f"/sql/1.0/warehouses/{sql_warehouse_id}",
        "access_token"    : access_token,
        "model_endpoint"  : os.getenv("MODEL_NAME", ""),
    }

    # Allow env-var override for the model endpoint; fall back to interactive
    if not creds["model_endpoint"]:
        creds["model_endpoint"] = os.getenv("model_endpoint") or os.getenv("MODEL_ENDPOINT", "")

    _init_all_modules(creds)

    print("\n  ✓ Credentials collected and all modules initialised.")
    return creds


def init_from_env() -> dict:
    """
    Alternative to collect_and_init_credentials() for non-interactive
    environments (CI, notebooks, etc.).  Reads all values from environment
    variables and raises clearly if any are missing.

    Required env vars
    -----------------
        DATABRICKS_HOSTNAME
        DATABRICKS_SQL_WAREHOUSE_ID
        DATABRICKS_ACCESS_TOKEN
        MODEL_NAME

    Returns
    -------
    Same credentials dict shape as collect_and_init_credentials().
    """
    required = {
        "hostname":         "DATABRICKS_HOSTNAME",
        "sql_warehouse_id": "DATABRICKS_SQL_WAREHOUSE_ID",
        "access_token":     "DATABRICKS_ACCESS_TOKEN",
        "model_endpoint":   "MODEL_NAME",
    }

    creds: dict = {}
    missing: list[str] = []

    for key, env_var in required.items():
        value = os.getenv(env_var, "")
        if not value:
            missing.append(env_var)
        creds[key] = value

    if missing:
        raise EnvironmentError(
            f"Missing required environment variable(s): {', '.join(missing)}\n"
            "Set them before calling init_from_env(), or use "
            "collect_and_init_credentials() for interactive mode."
        )

    _init_all_modules(creds)
    print("  ✓ Credentials loaded from environment and all modules initialised.")
    return creds


# ─────────────────────────────────────────────────────────────────────────────
# Internal helper
# ─────────────────────────────────────────────────────────────────────────────

def _init_all_modules(creds: dict) -> None:
    """
    Push credentials into every sub-module that maintains a module-level
    lazy OpenAI / Databricks client.  Add new init calls here whenever a
    new module follows the same lazy-init pattern.
    """
    init_data_model(
        hostname         = creds["hostname"],
        sql_warehouse_id = creds["sql_warehouse_id"],
        access_token     = creds["access_token"],
        model_endpoint   = creds["model_endpoint"],
    )

    init_gold_metadata(
        hostname         = creds["hostname"],
        sql_warehouse_id = creds["sql_warehouse_id"],
        access_token     = creds["access_token"],
        model_endpoint   = creds["model_endpoint"],
    )

    init_kpi_agent(
        hostname       = creds["hostname"],
        access_token   = creds["access_token"],
        model_endpoint = creds["model_endpoint"],
    )

    init_agg_agent(
        hostname         = creds["hostname"],
        sql_warehouse_id = creds["sql_warehouse_id"],
        access_token     = creds["access_token"],
        model_endpoint   = creds["model_endpoint"],
    )