"""
API/services/flow_b_service.py — Flow B Service Layer
=======================================================
Flow B = Azure SQL → Bronze (raw ingestion only)

Wraps the 7 bronze pipeline steps from existing agent files.
Does NOT rewrite any logic — only calls, orchestrates, and collects results.

Key design (mirrors flow_a_service.py exactly, adapted for Azure SQL):
  - NO session_id or mca_plan passed from the user
  - Service automatically loads the LATEST MCA plan from session_output/mca_plans/
  - session_id is extracted from the loaded plan
  - credentials_map is built from the single AzureSQLCredentials dict
    keyed by pipeline_id AND by source_type "azure_sql" as fallback

Step sequence (mirrors _run_bronze_steps in orchestrator.py):
  1. cred.py      — CredentialsCollectionAgent (inject credentials, no prompts)
  2. Vali.py      — run_validation
  3. metadata.py  — run_metadata
  4. pipeline.py  — run_pipeline_strategy
  5. schema.py    — run_schema_mapping
  6. Table.py     — run_table_creation
  7. final_gen.py — run_migration_plan

All steps run regardless of failure — each step status is collected and returned.
"""

import os
import sys
import glob
import json
import logging
import importlib.util
from datetime import datetime

logger = logging.getLogger("API.flow_b_service")

# ── Path setup ────────────────────────────────────────────────────────────────
_API_DIR = os.path.dirname(os.path.abspath(__file__))           # API/services/
_ROOT    = os.path.abspath(os.path.join(_API_DIR, "..", ".."))  # project root

_AGENT_PATHS = {
    "cred"     : os.path.join(_ROOT, "CRED",   "cred.py"),
    "vali"     : os.path.join(_ROOT, "BRONZE", "Vali.py"),
    "metadata" : os.path.join(_ROOT, "BRONZE", "metadata.py"),
    "pipeline" : os.path.join(_ROOT, "BRONZE", "pipeline.py"),
    "schema"   : os.path.join(_ROOT, "BRONZE", "schema.py"),
    "table"    : os.path.join(_ROOT, "BRONZE", "Table.py"),
    "final_gen": os.path.join(_ROOT, "BRONZE", "final_gen.py"),
}

MCA_PLANS_DIR      = os.path.join(_ROOT, "session_output", "mca_plans")
SESSION_OUTPUT_DIR = os.path.join(_ROOT, "session_output")


# ── Importlib loader (same pattern as orchestrator.py) ───────────────────────

def _load(module_name: str, file_path: str):
    """Load an agent module by absolute path. Uses cache if already loaded."""
    if module_name in sys.modules:
        return sys.modules[module_name]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Agent not found: {file_path}")
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod  = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return mod


# ── Step status builders ──────────────────────────────────────────────────────

def _ok(step: str, output: str = None) -> dict:
    return {"step": step, "status": "success", "output": output, "error": None}

def _fail(step: str, error: str) -> dict:
    return {"step": step, "status": "failed", "output": None, "error": error}


# ── Load latest MCA plan from disk ────────────────────────────────────────────

def _load_latest_mca_plan() -> tuple[dict, str]:
    """
    Automatically find and load the most recently saved MCA plan.
    Returns (plan_dict, session_id).
    Raises ValueError if no plan found.
    """
    plan_files = sorted(
        glob.glob(os.path.join(MCA_PLANS_DIR, "mca_plan_*.json")),
        key=os.path.getmtime,
        reverse=True,
    )
    if not plan_files:
        raise ValueError(
            "No MCA plan found. Please call POST /mca/execute first."
        )

    latest = plan_files[0]
    logger.info("Loading latest MCA plan: %s", latest)

    with open(latest, "r", encoding="utf-8") as f:
        plan = json.load(f)

    session_id = plan.get("session_id")
    if not session_id:
        raise ValueError("MCA plan is missing session_id.")

    if not plan.get("pipelines"):
        raise ValueError("MCA plan contains no pipelines.")

    return plan, session_id


# ── Credentials injection helper ──────────────────────────────────────────────

def _build_credentials_map(mca_plan: dict, sql_creds: dict) -> dict:
    """
    Build credentials_map for CredentialsCollectionAgent.
    Maps each pipeline_id AND the source_type "azure_sql" to the
    supplied Azure SQL credentials dict.
    No interactive prompts — all values injected from the API request.
    """
    creds_map = {}
    for pipeline in mca_plan.get("pipelines", []):
        pid               = pipeline.get("pipeline_id", "")
        creds_map[pid]    = sql_creds
    creds_map["azure_sql"] = sql_creds   # source_type fallback
    return creds_map


def _save_session_config(databricks_config: dict, config_path: str) -> None:
    """Save Databricks config so Table.py can load it via load_session_config."""
    os.makedirs(os.path.dirname(os.path.abspath(config_path)), exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(databricks_config, f, indent=2)


# ── Main service function ─────────────────────────────────────────────────────

def run_flow_b(
    sql_credentials  : dict,
    databricks_config: dict,
) -> dict:
    """
    Execute all 7 bronze pipeline steps for Flow B (Azure SQL → Bronze).

    Automatically loads the latest MCA plan from disk — no session_id
    or mca_plan needed from the user.

    All steps run regardless of individual failures.

    Args:
        sql_credentials:   Azure SQL fields (server, database, username, password, schema).
        databricks_config: Databricks config (url, warehouse_id, token, catalog, schema).

    Returns:
        dict with keys: status, session_id, flow, steps, migration_plan, output_dir, stage

    Raises:
        ValueError  — no MCA plan found or invalid inputs
        RuntimeError — critical setup failure
    """

    # ── Step 0: Load latest MCA plan from disk ────────────────────────────────
    mca_plan, session_id = _load_latest_mca_plan()
    logger.info("Flow B starting — session=%s", session_id)

    # ── Output directory for this run ─────────────────────────────────────────
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(SESSION_OUTPUT_DIR, f"{session_id}_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)
    logger.info("Flow B output dir: %s", output_dir)

    steps          = []
    cred_path      = None
    metadata_path  = None
    strategy_path  = None
    schema_path    = None
    table_path     = None
    migration_plan = None

    # ── Save Databricks session config (Table.py reads this) ──────────────────
    config_path = os.path.join(SESSION_OUTPUT_DIR, "session_config.json")
    _save_session_config(databricks_config, config_path)

    # ── Build credentials map for CredentialsCollectionAgent ──────────────────
    credentials_map = _build_credentials_map(mca_plan, sql_credentials)

    # =========================================================================
    # STEP 1 — Credentials Collection  [CRED/cred.py]
    # =========================================================================
    logger.info("Flow B Step 1: Credentials Collection")
    try:
        mod   = _load("fb_agent_cred", _AGENT_PATHS["cred"])
        agent = mod.CredentialsCollectionAgent()
        _, cred_path = agent.execute(
            source          = mca_plan,
            output_dir      = output_dir,
            credentials_map = credentials_map,
        )
        steps.append(_ok("credentials_collection", cred_path))
        logger.info("Step 1 OK — cred_path=%s", cred_path)
    except Exception as exc:
        logger.error("Step 1 FAILED: %s", exc)
        steps.append(_fail("credentials_collection", str(exc)))
        # Cannot continue without credentials
        return _build_response(session_id, steps, None, output_dir, failed=True)

    # =========================================================================
    # STEP 2 — Connection Validation  [BRONZE/Vali.py]
    # =========================================================================
    logger.info("Flow B Step 2: Connection Validation")
    try:
        mod = _load("fb_agent_vali", _AGENT_PATHS["vali"])
        _, cred_path = mod.run_validation(
            credentials_path = cred_path,
            output_dir       = output_dir,
        )
        vali_path = os.path.join(output_dir, "validation_results.json")
        steps.append(_ok("connection_validation", vali_path))
        logger.info("Step 2 OK")
    except RuntimeError as exc:
        # All Azure SQL pipelines failed validation
        logger.error("Step 2 FAILED (all pipelines): %s", exc)
        steps.append(_fail("connection_validation", str(exc)))
    except Exception as exc:
        logger.error("Step 2 FAILED: %s", exc)
        steps.append(_fail("connection_validation", str(exc)))

    # =========================================================================
    # STEP 3 — Metadata Extraction  [BRONZE/metadata.py]
    # =========================================================================
    logger.info("Flow B Step 3: Metadata Extraction")
    try:
        mod = _load("fb_agent_metadata", _AGENT_PATHS["metadata"])
        _, metadata_path = mod.run_metadata(
            credentials_path = cred_path,
            output_dir       = output_dir,
        )
        steps.append(_ok("metadata_extraction", metadata_path))
        logger.info("Step 3 OK — metadata_path=%s", metadata_path)
    except Exception as exc:
        logger.error("Step 3 FAILED: %s", exc)
        steps.append(_fail("metadata_extraction", str(exc)))

    # =========================================================================
    # STEP 4 — Pipeline Strategy  [BRONZE/pipeline.py]
    # =========================================================================
    logger.info("Flow B Step 4: Pipeline Strategy")
    try:
        if not metadata_path:
            raise RuntimeError("metadata_path unavailable — Step 3 failed.")
        mod = _load("fb_agent_pipeline", _AGENT_PATHS["pipeline"])
        _, strategy_path = mod.run_pipeline_strategy(
            mca_path      = cred_path,
            metadata_path = metadata_path,
            output_dir    = output_dir,
        )
        steps.append(_ok("pipeline_strategy", strategy_path))
        logger.info("Step 4 OK — strategy_path=%s", strategy_path)
    except Exception as exc:
        logger.error("Step 4 FAILED: %s", exc)
        steps.append(_fail("pipeline_strategy", str(exc)))

    # =========================================================================
    # STEP 5 — Schema Mapping  [BRONZE/schema.py]
    # =========================================================================
    logger.info("Flow B Step 5: Schema Mapping")
    try:
        if not strategy_path or not metadata_path:
            raise RuntimeError("strategy_path or metadata_path unavailable.")
        mod = _load("fb_agent_schema", _AGENT_PATHS["schema"])
        _, schema_path = mod.run_schema_mapping(
            strategy_path = strategy_path,
            metadata_path = metadata_path,
            output_dir    = output_dir,
        )
        steps.append(_ok("schema_mapping", schema_path))
        logger.info("Step 5 OK — schema_path=%s", schema_path)
    except Exception as exc:
        logger.error("Step 5 FAILED: %s", exc)
        steps.append(_fail("schema_mapping", str(exc)))

    # =========================================================================
    # STEP 6 — Table Creation  [BRONZE/Table.py]
    # =========================================================================
    logger.info("Flow B Step 6: Table Creation")
    try:
        if not schema_path:
            raise RuntimeError("schema_path unavailable — Step 5 failed.")
        mod = _load("fb_agent_table", _AGENT_PATHS["table"])
        _, table_path = mod.run_table_creation(
            schema_path   = schema_path,
            workspace_url = databricks_config["databricks_workspace_url"],
            warehouse_id  = databricks_config["databricks_warehouse_id"],
            token         = databricks_config["databricks_token"],
            catalog       = databricks_config["catalog"],
            schema_name   = databricks_config["schema"],
            output_dir    = output_dir,
            auto_approve  = True,  # API mode — no interactive prompts
        )
        steps.append(_ok("table_creation", table_path))
        logger.info("Step 6 OK — table_path=%s", table_path)
    except Exception as exc:
        logger.error("Step 6 FAILED: %s", exc)
        steps.append(_fail("table_creation", str(exc)))

    # =========================================================================
    # STEP 7 — Migration Plan  [BRONZE/final_gen.py]
    # =========================================================================
    logger.info("Flow B Step 7: Migration Plan")
    try:
        if not table_path or not strategy_path:
            raise RuntimeError("table_path or strategy_path unavailable.")
        mod = _load("fb_agent_final_gen", _AGENT_PATHS["final_gen"])
        migration_plan, plan_path = mod.run_migration_plan(
            credentials_path    = cred_path,
            table_creation_path = table_path,
            strategy_path       = strategy_path,
            output_dir          = output_dir,
            schema_path         = schema_path,
        )
        steps.append(_ok("migration_plan", plan_path))
        logger.info("Step 7 OK — plan_path=%s", plan_path)
    except Exception as exc:
        logger.error("Step 7 FAILED: %s", exc)
        steps.append(_fail("migration_plan", str(exc)))

    return _build_response(session_id, steps, migration_plan, output_dir)


# ── Response builder ──────────────────────────────────────────────────────────

def _build_response(
    session_id    : str,
    steps         : list,
    migration_plan: dict | None,
    output_dir    : str,
    failed        : bool = False,
) -> dict:
    failed_steps  = [s for s in steps if s["status"] == "failed"]
    success_steps = [s for s in steps if s["status"] == "success"]

    if failed or len(failed_steps) == len(steps):
        overall = "failed"
    elif failed_steps:
        overall = "partial"
    else:
        overall = "success"

    logger.info(
        "Flow B complete — overall=%s passed=%d failed=%d",
        overall, len(success_steps), len(failed_steps),
    )

    return {
        "status"        : overall,
        "session_id"    : session_id,
        "flow"          : "B",
        "steps"         : steps,
        "migration_plan": migration_plan,
        "output_dir"    : output_dir,
        "stage"         : "awaiting_execution",
    }