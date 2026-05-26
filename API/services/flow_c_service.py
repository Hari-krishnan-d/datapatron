"""
API/services/flow_c_service.py — Flow C Service Layer
=======================================================
Flow C = Bronze (Databricks) → Silver (Databricks)

Orchestrates the Silver Agents sequentially, maintaining state across 
HTTP requests using the SESSION_OUTPUT_DIR files.
"""

import os
import sys
import json
import logging
import importlib.util
from datetime import datetime, timezone

logger = logging.getLogger("API.flow_c_service")

# ── Path setup ────────────────────────────────────────────────────────────────
_API_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT    = os.path.abspath(os.path.join(_API_DIR, "..", ".."))

_AGENT_PATHS = {
    "silver_vali":       os.path.join(_ROOT, "SILVER", "silver_agent", "silver_validation.py"),
    "silver_metadata":   os.path.join(_ROOT, "SILVER", "silver_agent", "silver_metadata.py"),
    "silver_classifier": os.path.join(_ROOT, "SILVER", "silver_agent", "silver_classifier.py"),
    "silver_transform":  os.path.join(_ROOT, "SILVER", "silver_agent", "silver_transform.py"),
    "silver_schema":     os.path.join(_ROOT, "SILVER", "silver_agent", "silver_schema.py"),
    "silver_table":      os.path.join(_ROOT, "SILVER", "silver_agent", "silver_table.py"),
    "silver_final_gen":  os.path.join(_ROOT, "SILVER", "silver_agent", "silver_final_gen.py"),
}

SESSION_OUTPUT_DIR = os.path.join(_ROOT, "session_output")


def _load(module_name: str, file_path: str):
    """Dynamic loader for agent modules."""
    if module_name in sys.modules:
        return sys.modules[module_name]
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Agent not found: {file_path}")
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod  = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _get_output_dir(session_id: str) -> str:
    import glob
    dirs = sorted(glob.glob(os.path.join(SESSION_OUTPUT_DIR, f"{session_id}_*")), reverse=True)
    if not dirs:
        output_dir = os.path.join(SESSION_OUTPUT_DIR, f"{session_id}_api_flow_c")
        os.makedirs(output_dir, exist_ok=True)
        return output_dir
    return dirs[0]


# ── STEP 1: START ─────────────────────────────────────────────────────────────
def run_flow_c_start(session_id: str, bronze_config: dict, silver_config: dict) -> dict:
    output_dir = _get_output_dir(session_id)
    
    # 1. API Credentials Injection
    plan = {
        "session_id": session_id,
        "stage": "credentials_collected",
        "credentials_collected_at": datetime.now(timezone.utc).isoformat(),
        "pipelines": [
            {
                "pipeline_id": f"{session_id}_silver_001",
                "status": "credentials_collected",
                "connection_template": {
                    "workspace_url": bronze_config.get("databricks_workspace_url"),
                    "warehouse_id": bronze_config.get("databricks_warehouse_id"),
                    "token": bronze_config.get("databricks_token"),
                    "catalog": bronze_config.get("catalog"),
                    "schema": bronze_config.get("schema") or bronze_config.get("schema_name"),
                    "table": ""  
                },
                "destination": {
                    "catalog": silver_config.get("catalog"),
                    "schema": silver_config.get("schema") or silver_config.get("schema_name")
                }
            }
        ]
    }
    
    cred_path = os.path.join(output_dir, "silver_credentials.json")
    with open(cred_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2)
    logger.info(f"API generated silver_credentials.json at {cred_path}")
    
    # 2. Validation (FIXED FUNCTION NAME)
    mod_vali = _load("sc_vali", _AGENT_PATHS["silver_vali"])
    mod_vali.run_silver_validation(cred_path, output_dir)
    
    # 3. Metadata (FIXED FUNCTION NAME)
    mod_meta = _load("sc_meta", _AGENT_PATHS["silver_metadata"])
    metadata_data, metadata_path = mod_meta.run_silver_metadata(cred_path, output_dir)
    
    # 4. Classifier (FIXED FUNCTION NAME)
    mod_class = _load("sc_classifier", _AGENT_PATHS["silver_classifier"])
    classifier_output, classifier_path = mod_class.run_silver_classifier(metadata_path, output_dir)
    
    return {
        "status": "success",
        "session_id": session_id,
        "stage": "awaiting_taxonomy",
        "taxonomy_proposals": classifier_output.get("tables", [])
    }

# ── STEP 2: TAXONOMY (Mocked for API isolation) ───────────────────────────────
def run_flow_c_taxonomy(session_id: str, approved_taxonomy: dict) -> dict:
    output_dir = _get_output_dir(session_id)
    
    # For now, we simulate returning the structure so the frontend can proceed 
    # to the transform schema step without hitting the legacy input() terminals.
    return {
        "status": "success",
        "session_id": session_id,
        "stage": "awaiting_transform",
        "transform_proposals": []
    }

# ── STEP 3: TRANSFORM (Mocked for API isolation) ──────────────────────────────
def run_flow_c_transform(session_id: str, table_name: str, columns: list) -> dict:
    output_dir = _get_output_dir(session_id)
    
    # Write directly to JSON to bypass CLI prompts in silver_transform.py
    transform_plan = {
        "session_id": session_id,
        "tables": [{
            "pipeline_id": f"{session_id}_silver_001",
            "table": table_name.split('.')[-1],
            "full_table": table_name,
            "columns": columns,
            "operations": {
                "remove_nulls": False,
                "drop_duplicates": True,
                "standardize_names": True,
                "cast_types": True,
                "null_handling": {
                    "replace_placeholders": True,
                    "drop_if_all_columns_null": True,
                    "keep_partial_nulls": True
                }
            }
        }]
    }
    
    transform_path = os.path.join(output_dir, "transform_plan.json")
    with open(transform_path, "w", encoding="utf-8") as f:
        json.dump(transform_plan, f, indent=2)
        
    mod_schema = _load("sc_schema", _AGENT_PATHS["silver_schema"])
    schema_data, schema_path = mod_schema.run_silver_schema(transform_plan_path=transform_path, output_dir=output_dir)
    
    return {
        "status": "success",
        "session_id": session_id,
        "stage": "awaiting_execution",
        "proposed_table_name": schema_data["tables"][0]["table_name"],
        "ddl_preview": schema_data["tables"][0]["ddl"]
    }

# ── STEP 4: EXECUTE ───────────────────────────────────────────────────────────
def run_flow_c_execute(session_id: str, proposed_table: str, custom_table: str = None) -> dict:
    output_dir = _get_output_dir(session_id)
    final_table_name = custom_table if custom_table else proposed_table
    
    # Simulated execution response for testing UI
    return {
        "status": "success",
        "session_id": session_id,
        "stage": "completed",
        "target_table": final_table_name,
        "migration_plan": {"status": "Table deployed"}
    }