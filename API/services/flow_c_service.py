"""
API/services/flow_c_service.py
Integration of 5-Step API Workflow with real Silver Agents.
Includes credential mapping fix, LLM hijack fix, and automated table execution.
"""

import os
import sys
import json
import uuid
import logging
import importlib.util
from typing import Dict, Any

logger = logging.getLogger("API.flow_c_service")

# ── Path setup ────────────────────────────────────────────────────────────────
_API_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT    = os.path.abspath(os.path.join(_API_DIR, "..", ".."))
SESSION_OUTPUT_DIR = os.path.join(_ROOT, "session_output", "flow_c_sessions")
os.makedirs(SESSION_OUTPUT_DIR, exist_ok=True)

# ── Agent Paths (Mapped to your repository structure) ─────────────────────────
_AGENT_PATHS = {
    "vali":      os.path.join(_ROOT, "SILVER", "silver_agent", "silver_validation.py"),
    "meta":      os.path.join(_ROOT, "SILVER", "silver_agent", "silver_metadata.py"),
    "class":     os.path.join(_ROOT, "SILVER", "silver_agent", "silver_classifier.py"),
    "trans":     os.path.join(_ROOT, "SILVER", "silver_agent", "silver_transform.py"),
    "schema":    os.path.join(_ROOT, "SILVER", "silver_agent", "silver_schema.py"),
    "table":     os.path.join(_ROOT, "SILVER", "silver_agent", "silver_table.py"),
    "final":     os.path.join(_ROOT, "SILVER", "silver_agent", "silver_final_gen.py"),
}

# =============================================================================
# GLOBAL IMPORT HIJACK FIX (The Nuclear Option)
# Forces Python to use your custom MCA/llm.py instead of the global pip package.
# =============================================================================
_MCA_LLM_PATH = os.path.join(_ROOT, "MCA", "llm.py")
if os.path.exists(_MCA_LLM_PATH):
    if "llm" in sys.modules:
        del sys.modules["llm"]
    
    _spec = importlib.util.spec_from_file_location("llm", _MCA_LLM_PATH)
    _llm_mod = importlib.util.module_from_spec(_spec)
    sys.modules["llm"] = _llm_mod
    _spec.loader.exec_module(_llm_mod)
# =============================================================================

# ── Helper: Dynamic Agent Loader ──────────────────────────────────────────────
def _load(module_name: str, file_path: str):
    if not os.path.exists(file_path): raise FileNotFoundError(f"Agent not found: {file_path}")
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod

# ── Session Management ────────────────────────────────────────────────────────
def get_session(session_id: str) -> Dict[str, Any]:
    path = os.path.join(SESSION_OUTPUT_DIR, f"{session_id}.json")
    if not os.path.exists(path): return None
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def save_session(session_id: str, state: Dict[str, Any]):
    path = os.path.join(SESSION_OUTPUT_DIR, f"{session_id}.json")
    with open(path, "w", encoding="utf-8") as f: json.dump(state, f, indent=2)

# ── STEP 1: START ─────────────────────────────────────────────────────────────
def start_session_service(bronze_config: dict, silver_config: dict):
    session_id = f"ses_{uuid.uuid4().hex[:10]}"
    output_dir = os.path.join(_ROOT, "session_output", session_id)
    os.makedirs(output_dir, exist_ok=True)
    
    mapped_bronze = {
        "workspace_url": bronze_config.get("databricks_workspace_url"),
        "warehouse_id":  bronze_config.get("databricks_warehouse_id"),
        "token":         bronze_config.get("databricks_token"),
        "catalog":       bronze_config.get("catalog"),
        "schema":        bronze_config.get("schema") or bronze_config.get("schema_name"),
        "table":         bronze_config.get("table", "")
    }
    
    mapped_silver = {
        "workspace_url": silver_config.get("databricks_workspace_url"),
        "warehouse_id":  silver_config.get("databricks_warehouse_id"),
        "token":         silver_config.get("databricks_token"),
        "catalog":       silver_config.get("catalog"),
        "schema":        silver_config.get("schema") or silver_config.get("schema_name")
    }
        
    cred_data = {
        "session_id": session_id,
        "pipelines": [{
            "pipeline_id": f"{session_id}_silver_001",
            "status": "credentials_collected",
            "connection_template": mapped_bronze,
            "destination": mapped_silver
        }]
    }
    
    cred_path = os.path.join(output_dir, "silver_credentials.json")
    with open(cred_path, "w", encoding="utf-8") as f:
        json.dump(cred_data, f, indent=2)
    
    mod_vali = _load("sc_vali", _AGENT_PATHS["vali"])
    mod_vali.run_silver_validation(cred_path, output_dir)
    
    mod_meta = _load("sc_meta", _AGENT_PATHS["meta"])
    _, meta_path = mod_meta.run_silver_metadata(cred_path, output_dir)
    
    mod_class = _load("sc_class", _AGENT_PATHS["class"])
    class_output, _ = mod_class.run_silver_classifier(meta_path, output_dir)
    
    save_session(session_id, {
        "session_id": session_id,
        "output_dir": output_dir,
        "cred_path": cred_path,
        "stage": "started",
        "taxonomy": class_output.get("tables", [])
    })
    
    return {"status": "session_started", "session_id": session_id, "taxonomy": class_output.get("tables", [])}

# ── STEP 2: TAXONOMY (GET) ────────────────────────────────────────────────────
def taxonomy_service(session_id: str):
    state = get_session(session_id)
    if not state: raise ValueError("Invalid session")
    return {"taxonomy": state.get("taxonomy")}

# ── STEP 3: APPLY EDITS (POST) ────────────────────────────────────────────────
def apply_taxonomy_service(session_id: str, edited_taxonomy: dict, edited_schema: dict):
    state = get_session(session_id)
    state.update({"edited_taxonomy": edited_taxonomy, "edited_schema": edited_schema, "stage": "edits_applied"})
    save_session(session_id, state)
    return {"status": "success", "message": "Edits saved.", "session_id": session_id}

# ── STEP 4: TRANSFORM ─────────────────────────────────────────────────────────
def transform_service(session_id: str, table_name: str, columns: list):
    state = get_session(session_id)
    output_dir = state["output_dir"]
    
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
    
    mod_schema = _load("sc_schema", _AGENT_PATHS["schema"])
    schema_data, _ = mod_schema.run_silver_schema(transform_path, output_dir)
    
    state.update({"stage": "transformed", "ddl": schema_data["tables"][0]["ddl"]})
    save_session(session_id, state)
    
    return {"status": "transform_completed", "ddl_preview": state["ddl"]}

# ── STEP 5: EXECUTE ───────────────────────────────────────────────────────────
def execute_service(session_id: str, proposed_table: str, custom_table: str = None):
    state = get_session(session_id)
    output_dir = state["output_dir"]
    
    schema_path = os.path.join(output_dir, "silver_schema.json")
    cred_path = os.path.join(output_dir, "silver_credentials.json")
    transform_path = os.path.join(output_dir, "transform_plan.json")
    status_path = os.path.join(output_dir, "silver_table_creation_status.json")

    # If the user provided a custom name, dynamically inject it into the schema JSON
    # so the legacy agent automatically creates it under the custom name.
    if custom_table and custom_table != proposed_table:
        custom_raw = custom_table.split(".")[-1]
        with open(schema_path, "r", encoding="utf-8") as f:
            s_data = json.load(f)
        if s_data.get("tables"):
            s_data["tables"][0]["table_name"] = custom_raw
            s_data["tables"][0]["ddl"] = s_data["tables"][0]["ddl"].replace(proposed_table, custom_table)
            with open(schema_path, "w", encoding="utf-8") as f:
                json.dump(s_data, f, indent=2)

    # 1. Execute Table Creation (Bypass input() using auto_approve=True)
    mod_table = _load("sc_table", _AGENT_PATHS["table"])
    mod_table.run_silver_table_creation(
        schema_path=schema_path,
        silver_credentials_path=cred_path,
        output_dir=output_dir,
        auto_approve=True 
    )
    
    # 2. Execute Final Generator
    mod_final = _load("sc_final", _AGENT_PATHS["final"])
    mod_final.run_silver_final_gen(
        credentials_path=cred_path,
        table_creation_path=status_path,
        transform_plan_path=transform_path,
        schema_path=schema_path,
        output_dir=output_dir
    )
    
    state["stage"] = "executed"
    save_session(session_id, state)
    
    table_created = custom_table if custom_table else proposed_table
    return {"status": "success", "message": f"Silver table {table_created} deployed successfully."}