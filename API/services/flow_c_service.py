"""
API/services/flow_c_service.py
Integration of 5-Step API Workflow with real Silver Agents.
Includes exact Databricks taxonomy table schema and INSTANT taxonomy commits.
"""

import os
import sys
import json
import uuid
import logging
import importlib.util
import difflib
import time
import requests
from datetime import datetime, timezone
from typing import Dict, Any

logger = logging.getLogger("API.flow_c_service")

# ── Path setup ────────────────────────────────────────────────────────────────
_API_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT    = os.path.abspath(os.path.join(_API_DIR, "..", ".."))
SESSION_OUTPUT_DIR = os.path.join(_ROOT, "session_output", "flow_c_sessions")
os.makedirs(SESSION_OUTPUT_DIR, exist_ok=True)

# ── FIX FOR MODULE COLLISION (LLM HIJACK) ─────────────────────────────────────
_MCA_LLM_PATH = os.path.join(_ROOT, "MCA", "llm.py")
if os.path.exists(_MCA_LLM_PATH):
    if "llm" in sys.modules: 
        del sys.modules["llm"]
    _spec = importlib.util.spec_from_file_location("llm", _MCA_LLM_PATH)
    _llm_mod = importlib.util.module_from_spec(_spec)
    sys.modules["llm"] = _llm_mod
    _spec.loader.exec_module(_llm_mod)

# ── Agent Paths ───────────────────────────────────────────────────────────────
_AGENT_PATHS = {
    "vali":      os.path.join(_ROOT, "SILVER", "silver_agent", "silver_validation.py"),
    "meta":      os.path.join(_ROOT, "SILVER", "silver_agent", "silver_metadata.py"),
    "class":     os.path.join(_ROOT, "SILVER", "silver_agent", "silver_classifier.py"),
    "schema":    os.path.join(_ROOT, "SILVER", "silver_agent", "silver_schema.py"),
    "table":     os.path.join(_ROOT, "SILVER", "silver_agent", "silver_table.py"),
    "final":     os.path.join(_ROOT, "SILVER", "silver_agent", "silver_final_gen.py"),
}

def _load(module_name: str, file_path: str):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod

def get_session(session_id: str) -> Dict[str, Any]:
    path = os.path.join(SESSION_OUTPUT_DIR, f"{session_id}.json")
    with open(path, "r", encoding="utf-8") as f: return json.load(f)

def save_session(session_id: str, state: Dict[str, Any]):
    path = os.path.join(SESSION_OUTPUT_DIR, f"{session_id}.json")
    with open(path, "w", encoding="utf-8") as f: json.dump(state, f, indent=2)

# ── REST API DATABRICKS EXECUTION ─────────────────────────────────────────────
def _exec_databricks_sql(workspace_url: str, warehouse_id: str, token: str, query: str) -> list:
    try:
        workspace_url = workspace_url.rstrip("/")
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type" : "application/json",
        }
        resp = requests.post(
            f"{workspace_url}/api/2.0/sql/statements",
            headers=headers,
            json={
                "statement"    : query,
                "warehouse_id" : warehouse_id,
                "wait_timeout" : "30s",
            },
            timeout=60,
        )
        if resp.status_code != 200:
            logger.error(f"SQL Submit Failed: {resp.text}")
            return []

        result = resp.json()
        state  = result.get("status", {}).get("state", "")

        def extract_rows(res_json):
            return res_json.get("result", {}).get("data_array", [])

        if state == "SUCCEEDED":
            return extract_rows(result)
        
        if state in ("FAILED", "CANCELED"):
            logger.error(f"SQL Failed: {result.get('status', {}).get('error', {})}")
            return []

        stmt_id    = result["statement_id"]
        status_url = f"{workspace_url}/api/2.0/sql/statements/{stmt_id}"
        start      = time.time()

        while True:
            sr    = requests.get(status_url, headers=headers, timeout=15)
            sr_json = sr.json()
            state = sr_json.get("status", {}).get("state", "")
            
            if state == "SUCCEEDED":
                return extract_rows(sr_json)
            if state in ("FAILED", "CANCELED"):
                logger.error(f"SQL Failed: {sr_json.get('status', {}).get('error', {})}")
                return []
            if time.time() - start > 120:
                logger.error("SQL Timed out")
                return []
            time.sleep(2)
            
    except Exception as e:
        logger.error(f"Databricks SQL Error: {e}")
        raise

# ── STEP 1: START ─────────────────────────────────────────────────────────────
def start_session_service(bronze_config: dict, silver_config: dict):
    session_id = f"ses_{uuid.uuid4().hex[:10]}"
    output_dir = os.path.join(_ROOT, "session_output", session_id)
    os.makedirs(output_dir, exist_ok=True)
    
    workspace = bronze_config["databricks_workspace_url"]
    warehouse = bronze_config["databricks_warehouse_id"]
    token = bronze_config["databricks_token"]
    
    b_catalog = bronze_config["catalog"]
    b_schema = bronze_config.get("schema", bronze_config.get("schema_name"))
    b_table = bronze_config["table"]

    s_catalog = silver_config["catalog"]
    s_schema = silver_config.get("schema", silver_config.get("schema_name"))

    bronze_internal = {
        "workspace_url": workspace,
        "warehouse_id": warehouse,
        "token": token,
        "catalog": b_catalog,
        "schema": b_schema,
        "table": b_table
    }
    
    silver_dest = {
        "workspace_url": workspace, 
        "warehouse_id": warehouse, 
        "token": token,
        "catalog": s_catalog, 
        "schema": s_schema
    }
    
    cred_data = {
        "session_id": session_id,
        "pipelines": [{
            "pipeline_id": f"{session_id}_silver_001",
            "status": "credentials_collected",
            "connection_template": bronze_internal,
            "destination": silver_dest
        }]
    }
    
    cred_path = os.path.join(output_dir, "silver_credentials.json")
    with open(cred_path, "w", encoding="utf-8") as f: json.dump(cred_data, f, indent=2)
    
    _load("sc_vali", _AGENT_PATHS["vali"]).run_silver_validation(cred_path, output_dir)
    _, meta_path = _load("sc_meta", _AGENT_PATHS["meta"]).run_silver_metadata(cred_path, output_dir)
    _, class_path = _load("sc_class", _AGENT_PATHS["class"]).run_silver_classifier(meta_path, output_dir)
    
    logger.info(f"Preparing taxonomy table at {s_catalog}.{s_schema}…")
    _exec_databricks_sql(workspace, warehouse, token, f"CREATE CATALOG IF NOT EXISTS `{s_catalog}`")
    _exec_databricks_sql(workspace, warehouse, token, f"CREATE SCHEMA IF NOT EXISTS `{s_catalog}`.`{s_schema}`")
    
    tax_table = f"`{s_catalog}`.`{s_schema}`.silver_taxonomy_lookup"
    _exec_databricks_sql(workspace, warehouse, token, f"""
        CREATE TABLE IF NOT EXISTS {tax_table} (
            source_schema      STRING,
            source_table       STRING,
            source_column      STRING,
            raw_value          STRING,
            standardized_value STRING,
            data_type          STRING,
            confidence         STRING,
            last_updated       TIMESTAMP
        ) USING DELTA""")

    save_session(session_id, {
        "session_id": session_id, "output_dir": output_dir, "cred_path": cred_path, 
        "bronze": bronze_config, "silver": silver_dest, "stage": "started"
    })
    
    return {"status": "session_started", "session_id": session_id, "message": "Profiling complete and empty taxonomy table created."}

# ── STEP 2: TAXONOMY GENERATION (GET) ─────────────────────────────────────────
def generate_taxonomy_proposals(session_id: str):
    state = get_session(session_id)
    class_path = os.path.join(state["output_dir"], "silver_classifier_output.json")
    
    with open(class_path, "r", encoding="utf-8") as f: class_data = json.load(f)
    
    target_cols = []
    for table in class_data.get("tables", []):
        for col, rules in table.get("columns", {}).items():
            if rules.get("label") == "STANDARDIZE_CATEGORICAL":
                target_cols.append(col)

    workspace = state["bronze"]["databricks_workspace_url"]
    warehouse = state["bronze"]["databricks_warehouse_id"]
    token = state["bronze"]["databricks_token"]
    b_cat = state["bronze"]["catalog"]
    b_sch = state["bronze"].get("schema", state["bronze"].get("schema_name"))
    b_tbl = state["bronze"]["table"]

    proposed_mappings = {}
    
    for col in target_cols:
        query = f"SELECT DISTINCT `{col}` FROM `{b_cat}`.`{b_sch}`.`{b_tbl}` WHERE `{col}` IS NOT NULL"
        rows = _exec_databricks_sql(workspace, warehouse, token, query)
        
        distinct_vals = [r[0] for r in rows if r and r[0]]
        
        col_mappings = {}
        for val in distinct_vals:
            matches = difflib.get_close_matches(val.title(), [v.title() for v in distinct_vals], n=1, cutoff=0.75)
            canonical = matches[0] if matches else val.title()
            if val != canonical:
                col_mappings[val] = canonical
        if col_mappings:
            proposed_mappings[col] = col_mappings

    return {
        "session_id": session_id,
        "categorical_columns": target_cols,
        "proposed_mappings": proposed_mappings
    }

# ── STEP 3: APPLY TAXONOMY (NOW WITH INSTANT DATABRICKS SYNC) ─────────────────
def apply_taxonomy_service(session_id: str, edited_taxonomy: dict):
    state = get_session(session_id)
    
    # Save to local session
    state["edited_taxonomy"] = edited_taxonomy
    save_session(session_id, state)

    # Instantly sync to Databricks (Matching CLI behavior)
    if edited_taxonomy:
        dest = state["silver"]
        workspace, warehouse, token = dest["workspace_url"], dest["warehouse_id"], dest["token"]
        catalog, schema = dest["catalog"], dest["schema"]
        
        tax_table = f"`{catalog}`.`{schema}`.silver_taxonomy_lookup"
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        b_schema = state["bronze"].get("schema", state["bronze"].get("schema_name", "bronze"))
        b_table = state["bronze"]["table"]
        
        rows = []
        for col, mappings in edited_taxonomy.items():
            for raw, canon in mappings.items():
                # Escape single quotes for SQL
                raw_safe = str(raw).replace("'", "''")
                canon_safe = str(canon).replace("'", "''")
                rows.append(f"('{b_schema}', '{b_table}', '{col}', '{raw_safe}', '{canon_safe}', 'STRING', 'high', TO_TIMESTAMP('{now_str}'))")
                
        if rows:
            # Wipe existing taxonomy for these columns to prevent duplicates
            cols_str = ",".join([f"'{k}'" for k in edited_taxonomy.keys()])
            _exec_databricks_sql(workspace, warehouse, token, f"DELETE FROM {tax_table} WHERE source_table='{b_table}' AND source_column IN ({cols_str})")
            
            # Insert the new mappings
            insert_sql = f"""
                INSERT INTO {tax_table} 
                (source_schema, source_table, source_column, raw_value, standardized_value, data_type, confidence, last_updated)
                VALUES {','.join(rows)}
            """
            _exec_databricks_sql(workspace, warehouse, token, insert_sql)
            
    return {"status": "success", "message": "Taxonomy saved and instantly pushed to Databricks.", "session_id": session_id}

# ── STEP 4: TRANSFORM ─────────────────────────────────────────────────────────
def transform_service(session_id: str, table_name: str, columns: list):
    state = get_session(session_id)
    output_dir = state["output_dir"]
    
    # 1. Map API keys to the exact keys expected by the internal Databricks agents
    agent_columns = []
    for col in columns:
        # Handle dict vs Pydantic model depending on how FastAPI passes it
        if hasattr(col, "model_dump"): 
            col = col.model_dump()
            
        agent_columns.append({
            "source_column": col.get("source_column", ""),
            "target_column": col.get("silver_column", ""), # Agent expects 'target_column'
            "target_type":   col.get("silver_type", "STRING"),   # Agent expects 'target_type'
            "null_handling": col.get("null_strategy", "keep"), # Agent expects 'null_handling'
            "fill_value":    col.get("fill_value", "")
        })
    
    # 2. Build the transform plan
    transform_plan = {
        "session_id": session_id,
        "tables": [{
            "pipeline_id": f"{session_id}_silver_001",
            "table": table_name.split('.')[-1],
            "full_table": table_name,
            "columns": agent_columns,
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
    
    meta_path = os.path.join(output_dir, "silver_metadata.json")
    mod_schema = _load("sc_schema", _AGENT_PATHS["schema"])
    schema_data, _ = mod_schema.run_silver_schema(
        transform_plan_path=transform_path, 
        metadata_path=meta_path, 
        output_dir=output_dir
    )
    
    ddl_preview = schema_data["tables"][0]["ddl"]
    state["ddl"] = ddl_preview
    save_session(session_id, state)
    
    return {"status": "transform_completed", "ddl_preview": ddl_preview}

# ── STEP 5: EXECUTE ───────────────────────────────────────────────────────────
def execute_service(session_id: str, proposed_table: str, custom_table: str = None):
    state = get_session(session_id)
    output_dir = state["output_dir"]
    cred_path = state["cred_path"]
    schema_path = os.path.join(output_dir, "silver_schema.json")
    transform_path = os.path.join(output_dir, "transform_plan.json")
    
    dest = state["silver"]
    catalog, schema = dest["catalog"], dest["schema"]

    # 1. Determine final table name (Custom overrides Proposed)
    final_table_raw = custom_table.strip() if custom_table else proposed_table.strip()
    final_table_raw = final_table_raw.split(".")[-1] # Ensure bare table name

    # 2. Update DDL in silver_schema.json if the user altered the name
    with open(schema_path, "r", encoding="utf-8") as f: 
        s_data = json.load(f)
        
    old_table = s_data["tables"][0]["table_name"]
    
    if old_table != final_table_raw:
        s_data["tables"][0]["table_name"] = final_table_raw
        s_data["tables"][0]["ddl"] = s_data["tables"][0]["ddl"].replace(old_table, final_table_raw)
        with open(schema_path, "w", encoding="utf-8") as f: 
            json.dump(s_data, f, indent=2)

    # 3. Trigger Table Creation Agent (auto_approve=True bypasses CLI inputs)
    # Note: silver_table.py natively handles Catalog and Schema creation before table creation.
    mod_table = _load("sc_table", _AGENT_PATHS["table"])
    status_dict, status_path = mod_table.run_silver_table_creation(
        schema_path=schema_path, 
        silver_credentials_path=cred_path, 
        session_config_path=None, 
        output_dir=output_dir, 
        auto_approve=True 
    )
    
    # 4. Trigger Final Migration Plan Generator
    mod_final = _load("sc_final", _AGENT_PATHS["final"])
    mod_final.run_silver_final_gen(
        credentials_path=cred_path, 
        table_creation_path=status_path,
        transform_plan_path=transform_path, 
        schema_path=schema_path, 
        output_dir=output_dir
    )
    
    return {
        "status": "success", 
        "message": f"Table {catalog}.{schema}.{final_table_raw} deployed successfully and migration plan generated."
    }