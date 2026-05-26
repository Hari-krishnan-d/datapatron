"""
orchestrator.py — Unified Platform Orchestrator
=================================================
Routes execution based on flow_type from MCA (A / B / C / D / E / F / G / H / I / J / K).

EXISTING FLOWS (A–G) — UNCHANGED:
  FLOW A  →  S3        → Bronze
  FLOW B  →  Azure SQL → Bronze
  FLOW C  →  Databricks Bronze → Silver
  FLOW D  →  S3        → Bronze → Silver
  FLOW E  →  Azure SQL → Bronze → Silver
  FLOW F  →  Mixed     → Bronze
  FLOW G  →  Mixed     → Bronze → Silver

GOLD LAYER FLOWS (H–K):
  FLOW H  →  Bronze Source → Bronze → Silver → Gold (Data Modeling)
  FLOW I  →  Bronze Source → Bronze → Silver → KPI Calculation
  FLOW J  →  Silver (existing) → Gold (Data Modeling)
  FLOW K  →  Silver (existing) → KPI Calculation
"""

import json
import logging
import os
import sys
import importlib.util
from datetime import datetime
from dotenv import load_dotenv
import traceback

load_dotenv()

logging.disable(logging.CRITICAL)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Orchestrator")


# =============================================================================
# IMPORTLIB LOADER
# =============================================================================

def _load(module_name: str, file_path: str):
    # Return cached module if already loaded
    if module_name in sys.modules:
        return sys.modules[module_name]

    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"\n  ⛔ Agent not found : {file_path}\n"
            f"  Ensure '{os.path.basename(file_path)}' exists at that location."
        )
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod  = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return mod


# =============================================================================
# DATABRICKS SESSION CONFIG
# =============================================================================

def load_session_config(config_path: str) -> dict:
    if os.path.exists(config_path):
        logger.info("Loading Databricks config: %s", config_path)
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    print("\n" + "=" * 62)
    print("  DATABRICKS DESTINATION SETUP")
    print("  (saved once — won't be asked again)")
    print("=" * 62)
    config = {
        "databricks_workspace_url": input("  Workspace URL     : ").strip().rstrip("/"),
        "databricks_warehouse_id":  input("  Warehouse ID      : ").strip(),
        "databricks_token":         input("  Access Token      : ").strip(),
        "catalog":                  input("  Catalog name      : ").strip(),
        "schema":                   input("  Schema name       : ").strip(),
    }
    os.makedirs(os.path.dirname(os.path.abspath(config_path)), exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    logger.info("Databricks config saved: %s", config_path)
    return config


# =============================================================================
# AGENT PATH REGISTRY
# Updated to use BRONZE/, SILVER/, and GOLD/ subdirectory structure.
# =============================================================================

def _agent_paths(root: str) -> dict:
    return {
        # ── Bronze agents ──────────────────────────────────────────────────
        "mca"      : os.path.join(root, "MCA",    "new_mca.py"),
        "cred"     : os.path.join(root, "CRED",   "cred.py"),
        "vali"     : os.path.join(root, "BRONZE", "Vali.py"),
        "metadata" : os.path.join(root, "BRONZE", "metadata.py"),
        "pipeline" : os.path.join(root, "BRONZE", "pipeline.py"),
        "schema"   : os.path.join(root, "BRONZE", "schema.py"),
        "table"    : os.path.join(root, "BRONZE", "Table.py"),
        "final_gen": os.path.join(root, "BRONZE", "final_gen.py"),
        # ── Silver agents ──────────────────────────────────────────────────
        "silver_cred"       : os.path.join(root, "SILVER", "silver_agent", "silver_credentials.py"),
        "silver_vali"       : os.path.join(root, "SILVER", "silver_agent", "silver_validation.py"),
        "silver_metadata"   : os.path.join(root, "SILVER", "silver_agent", "silver_metadata.py"),
        "silver_classifier" : os.path.join(root, "SILVER", "silver_agent", "silver_classifier.py"),
        "silver_mapping"    : os.path.join(root, "SILVER", "silver_agent", "silver_mapping.py"),
        "silver_transform"  : os.path.join(root, "SILVER", "silver_agent", "silver_transform.py"),
        "silver_schema"     : os.path.join(root, "SILVER", "silver_agent", "silver_schema.py"),
        "silver_table"      : os.path.join(root, "SILVER", "silver_agent", "silver_table.py"),
        "silver_final_gen"  : os.path.join(root, "SILVER", "silver_agent", "silver_final_gen.py"),
        # ── Gold agents ────────────────────────────────────────────────────
        "gold_credentials"   : os.path.join(root, "GOLD", "gold_layer",        "gold_credentials.py"),
        "gold_metadata_agent": os.path.join(root, "GOLD", "gold_layer",        "gold_metadata.py"),
        "gold_data_model"    : os.path.join(root, "GOLD", "gold_layer",        "data_model.py"),
        # "gold_bk_agent"      : os.path.join(root, "GOLD", "business_key_flow", "bk_agent.py"),
        "gold_kpi_agent"     : os.path.join(root, "GOLD", "gold_layer",        "kpi_agent.py"),
        "gold_agg_agent"     : os.path.join(root, "GOLD", "gold_layer",        "agg_agent.py"),
        # "gold_pk_null_agent": os.path.join(root, "GOLD", "pk_null_flow",       "pk_null_agent.py"),
        "gold_enrichment_agent": os.path.join(root, "GOLD", "gold_layer",       "enriched_agent.py"),

    }


# =============================================================================
# INDIVIDUAL STEP RUNNERS  (Bronze)
# =============================================================================

def _step_cred(agents, plan, output_dir, artefacts, label="") -> str:
    tag = f" [{label}]" if label else ""
    print(f"\n▶  STEP 2{tag} — Credentials Collection  [CRED/cred.py]")
    print("-" * 62)
    mod        = _load("agent_cred", agents["cred"])
    agent      = mod.CredentialsCollectionAgent()
    _, path    = agent.execute(source=plan, output_dir=output_dir)
    key        = f"credentials_{label}" if label else "credentials"
    artefacts[key] = path
    return path


def _step_vali(agents, cred_path, output_dir, artefacts, label="") -> str:
    tag = f" [{label}]" if label else ""
    print(f"\n▶  STEP 3{tag} — Connection Validation  [BRONZE/Vali.py]")
    print("-" * 62)
    mod = _load("agent_vali", agents["vali"])
    try:
        _, cred_path = mod.run_validation(cred_path, output_dir=output_dir)
    except RuntimeError as e:
        print(f"\n  ⛔ {e}")
        print("  Fix credentials and restart.\n")
        raise
    except SystemExit:
        raise
    key = f"validation_{label}" if label else "validation"
    artefacts[key] = os.path.join(output_dir, "validation_results.json")
    return cred_path


def _step_metadata(agents, cred_path, output_dir, artefacts, label="") -> str:
    tag = f" [{label}]" if label else ""
    print(f"\n▶  STEP 4{tag} — Metadata Extraction  [BRONZE/metadata.py]")
    print("-" * 62)
    mod     = _load("agent_metadata", agents["metadata"])
    _, path = mod.run_metadata(cred_path, output_dir=output_dir)
    key     = f"metadata_{label}" if label else "metadata"
    artefacts[key] = path
    return path


def _step_pipeline(agents, cred_path, metadata_path, output_dir, artefacts, label="") -> str:
    tag = f" [{label}]" if label else ""
    print(f"\n▶  STEP 5{tag} — Pipeline Strategy  [BRONZE/pipeline.py]")
    print("-" * 62)
    mod     = _load("agent_pipeline", agents["pipeline"])
    _, path = mod.run_pipeline_strategy(
        mca_path      = cred_path,
        metadata_path = metadata_path,
        output_dir    = output_dir,
    )
    key = f"strategy_{label}" if label else "strategy"
    artefacts[key] = path
    return path


def _step_schema(agents, strategy_path, metadata_path, output_dir, artefacts, label="") -> str:
    tag = f" [{label}]" if label else ""
    print(f"\n▶  STEP 6{tag} — Schema Mapping  [BRONZE/schema.py]")
    print("-" * 62)
    mod     = _load("agent_schema", agents["schema"])
    _, path = mod.run_schema_mapping(
        strategy_path = strategy_path,
        metadata_path = metadata_path,
        output_dir    = output_dir,
    )
    key = f"schema_{label}" if label else "schema"
    artefacts[key] = path
    return path


def _step_table(agents, schema_path, db_config, output_dir, artefacts, label="") -> str:
    tag = f" [{label}]" if label else ""
    print(f"\n▶  STEP 7{tag} — Table Creation  [BRONZE/Table.py]")
    print("-" * 62)
    mod     = _load("agent_table", agents["table"])
    _, path = mod.run_table_creation(
        schema_path   = schema_path,
        workspace_url = db_config["databricks_workspace_url"],
        warehouse_id  = db_config["databricks_warehouse_id"],
        token         = db_config["databricks_token"],
        catalog       = db_config["catalog"],
        schema_name   = db_config["schema"],
        output_dir    = output_dir,
    )
    key = f"table_creation_{label}" if label else "table_creation"
    artefacts[key] = path
    return path


def _step_final_gen(agents, cred_path, table_path, strategy_path, schema_path, output_dir, artefacts, label="") -> str:
    tag = f" [{label}]" if label else ""
    print(f"\n▶  STEP 8{tag} — Migration Plan  [BRONZE/final_gen.py]")
    print("-" * 62)
    mod     = _load("agent_final_gen", agents["final_gen"])
    _, path = mod.run_migration_plan(
        credentials_path    = cred_path,
        table_creation_path = table_path,
        strategy_path       = strategy_path,
        output_dir          = output_dir,
        schema_path         = schema_path,
    )
    key = f"migration_plan_{label}" if label else "migration_plan"
    artefacts[key] = path
    return path


# =============================================================================
# SHARED BRONZE RUNNER
# =============================================================================

def _run_bronze_steps(agents, plan, output_dir, config_path, artefacts, label="bronze"):
    cred_path     = _step_cred(agents, plan, output_dir, artefacts, label)
    cred_path     = _step_vali(agents, cred_path, output_dir, artefacts, label)
    metadata_path = _step_metadata(agents, cred_path, output_dir, artefacts, label)
    strategy_path = _step_pipeline(agents, cred_path, metadata_path, output_dir, artefacts, label)
    schema_path   = _step_schema(agents, strategy_path, metadata_path, output_dir, artefacts, label)
    db_config     = load_session_config(config_path)
    table_path    = _step_table(agents, schema_path, db_config, output_dir, artefacts, label)
    _step_final_gen(agents, cred_path, table_path, strategy_path, schema_path, output_dir, artefacts, label)
    return cred_path, table_path, strategy_path, schema_path


# =============================================================================
# SHARED SILVER RUNNER
# =============================================================================

def _run_silver_steps(agents, plan, output_dir, config_path, artefacts, label="silver", reuse_creds=False):
    session_config_path = config_path if reuse_creds else None

    print(f"\n▶  STEP S2 [{label}] — Silver Credentials  [SILVER/silver_agent/silver_credentials.py]")
    print("-" * 62)
    mod = _load("silver_cred", agents["silver_cred"])
    _, cred_path = mod.SilverCredentialsAgent().execute(
        silver_plan         = plan,
        session_config_path = session_config_path,
        output_dir          = output_dir,
    )
    artefacts[f"credentials_{label}"] = cred_path

    print(f"\n▶  STEP S3 [{label}] — Silver Validation  [SILVER/silver_agent/silver_validation.py]")
    print("-" * 62)
    mod = _load("silver_vali", agents["silver_vali"])
    try:
        _, cred_path = mod.run_silver_validation(cred_path, output_dir=output_dir)
    except RuntimeError as e:
        print(f"\n  ⛔ {e}")
        raise
    except SystemExit:
        raise
    artefacts[f"validation_{label}"] = os.path.join(output_dir, "silver_validation_results.json")

    print(f"\n▶  STEP S4 [{label}] — Silver Metadata  [SILVER/silver_agent/silver_metadata.py]")
    print("-" * 62)
    mod = _load("silver_metadata", agents["silver_metadata"])
    _, metadata_path = mod.run_silver_metadata(cred_path, output_dir=output_dir)
    artefacts[f"metadata_{label}"] = metadata_path

    print(f"\n▶  STEP S4b [{label}] — Column Classifier  [SILVER/silver_agent/silver_classifier.py]")
    print("-" * 62)
    mod = _load("silver_classifier", agents["silver_classifier"])
    _, classifier_path = mod.run_silver_classifier(metadata_path=metadata_path, output_dir=output_dir)
    artefacts[f"classifier_{label}"] = classifier_path

    print(f"\n▶  STEP S4c [{label}] — Mapping Agent  [SILVER/silver_agent/silver_mapping.py]")
    print("-" * 62)
    mod = _load("silver_mapping", agents["silver_mapping"])
    _, mapping_path = mod.run_silver_mapping(
        classifier_path  = classifier_path,
        credentials_path = cred_path,
        output_dir       = output_dir,
    )
    artefacts[f"mapping_{label}"] = mapping_path

    print(f"\n▶  STEP S5 [{label}] — Silver Transform  [SILVER/silver_agent/silver_transform.py]")
    print("-" * 62)
    mod = _load("silver_transform", agents["silver_transform"])
    _, transform_path = mod.run_silver_transform(metadata_path=metadata_path, output_dir=output_dir)
    artefacts[f"transform_{label}"] = transform_path

    print(f"\n▶  STEP S6 [{label}] — Silver Schema Mapping  [SILVER/silver_agent/silver_schema.py]")
    print("-" * 62)
    mod = _load("silver_schema", agents["silver_schema"])
    _, schema_path = mod.run_silver_schema(
        transform_plan_path = transform_path,
        metadata_path       = metadata_path,
        output_dir          = output_dir,
    )
    artefacts[f"schema_{label}"] = schema_path

    print(f"\n▶  STEP S7 [{label}] — Silver Table Creation  [SILVER/silver_agent/silver_table.py]")
    print("-" * 62)
    mod = _load("silver_table", agents["silver_table"])
    _, table_path = mod.run_silver_table_creation(
        schema_path             = schema_path,
        silver_credentials_path = artefacts.get(f"credentials_{label}"),
        session_config_path     = session_config_path,
        output_dir              = output_dir,
    )
    artefacts[f"table_creation_{label}"] = table_path

    print(f"\n▶  STEP S8 [{label}] — Silver Migration Plan  [SILVER/silver_agent/silver_final_gen.py]")
    print("-" * 62)
    mod = _load("silver_final_gen", agents["silver_final_gen"])
    _, migration_path = mod.run_silver_final_gen(
        credentials_path    = cred_path,
        table_creation_path = table_path,
        transform_plan_path = transform_path,
        schema_path         = schema_path,
        output_dir          = output_dir,
    )
    artefacts[f"migration_plan_{label}"] = migration_path

    return cred_path, table_path, transform_path, schema_path


# =============================================================================
# CREDENTIALS REUSE HELPER
# =============================================================================

def _ask_reuse_credentials(stored_creds: dict) -> dict:
    workspace  = stored_creds.get("workspace_url",  "N/A")
    warehouse  = stored_creds.get("warehouse_id",   "N/A")
    token_raw  = stored_creds.get("token", "")
    masked     = (token_raw[:6] + "****") if len(token_raw) > 6 else "****"
    sv_catalog = stored_creds.get("silver_catalog", "N/A")
    sv_schema  = stored_creds.get("silver_schema",  "N/A")

    print("\n" + "=" * 62)
    print("  GOLD PHASE — CREDENTIAL SELECTION")
    print("=" * 62)
    print("\n  Silver phase used the following Databricks credentials:")
    print(f"    Workspace URL    : {workspace}")
    print(f"    SQL Warehouse ID : {warehouse}")
    print(f"    Access Token     : {masked}")
    print(f"    Silver catalog   : {sv_catalog}")
    print(f"    Silver schema    : {sv_schema}")
    print()

    while True:
        choice = input(
            "  Reuse these credentials for the Gold phase?\n"
            "  Y = Yes, reuse   |   N = Enter new credentials\n"
            "  > "
        ).strip().upper()
        if choice in ("Y", "YES"):
            model_endpoint = os.getenv("MODEL_ENDPOINT")
            if not model_endpoint:
                raise Exception("MODEL_ENDPOINT not found in .env file")
            stored_creds["model_endpoint"] = model_endpoint
            print("\n  ✅ Credentials loaded from .env. Proceeding to Gold phase.\n")
            return stored_creds
        if choice in ("N", "NO"):
            print("\n  Collecting new Gold credentials...\n")
            return None
        print("  ⚠  Please enter Y or N.")


# =============================================================================
# TABLE / VIEW PROMPT HELPER
# =============================================================================

def _ask_table_or_view() -> str:
    print("\n" + "=" * 62)
    print("  KPI AGGREGATES — DEPLOYMENT TYPE")
    print("=" * 62)
    print()
    print("  How should the KPI aggregate results be deployed?")
    print("  1  →  Tables  (materialized, faster queries)")
    print("  2  →  Views   (always reflects latest data)")
    print()
    while True:
        choice = input("  Your choice (1 / 2): ").strip()
        if choice == "1":
            print("\n  ✅ Deploying KPI aggregates as TABLES.\n")
            return "table"
        if choice == "2":
            print("\n  ✅ Deploying KPI aggregates as VIEWS.\n")
            return "view"
        print("  ⚠  Please enter 1 or 2.")


# =============================================================================
# GOLD STEP RUNNERS
# =============================================================================

def _gold_collect_credentials(agents, output_dir, artefacts, existing_creds: dict = None) -> dict:
    if existing_creds is not None:
        creds = {
            "hostname"        : existing_creds.get("workspace_url", "").replace("https://", "").replace("http://", "").rstrip("/"),
            "sql_warehouse_id": f"/sql/1.0/warehouses/{existing_creds.get('warehouse_id', '')}",
            "access_token"    : existing_creds.get("token",          ""),
            "model_endpoint"  : existing_creds.get("model_endpoint", ""),
            "silver_catalog"  : existing_creds.get("silver_catalog", ""),
            "silver_schema"   : existing_creds.get("silver_schema",  ""),
            "bronze_catalog"  : existing_creds.get("bronze_catalog", ""),
            "bronze_schema"   : existing_creds.get("bronze_schema",  ""),
        }
        artefacts["gold_credentials"] = creds
        return creds

    print(f"\n▶  GOLD STEP 1 — Gold Credentials  [GOLD/gold_layer/gold_credentials.py]")
    print("-" * 62)
    mod   = _load("gold_credentials", agents["gold_credentials"])
    creds = mod.collect_and_init_credentials()
    artefacts["gold_credentials"] = creds
    return creds


def _gold_fetch_silver_metadata(agents, creds: dict, silver_catalog: str, silver_schema: str, output_dir: str, artefacts: dict, silver_table: str | None = None, filename: str = "gold_silver_metadata.json",) -> dict:
    print(f"\n▶  GOLD STEP 2 — Metadata Extraction  [GOLD/gold_layer/gold_metadata.py]")
    print("-" * 62)
    mod             = _load("gold_metadata_agent", agents["gold_metadata_agent"])
    silver_metadata = mod.fetch_metadata_grouped(
        silver_catalog,
        silver_schema,
        hostname         = creds["hostname"],
        sql_warehouse_id = creds["sql_warehouse_id"],
        access_token     = creds["access_token"],
        model_endpoint   = creds["model_endpoint"],
        silver_table     = silver_table,
    )
    meta_path = os.path.join(output_dir, filename)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(silver_metadata, f, indent=2, default=str)
    artefacts[filename.replace(".json", "")] = meta_path
    return silver_metadata


def _ask_gold_location(prompt_label: str, default: str) -> str:
    val = input(f"  {prompt_label} [default: {default}]: ").strip()
    return val if val else default

def _ask_gold_table() -> str | None:
    """
    Prompt for an optional Silver table name.
    Returns the table name string, or None if the user pressed Enter (= all tables).
    """
    val = input("  Silver table   [press Enter for ALL tables]: ").strip()
    return val if val else None

# =============================================================================
# APPLY BK RESULTS TO MODEL
# =============================================================================

def _apply_bk_results_to_model(model: dict, bk_result: dict) -> dict:
    """
    After BK generation:
    1. Replace primary_key_temp columns with real _id columns (role: primary_key)
    2. Demote business_key columns back to descriptive
    3. Wire the new _id as a foreign key in the fact table
    """
    import copy
    model = copy.deepcopy(model)

    # Build map: entity → generated bk_column_name
    generated_bks: dict[str, str] = {}
    for table_result in bk_result.get("results", []):
        for col_result in table_result.get("columns", []):
            if col_result.get("status") == "generated":
                entity = col_result.get("entity", "")
                bk_col = col_result.get("bk_column_name", "")
                if entity and bk_col:
                    generated_bks[entity.lower()] = bk_col

    if not generated_bks:
        return model

    # Track placeholder → real name for fact FK update
    placeholder_to_real: dict[str, str] = {}

    # Update dimensions
    for dim in model.get("dimensions", []):
        dim_name    = dim.get("name", "")
        entity_name = dim_name.replace("dim_", "").lower()

        if entity_name not in generated_bks:
            continue

        real_bk_col = generated_bks[entity_name]
        new_columns = []

        for col in dim.get("columns", []):
            if not isinstance(col, dict):
                new_columns.append(col)
                continue
            if col.get("role") == "primary_key_temp":
                placeholder_to_real[col["column"]] = real_bk_col
                new_columns.append({
                    "column": real_bk_col,
                    "type":   "STRING",
                    "role":   "primary_key",
                })
            elif col.get("role") == "business_key":
                new_columns.append({**col, "role": "descriptive"})
            else:
                new_columns.append(col)

        dim["columns"] = new_columns

    # Update fact table FKs
    fact_columns  = model.get("fact_table", {}).get("columns", [])
    new_fact_cols = []

    for col in fact_columns:
        if not isinstance(col, dict):
            new_fact_cols.append(col)
            continue
        col_name = col.get("column", "")
        role     = col.get("role", "")
        if role == "foreign_key" and col_name in placeholder_to_real:
            new_fact_cols.append({
                "column": placeholder_to_real[col_name],
                "type":   "STRING",
                "role":   "foreign_key",
            })
        else:
            new_fact_cols.append(col)

    # Add any generated BK as FK if not already present
    for entity, real_bk_col in generated_bks.items():
        already_present = any(
            c.get("column") == real_bk_col
            for c in new_fact_cols
            if isinstance(c, dict)
        )
        if not already_present:
            new_fact_cols.append({
                "column": real_bk_col,
                "type":   "STRING",
                "role":   "foreign_key",
            })

    model["fact_table"]["columns"] = new_fact_cols
    return model

# =============================================================================
# STAR SCHEMA DISPLAY HELPER
# =============================================================================

def _display_star_schema(mdl: dict) -> None:
    fact = mdl.get("fact_table", {})
    dims = mdl.get("dimensions", [])

    col_w  = 32
    type_w = 14
    role_w = 22
    header  = f"  {'Column':<{col_w}} {'Type':<{type_w}} {'Role'}"
    divider = f"  {'-'*col_w} {'-'*type_w} {'-'*role_w}"

    ROLE_TAGS = {
        "primary_key"      : "🔑 primary_key",
        "primary_key_temp" : "⏳ primary_key_temp",
        "business_key"     : "🏷  business_key",
        "foreign_key"      : "🔗 foreign_key",
        "measure"          : "📊 measure",
        "descriptive"      : "   descriptive",
    }

    print(f"  {'─' * 60}")
    print(f"  FACT TABLE : {fact.get('name', '(unnamed)').upper()}")
    print(f"  {'─' * 60}")
    print(header)
    print(divider)
    for col in fact.get("columns", []):
        if not isinstance(col, dict):
            continue
        role_tag = ROLE_TAGS.get(col.get("role", ""), f"   {col.get('role','')}")
        print(f"  {col.get('column',''):<{col_w}} {col.get('type',''):<{type_w}} {role_tag}")

    print(f"\n  {'─' * 60}")
    print(f"  DIMENSIONS  ({len(dims)} table(s))")
    print(f"  {'─' * 60}")
    for dim in dims:
        print(f"\n  ▸ {dim.get('name', '(unnamed)').upper()}")
        print(header)
        print(divider)
        for col in dim.get("columns", []):
            if not isinstance(col, dict):
                continue
            role_tag = ROLE_TAGS.get(col.get("role", ""), f"   {col.get('role','')}")
            print(f"  {col.get('column',''):<{col_w}} {col.get('type',''):<{type_w}} {role_tag}")

    print(f"\n  {'─' * 60}")


# =============================================================================
# NLP MODEL EDITOR  (DM-3 Edit Mode)
# =============================================================================

def _nlp_edit_model(model: dict, creds: dict) -> dict:
    """
    Opens an interactive NLP chatbot loop allowing the user to modify the
    star schema model using plain English instructions.

    The LLM receives the current model as JSON context and the user's
    instruction, then returns a patched model JSON.  The loop continues
    until the user types 'done' / 'continue' / presses Enter with no input.

    Returns the (potentially modified) model dict.
    """
    import copy
    import re

    SYSTEM_PROMPT = """You are a data modelling assistant specialising in Databricks star schemas.
The user will give you a current star-schema model as JSON and a natural-language instruction.
You MUST return ONLY the updated model as a single valid JSON object — no markdown fences,
no explanation, no extra keys.  Keep every field that was not mentioned in the instruction unchanged.

The model has this top-level structure:
{
  "fact_table": {
    "name": "<string>",
    "columns": [ {"column": "<name>", "type": "<SPARK_TYPE>", "role": "<role>"}, ... ]
  },
  "dimensions": [
    {
      "name": "<string>",
      "columns": [ {"column": "<name>", "type": "<SPARK_TYPE>", "role": "<role>"}, ... ]
    },
    ...
  ]
}

Valid roles: primary_key, primary_key_temp, business_key, foreign_key, measure, descriptive.
Valid Spark types: STRING, INT, BIGINT, DOUBLE, DECIMAL(p,s), DATE, TIMESTAMP, BOOLEAN.
"""

    model_endpoint = creds.get("model_endpoint", "")
    hostname       = creds.get("hostname", "")
    access_token   = creds.get("access_token", "")

    def _call_llm(current_model: dict, instruction: str) -> dict:
        payload = {
            "messages": [
                {
                    "role": "system",           # ← ADD THIS
                    "content": SYSTEM_PROMPT,   # ← ADD THIS
                },
                {
                    "role": "user",
                    "content": (
                        f"{SYSTEM_PROMPT}\n\n"
                        f"Current model:\n{json.dumps(current_model, indent=2)}\n\n"
                        f"Instruction: {instruction}"
                    ),
                }
            ],
            "max_tokens": 4096,
        }

        import urllib.request, urllib.error

        # ── Databricks serving endpoint ────────────────────────────────────
        url     = f"https://{hostname}/serving-endpoints/{model_endpoint}/invocations"
        req     = urllib.request.Request(
            url,
            data    = json.dumps(payload).encode(),
            headers = {
                "Content-Type" : "application/json",
                "Authorization": f"Bearer {access_token}",
            },
            method  = "POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            raise RuntimeError(f"LLM HTTP {e.code}: {body}") from e

        # Extract text from the response (handles both chat and completion styles)
        text = ""
        choices = raw.get("choices", [])
        if choices:
            msg = choices[0].get("message", {})
            text = msg.get("content", "") or choices[0].get("text", "")
        if not text:
            text = raw.get("content", "") or str(raw)

        # Strip markdown fences if present
        text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.IGNORECASE)
        text = re.sub(r"\s*```$",          "", text.strip())

        return json.loads(text)

    print("\n" + "=" * 62)
    print("  DATA MODEL EDITOR  —  NLP Chat Mode")
    print("=" * 62)
    print("  Describe your changes in plain English.")
    print("  Examples:")
    print('    "Add a column called discount_amount DOUBLE measure to the fact table"')
    print('    "Rename dim_customer to dim_client"')
    print('    "Remove the column notes from dim_product"')
    print('    "Add a new dimension dim_region with columns region_id INT primary_key')
    print('     and region_name STRING descriptive"')
    print()
    print("  Type  done / continue  (or press Enter)  to finish editing.")
    print("-" * 62)

    current_model = copy.deepcopy(model)
    edit_count    = 0

    while True:
        try:
            instruction = input(f"\n  Edit #{edit_count + 1} → ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  (Editor interrupted — keeping last saved model.)")
            break

        # Exit commands
        if instruction.lower() in ("", "done", "continue", "exit", "quit"):
            if edit_count == 0:
                print("  No changes made.")
            else:
                print(f"  Exiting editor — {edit_count} edit(s) applied.")
            break

        print("  ⏳ Applying edit...")
        try:
            updated = _call_llm(current_model, instruction)
        except json.JSONDecodeError as e:
            print(f"  ⚠  LLM returned invalid JSON — edit not applied. ({e})")
            print(f"  Raw response (first 300 chars): {text[:300]!r}")
            print("  Try rephrasing your instruction.")
            continue
        except Exception as e:
            print(f"  ⚠  Edit failed: {e}")
            print("  Try rephrasing your instruction.")
            continue

        # Validate minimal structure
        if "fact_table" not in updated or "dimensions" not in updated:
            print("  ⚠  LLM response is missing required keys — edit not applied.")
            print("  Try rephrasing your instruction.")
            continue

        current_model = updated
        edit_count   += 1

        print("\n  ✅  Edit applied. Updated model:")
        _display_star_schema(current_model)

        try:
            more = input("\n  Make another change? (yes / no  or just press Enter to finish): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break

        if more not in ("yes", "y"):
            print(f"  Exiting editor — {edit_count} edit(s) applied.")
            break

    return current_model


# =============================================================================
# DM-3  APPROVAL GATE  (approve / edit / skip)
# =============================================================================

def _dm3_approval_gate(model: dict, creds: dict) -> dict | None:
    """
    Presents the star schema and asks the user:
      A — Approve  → returns the (possibly edited) model unchanged
      E — Edit     → opens NLP chat editor, then re-runs this gate
      S — Skip     → returns None  (pipeline aborted by caller)

    Returns:
      dict  — the approved model to proceed with
      None  — user chose Skip; caller must abort the pipeline
    """
    while True:
        print("\n" + "=" * 62)
        print("  GOLD DM-3 — STAR SCHEMA REVIEW & APPROVAL")
        print("=" * 62)
        print()
        print("  Review the suggested star schema above, then choose:")
        print()
        print("    A — Approve    (proceed to Business Key detection)")
        print("    E — Edit       (modify the model using natural language)")
        print("    S — Skip       (abort the pipeline — nothing will be deployed)")
        print()

        choice = input("  Your choice (A / E / S): ").strip().upper()

        if choice == "A":
            print("\n  ✅  Model approved. Proceeding to Business Key detection...\n")
            return model

        if choice == "S":
            print("\n  ⛔  Pipeline skipped by user — no Gold tables will be deployed.\n")
            return None

        if choice == "E":
            # Open NLP editor; returns the (modified) model
            edited_model = _nlp_edit_model(model, creds)

            # Show the updated model and re-run approval
            print("\n" + "=" * 62)
            print("  UPDATED STAR SCHEMA MODEL")
            print("=" * 62)
            _display_star_schema(edited_model)

            # Re-ask approval for the edited model
            print("\n  Your edited model is shown above.")
            print("  Options:")
            print("    A — Approve and proceed to Business Key detection")
            print("    E — Make more changes")
            print("    S — Skip / abort the pipeline")
            print()

            inner_choice = input("  Your choice (A / E / S): ").strip().upper()

            if inner_choice == "A":
                print("\n  ✅  Edited model approved. Proceeding...\n")
                return edited_model

            if inner_choice == "S":
                print("\n  ⛔  Pipeline skipped by user — no Gold tables will be deployed.\n")
                return None

            if inner_choice == "E":
                # Continue editing — update model and loop back to top
                model = edited_model
                continue

            # Unrecognised → loop back and re-display
            print("  ⚠  Please enter A, E, or S.")
            model = edited_model
            continue

        print("  ⚠  Please enter A, E, or S.")


def _ask_enrichment_location(silver_catalog: str, silver_schema: str, creds: dict) -> tuple:
    """
    Prompt the user for the enriched-output catalog and schema.
    • If the catalog already exists in Databricks  → reused as-is.
    • If not                                        → created automatically.
    Same logic for the schema.

    Returns (target_catalog, target_schema).
    """
    from databricks import sql as _dbsql

    default_catalog = silver_catalog
    default_schema  = f"{silver_schema}_enriched"

    print("\n" + "=" * 62)
    print("  ENRICHMENT — OUTPUT LOCATION")
    print("=" * 62)
    print("  Where should the enriched tables be written?")
    print("  (Press Enter to accept the defaults shown in brackets)")
    print()

    entered_catalog = input(f"  Catalog [{default_catalog}]: ").strip()
    target_catalog  = entered_catalog if entered_catalog else default_catalog

    entered_schema  = input(f"  Schema  [{default_schema}]: ").strip()
    target_schema   = entered_schema if entered_schema else default_schema

    # ── Verify / create catalog and schema in Databricks ──────────────────
    print()
    print(f"  Verifying {target_catalog}.{target_schema} on Databricks...")

    try:
        conn = _dbsql.connect(
            server_hostname = creds["hostname"],
            http_path       = creds["sql_warehouse_id"],
            access_token    = creds["access_token"],
        )
        cur = conn.cursor()
        try:
            # Catalog
            cur.execute(f"SHOW CATALOGS LIKE '{target_catalog}'")
            if not cur.fetchall():
                print(f"  Catalog '{target_catalog}' not found — creating...")
                cur.execute(f"CREATE CATALOG IF NOT EXISTS `{target_catalog}`")
                print(f"  ✅ Catalog '{target_catalog}' created.")
            else:
                print(f"  ✅ Catalog '{target_catalog}' already exists — reusing.")

            # Schema
            cur.execute(f"SHOW SCHEMAS IN `{target_catalog}` LIKE '{target_schema}'")
            if not cur.fetchall():
                print(f"  Schema '{target_schema}' not found — creating...")
                cur.execute(
                    f"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{target_schema}`"
                )
                print(f"  ✅ Schema '{target_catalog}'.'{target_schema}' created.")
            else:
                print(f"  ✅ Schema '{target_schema}' already exists — reusing.")

        finally:
            cur.close()
            conn.close()

    except Exception as e:
        print(f"  ⚠  Could not verify/create location on Databricks: {e}")
        print("     Proceeding anyway — enrichment will attempt to write there.")

    print(f"\n  Enriched tables → {target_catalog}.{target_schema}")
    return target_catalog, target_schema

# =============================================================================
# GOLD DATA MODELING  (DM-1 → DM-5)
# =============================================================================

def _gold_run_data_modeling(
    agents, silver_metadata: dict, creds: dict,
    gold_catalog: str, gold_schema: str,
    silver_catalog: str, silver_schema: str,
    output_dir: str, artefacts: dict,
) -> None:

    print("\n" + "=" * 62)
    print("  GOLD PHASE — DATA MODELING")
    print("=" * 62)

    dm_mod = _load("gold_data_model", agents["gold_data_model"])

    dm_mod.init_data_model(
        hostname         = creds["hostname"],
        sql_warehouse_id = creds["sql_warehouse_id"],
        access_token     = creds["access_token"],
        model_endpoint   = creds["model_endpoint"],
    )

    # ── DM-1 ──────────────────────────────────────────────────────────────────
    print("\n▶  GOLD DM-1 — Schema Understanding")
    print("-" * 62)
    try:
        schema_output = dm_mod.data_understanding_agent(silver_metadata)
    except Exception as e:
        import traceback
        print(f"\n  ❌ DM-1 failed: {e}")
        traceback.print_exc()
        return

    # ── DM-2 ──────────────────────────────────────────────────────────────────
    print("\n▶  GOLD DM-2 — Star Schema Data Modeling")
    print("-" * 62)
    raw_model = dm_mod.data_modeling_agent(schema_output)
    if raw_model is None:
        print("  ⛔  Data modeling failed — cannot proceed.")
        return

    all_silver_cols = [
        col["column"]
        for table_meta in silver_metadata.values()
        for col in table_meta.get("columns", [])
    ]
    model = dm_mod.normalize_model(raw_model, silver_col_names=all_silver_cols)

    existing_dim_names = [d.get("name", "") for d in model.get("dimensions", [])]

    if "dim_date" not in existing_dim_names:
        print("  [PATCH] dim_date missing from model — injecting standard date dimension.")
        model.setdefault("dimensions", []).append({
            "name": "dim_date",
            "columns": [
                {"column": "date_id",    "type": "INT",     "role": "primary_key"},
                {"column": "year",       "type": "INT",     "role": "descriptive"},
                {"column": "month",      "type": "INT",     "role": "descriptive"},
                {"column": "month_name", "type": "STRING",  "role": "descriptive"},
                {"column": "quarter",    "type": "INT",     "role": "descriptive"},
                {"column": "week",       "type": "INT",     "role": "descriptive"},
                {"column": "day",        "type": "INT",     "role": "descriptive"},
                {"column": "day_name",   "type": "STRING",  "role": "descriptive"},
                {"column": "is_weekend", "type": "BOOLEAN", "role": "descriptive"},
            ]
        })

    fact_cols         = model.get("fact_table", {}).get("columns", [])
    existing_fk_names = {c["column"] for c in fact_cols}

    if "date_id" not in existing_fk_names:
        print("  [PATCH] date_id FK missing from fact table — injecting.")
        last_fk_idx = next(
            (i for i, c in reversed(list(enumerate(fact_cols)))
             if c.get("role") == "foreign_key"),
            0
        )
        fact_cols.insert(last_fk_idx + 1, {
            "column": "date_id",
            "type"  : "INT",
            "role"  : "foreign_key",
        })

    # ── Display the suggested star schema ─────────────────────────────────────
    print("\n" + "=" * 62)
    print("  SUGGESTED STAR SCHEMA MODEL")
    print("=" * 62)
    _display_star_schema(model)

    # ── DM-3  Approval Gate (A / E / S) ───────────────────────────────────────
    print("\n▶  GOLD DM-3 — Star Schema Review & Approval")
    print("-" * 62)

    approved_model = _dm3_approval_gate(model, creds)

    # S — user chose Skip → abort the entire data-modeling pipeline
    if approved_model is None:
        return

    # ── ▶ GOLD DM-4 — Enrichment: Business Keys + PK NULL Fill ───────────────
    print("\n▶  GOLD DM-4 — Enrichment: Business Keys + PK NULL Fill  [GOLD/enriched_agent.py]")
    print("-" * 62)

    enrichment_result        = {}
    effective_silver_catalog = silver_catalog
    effective_silver_schema  = silver_schema

    try:
        enrich_mod = _load("gold_enrichment_agent", agents["gold_enrichment_agent"])

        # ── Identify dimensions that still need a surrogate BK ─────────────────
        dims_needing_bk = [
            dim for dim in approved_model.get("dimensions", [])
            if any(
                isinstance(col, dict) and col.get("role") == "primary_key_temp"
                for col in dim.get("columns", [])
            )
        ]

        if dims_needing_bk:
            print(f"\n  {len(dims_needing_bk)} dimension(s) need business key generation:")
            for d in dims_needing_bk:
                print(f"    → {d['name']}")
            print("\n  ℹ  Running unified enrichment (BK generation + PK NULL fill).")
        else:
            print("\n  ✅  All dimensions already have proper business keys.")
            print("       Running PK NULL scan only.")

        print(f"     Silver source : {silver_catalog}.{silver_schema}")

        # ── Ask user where enriched tables should land ─────────────────────────
        enrich_target_catalog, enrich_target_schema = _ask_enrichment_location(
            silver_catalog = silver_catalog,
            silver_schema  = silver_schema,
            creds          = creds,
        )

        # ── Build the AgentConfig and run the enrichment pipeline ──────────────
        print("\n  🚀  Starting EnrichedAgent pipeline...")

        agent_config = enrich_mod.AgentConfig(
            hostname         = creds["hostname"],
            sql_warehouse_id = creds["sql_warehouse_id"],
            access_token     = creds["access_token"],
            model_endpoint   = creds.get("model_endpoint", ""),
            source_catalog   = silver_catalog,
            source_schema    = silver_schema,
            target_catalog   = enrich_target_catalog,
            target_schema    = enrich_target_schema,
            dropped_rows_dir = os.path.join(output_dir, "dropped_null_audit"),
        )

        # root_dir is this file's parent's parent (project root)
        enriched_agent = enrich_mod.EnrichedAgent(
            config   = agent_config,
            root_dir = os.path.dirname(os.path.abspath(__file__)),
        )

        enrichment_result = enriched_agent.run_pipeline_with_context(
            tables_metadata = silver_metadata,
            flagged_dims    = dims_needing_bk,
        )

        # ── Update effective Silver location for DM-5 deploy ───────────────────
        if enrichment_result.get("enriched_tables"):
            effective_silver_catalog = enrichment_result.get("target_catalog", silver_catalog)
            effective_silver_schema  = enrichment_result.get("target_schema",  silver_schema)

            bk_compat      = {"results": enrichment_result.get("generation_results", [])}
            approved_model = _apply_bk_results_to_model(approved_model, bk_compat)

            print(
                f"\n  ✅  Enrichment complete — "
                f"{len(enrichment_result['enriched_tables'])} table(s) written.\n"
                f"      DM-5 will deploy from: "
                f"{effective_silver_catalog}.{effective_silver_schema}"
            )
        else:
            print(
                "\n  ⚠  Enrichment produced no output tables.\n"
                "      DM-5 will deploy from the original Silver source."
            )

        artefacts["gold_enrichment_result"] = enrichment_result

    except Exception as outer_e:
        import traceback
        print(f"\n  ❌  Exception in enrichment pipeline: {outer_e}")
        traceback.print_exc()
        enrichment_result = {}

    # ── Promote any remaining primary_key_temp → primary_key ─────────────────
    for dim in approved_model.get("dimensions", []):
        for col in dim.get("columns", []):
            if isinstance(col, dict) and col.get("role") == "primary_key_temp":
                col["role"] = "primary_key"

    # Ensure every promoted PK column also has a matching FK in the fact table
    fact_cols         = approved_model.get("fact_table", {}).get("columns", [])
    existing_fk_names = {c["column"] for c in fact_cols if isinstance(c, dict)}

    for dim in approved_model.get("dimensions", []):
        for col in dim.get("columns", []):
            if not isinstance(col, dict):
                continue
            if col.get("role") == "primary_key" and col["column"] not in existing_fk_names:
                fact_cols.append({
                    "column": col["column"],
                    "type"  : col.get("type", "STRING"),
                    "role"  : "foreign_key",
                })

    # ── DM-3b  Post-Enrichment Review & Approval ───────────────────────────────
    print("\n▶  GOLD DM-3b — Final Model Review & Deployment Approval")
    print("-" * 62)
    print("\n  Enrichment is complete. Review the final enriched star schema")
    print("  before it is deployed to the Gold layer.\n")

    _display_star_schema(approved_model)

    print("\n  This is the final model that will be deployed to Gold.")
    print("  Options:")
    print("    A — Approve and deploy to Gold layer")
    print("    R — Reject and abort (nothing will be deployed)")
    print()

    while True:
        dm3b_choice = input("  Your choice (A / R): ").strip().upper()
        if dm3b_choice == "R":
            print("\n  ⛔  Deployment rejected by user — aborting. No Gold tables written.")
            return
        if dm3b_choice == "A":
            print("\n  ✅  Model approved. Proceeding to Gold deployment...\n")
            break
        print("  ⚠   Please enter A to approve or R to reject.")

    # ── DM-5  Deploy ───────────────────────────────────────────────────────────
    print("\n▶  GOLD DM-5 — Deploy Gold Tables")
    print("-" * 62)
    print(f"\n  Target Gold catalog : {gold_catalog}")
    print(f"  Target Gold schema  : {gold_schema}")

    rel_map = dm_mod.deploy_to_databricks(
        model          = approved_model,
        catalog        = gold_catalog,
        schema         = gold_schema,
        source_catalog = effective_silver_catalog,
        source_schema  = effective_silver_schema,
    )

    artefacts["gold_relationship_map"] = rel_map
    artefacts["gold_model_catalog"]    = gold_catalog
    artefacts["gold_model_schema"]     = gold_schema

    model_path = os.path.join(output_dir, "plan.json")
    plan_output = {
        **approved_model,
        "gold_catalog"  : gold_catalog,
        "gold_schema"   : gold_schema,
        "silver_catalog": effective_silver_catalog,
        "silver_schema" : effective_silver_schema,
    }
    with open(model_path, "w", encoding="utf-8") as f:
        json.dump(plan_output, f, indent=2, default=str)
    artefacts["gold_data_model"] = model_path

    print(f"\n  ✅  plan.json saved → {os.path.abspath(model_path)}")
    print(f"\n  ✅  Gold data model deployed → {gold_catalog}.{gold_schema}")

    # ── Checkpoint ────────────────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("  GOLD DEPLOYMENT CHECKPOINT")
    print("=" * 62)
    print()
    print("  Before deployment can proceed, complete these steps:")
    print()
    print("  1️⃣   Upload plan.json to your Databricks Volume:")
    print(f"        Local path : {os.path.abspath(model_path)}")
    print(f"        Volume path: /Volumes/{gold_catalog}/{gold_schema}/plan/plan.json")
    print()
    print("  2️⃣   Open gold_execute.py in Databricks and update CONFIG:")
    print(f"        GOLD_MODEL_PATH = '/Volumes/{gold_catalog}/{gold_schema}/plan/plan.json'")
    print(f"        SILVER_CATALOG  = '{effective_silver_catalog}'")
    print(f"        SILVER_SCHEMA   = '{effective_silver_schema}'")
    print(f"        GOLD_CATALOG    = '{gold_catalog}'")
    print(f"        GOLD_SCHEMA     = '{gold_schema}'")
    print()
    print("  3️⃣   Run gold_execute.py as a Databricks notebook")
    print()
    print("  4️⃣   Wait until you see:")
    print("        ✅  GOLD MIGRATION COMPLETE — all tables populated")
    print()
    print("  Once the notebook completes successfully, press Enter to continue.")
    print()

    input("  ▶  Press Enter when Databricks deployment is complete... ")

    # ── Post-deployment: offer KPI ─────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("  GOLD DEPLOYMENT COMPLETE")
    print("=" * 62)
    print("\n  Would you like to proceed to KPI Calculation?")
    print("  KPIs will be generated from the deployed Gold tables.\n")

    kpi_choice = input("  Proceed to KPI? (yes / no): ").strip().lower()

    if kpi_choice in ("yes", "y"):
        print("\n  Extracting metadata from deployed Gold tables...")
        gold_metadata = _gold_fetch_silver_metadata(
            agents, creds,
            silver_catalog = gold_catalog,
            silver_schema  = gold_schema,
            output_dir     = output_dir,
            artefacts      = artefacts,
            filename       = "deployed_gold_metadata.json",
        )

        print("\n  Enter the catalog and schema where KPI aggregates will be saved:")
        kpi_catalog = _ask_gold_location("KPI catalog", gold_catalog + "_kpis")
        kpi_schema  = _ask_gold_location("KPI schema",  "gold_kpis")

        deploy_type = _ask_table_or_view()

        _gold_run_kpi_pipeline(
            agents, gold_metadata, creds,
            kpi_catalog    = kpi_catalog,
            kpi_schema     = kpi_schema,
            silver_catalog = gold_catalog,
            silver_schema  = gold_schema,
            deploy_type    = deploy_type,
            output_dir     = output_dir,
            artefacts      = artefacts,
        )
    else:
        print("\n  Skipping KPI generation. Pipeline complete.")

# =============================================================================
# GOLD KPI PIPELINE
# =============================================================================

def _gold_run_kpi_pipeline(
    agents, silver_metadata: dict, creds: dict,
    kpi_catalog: str, kpi_schema: str,
    silver_catalog: str = "",
    silver_schema: str  = "",
    deploy_type: str    = "",
    output_dir: str     = "",
    artefacts: dict     = None,
) -> None:
    if artefacts is None:
        artefacts = {}

    print("\n" + "=" * 62)
    print("  GOLD PHASE — KPI CALCULATION")
    print("=" * 62)

    kpi_mod = _load("gold_kpi_agent", agents["gold_kpi_agent"])
    agg_mod = _load("gold_agg_agent", agents["gold_agg_agent"])

    kpi_mod.init_kpi_agent(
        hostname       = creds["hostname"],
        access_token   = creds["access_token"],
        model_endpoint = creds["model_endpoint"],
    )
    agg_mod.init_agg_agent(
        hostname         = creds["hostname"],
        sql_warehouse_id = creds["sql_warehouse_id"],
        access_token     = creds["access_token"],
        model_endpoint   = creds["model_endpoint"],
    )

    print("\n▶  GOLD KPI-1/2 — KPI Suggestion & Collection  [GOLD/gold_layer/kpi_agent.py]")
    print("-" * 62)
    kpi_output_dir = os.path.join(output_dir, "KPIs")
    final_kpis     = kpi_mod.kpi_pipeline_flow(silver_metadata)

    kpi_path = kpi_mod.save_kpis(
        kpis       = final_kpis,
        catalog    = kpi_catalog,
        schema     = kpi_schema,
        output_dir = kpi_output_dir,
    )
    artefacts["gold_kpis"] = kpi_path

    if not final_kpis.get("kpis"):
        print("\n  ⚠  No KPIs collected — skipping aggregate generation.")
        return

    print("\n▶  GOLD AGG — Aggregate Table/View Generation  [GOLD/gold_layer/agg_agent.py]")
    print("-" * 62)
    deployed = agg_mod.agg_pipeline_flow(
        final_kpis     = final_kpis,
        catalog        = silver_catalog,
        source_schema  = silver_schema,
        metadata       = silver_metadata,
        deploy_type    = deploy_type,
        target_catalog = kpi_catalog,
        target_schema  = kpi_schema,
    )

    if deployed:
        artefacts["gold_aggregates_deployed"] = len(
            [d for d in deployed if d.get("status") == "deployed"]
        )


# =============================================================================
# PAUSE HELPERS
# =============================================================================

def _pause_for_bronze_execution(migration_plan_path: str) -> None:
    print("\n" + "=" * 62)
    print("  ✅  PHASE 1 COMPLETE — Bronze plan is ready.")
    print("=" * 62)
    print()
    print("  Before Phase 2 can start, you must move the data")
    print("  into the bronze layer. Do the following steps:")
    print()
    print("  1️⃣  Run upload_plan.py to upload migration_plan.json")
    print(f"      Plan location : {migration_plan_path}")
    print()
    print("  2️⃣  Open Databricks and run execution.py")
    print("      Point it at the volume path shown by upload_plan.py")
    print()
    print("  3️⃣  Wait until execution.py completes successfully")
    print("      (all bronze tables are populated)")
    print()
    input("  ▶  Press Enter when bronze execution is complete... ")
    print()
    print("=" * 62)
    print("  ▶  Starting Phase 2 — Silver transformation...")
    print("=" * 62)


def _pause_for_silver_completion(migration_plan_path: str = "session_output/") -> None:
    print("\n" + "=" * 62)
    print("  ✅  SILVER PHASE COMPLETE.")
    print("=" * 62)
    print()
    print("  Before the Gold phase can start, you must transform the data")
    print("  into the silver layer. Follow these steps:")
    print()
    print("  1️⃣  Upload the silver plan to your Databricks Volume:")
    print(f"      Local path : {migration_plan_path}")
    print()
    print("  2️⃣  Open Databricks and run silver_execution.py")
    print("  3️⃣  Wait until you see: ✅ SILVER INGESTION COMPLETE")
    print()
    input("  ▶  Press Enter when silver execution is complete... ")
    print()
    print("=" * 62)
    print("  ▶  Starting Gold phase...")
    print("=" * 62)


# =============================================================================
# GOLD BRONZE SOURCE → BRONZE+SILVER PHASE ROUTER  (H / I only)
# =============================================================================

def _run_bronze_silver_for_gold(gold_bronze_source, agents, mca_plan, output_dir, config_path, artefacts):
    if gold_bronze_source == "databricks":
        print("\n  Bronze source : Databricks (existing bronze tables in Databricks)")
        print("  Routing       : Flow C path — skipping raw ingest, going direct to Silver")
        print("-" * 62)

        silver_plan = {
            **mca_plan,
            "pipeline_type": "silver",
            "pipelines"    : mca_plan.get("silver_pipelines", []),
        }
        silver_output_dir = os.path.join(output_dir, "silver")
        os.makedirs(silver_output_dir, exist_ok=True)

        _run_silver_steps(agents, silver_plan, silver_output_dir, config_path, artefacts, label="silver", reuse_creds=False)

        silver_creds_path = artefacts.get("credentials_silver")
        if silver_creds_path and os.path.exists(silver_creds_path):
            with open(silver_creds_path, "r", encoding="utf-8") as f:
                saved_plan = json.load(f)
            pipelines = saved_plan.get("pipelines", [])
            if pipelines:
                conn = pipelines[0].get("connection_template", {})
                dest = pipelines[0].get("destination", {})
                return {
                    "workspace_url"  : conn.get("workspace_url", ""),
                    "warehouse_id"   : conn.get("warehouse_id",  ""),
                    "token"          : conn.get("token",          ""),
                    "bronze_catalog" : conn.get("catalog",        ""),
                    "bronze_schema"  : conn.get("schema",         ""),
                    "silver_catalog" : dest.get("catalog",        ""),
                    "silver_schema"  : dest.get("schema",         ""),
                }
        return None

    source_label = {
        "s3"    : "S3             (mirroring Flow D: S3 → Bronze → Silver)",
        "sql"   : "Azure SQL      (mirroring Flow E: SQL → Bronze → Silver)",
        "hybrid": "Mixed S3 + SQL (mirroring Flow G: Mixed → Bronze → Silver)",
    }.get(gold_bronze_source, gold_bronze_source)

    print(f"\n  Bronze source : {source_label}")
    print("-" * 62)

    bronze_plan = {
        **mca_plan,
        "pipeline_type": "bronze",
        "pipelines"    : mca_plan.get("bronze_pipelines", mca_plan.get("pipelines", [])),
    }
    bronze_output_dir = os.path.join(output_dir, "bronze")
    os.makedirs(bronze_output_dir, exist_ok=True)

    _run_bronze_steps(agents, bronze_plan, bronze_output_dir, config_path, artefacts, label="bronze")

    migration_plan_path = artefacts.get("migration_plan_bronze", "session_output/")
    _pause_for_bronze_execution(migration_plan_path)

    silver_plan = {
        **mca_plan,
        "pipeline_type": "silver",
        "pipelines"    : mca_plan.get("silver_pipelines", []),
    }
    silver_output_dir = os.path.join(output_dir, "silver")
    os.makedirs(silver_output_dir, exist_ok=True)

    _run_silver_steps(agents, silver_plan, silver_output_dir, config_path, artefacts, label="silver", reuse_creds=True)
    return None


# =============================================================================
# FLOW RUNNERS  (A–G)
# =============================================================================

def _run_flow_a(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW A  —  S3 → Bronze")
    print("=" * 62)
    _run_bronze_steps(agents, mca_plan, output_dir, config_path, artefacts)


def _run_flow_b(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW B  —  Azure SQL → Bronze")
    print("=" * 62)
    _run_bronze_steps(agents, mca_plan, output_dir, config_path, artefacts)


def _run_flow_f(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW F  —  Mixed Sources → Bronze")
    print("=" * 62)
    _run_bronze_steps(agents, mca_plan, output_dir, config_path, artefacts)


def _run_flow_c(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW C  —  Databricks Bronze → Silver")
    print("=" * 62)
    _run_silver_steps(agents, mca_plan, output_dir, config_path, artefacts, reuse_creds=False)


def _run_flow_d(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW D  —  S3 → Bronze → Silver")
    print("  Phase 1 of 2 : Bronze Raw Load")
    print("=" * 62)

    bronze_plan = {**mca_plan, "pipeline_type": "bronze", "pipelines": mca_plan.get("bronze_pipelines", mca_plan.get("pipelines", []))}
    bronze_output_dir = os.path.join(output_dir, "bronze")
    os.makedirs(bronze_output_dir, exist_ok=True)
    _run_bronze_steps(agents, bronze_plan, bronze_output_dir, config_path, artefacts, label="bronze")

    migration_plan_path = artefacts.get("migration_plan_bronze", "session_output/")
    _pause_for_bronze_execution(migration_plan_path)

    silver_plan = {**mca_plan, "pipeline_type": "silver", "pipelines": mca_plan.get("silver_pipelines", [])}
    silver_output_dir = os.path.join(output_dir, "silver")
    os.makedirs(silver_output_dir, exist_ok=True)
    _run_silver_steps(agents, silver_plan, silver_output_dir, config_path, artefacts, label="silver", reuse_creds=True)


def _run_flow_e(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW E  —  Azure SQL → Bronze → Silver")
    print("  Phase 1 of 2 : Bronze Raw Load")
    print("=" * 62)

    bronze_plan = {**mca_plan, "pipeline_type": "bronze", "pipelines": mca_plan.get("bronze_pipelines", mca_plan.get("pipelines", []))}
    bronze_output_dir = os.path.join(output_dir, "bronze")
    os.makedirs(bronze_output_dir, exist_ok=True)
    _run_bronze_steps(agents, bronze_plan, bronze_output_dir, config_path, artefacts, label="bronze")

    migration_plan_path = artefacts.get("migration_plan_bronze", "session_output/")
    _pause_for_bronze_execution(migration_plan_path)

    silver_plan = {**mca_plan, "pipeline_type": "silver", "pipelines": mca_plan.get("silver_pipelines", [])}
    silver_output_dir = os.path.join(output_dir, "silver")
    os.makedirs(silver_output_dir, exist_ok=True)
    _run_silver_steps(agents, silver_plan, silver_output_dir, config_path, artefacts, label="silver", reuse_creds=True)


def _run_flow_g(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW G  —  Mixed Sources → Bronze → Silver")
    print("  Phase 1 of 2 : Bronze Raw Load")
    print("=" * 62)

    bronze_plan = {**mca_plan, "pipeline_type": "bronze", "pipelines": mca_plan.get("bronze_pipelines", mca_plan.get("pipelines", []))}
    bronze_output_dir = os.path.join(output_dir, "bronze")
    os.makedirs(bronze_output_dir, exist_ok=True)
    _run_bronze_steps(agents, bronze_plan, bronze_output_dir, config_path, artefacts, label="bronze")

    migration_plan_path = artefacts.get("migration_plan_bronze", "session_output/")
    _pause_for_bronze_execution(migration_plan_path)

    silver_plan = {**mca_plan, "pipeline_type": "silver", "pipelines": mca_plan.get("silver_pipelines", [])}
    silver_output_dir = os.path.join(output_dir, "silver")
    os.makedirs(silver_output_dir, exist_ok=True)
    _run_silver_steps(agents, silver_plan, silver_output_dir, config_path, artefacts, label="silver", reuse_creds=True)


# =============================================================================
# GOLD FLOW RUNNERS  (H / I / J / K)
# =============================================================================

def _run_flow_h(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW H  —  Bronze → Silver → Gold Data Modeling")
    print("=" * 62)

    gold_bronze_source = (
        mca_plan.get("gold_bronze_source")
        or mca_plan.get("classification", {}).get("gold_bronze_source", "s3")
    )

    stored_silver_creds = _run_bronze_silver_for_gold(
        gold_bronze_source, agents, mca_plan, output_dir, config_path, artefacts
    )
    silver_plan_path = artefacts.get("migration_plan_silver", "session_output/")
    _pause_for_silver_completion(silver_plan_path)

    gold_output_dir = os.path.join(output_dir, "gold")
    os.makedirs(gold_output_dir, exist_ok=True)

    existing_creds_to_reuse = None
    if gold_bronze_source == "databricks" and stored_silver_creds:
        reuse_decision = _ask_reuse_credentials(stored_silver_creds)
        if reuse_decision is not None:
            existing_creds_to_reuse = reuse_decision

    creds = _gold_collect_credentials(agents, gold_output_dir, artefacts, existing_creds=existing_creds_to_reuse)

    if existing_creds_to_reuse:
        silver_catalog = existing_creds_to_reuse.get("silver_catalog", "")
        silver_schema  = existing_creds_to_reuse.get("silver_schema",  "")
        print(f"\n  Silver catalog (from reused creds): {silver_catalog or '(empty)'}")
        print(f"  Silver schema  (from reused creds): {silver_schema  or '(empty)'}")
        if not silver_catalog:
            silver_catalog = _ask_gold_location("Silver catalog", "silver_catalog")
        if not silver_schema:
            silver_schema = _ask_gold_location("Silver schema", "silver_clean")
        silver_table = _ask_gold_table()
    else:
        print("\n  Enter the Silver layer location to model from:")
        silver_catalog = _ask_gold_location("Silver catalog", "silver_catalog")
        silver_schema  = _ask_gold_location("Silver schema",  "silver_clean")
        silver_table   = _ask_gold_table()

    silver_metadata = _gold_fetch_silver_metadata(
        agents, creds, silver_catalog, silver_schema,
        gold_output_dir, artefacts,
        silver_table=silver_table,
    )

    print("\n  Enter the Gold layer destination:")
    gold_catalog = _ask_gold_location("Gold catalog", silver_catalog + "_gold")
    gold_schema  = _ask_gold_location("Gold schema",  "gold_layer")

    _gold_run_data_modeling(
        agents, silver_metadata, creds,
        gold_catalog, gold_schema,
        silver_catalog, silver_schema,
        gold_output_dir, artefacts,
    )


def _run_flow_i(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW I  —  Bronze → Silver → KPI Calculation")
    print("=" * 62)

    gold_bronze_source = (
        mca_plan.get("gold_bronze_source")
        or mca_plan.get("classification", {}).get("gold_bronze_source", "s3")
    )

    stored_silver_creds = _run_bronze_silver_for_gold(
        gold_bronze_source, agents, mca_plan, output_dir, config_path, artefacts
    )
    silver_plan_path = artefacts.get("migration_plan_silver", "session_output/")
    _pause_for_silver_completion(silver_plan_path)

    gold_output_dir = os.path.join(output_dir, "gold")
    os.makedirs(gold_output_dir, exist_ok=True)

    existing_creds_to_reuse = None
    if gold_bronze_source == "databricks" and stored_silver_creds:
        reuse_decision = _ask_reuse_credentials(stored_silver_creds)
        if reuse_decision is not None:
            existing_creds_to_reuse = reuse_decision

    creds = _gold_collect_credentials(agents, gold_output_dir, artefacts, existing_creds=existing_creds_to_reuse)

    if existing_creds_to_reuse:
        silver_catalog = existing_creds_to_reuse.get("silver_catalog", "")
        silver_schema  = existing_creds_to_reuse.get("silver_schema",  "")
        print(f"\n  Silver catalog (from reused creds): {silver_catalog or '(empty)'}")
        print(f"  Silver schema  (from reused creds): {silver_schema  or '(empty)'}")
        if not silver_catalog:
            silver_catalog = _ask_gold_location("Silver catalog", "silver_catalog")
        if not silver_schema:
            silver_schema = _ask_gold_location("Silver schema", "silver_clean")
        silver_table = _ask_gold_table()
    else:
        print("\n  Enter the Silver layer location to compute KPIs from:")
        silver_catalog = _ask_gold_location("Silver catalog", "silver_catalog")
        silver_schema  = _ask_gold_location("Silver schema",  "silver_clean")
        silver_table   = _ask_gold_table()

    silver_metadata = _gold_fetch_silver_metadata(
        agents, creds, silver_catalog, silver_schema,
        gold_output_dir, artefacts,
        silver_table=silver_table,
    )

    print("\n  Enter the catalog and schema where KPI aggregates will be saved:")
    kpi_catalog = _ask_gold_location("KPI catalog", silver_catalog + "_kpis")
    kpi_schema  = _ask_gold_location("KPI schema",  "gold_kpis")

    deploy_type = _ask_table_or_view()

    _gold_run_kpi_pipeline(
        agents, silver_metadata, creds,
        kpi_catalog    = kpi_catalog,
        kpi_schema     = kpi_schema,
        silver_catalog = silver_catalog,
        silver_schema  = silver_schema,
        deploy_type    = deploy_type,
        output_dir     = gold_output_dir,
        artefacts      = artefacts,
    )


def _run_flow_j(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW J  —  Silver (existing) → Gold Data Modeling")
    print("=" * 62)

    gold_output_dir = os.path.join(output_dir, "gold")
    os.makedirs(gold_output_dir, exist_ok=True)

    creds = _gold_collect_credentials(agents, gold_output_dir, artefacts)

    print("\n  Enter the existing Silver layer location:")
    silver_catalog = _ask_gold_location("Silver catalog", "silver_catalog")
    silver_schema  = _ask_gold_location("Silver schema",  "silver_clean")
    silver_table   = _ask_gold_table()

    silver_metadata = _gold_fetch_silver_metadata(
        agents, creds, silver_catalog, silver_schema,
        gold_output_dir, artefacts,
        silver_table=silver_table,
    )

    print("\n  Enter the Gold layer destination:")
    gold_catalog = _ask_gold_location("Gold catalog", silver_catalog + "_gold")
    gold_schema  = _ask_gold_location("Gold schema",  "gold_layer")

    _gold_run_data_modeling(
        agents, silver_metadata, creds,
        gold_catalog, gold_schema,
        silver_catalog, silver_schema,
        gold_output_dir, artefacts,
    )


def _run_flow_k(agents, mca_plan, output_dir, config_path, artefacts):
    print("\n" + "=" * 62)
    print("  FLOW K  —  Silver (existing) → KPI Calculation")
    print("=" * 62)

    gold_output_dir = os.path.join(output_dir, "gold")
    os.makedirs(gold_output_dir, exist_ok=True)

    creds = _gold_collect_credentials(agents, gold_output_dir, artefacts)

    print("\n  Enter the existing Silver layer location:")
    silver_catalog = _ask_gold_location("Silver catalog", "silver_catalog")
    silver_schema  = _ask_gold_location("Silver schema",  "silver_clean")
    silver_table   = _ask_gold_table()

    silver_metadata = _gold_fetch_silver_metadata(
        agents, creds, silver_catalog, silver_schema,
        gold_output_dir, artefacts,
        silver_table=silver_table,
    )

    print("\n  Enter the catalog and schema where KPI aggregates will be saved:")
    kpi_catalog = _ask_gold_location("KPI catalog", silver_catalog + "_kpis")
    kpi_schema  = _ask_gold_location("KPI schema",  "gold_kpis")

    deploy_type = _ask_table_or_view()

    _gold_run_kpi_pipeline(
        agents, silver_metadata, creds,
        kpi_catalog    = kpi_catalog,
        kpi_schema     = kpi_schema,
        silver_catalog = silver_catalog,
        silver_schema  = silver_schema,
        deploy_type    = deploy_type,
        output_dir     = gold_output_dir,
        artefacts      = artefacts,
    )


# =============================================================================
# FLOW DISPATCH TABLE
# =============================================================================

FLOW_RUNNERS = {
    "A": _run_flow_a,
    "B": _run_flow_b,
    "C": _run_flow_c,
    "D": _run_flow_d,
    "E": _run_flow_e,
    "F": _run_flow_f,
    "G": _run_flow_g,
    "H": _run_flow_h,
    "I": _run_flow_i,
    "J": _run_flow_j,
    "K": _run_flow_k,
}


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_pipeline() -> dict:

    root_dir = os.environ.get("BRONZE_ROOT_DIR")
    if not root_dir:
        root_dir = os.path.dirname(os.path.abspath(__file__))

    agents  = _agent_paths(root_dir)
    mca_dir = os.path.join(root_dir, "MCA")
    if mca_dir not in sys.path:
        sys.path.insert(0, mca_dir)

    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir  = os.path.join(root_dir, "session_output", f"run_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)

    config_path = os.path.join(root_dir, "session_output", "session_config.json")
    os.makedirs(os.path.dirname(config_path), exist_ok=True)

    artefacts: dict = {}

    print("\n" + "=" * 62)
    print("  UNIFIED PLATFORM ORCHESTRATOR  —  Bronze + Silver + Gold")
    print(f"  Output : {os.path.abspath(output_dir)}")
    print("=" * 62)

    print("\n▶  STEP 1 — Master Control Agent  [MCA/new_mca.py]")
    print("-" * 62)

    mca_mod            = _load("agent_mca", agents["mca"])
    user_input         = input("  Describe your request:\n  > ").strip()
    mca                = mca_mod.MasterControlAgent()
    mca_plan, mca_path = mca.execute(user_input, output_dir=output_dir)

    if mca_plan is None:
        logger.info("MCA returned no plan — off-topic input.")
        return artefacts

    artefacts["mca_plan"] = mca_path
    flow_type             = mca_plan.get("flow_type", "").upper()

    print(f"\n  📋 Flow type     : {flow_type}")
    print(f"  📋 Flow          : {mca_plan.get('flow', '')}")
    print(f"  📋 Pipeline type : {mca_plan.get('pipeline_type', '').upper()}")
    print(f"  📋 Reason        : {mca_plan.get('classification', {}).get('reason', '')}")

    if flow_type in ("H", "I") and mca_plan.get("gold_bronze_source"):
        gbs = mca_plan["gold_bronze_source"]
        routing = {
            "s3"        : "Flow D path  (S3 → Bronze → Silver → Gold)",
            "sql"       : "Flow E path  (SQL → Bronze → Silver → Gold)",
            "hybrid"    : "Flow G path  (Mixed → Bronze → Silver → Gold)",
            "databricks": "Flow C path  (existing bronze → Silver → Gold)",
        }.get(gbs, gbs)
        print(f"  📋 Bronze source : {gbs}")
        print(f"  📋 Routing       : {routing}")
    print()

    runner = FLOW_RUNNERS.get(flow_type)

    if runner is None:
        print(f"\n  ⛔ Unknown flow type: '{flow_type}'. Exiting.\n")
        return artefacts

    try:
        runner(agents, mca_plan, output_dir, config_path, artefacts)
    except (RuntimeError, SystemExit):
        return artefacts

    print("\n" + "=" * 62)
    print("  ✅  ALL STEPS COMPLETE")
    print("=" * 62)
    print(f"\n  Flow          : {mca_plan.get('flow', '')}")
    print(f"  Output dir    : {os.path.abspath(output_dir)}\n")
    print("  Artefacts:")
    for name, path in artefacts.items():
        if path and isinstance(path, str):
            print(f"    {name:<35} → {path}")

    manifest_path = os.path.join(output_dir, f"manifest_{timestamp}.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(artefacts, f, indent=2, default=str)
    print(f"\n  📄 Manifest → {manifest_path}\n")

    return artefacts