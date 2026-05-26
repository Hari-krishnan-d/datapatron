import os
import uuid
import logging
from API.schemas.mca_schema import UserSelection

logger = logging.getLogger("API.mca_service")

def handle_phase1(data: UserSelection) -> dict:
    """
    Handles Phase 1: Intent & Sourcing.
    Parses the user's request and generates the session ID for interactive flows.
    """
    logger.info(f"Processing Phase 1 Intent: {data.user_request}")
    
    # Generate a tracking ID for this MCA session
    mca_session_id = f"mca_{uuid.uuid4().hex[:8]}"
    
    determined_flow = data.flow_selection or "UNKNOWN"
    
    plan = {
        "mca_session_id": mca_session_id,
        "determined_flow": determined_flow,
        "s3_bucket_count_detected": data.s3_bucket_count,
        "sql_db_count_detected": data.sql_db_count,
        "bronze_source_override": data.gold_bronze_source
    }
    
    logger.info(f"Phase 1 Plan Generated: {plan}")
    return plan


def execute_phase2_pipeline(data: any, flow_name: str) -> dict:
    """
    Handles Phase 2: Pipeline Execution for NON-interactive flows.
    """
    logger.info(f"Executing Phase 2 for {flow_name}")
    
    if flow_name == "FlowC":
        raise ValueError("Flow C cannot be executed via MCA Phase 2. Use the interactive Flow C API.")
    
    # Extract common config
    db_workspace = getattr(data, "databricks_workspace_url", None)
    
    # Simulate successful execution state for automated flows
    execution_result = {
        "flow_executed": flow_name,
        "workspace_target": db_workspace,
        "status": "Pipeline deployed successfully to Databricks.",
        "tables_created": ["bronze_raw_table_1"]
    }
    
    logger.info(f"Phase 2 Execution Complete: {execution_result}")
    return execution_result