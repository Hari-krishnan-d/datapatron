"""
API/schemas/flow_c_schema.py — Flow C Request & Response Models
================================================================
Flow C = Bronze (Databricks) → Silver (Databricks)
5-Step Human-in-the-loop Workflow
"""

from typing import Dict, Any, Literal, Optional
from pydantic import BaseModel, Field

# ── Databricks Credentials ────────────────────────────────────────────────────
class DatabricksConfig(BaseModel):
    databricks_workspace_url: str = Field(..., description="Databricks workspace URL")
    databricks_warehouse_id:  str = Field(..., description="SQL warehouse ID")
    databricks_token:         str = Field(..., description="Databricks personal access token")
    catalog:                  str = Field(..., description="Databricks catalog name")
    schema_name:              str = Field(..., alias="schema", description="Databricks schema name")

    class Config:
        populate_by_name = True

# ── STEP 1: START ─────────────────────────────────────────────────────────────
class FlowCStartRequest(BaseModel):
    bronze_config: DatabricksConfig = Field(..., description="Source (Bronze) configuration")
    silver_config: DatabricksConfig = Field(..., description="Target (Silver) configuration")

class FlowCStartResponse(BaseModel):
    status: Literal["session_started", "failed"]
    session_id: str
    taxonomy: Optional[list] = None

# ── STEP 3: APPLY TAXONOMY (User Edits) ───────────────────────────────────────
class ApplyTaxonomyRequest(BaseModel):
    edited_taxonomy: Dict[str, Dict[str, str]] = Field(
        ..., description="{column: {raw_value: canonical_value}}"
    )
    edited_schema: Dict[str, Dict[str, Any]] = Field(
        ..., description="{column: {'dtype': '...', 'rename_to': '...'}}"
    )

class ApplyTaxonomyResponse(BaseModel):
    status: Literal["success", "failed"]
    message: str
    session_id: str

# ── STEP 4: TRANSFORM ─────────────────────────────────────────────────────────
class ColumnTransform(BaseModel):
    source_column: str
    silver_column: str
    silver_type: str
    null_strategy: str
    fill_value: Optional[str] = None

class FlowCTransformRequest(BaseModel):
    table_name: str
    columns: list[ColumnTransform]

class TransformResponse(BaseModel):
    status: Literal["transform_completed", "failed"]
    ddl_preview: str

# ── STEP 5: EXECUTE ───────────────────────────────────────────────────────────
class ExecuteRequest(BaseModel):
    proposed_table_name: str
    custom_table_name: Optional[str] = None

class ExecuteResponse(BaseModel):
    status: Literal["success", "failed"]
    message: str