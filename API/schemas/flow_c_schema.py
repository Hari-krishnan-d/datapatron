"""
API/schemas/flow_c_schema.py — Flow C Request & Response Models
================================================================
Flow C = Bronze (Databricks) → Silver (Databricks)
"""

import uuid
from typing import Literal, Optional, Any, Dict, List, Union
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
    session_id:    Optional[str]    = Field(default=None, description="If omitted, a new UUID session will be generated.")
    bronze_config: DatabricksConfig = Field(..., description="Source (Bronze) configuration")
    silver_config: DatabricksConfig = Field(..., description="Target (Silver) configuration")

class TaxonomyGroup(BaseModel):
    group_id:  int
    canonical: str
    variants:  List[str]

class FlowCStartResponse(BaseModel):
    status:     Literal["success", "failed"]
    session_id: str
    stage:      Literal["awaiting_taxonomy"]
    taxonomy_proposals: List[Any] = Field(
        description="List of tables and their column classifications."
    )

# ── STEP 2: TAXONOMY ──────────────────────────────────────────────────────────

class FlowCTaxonomyRequest(BaseModel):
    approved_taxonomy: Dict[str, Union[List[TaxonomyGroup], Literal["SKIP"]]] = Field(
        description="User-edited taxonomy groups. Send 'SKIP' to skip a column."
    )

class TransformProposal(BaseModel):
    source_column:   str
    proposed_rename: str
    bronze_type:     str
    proposed_type:   str
    null_count:      int
    null_percentage: float
    null_options:    List[Dict[str, Any]] = Field(
        default_factory=list, 
        description="Dynamic list of null handling options based on data type."
    )

class FlowCTaxonomyResponse(BaseModel):
    status:     Literal["success", "failed"]
    session_id: str
    stage:      Literal["awaiting_transform"]
    transform_proposals: List[TransformProposal]

# ── STEP 3: TRANSFORM ─────────────────────────────────────────────────────────

class ColumnTransform(BaseModel):
    source_column: str
    silver_column: str
    silver_type:   str
    null_strategy: Literal[
        "keep", "drop", "fill_mean", "fill_median", 
        "fill_min", "fill_max", "fill_constant", "ffill", "bfill"
    ]
    fill_value:    Optional[str] = None

class FlowCTransformRequest(BaseModel):
    table_name: str
    columns:    List[ColumnTransform] = Field(description="Must contain exactly 30 columns based on previous step")

class FlowCTransformResponse(BaseModel):
    status:              Literal["success", "failed"]
    session_id:          str
    stage:               Literal["awaiting_execution"]
    proposed_table_name: str
    ddl_preview:         str

# ── STEP 4: EXECUTE ───────────────────────────────────────────────────────────

class FlowCExecuteRequest(BaseModel):
    proposed_table_name: str
    custom_table_name:   Optional[str] = Field(default=None, description="Provide to override proposed name")

class FlowCExecuteResponse(BaseModel):
    status:         Literal["success", "failed"]
    session_id:     str
    stage:          Literal["completed"]
    target_table:   str
    migration_plan: dict