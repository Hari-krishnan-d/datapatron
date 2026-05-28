"""
API/schemas/flow_c_schema.py
Models for Flow C (Bronze Databricks → Silver Databricks)
"""
from typing import Dict, Any, Literal, Optional, List
from pydantic import BaseModel, Field

# ── Credentials ──
class BronzeConfig(BaseModel):
    databricks_workspace_url: str = Field(..., description="Databricks workspace URL")
    databricks_warehouse_id:  str = Field(..., description="SQL warehouse ID")
    databricks_token:         str = Field(..., description="Databricks access token")
    catalog:                  str = Field(..., description="Bronze catalog name")
    schema_name:              str = Field(..., alias="schema", description="Bronze schema name")
    table:                    str = Field(..., description="Bronze table name (e.g., carsales)")
    
    class Config:
        populate_by_name = True

class SilverConfig(BaseModel):
    # Only requires catalog and schema. Inherits connection from Bronze.
    catalog:     str = Field(..., description="Silver target catalog name")
    schema_name: str = Field(..., alias="schema", description="Silver target schema name")
    
    class Config:
        populate_by_name = True

# ── STEP 1: START ──
class FlowCStartRequest(BaseModel):
    bronze_config: BronzeConfig
    silver_config: SilverConfig

class FlowCStartResponse(BaseModel):
    status: Literal["session_started", "failed"]
    session_id: str
    message: str

# ── STEP 2: TAXONOMY (GET & POST) ──
class TaxonomyProposalsResponse(BaseModel):
    session_id: str
    categorical_columns: List[str]
    proposed_mappings: Dict[str, Dict[str, str]] # { "column_name": { "raw_value": "Canonical Value" } }

class ApplyTaxonomyRequest(BaseModel):
    # The user/frontend submits the confirmed mappings
    edited_taxonomy: Dict[str, Dict[str, str]] 

class ApplyTaxonomyResponse(BaseModel):
    status: Literal["success", "failed"]
    message: str
    session_id: str

# ── STEP 3: TRANSFORM ──
class ColumnTransform(BaseModel):
    source_column: str
    silver_column: str
    silver_type: str
    null_strategy: Literal["keep", "drop", "fill_mean", "fill_median", "fill_min", "fill_max", "fill_constant", "forward_fill", "backward_fill"]
    fill_value: Optional[str] = None

class FlowCTransformRequest(BaseModel):
    table_name: str
    columns: List[ColumnTransform]

class TransformResponse(BaseModel):
    status: Literal["transform_completed", "failed"]
    ddl_preview: str

# ── STEP 4: EXECUTE ──
class ExecuteRequest(BaseModel):
    proposed_table_name: str
    custom_table_name: Optional[str] = None  # User-provided final name

class ExecuteResponse(BaseModel):
    status: Literal["success", "failed"]
    message: str