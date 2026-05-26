"""
API/schemas/flow_b_schema.py — Flow B Request & Response Models
================================================================
Flow B = Azure SQL → Bronze (raw ingestion only)

Request fields:
  credentials       — Azure SQL source credentials
  databricks_config — Databricks destination config

No session_id or mca_plan needed from the user.
The service automatically loads the latest MCA plan from disk,
exactly mirroring the Flow A pattern.
"""

from typing import Literal, Optional
from pydantic import BaseModel, Field


# ── Azure SQL Source Credentials ──────────────────────────────────────────────

class AzureSQLCredentials(BaseModel):
    server  : str           = Field(...,          description="Azure SQL server FQDN, e.g. myserver.database.windows.net")
    database: str           = Field(...,          description="Database name")
    username: str           = Field(...,          description="SQL login username")
    password: str           = Field(...,          description="SQL login password")
    schema  : Optional[str] = Field(default="dbo", description="Source schema (default: dbo)")


# ── Databricks Destination Config ─────────────────────────────────────────────

class DatabricksConfig(BaseModel):
    databricks_workspace_url: str = Field(..., description="Databricks workspace URL")
    databricks_warehouse_id : str = Field(..., description="SQL warehouse ID")
    databricks_token        : str = Field(..., description="Databricks personal access token")
    catalog                 : str = Field(..., description="Databricks catalog name")
    schema                  : str = Field(..., description="Databricks schema name")


# ── Request ───────────────────────────────────────────────────────────────────

class FlowBExecuteRequest(BaseModel):
    credentials      : AzureSQLCredentials = Field(..., description="Azure SQL source credentials")
    databricks_config: DatabricksConfig     = Field(..., description="Databricks destination config")


# ── Per-step status ───────────────────────────────────────────────────────────

class StepStatus(BaseModel):
    step  : str                              = Field(description="Step name")
    status: Literal["success", "failed"]
    output: Optional[str] = None             # file path saved
    error : Optional[str] = None             # error message if failed


# ── Response — success / partial / failed ─────────────────────────────────────

class FlowBExecuteResponse(BaseModel):
    status        : Literal["success", "partial", "failed"]
    session_id    : str
    flow          : Literal["B"]
    steps         : list[StepStatus] = Field(description="Status of each pipeline step")
    migration_plan: Optional[dict]   = Field(default=None, description="Final migration plan — passed to Databricks executor")
    output_dir    : str              = Field(description="Directory where all step outputs are saved")
    stage         : Literal["awaiting_execution"]


# ── Response — error ──────────────────────────────────────────────────────────

class FlowBErrorResponse(BaseModel):
    status : Literal["error"]
    code   : int
    message: str
    detail : Optional[str] = None