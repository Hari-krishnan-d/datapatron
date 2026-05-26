"""
API/schemas/flow_a_schema.py — Flow A Request & Response Models
================================================================
Flow A = S3 → Bronze (raw ingestion only)

Request fields:
  credentials       — S3 source credentials
  databricks_config — Databricks destination config

No session_id or mca_plan needed from the user.
The service automatically loads the latest MCA plan from disk.
"""

from typing import Literal, Optional
from pydantic import BaseModel, Field


# ── S3 Source Credentials ─────────────────────────────────────────────────────

class S3Credentials(BaseModel):
    aws_access_key_id:     str = Field(..., description="AWS access key ID")
    aws_secret_access_key: str = Field(..., description="AWS secret access key")
    bucket:                str = Field(..., description="S3 bucket name")
    region:                str = Field(..., description="AWS region (e.g. us-east-1)")
    prefix:                str = Field(default="", description="Optional S3 key prefix/path")
    file_format:           str = Field(default="csv", description="File format: csv or json")


# ── Databricks Destination Config ─────────────────────────────────────────────

class DatabricksConfig(BaseModel):
    databricks_workspace_url: str = Field(..., description="Databricks workspace URL")
    databricks_warehouse_id:  str = Field(..., description="SQL warehouse ID")
    databricks_token:         str = Field(..., description="Databricks personal access token")
    catalog:                  str = Field(..., description="Databricks catalog name")
    schema:                   str = Field(..., description="Databricks schema name")


# ── Request ───────────────────────────────────────────────────────────────────

class FlowAExecuteRequest(BaseModel):
    credentials:       S3Credentials   = Field(..., description="S3 source credentials")
    databricks_config: DatabricksConfig = Field(..., description="Databricks destination config")


# ── Per-step status ───────────────────────────────────────────────────────────

class StepStatus(BaseModel):
    step:   str                              = Field(description="Step name")
    status: Literal["success", "failed"]
    output: Optional[str] = None             # file path saved
    error:  Optional[str] = None             # error message if failed


# ── Response — success ────────────────────────────────────────────────────────

class FlowAExecuteResponse(BaseModel):
    status:          Literal["success", "partial", "failed"]
    session_id:      str
    flow:            Literal["A"]
    steps:           list[StepStatus] = Field(description="Status of each pipeline step")
    migration_plan:  Optional[dict]   = Field(default=None, description="Final migration plan — passed to Databricks executor")
    output_dir:      str              = Field(description="Directory where all step outputs are saved")
    stage:           Literal["awaiting_execution"]


# ── Response — error ──────────────────────────────────────────────────────────

class FlowAErrorResponse(BaseModel):
    status:  Literal["error"]
    code:    int
    message: str
    detail:  Optional[str] = None