from pydantic import BaseModel, Field
from typing import Optional, List

# ══════════════════════════════════════════════════════════════════════════
# PHASE 1: INTENT & SELECTION
# ══════════════════════════════════════════════════════════════════════════

class UserSelection(BaseModel):
    user_request: str = Field(..., description="Natural language prompt")
    flow_selection: Optional[str] = Field(None, description="Direct flow override (A-K)")
    gold_bronze_source: Optional[str] = Field(None, description="Direct bronze source override (A-K)")
    s3_bucket_count: Optional[int] = Field(1, description="Number of S3 buckets")
    sql_db_count: Optional[int] = Field(1, description="Number of SQL databases")
# ══════════════════════════════════════════════════════════════════════════
# BASE CREDENTIAL BLOCKS
# ══════════════════════════════════════════════════════════════════════════

class DatabricksBase(BaseModel):
    databricks_workspace_url: Optional[str] = Field(None)
    databricks_warehouse_id: Optional[str] = Field(None)
    databricks_token: Optional[str] = Field(None)

class S3Credentials(BaseModel):
    s3_bucket: Optional[str] = Field(None)
    s3_region: Optional[str] = Field(None)
    aws_access_key: Optional[str] = Field(None)
    aws_secret_key: Optional[str] = Field(None)

class SQLCredentials(BaseModel):
    sql_server: Optional[str] = Field(None)
    sql_database: Optional[str] = Field(None)
    sql_username: Optional[str] = Field(None)
    sql_password: Optional[str] = Field(None)

class BronzeTarget(BaseModel):
    bronze_catalog: Optional[str] = Field(None)
    bronze_schema: Optional[str] = Field(None)
    bronze_table: Optional[str] = Field("")

class SilverTarget(BaseModel):
    silver_catalog: Optional[str] = Field(None)
    silver_schema: Optional[str] = Field(None)

class GoldTarget(BaseModel):
    gold_catalog: Optional[str] = Field(None)
    gold_schema: Optional[str] = Field(None)

# ══════════════════════════════════════════════════════════════════════════
# FLOW CLASSES (A - K)
# ══════════════════════════════════════════════════════════════════════════

class FlowA(DatabricksBase, BronzeTarget):
    s3_credentials: Optional[List[S3Credentials]] = None

class FlowB(DatabricksBase, BronzeTarget):
    sql_credentials: Optional[List[SQLCredentials]] = None

class FlowC(DatabricksBase, BronzeTarget, SilverTarget):
    pass

class FlowD(DatabricksBase, BronzeTarget, SilverTarget):
    s3_credentials: Optional[List[S3Credentials]] = None

class FlowE(DatabricksBase, BronzeTarget, SilverTarget):
    sql_credentials: Optional[List[SQLCredentials]] = None

class FlowF(DatabricksBase, BronzeTarget):
    s3_credentials: Optional[List[S3Credentials]] = None
    sql_credentials: Optional[List[SQLCredentials]] = None

class FlowG(DatabricksBase, BronzeTarget, SilverTarget):
    s3_credentials: Optional[List[S3Credentials]] = None
    sql_credentials: Optional[List[SQLCredentials]] = None

class FlowH(DatabricksBase, BronzeTarget, SilverTarget, GoldTarget):
    s3_credentials: Optional[List[S3Credentials]] = None
    sql_credentials: Optional[List[SQLCredentials]] = None

class FlowI(DatabricksBase, BronzeTarget, SilverTarget, GoldTarget):
    s3_credentials: Optional[List[S3Credentials]] = None
    sql_credentials: Optional[List[SQLCredentials]] = None

class FlowJ(DatabricksBase, SilverTarget, GoldTarget):
    pass

class FlowK(DatabricksBase, SilverTarget, GoldTarget):
    pass