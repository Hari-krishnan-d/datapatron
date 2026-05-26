"""
API/routes/flow_a_routes.py — Flow A Router
============================================
Endpoint:
  POST /flow-a/execute  — run all 7 bronze steps for Flow A (S3 → Bronze)

User only passes S3 credentials + Databricks config.
The service automatically picks up the latest MCA plan from disk.
"""

import logging
from fastapi import APIRouter, HTTPException, status

from schemas.flow_a_schema import (
    FlowAExecuteRequest,
    FlowAExecuteResponse,
    FlowAErrorResponse,
)
from services.flow_a_service import run_flow_a

logger = logging.getLogger("API.flow_a_routes")

router = APIRouter(prefix="/flow-a", tags=["Flow A — S3 → Bronze"])


# =============================================================================
# POST /flow-a/execute
# =============================================================================

@router.post(
    "/execute",
    response_model=FlowAExecuteResponse,
    status_code=status.HTTP_200_OK,
    summary="Execute Flow A — S3 → Bronze raw ingestion",
    description="""
Runs all 7 bronze pipeline steps for Flow A (S3 → Bronze) in sequence.

**No session_id or mca_plan needed** — the service automatically loads
the latest MCA plan saved by `POST /mca/execute`.

Steps executed:
1. **Credentials Collection** — injects S3 credentials, no interactive prompts
2. **Connection Validation** — validates S3 connectivity
3. **Metadata Extraction** — samples files from S3 bucket
4. **Pipeline Strategy** — determines write strategy, watermark columns, parallelism
5. **Schema Mapping** — infers Databricks column types (rules + LLM fallback)
6. **Table Creation** — creates bronze tables in Databricks
7. **Migration Plan** — generates final migration plan JSON for the executor

**All steps run regardless of individual failures** — each step status is reported.

**Response status values:**
- `success` — all 7 steps passed
- `partial`  — some steps failed, migration_plan may still be available
- `failed`   — critical failure at Step 1, pipeline cannot continue
""",
    responses={
        200: {"description": "Flow A executed — check `status` field for partial/failed", "model": FlowAExecuteResponse},
        400: {"description": "No MCA plan found or invalid inputs", "model": FlowAErrorResponse},
        500: {"description": "Unexpected server error", "model": FlowAErrorResponse},
    },
)
async def execute_flow_a(request: FlowAExecuteRequest):
    logger.info(
        "POST /flow-a/execute — bucket=%s catalog=%s schema=%s",
        request.credentials.bucket,
        request.databricks_config.catalog,
        request.databricks_config.schema,
    )

    try:
        result = run_flow_a(
            s3_credentials    = request.credentials.model_dump(),
            databricks_config = request.databricks_config.model_dump(),
        )
        return result

    except ValueError as exc:
        logger.warning("Flow A execute — validation error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status" : "error",
                "code"   : 400,
                "message": str(exc),
            },
        )

    except Exception as exc:
        logger.exception("Flow A execute — unexpected error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "status" : "error",
                "code"   : 500,
                "message": "Unexpected server error.",
                "detail" : str(exc),
            },
        )