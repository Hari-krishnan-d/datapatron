"""
API/routes/flow_b_routes.py — Flow B Router
============================================
Endpoint:
  POST /flow-b/execute  — run all 7 bronze steps for Flow B (Azure SQL → Bronze)

User only passes Azure SQL credentials + Databricks config.
The service automatically picks up the latest MCA plan from disk.
"""

import logging
from fastapi import APIRouter, HTTPException, status

from schemas.flow_b_schema import (
    FlowBExecuteRequest,
    FlowBExecuteResponse,
    FlowBErrorResponse,
)
from services.flow_b_service import run_flow_b

logger = logging.getLogger("API.flow_b_routes")

router = APIRouter(prefix="/flow-b", tags=["Flow B — Azure SQL → Bronze"])


# =============================================================================
# POST /flow-b/execute
# =============================================================================

@router.post(
    "/execute",
    response_model=FlowBExecuteResponse,
    status_code=status.HTTP_200_OK,
    summary="Execute Flow B — Azure SQL → Bronze raw ingestion",
    description="""
Runs all 7 bronze pipeline steps for Flow B (Azure SQL → Bronze) in sequence.

**No session_id or mca_plan needed** — the service automatically loads
the latest MCA plan saved by `POST /mca/execute`.

Steps executed:
1. **Credentials Collection** — injects Azure SQL credentials, no interactive prompts
2. **Connection Validation** — validates Azure SQL connectivity via pyodbc
3. **Metadata Extraction** — samples top-5 tables × 5 rows per source schema
4. **Pipeline Strategy** — determines write strategy, watermark columns, parallelism
5. **Schema Mapping** — infers Databricks column types (rules + LLM fallback)
6. **Table Creation** — creates bronze Delta tables in Databricks
7. **Migration Plan** — generates final migration plan JSON for the executor

**All steps run regardless of individual failures** — each step status is reported.

**Response status values:**
- `success` — all 7 steps passed
- `partial`  — some steps failed, migration_plan may still be available
- `failed`   — critical failure at Step 1, pipeline cannot continue
""",
    responses={
        200: {"description": "Flow B executed — check `status` field for partial/failed", "model": FlowBExecuteResponse},
        400: {"description": "No MCA plan found or invalid inputs", "model": FlowBErrorResponse},
        500: {"description": "Unexpected server error", "model": FlowBErrorResponse},
    },
)
async def execute_flow_b(request: FlowBExecuteRequest):
    logger.info(
        "POST /flow-b/execute — server=%s database=%s catalog=%s schema=%s",
        request.credentials.server,
        request.credentials.database,
        request.databricks_config.catalog,
        request.databricks_config.schema,
    )

    try:
        result = run_flow_b(
            sql_credentials   = request.credentials.model_dump(),
            databricks_config = request.databricks_config.model_dump(),
        )
        return result

    except ValueError as exc:
        logger.warning("Flow B execute — validation error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status" : "error",
                "code"   : 400,
                "message": str(exc),
            },
        )

    except Exception as exc:
        logger.exception("Flow B execute — unexpected error: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "status" : "error",
                "code"   : 500,
                "message": "Unexpected server error.",
                "detail" : str(exc),
            },
        )