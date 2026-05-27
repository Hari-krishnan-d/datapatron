"""
API/routes/flow_c_routes.py — Flow C Router
============================================
Endpoints for interactive Flow C (Bronze → Silver).
Requires 4 sequential steps to handle user interactions.
"""

import logging
from fastapi import APIRouter, HTTPException, status, Response, Cookie

from API.schemas.flow_c_schema import (
    FlowCStartRequest,
    FlowCStartResponse,
    FlowCTaxonomyRequest,
    FlowCTaxonomyResponse,
    FlowCTransformRequest,
    FlowCTransformResponse,
    FlowCExecuteRequest,
    FlowCExecuteResponse,
)
from API.services.flow_c_service import (
    run_flow_c_start,
    run_flow_c_taxonomy,
    run_flow_c_transform,
    run_flow_c_execute,
)

logger = logging.getLogger("API.flow_c_routes")

router = APIRouter(prefix="/flow-c", tags=["Flow C — Bronze → Silver"])


# =============================================================================
# POST /flow-c/start
# =============================================================================
@router.post(
    "/start",
    response_model=FlowCStartResponse,
    status_code=status.HTTP_200_OK,
    summary="Step 1: Start Flow C & Extract Taxonomy Proposals",
)
async def start_flow_c(request: FlowCStartRequest, response: Response):
    logger.info("POST /flow-c/start — session=%s", request.session_id)
    try:
        result = run_flow_c_start(
            session_id=request.session_id,
            bronze_config=request.bronze_config.model_dump(),
            silver_config=request.silver_config.model_dump(),
        )
        # Set the session_id as an HTTP-only cookie automatically detected by Postman
        response.set_cookie(
            key="flow_c_session_id",
            value=result["session_id"],
            httponly=True,
            samesite="lax"
        )
        return result
    except ValueError as exc:
        logger.warning("Flow C start — validation error: %s", exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except Exception as exc:
        logger.exception("Flow C start — unexpected error: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


# =============================================================================
# POST /flow-c/taxonomy
# =============================================================================
@router.post(
    "/taxonomy",
    response_model=FlowCTaxonomyResponse,
    status_code=status.HTTP_200_OK,
    summary="Step 2: Submit Taxonomy & Get Transform Proposals",
)
async def submit_taxonomy(request: FlowCTaxonomyRequest, flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session missing. Call /start first.")
        
    logger.info("POST /flow-c/taxonomy — session=%s", flow_c_session_id)
    try:
        approved_tax = {
            k: [item.model_dump() for item in v] if isinstance(v, list) else v
            for k, v in request.approved_taxonomy.items()
        }
        
        result = run_flow_c_taxonomy(
            session_id=flow_c_session_id,
            approved_taxonomy=approved_tax,
        )
        return result
    except ValueError as exc:
        logger.warning("Flow C taxonomy — validation error: %s", exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except Exception as exc:
        logger.exception("Flow C taxonomy — unexpected error: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


# =============================================================================
# POST /flow-c/transform
# =============================================================================
@router.post(
    "/transform",
    response_model=FlowCTransformResponse,
    status_code=status.HTTP_200_OK,
    summary="Step 3: Submit Transforms & Get Schema DDL",
)
async def submit_transform(request: FlowCTransformRequest, flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session missing. Call /start first.")
        
    logger.info("POST /flow-c/transform — session=%s", flow_c_session_id)
    try:
        columns_dump = [col.model_dump() for col in request.columns]
        result = run_flow_c_transform(
            session_id=flow_c_session_id,
            table_name=request.table_name,
            columns=columns_dump,
        )
        return result
    except ValueError as exc:
        logger.warning("Flow C transform — validation error: %s", exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except Exception as exc:
        logger.exception("Flow C transform — unexpected error: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))


# =============================================================================
# POST /flow-c/execute
# =============================================================================
@router.post(
    "/execute",
    response_model=FlowCExecuteResponse,
    status_code=status.HTTP_200_OK,
    summary="Step 4: Execute Table Creation & Generate Migration Plan",
)
async def execute_flow_c(request: FlowCExecuteRequest, flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session missing. Call /start first.")
        
    logger.info("POST /flow-c/execute — session=%s", flow_c_session_id)
    try:
        result = run_flow_c_execute(
            session_id=flow_c_session_id,
            proposed_table=request.proposed_table_name,
            custom_table=request.custom_table_name,
        )
        return result
    except ValueError as exc:
        logger.warning("Flow C execute — validation error: %s", exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except Exception as exc:
        logger.exception("Flow C execute — unexpected error: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))