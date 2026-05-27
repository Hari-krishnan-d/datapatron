"""
API/routes/flow_c_routes.py
============================================
5-Step API workflow using HTTP Cookies for automatic state tracking.
"""

import logging
from fastapi import APIRouter, Cookie, Response, HTTPException, status

from API.schemas.flow_c_schema import (
    FlowCStartRequest,
    FlowCStartResponse,
    ApplyTaxonomyRequest,
    ApplyTaxonomyResponse,
    FlowCTransformRequest,
    TransformResponse,
    ExecuteRequest,
    ExecuteResponse
)
from API.services.flow_c_service import (
    start_session_service,
    taxonomy_service,
    apply_taxonomy_service,
    transform_service,
    execute_service
)

logger = logging.getLogger("API.flow_c_routes")
router = APIRouter(prefix="/flow-c", tags=["Flow C — Bronze → Silver"])

# ── STEP 1: START ─────────────────────────────────────────────────────────────
@router.post("/start", response_model=FlowCStartResponse, summary="1. Start Session")
async def start(request: FlowCStartRequest, response: Response):
    try:
        # Pass the configs from the request body into the service
        result = start_session_service(
            bronze_config=request.bronze_config.model_dump(), 
            silver_config=request.silver_config.model_dump()
        )
        
        # INJECT INVISIBLE COOKIE HERE
        response.set_cookie(
            key="flow_c_session_id",
            value=result["session_id"],
            httponly=True,
            samesite="lax"
        )
        return result
    except Exception as e:
        logger.error(f"Start Session Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── STEP 2: TAXONOMY (GET) ────────────────────────────────────────────────────
@router.get("/taxonomy", summary="2. Get Taxonomy & Proposed Schema")
async def taxonomy(flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session cookie missing. Call /start first.")
    try:
        return taxonomy_service(flow_c_session_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Taxonomy Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── STEP 3: APPLY EDITS ───────────────────────────────────────────────────────
@router.post("/apply-taxonomy", response_model=ApplyTaxonomyResponse, summary="3. Apply User Edits")
async def apply_taxonomy(
    req: ApplyTaxonomyRequest, 
    flow_c_session_id: str = Cookie(None)
):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session cookie missing. Call /start first.")
    try:
        return apply_taxonomy_service(
            session_id=flow_c_session_id,
            edited_taxonomy=req.edited_taxonomy,
            edited_schema=req.edited_schema
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Apply Taxonomy Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── STEP 4: TRANSFORM ─────────────────────────────────────────────────────────
@router.post("/transform", response_model=TransformResponse, summary="4. Generate SQL & Preview")
async def transform(request: FlowCTransformRequest, flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session cookie missing. Call /start first.")
    try:
        # Pass the table_name and columns from the JSON body to the service
        return transform_service(
            session_id=flow_c_session_id,
            table_name=request.table_name,
            columns=request.model_dump()["columns"]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Transform Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── STEP 5: EXECUTE ───────────────────────────────────────────────────────────
@router.post("/execute", response_model=ExecuteResponse, summary="5. Execute Final Table Creation")
async def execute(request: ExecuteRequest, flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session cookie missing. Call /start first.")
    try:
        return execute_service(
            session_id=flow_c_session_id,
            proposed_table=request.proposed_table_name,
            custom_table=request.custom_table_name
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Execute Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))