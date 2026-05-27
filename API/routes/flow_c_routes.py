"""
API/routes/flow_c_routes.py
Stateful API workflow using HTTP Cookies.
"""
import logging
from fastapi import APIRouter, Cookie, Response, HTTPException
from API.schemas.flow_c_schema import (
    FlowCStartRequest, FlowCStartResponse,
    TaxonomyProposalsResponse,
    ApplyTaxonomyRequest, ApplyTaxonomyResponse,
    FlowCTransformRequest, TransformResponse,
    ExecuteRequest, ExecuteResponse
)
from API.services.flow_c_service import (
    start_session_service,
    generate_taxonomy_proposals,
    apply_taxonomy_service,
    transform_service,
    execute_service
)

logger = logging.getLogger("API.flow_c_routes")
router = APIRouter(prefix="/flow-c", tags=["Flow C — Bronze → Silver"])

@router.post("/start", response_model=FlowCStartResponse, summary="1. Start & Profile")
async def start(request: FlowCStartRequest, response: Response):
    try:
        result = start_session_service(
            bronze_config=request.bronze_config.model_dump(by_alias=True), 
            silver_config=request.silver_config.model_dump(by_alias=True)
        )
        response.set_cookie(key="flow_c_session_id", value=result["session_id"], httponly=True, samesite="lax")
        return result
    except Exception as e:
        logger.error(f"Start Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/taxonomy", response_model=TaxonomyProposalsResponse, summary="2. Get Taxonomy Proposals")
async def get_taxonomy(flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session cookie missing.")
    try:
        return generate_taxonomy_proposals(flow_c_session_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/apply-taxonomy", response_model=ApplyTaxonomyResponse, summary="3. Save User Mappings")
async def apply_taxonomy(req: ApplyTaxonomyRequest, flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session cookie missing.")
    try:
        return apply_taxonomy_service(flow_c_session_id, req.edited_taxonomy)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/transform", response_model=TransformResponse, summary="4. Apply Types & Null Rules")
async def transform(request: FlowCTransformRequest, flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session cookie missing.")
    try:
        return transform_service(flow_c_session_id, request.table_name, request.model_dump()["columns"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/execute", response_model=ExecuteResponse, summary="5. Deploy Tables to Databricks")
async def execute(request: ExecuteRequest, flow_c_session_id: str = Cookie(None)):
    if not flow_c_session_id:
        raise HTTPException(status_code=401, detail="Session cookie missing.")
    try:
        return execute_service(flow_c_session_id, request.final_table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))