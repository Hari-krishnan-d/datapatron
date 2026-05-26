from fastapi import APIRouter, HTTPException
from API.schemas.mca_schema import (
    UserSelection,
    FlowA, FlowB, FlowC, FlowD, FlowE, FlowF, 
    FlowG, FlowH, FlowI, FlowJ, FlowK
)
from API.services.mca_service import handle_phase1, execute_phase2_pipeline

router = APIRouter()

# ══════════════════════════════════════════════════════════════════════════
# PHASE 1: INTENT & SOURCING
# ══════════════════════════════════════════════════════════════════════════

@router.post("/phase1-intent", summary="Step 1: Define Intent & Flow")
def phase1_intent(data: UserSelection):
    result = handle_phase1(data)
    return {
        "status": "success",
        "message": "Phase 1 pipeline configured in background. You may now execute the specific Phase 2 Flow endpoint."
    }

# ══════════════════════════════════════════════════════════════════════════
# PHASE 2: INDIVIDUAL FLOW EXECUTIONS
# ══════════════════════════════════════════════════════════════════════════

def _run_phase2(data):
    """Helper function to execute the pipeline and catch memory state errors."""
    try:
        result = execute_phase2_pipeline(data)
        return {"status": "success", "result": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/phase2/flow-a", summary="Flow A: S3 → Bronze")
def execute_flow_a(data: FlowA):
    return _run_phase2(data)

@router.post("/phase2/flow-b", summary="Flow B: Azure SQL → Bronze")
def execute_flow_b(data: FlowB):
    return _run_phase2(data)

@router.post("/phase2/flow-c", summary="Flow C: Databricks → Silver")
def execute_flow_c(data: FlowC):
    return _run_phase2(data)

@router.post("/phase2/flow-d", summary="Flow D: S3 → Bronze → Silver")
def execute_flow_d(data: FlowD):
    return _run_phase2(data)

@router.post("/phase2/flow-e", summary="Flow E: Azure SQL → Bronze → Silver")
def execute_flow_e(data: FlowE):
    return _run_phase2(data)

@router.post("/phase2/flow-f", summary="Flow F: Hybrid (S3 + SQL) → Bronze")
def execute_flow_f(data: FlowF):
    return _run_phase2(data)

@router.post("/phase2/flow-g", summary="Flow G: Hybrid → Bronze → Silver")
def execute_flow_g(data: FlowG):
    return _run_phase2(data)

@router.post("/phase2/flow-h", summary="Flow H: Source → Bronze → Silver → Gold Data Model")
def execute_flow_h(data: FlowH):
    return _run_phase2(data)

@router.post("/phase2/flow-i", summary="Flow I: Source → Bronze → Silver → KPI Tables")
def execute_flow_i(data: FlowI):
    return _run_phase2(data)

@router.post("/phase2/flow-j", summary="Flow J: Existing Silver → Gold Data Model")
def execute_flow_j(data: FlowJ):
    return _run_phase2(data)

@router.post("/phase2/flow-k", summary="Flow K: Existing Silver → KPI Tables")
def execute_flow_k(data: FlowK):
    return _run_phase2(data)