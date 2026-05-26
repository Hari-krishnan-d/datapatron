import subprocess
import os
from typing import Any
from API.schemas.mca_schema import UserSelection

# Global variable to hold Phase 1 memory automatically
CURRENT_PIPELINE_STATE = None

def handle_phase1(data: UserSelection):
    global CURRENT_PIPELINE_STATE
    # Memorize the exact Phase 1 inputs the user just sent
    CURRENT_PIPELINE_STATE = data
    return {"status": "ready"}

def execute_phase2_pipeline(phase2_data: Any):
    """
    Accepts ANY flow model (FlowA - FlowK) and dynamically extracts the available 
    credentials to build the terminal input string.
    """
    global CURRENT_PIPELINE_STATE
    
    # Check if Phase 1 was actually run first
    if CURRENT_PIPELINE_STATE is None:
        raise ValueError("No pipeline intent found! Please run the Phase 1 endpoint first.")

    phase1_data = CURRENT_PIPELINE_STATE
    inputs = []
    
    # ─── PHASE 1: INTENT & CLASSIFICATION (Pulled from API memory) ─────────────
    inputs.append(phase1_data.user_request or "build pipeline")
    flow = (phase1_data.flow_selection or "A").upper()
    inputs.append(flow)
    
    s3_count = str(phase1_data.s3_bucket_count or 1)
    sql_count = str(phase1_data.sql_db_count or 1)

    # Automatically answer the source count/type prompts based on the flow
    if flow in ("A", "D"):
        inputs.append(s3_count)
    elif flow in ("B", "E"):
        inputs.append(sql_count)
    elif flow in ("F", "G"):
        inputs.append(s3_count)
        inputs.append(sql_count)
    elif flow in ("H", "I"):
        gold_src = str(phase1_data.gold_bronze_source or "1")
        inputs.append(gold_src)
        if gold_src == "1":
            inputs.append(s3_count)
        elif gold_src == "2":
            inputs.append(sql_count)
        elif gold_src == "3":
            inputs.append(s3_count)
            inputs.append(sql_count)

    # ─── PHASE 2: DYNAMIC CREDENTIAL EXTRACTION ────────────────────────────────
    
    # Helper function to safely grab a field if it exists in the Pydantic model
    def get_val(field_name, default=""):
        val = getattr(phase2_data, field_name, default)
        return str(val) if val is not None else default

    # 1. Source Credentials (S3 / SQL) if the specific Flow class has them
    if hasattr(phase2_data, "s3_credentials") and phase2_data.s3_credentials:
        for s3_cred in phase2_data.s3_credentials:
            inputs.append(str(s3_cred.s3_bucket or ""))
            inputs.append(str(s3_cred.s3_region or ""))
            # Assuming prefix and file_format might be needed by the CLI prompt
            inputs.append(str(getattr(s3_cred, 'prefix', '')))
            inputs.append(str(getattr(s3_cred, 'file_format', '')))
            inputs.append(str(s3_cred.aws_access_key or ""))
            inputs.append(str(s3_cred.aws_secret_key or ""))

    if hasattr(phase2_data, "sql_credentials") and phase2_data.sql_credentials:
        for sql_cred in phase2_data.sql_credentials:
            inputs.append(str(sql_cred.sql_server or ""))
            inputs.append(str(sql_cred.sql_database or ""))
            inputs.append(str(sql_cred.sql_username or ""))
            inputs.append(str(sql_cred.sql_password or ""))
    # 2. Base Databricks Credentials (Required by all flows)
    inputs.append(get_val("databricks_workspace_url"))
    inputs.append(get_val("databricks_warehouse_id"))
    inputs.append(get_val("databricks_token"))
    
    # 3. Bronze Targets (If applicable to the flow)
    if hasattr(phase2_data, "bronze_catalog"):
        inputs.append(get_val("bronze_catalog"))
        inputs.append(get_val("bronze_schema"))
        inputs.append(get_val("bronze_table"))
    
    # 4. Silver Targets (If applicable to the flow)
    if hasattr(phase2_data, "silver_catalog"):
        inputs.append(get_val("silver_catalog"))
        inputs.append(get_val("silver_schema"))

    # 5. Gold Targets (If applicable to the flow, like Flow H, I, J, K)
    if hasattr(phase2_data, "gold_catalog"):
        inputs.append(get_val("gold_catalog"))
        inputs.append(get_val("gold_schema"))

    # ─── BYPASS MENUS AND EXECUTE ──────────────────────────────────────────────
    
    # Add extra Enters to bypass hidden prompts (including "X" for Mapping Agent)
    inputs.extend(["X", "X", "X", "", ""])
    
    simulated_terminal_input = "\n".join(inputs) + "\n"

    # WIPE THE MEMORY CLEAN so the next pipeline run starts fresh
    CURRENT_PIPELINE_STATE = None
    
    print("\n=== API PAYLOAD SENT TO TERMINAL ===", flush=True)
    print(repr(simulated_terminal_input), flush=True)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))

    safe_root = project_root.replace("\\", "/")
    safe_main = os.path.join(safe_root, "main.py").replace("\\", "/")

    # Robust wrapper script to catch and print specific Python errors
    wrapper_script = f'''
import sys, getpass, builtins, runpy, traceback
sys.path.insert(0, "{safe_root}")
getpass.getpass = builtins.input
try:
    runpy.run_path("{safe_main}", run_name="__main__")
except Exception:
    print("=== BACKGROUND SCRIPT CRASHED ===")
    traceback.print_exc()
'''

    try:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        process = subprocess.Popen(
            ["python", "-u", "-c", wrapper_script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env=env,
            cwd=project_root 
        )

        stdout, stderr = process.communicate(input=simulated_terminal_input, timeout=120)

        full_output = (stdout if stdout else "") + "\n" + (stderr if stderr else "")

        return {
            "output": full_output.strip() if full_output.strip() else "Process exited completely empty.",
            "error": ""
        }
        
    except subprocess.TimeoutExpired:
        process.kill()
        return {"output": "", "error": "Process timed out."}
    except Exception as e:
        return {"output": "", "error": str(e)}