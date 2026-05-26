"""
kpi_agent.py — KPI Generation Pipeline
=======================================
FIX: Removed module-level import of DATABRICKS_ACCESS_TOKEN / DATABRICKS_HOSTNAME
     from metadata.py (those constants no longer exist there).
     Credentials are now injected once via init_kpi_agent() called from main.py,
     matching the same lazy-init pattern used by data_model.py.

FIXES APPLIED
-------------
FIX-1  _get_client() previously raised a generic RuntimeError with no guidance.
        Now raises a clear message telling the developer exactly what to call
        and where, so the failure is immediately actionable.

FIX-2  init_kpi_agent() now validates all three credentials before building the
        OpenAI client, using the same _validate_credentials() logic as metadata.py.
        This surfaces misconfigured values (wrong hostname, bad token, missing
        model name) at init time, not buried inside a thread or LLM call.

FIX-3  save_kpis() previously fell back to a hardcoded Windows path
        `C:\\Users\\Admin\\Documents\\...` when KPI_OUTPUT_DIR env var was not set.
        This caused FileNotFoundError on every non-Windows machine, any other
        Windows user account, and all deployment environments.

        Fixed to resolve a portable path relative to this file's location
        (gold_layer/ → ../session_output/KPIs/) as the default fallback.

        save_kpis() also now accepts an optional `output_dir` argument so the
        orchestrator can explicitly pass its own KPI_DIR constant, giving the
        orchestrator full control over where outputs land.

FIX-4  Hostname is sanitised (strips accidental https:// / trailing slash)
        in init_kpi_agent() so base_url is always well-formed regardless of
        what the caller passes in.
"""

import json
import os
import re
import csv
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# ── Module-level credential store (populated by init_kpi_agent) ───────────────
_HOSTNAME:     str = ""
_ACCESS_TOKEN: str = ""
_MODEL_NAME:   str = os.getenv("MODEL_NAME", "")

# Databricks LLM credentials (may differ from SQL warehouse creds)
_LLM_HOST:  str = ""
_LLM_TOKEN: str = ""

_client: "OpenAI | None" = None


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _sanitise_hostname(hostname: str) -> str:
    """
    Strip any accidental protocol prefix / trailing slash so the hostname
    can be safely embedded in  f"https://{hostname}/serving-endpoints".

    Examples
    --------
    "https://dbc-abc.cloud.databricks.com/"  →  "dbc-abc.cloud.databricks.com"
    "dbc-abc.cloud.databricks.com"           →  "dbc-abc.cloud.databricks.com"
    """
    h = hostname.strip()
    for prefix in ("https://", "http://"):
        if h.startswith(prefix):
            h = h[len(prefix):]
    return h.rstrip("/")


def _validate_init_credentials(
    hostname: str,
    access_token: str,
    model_endpoint: str,
) -> None:
    """
    Raise ValueError with a clear message if any credential looks wrong.
    Called inside init_kpi_agent() so problems surface at startup,
    not buried inside suggest_kpis() or generate_kpi_from_name().
    """
    errors = []

    if not hostname or hostname.strip() in ("", "YOUR_HOSTNAME"):
        errors.append(
            "hostname is empty or not set. "
            "Expected format: dbc-xxxxxxxx.cloud.databricks.com"
        )

    if not access_token or not access_token.startswith("dapi"):
        errors.append(
            "access_token looks invalid (must start with 'dapi'). "
            "Generate a new token: Databricks → User Settings → Developer → Access Tokens."
        )

    if not model_endpoint or model_endpoint.strip() in ("", "YOUR_MODEL"):
        errors.append(
            "model_endpoint is empty. "
            "Expected: exact serving endpoint name shown in Databricks → Serving "
            "(e.g. 'databricks-llama-4-maverick')."
        )

    if errors:
        msg = "\n".join(f"  ✗ {e}" for e in errors)
        raise ValueError(
            f"\n[kpi_agent] init_kpi_agent() credential validation failed:\n{msg}\n"
            "Fix the values above in main.py (or wherever you call init_kpi_agent) and retry."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Public init — call once from main.py before kpi_pipeline_flow()
# ─────────────────────────────────────────────────────────────────────────────

def init_kpi_agent(
    hostname: str,
    access_token: str,
    model_endpoint: str = "",
    llm_host: str = "",
    llm_token: str = "",
) -> None:
    """
    Inject Databricks / LLM credentials into this module.

    When groq_api_key is provided, all LLM calls go to Groq.
    Databricks credentials are still used for context (hostname stored).

    Parameters
    ----------
    hostname       : Databricks workspace hostname.
    access_token   : Personal access token (starts with 'dapi').
    model_endpoint : Groq model name or Databricks endpoint name.
    groq_api_key   : If set, uses Groq instead of Databricks for LLM.
    groq_base_url  : Groq API base URL (default: https://api.groq.com/openai/v1)
    """
    global _HOSTNAME, _ACCESS_TOKEN, _MODEL_NAME, _LLM_HOST, _LLM_TOKEN, _client

    hostname           = _sanitise_hostname(hostname)
    resolved_model     = model_endpoint or os.getenv("MODEL_NAME", "")
    resolved_llm_host  = _sanitise_hostname(llm_host) if llm_host else hostname
    resolved_llm_token = llm_token if llm_token else access_token

    _validate_init_credentials(hostname, access_token, resolved_model)
    _client = OpenAI(
        api_key  = resolved_llm_token,
        base_url = f"https://{resolved_llm_host}/serving-endpoints",
    )
    print(f"[kpi_agent] ✅ Using Databricks LLM — endpoint: {resolved_model}")

    _HOSTNAME     = hostname
    _ACCESS_TOKEN = access_token
    _MODEL_NAME   = resolved_model
    _LLM_HOST     = resolved_llm_host
    _LLM_TOKEN    = resolved_llm_token


def _get_client() -> OpenAI:
    """
    Return the shared OpenAI client.
    Raises a clear, actionable RuntimeError if init_kpi_agent() was never called.
    """
    if _client is None:
        raise RuntimeError(
            "\n[kpi_agent] _get_client() called before initialisation.\n"
            "Add the following call in main.py BEFORE run_pipeline():\n\n"
            "    from gold_layer.kpi_agent import init_kpi_agent\n"
            "    init_kpi_agent(\n"
            "        hostname       = DATABRICKS_HOST,\n"
            "        access_token   = DATABRICKS_TOKEN,\n"
            "        model_endpoint = MODEL_NAME,\n"
            "    )\n"
        )
    return _client


# ── Default output directory (portable, relative to this file) ────────────────
# gold_layer/kpi_agent.py  →  ../session_output/KPIs
_DEFAULT_KPI_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "session_output",
    "KPIs",
)


# =============================
# Utility: Extract JSON
# =============================
def extract_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
        raise ValueError(f"No valid JSON found in response. Raw output:\n{text[:500]}")


# =============================
# Utility: Slim Metadata
# =============================
def slim_metadata_for_kpi(metadata: dict) -> dict:
    """
    Keeps only table names, column names, types, and roles.
    Prevents token overflow and LLM confusion from large metadata blobs.
    """
    if not metadata:
        print("⚠ Metadata is empty or None — cannot slim.")
        return {}

    slimmed = {}
    for table, details in metadata.items():
        cols = details.get("columns", [])
        slimmed[table] = {
            "columns": [
                {
                    "column": col.get("column") or col.get("name", "unknown"),
                    "type":   col.get("type", "STRING"),
                    "role":   col.get("role", "unknown"),
                }
                for col in cols
            ]
        }
    return slimmed


# =============================
# Step 1: Suggest KPIs
# =============================
def suggest_kpis(metadata: dict) -> dict:
    prompt = f"""
You are a data analytics expert.

Based on the GOLD layer metadata below, suggest 5-8 meaningful business KPIs.

STRICT RULES:
- Look at fact tables for measure columns (numeric: counts, amounts, scores, durations)
- Look at dimension tables for slicing and grouping context
- Suggest KPIs relevant to the domain (e.g. Total Medals, Medal Count by Country, Athlete Performance, etc.)
- Each KPI must reference real table/column names from the metadata
- Output ONLY valid JSON, no explanation, no markdown

Metadata:
{json.dumps(metadata, indent=2, default=str)}

Return ONLY this JSON structure:
{{
    "kpis": [
        {{
            "name": "Total Medals Won",
            "description": "Total number of medals won across all events",
            "formula": "SUM(fact_table.medal_count)",
            "tables": ["fact_table", "dim_athlete"],
            "dimensions": ["athlete_name", "nationality"]
        }}
    ]
}}
"""
    try:
        response = _get_client().chat.completions.create(
            model       = _MODEL_NAME,
            messages    = [{"role": "user", "content": prompt}],
            temperature = 0.3,
            max_tokens  = 4000,
        )
    except Exception as exc:
        raise RuntimeError(
            f"[kpi_agent] LLM call failed. Model: {_MODEL_NAME}. Error: {exc}"
        ) from exc
    content = response.choices[0].message.content
    if isinstance(content, list):
        content = " ".join(
            b.get("text", "") for b in content
            if isinstance(b, dict) and b.get("type") == "text"
        ).strip()

    print(f"\n[DEBUG] Raw KPI suggestion response (first 1000 chars):\n{content[:1000]}\n")

    result = extract_json(content)

    if not result.get("kpis"):
        print("⚠ LLM returned empty KPI list. Check metadata structure.")
        return {"kpis": []}

    return result


# =============================
# Step 2: Generate KPI by Name
# =============================
def generate_kpi_from_name(kpi_name: str, metadata: dict) -> dict:
    prompt = f"""
You are a data analytics expert.

A user wants to define this KPI:
"{kpi_name}"

Based on the GOLD metadata below:
{json.dumps(metadata, indent=2, default=str)}

Generate a complete KPI definition.

Rules:
- Identify the correct fact table(s)
- Identify the measure columns
- Build a SQL-style formula
- Suggest useful dimensions
- Be business-friendly in the description
- Output ONLY valid JSON, no explanation, no markdown

Return ONLY this JSON structure:
{{
    "name": "{kpi_name}",
    "description": "",
    "formula": "",
    "tables": [],
    "dimensions": []
}}
"""
    try:
        response = _get_client().chat.completions.create(
            model       = _MODEL_NAME,
            messages    = [{"role": "user", "content": prompt}],
            temperature = 0.3,
            max_tokens  = 2000,
        )
    except Exception as exc:
        raise RuntimeError(
            f"[kpi_agent] LLM call failed. Model: {_MODEL_NAME}. Error: {exc}"
        ) from exc
    content = response.choices[0].message.content
    if isinstance(content, list):
        content = " ".join(
            b.get("text", "") for b in content
            if isinstance(b, dict) and b.get("type") == "text"
        ).strip()
    return extract_json(content)


# =============================
# Step 3: Extract KPIs from File
# =============================
def extract_kpis_from_file(file_path: str) -> list:
    """
    Supports: .txt, .csv, .xlsx, .xls, .docx, .pdf
    Extracts KPI names (one per line / cell) and returns a list of strings.
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".txt":
        with open(file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]

    elif ext == ".csv":
        kpi_names = []
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                for cell in row:
                    val = cell.strip()
                    if val:
                        kpi_names.append(val)
        return kpi_names

    elif ext in (".xlsx", ".xls"):
        try:
            import openpyxl
            wb = openpyxl.load_workbook(file_path, read_only=True)
            ws = wb.active
            kpi_names = []
            for row in ws.iter_rows(values_only=True):
                for cell in row:
                    if cell and str(cell).strip():
                        kpi_names.append(str(cell).strip())
            return kpi_names
        except Exception:
            import pandas as pd
            engine = "xlrd" if ext == ".xls" else "openpyxl"
            df = pd.read_excel(file_path, engine=engine, header=None)
            return [
                str(v).strip()
                for v in df.values.flatten()
                if str(v).strip() and str(v) != "nan"
            ]

    elif ext == ".docx":
        from docx import Document
        doc = Document(file_path)
        return [para.text.strip() for para in doc.paragraphs if para.text.strip()]

    elif ext == ".pdf":
        try:
            from pypdf import PdfReader
        except ImportError:
            from PyPDF2 import PdfReader

        reader    = PdfReader(file_path)
        kpi_names = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                for line in text.splitlines():
                    line = line.strip()
                    if line:
                        kpi_names.append(line)
        return kpi_names

    else:
        raise ValueError(
            f"Unsupported file type: '{ext}'. "
            "Supported: .txt, .csv, .xlsx, .xls, .docx, .pdf"
        )


# =============================
# Step 4: Analyze & Confirm KPIs
# =============================
def analyze_and_confirm_kpis(kpi_names: list) -> list:
    print(f"\n---- Extracted {len(kpi_names)} KPI(s) ----")
    for i, name in enumerate(kpi_names, 1):
        print(f"  {i}. {name}")

    confirm = input("\nProceed with these KPIs? (yes/no): ").strip().lower()

    if confirm != "yes":
        print("⚠ Skipping these KPIs.")
        return []

    return kpi_names


# =============================
# Main KPI Pipeline
# =============================
def kpi_pipeline_flow(metadata: dict) -> dict:
    final_kpis = []

    slim_meta = slim_metadata_for_kpi(metadata)

    # Phase 1: AI Suggests KPIs
    print("\n Analyzing metadata and suggesting KPIs...")
    suggested = suggest_kpis(slim_meta)
    kpis_list = suggested.get("kpis", [])

    if not kpis_list:
        print("\n⚠ No KPIs were suggested by AI. Proceeding to manual input.")
    else:
        print("\n---- Suggested KPIs ----")
        for i, kpi in enumerate(kpis_list, 1):
            print(f"  {i}. {kpi['name']} — {kpi.get('description', '')}")

        decision = input("\nApprove suggested KPIs? (yes/no): ").strip().lower()

        if decision == "yes":
            final_kpis.extend(kpis_list)
            print(f"✅ {len(kpis_list)} suggested KPI(s) added.")

    # Phase 2: KPI Input Loop
    while True:
        print("\n==== KPI INPUT MODE ====")
        print("  1. Enter KPI name(s) manually")
        print("  2. Upload KPI file (.txt / .csv / .xlsx / .xls / .docx / .pdf)")
        print("  3. Exit")

        choice = input("\nChoose option: ").strip()

        if choice == "1":
            raw       = input("Enter KPI name(s), comma-separated: ").strip()
            kpi_names = [k.strip() for k in raw.split(",") if k.strip()]

            if not kpi_names:
                print("⚠ No KPI names entered.")
            else:
                confirmed = analyze_and_confirm_kpis(kpi_names)
                for name in confirmed:
                    print(f"🔄 Generating KPI: {name}")
                    try:
                        kpi = generate_kpi_from_name(name, slim_meta)
                        final_kpis.append(kpi)
                        print(f"  ✅ Done: {kpi['name']}")
                    except Exception as e:
                        print(f"  ❌ Failed to generate KPI '{name}': {e}")

        elif choice == "2":
            file_path = input("Enter file path: ").strip().strip('"')

            if not os.path.exists(file_path):
                print(f"❌ File not found: {file_path}")
            else:
                try:
                    kpi_names = extract_kpis_from_file(file_path)

                    if not kpi_names:
                        print("⚠ No KPI names found in the file.")
                    else:
                        confirmed = analyze_and_confirm_kpis(kpi_names)
                        for name in confirmed:
                            print(f"🔄 Generating KPI: {name}")
                            try:
                                kpi = generate_kpi_from_name(name, slim_meta)
                                final_kpis.append(kpi)
                                print(f"  ✅ Done: {kpi['name']}")
                            except Exception as e:
                                print(f"  ❌ Failed to generate KPI '{name}': {e}")

                except Exception as e:
                    print(f"❌ Error reading file: {e}")

        elif choice == "3":
            break

        else:
            print("⚠ Invalid option, please choose 1, 2, or 3.")
            continue

        cont = input("\nDo you want to continue adding more KPIs? (yes/no): ").strip().lower()
        if cont != "yes":
            break

    print(f"\n✅ Total KPIs collected: {len(final_kpis)}")
    return {"kpis": final_kpis}


# =============================
# Save KPIs to JSON
# =============================
def save_kpis(kpis: dict, catalog: str, schema: str, output_dir: str = "") -> str:
    """
    Persist the KPI dict to a JSON file.

    Parameters
    ----------
    kpis       : KPI dict returned by kpi_pipeline_flow()
    catalog    : Databricks catalog name (used in filename)
    schema     : Databricks schema name (used in filename)
    output_dir : Directory to write the file into.
                 Priority order:
                   1. output_dir argument  (set by gold_orchestrator.py → KPI_DIR)
                   2. KPI_OUTPUT_DIR environment variable
                   3. Portable default: <project_root>/session_output/KPIs/

    Returns
    -------
    str  Absolute path of the saved JSON file.
    """
    # FIX-3: portable path resolution — no more hardcoded C:\\Users\\Admin\\...
    base_path = (
        output_dir
        or os.getenv("KPI_OUTPUT_DIR")
        or _DEFAULT_KPI_DIR
    )
    os.makedirs(base_path, exist_ok=True)

    safe_catalog = catalog.replace(" ", "_")
    safe_schema  = schema.replace(" ", "_")
    file_path    = os.path.join(base_path, f"{safe_catalog}_{safe_schema}_kpis.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(kpis, f, indent=2, default=str)

    print(f"\n --- KPI file saved to: {file_path} ---")
    return file_path