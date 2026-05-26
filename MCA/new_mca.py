"""
MCA/new_mca.py — Master Control Agent
========================================
Detects the user's pipeline flow from plain English and builds a structured plan.

Supported flows:
  FLOW_A  →  S3          → Bronze                      (raw load only)
  FLOW_B  →  Azure SQL   → Bronze                      (raw load only)
  FLOW_C  →  Databricks  → Silver                      (bronze already exists, transform only)
  FLOW_D  →  S3          → Bronze → Silver             (full flow: load raw then transform)
  FLOW_E  →  Azure SQL   → Bronze → Silver             (full flow: load raw then transform)
  FLOW_F  →  Mixed       → Bronze                      (load from S3 and SQL together)
  FLOW_G  →  Mixed       → Bronze → Silver             (load from S3 and SQL, then transform)

  ── Gold Layer Extensions ─────────────────────────────────────────────────────
  FLOW_H  →  Bronze Source → Bronze → Silver → Gold (Data Modeling)
             User picks source: S3 / SQL / Hybrid / Databricks
             S3        → mirrors Flow D path
             SQL       → mirrors Flow E path
             Hybrid    → mirrors Flow G path
             Databricks→ mirrors Flow C path (no raw ingest)
  FLOW_I  →  Bronze Source → Bronze → Silver → KPI Calculation
             User picks source: S3 / SQL / Hybrid / Databricks
             Same routing logic as Flow H
  FLOW_J  →  Silver (existing) → Gold (Data Modeling)
  FLOW_K  →  Silver (existing) → KPI Calculation

Detection order:
  1. IntentGate         — is this even about data pipelines? reject off-topic early.
  2. FlowClassifier     — which of the flows? high / medium / low confidence.
  3. Confidence routing — high → proceed | medium → confirm Y/N | low → A–K manual menu.
  4. PlanBuilder        — builds mca_plan.json with explicit source + destination connections.

LLM client: DatabricksLLMClient from llm.py (unchanged).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import uuid
import logging
from datetime import datetime, timezone

from llm import DatabricksLLMClient, LLMCallError, LLMParseError, extract_json

logger = logging.getLogger("MCA")

# ── Constants ─────────────────────────────────────────────────────────────────

VALID_MODES      = {"full", "incremental"}
VALID_SCHEDULES  = {"one_time", "hourly", "daily"}
REST_FORCED_MODE = "incremental"

# ── All flow labels (A–G unchanged, H–K new) ──────────────────────────────────
FLOW_LABELS = {
    "A": "S3          → Bronze                   (raw load from S3)",
    "B": "Azure SQL   → Bronze                   (raw load from Azure SQL)",
    "C": "Databricks  → Silver                   (transform existing bronze)",
    "D": "S3          → Bronze → Silver          (load from S3 then transform)",
    "E": "Azure SQL   → Bronze → Silver          (load from Azure SQL then transform)",
    "F": "Mixed (S3 + SQL) → Bronze              (load from multiple source types)",
    "G": "Mixed (S3 + SQL) → Bronze → Silver     (load from multiple types then transform)",
    # ── Gold extensions ───────────────────────────────────────────────────────
    "H": "Bronze Source → Bronze → Silver → Gold Data Modeling",
    "I": "Bronze Source → Bronze → Silver → KPI Calculation",
    "J": "Silver (existing) → Gold Data Modeling",
    "K": "Silver (existing) → KPI Calculation",
}

# Maps flow → which source handler(s) are needed
FLOW_SOURCE_TYPES = {
    "A": ["s3"],
    "B": ["azure_sql"],
    "C": ["databricks_bronze"],
    "D": ["s3"],
    "E": ["azure_sql"],
    "F": ["s3", "azure_sql"],
    "G": ["s3", "azure_sql"],
    # Gold flows — source types resolved interactively for H/I; J/K use silver
    "H": ["bronze_source"],          # resolved at runtime (S3/SQL/Hybrid/Databricks)
    "I": ["bronze_source"],          # resolved at runtime
    "J": ["databricks_silver"],      # reads from existing silver
    "K": ["databricks_silver"],      # reads from existing silver
}

# Maps flow → pipeline_type tag used by orchestrator
FLOW_PIPELINE_TYPE = {
    "A": "bronze",
    "B": "bronze",
    "C": "silver",
    "D": "bronze_silver",
    "E": "bronze_silver",
    "F": "bronze",
    "G": "bronze_silver",
    "H": "bronze_silver_gold_model",
    "I": "bronze_silver_gold_kpi",
    "J": "silver_gold_model",
    "K": "silver_gold_kpi",
}

# Maps flow → human-readable data flow string
FLOW_DESCRIPTION = {
    "A": "s3 → bronze",
    "B": "azure_sql → bronze",
    "C": "databricks_bronze → silver",
    "D": "s3 → bronze → silver",
    "E": "azure_sql → bronze → silver",
    "F": "mixed → bronze",
    "G": "mixed → bronze → silver",
    "H": "bronze_source → bronze → silver → gold_model",
    "I": "bronze_source → bronze → silver → gold_kpi",
    "J": "silver → gold_model",
    "K": "silver → gold_kpi",
}

# ── Gold-layer bronze source types ────────────────────────────────────────────
GOLD_BRONZE_SOURCES = {
    "1": "s3",
    "2": "sql",
    "3": "hybrid",   # S3 + SQL
    "4": "databricks",
}

GOLD_BRONZE_SOURCE_LABELS = {
    "1": "S3             (AWS S3 buckets)                    → mirrors Flow D",
    "2": "SQL            (Azure SQL / relational database)   → mirrors Flow E",
    "3": "Hybrid         (S3 + SQL combined)                 → mirrors Flow G",
    "4": "Databricks     (existing Databricks bronze tables) → mirrors Flow C",
}


# =============================================================================
# EXCEPTIONS
# =============================================================================

class ValidationError(Exception):
    pass

class FlowClassificationError(Exception):
    pass


# =============================================================================
# INTENT GATE  — UNCHANGED
# Rejects off-topic input before any pipeline logic runs.
# =============================================================================

class IntentGate:

    SYSTEM_PROMPT = (
        "You are a strict intent classifier for a data pipeline platform. "
        "Your ONLY job: decide if the message is about data ingestion, migration, "
        "transformation, or pipeline setup involving S3, Azure SQL, PostgreSQL, "
        "REST APIs, Databricks, or similar data engineering topics.\n\n"
        "Rules:\n"
        "1. Related to data engineering → return ONLY:\n"
        '   {"relevant": true, "message": ""}\n'
        "2. Greeting, small talk, unrelated → return ONLY:\n"
        '   {"relevant": false, "message": "<helpful redirect>"}\n'
        "3. The redirect message must tell the user what they CAN ask about.\n"
        "4. Return VALID JSON ONLY. No markdown, no explanation."
    )

    def __init__(self, llm: DatabricksLLMClient):
        self.llm = llm

    def check(self, user_input: str) -> tuple:
        """Returns (is_relevant: bool, redirect_message: str)."""
        try:
            raw    = self.llm.call(self.SYSTEM_PROMPT, user_input)
            result = extract_json(raw)
            return bool(result.get("relevant", False)), result.get("message", "")
        except (LLMCallError, LLMParseError):
            logger.warning("IntentGate LLM call failed — failing open.")
            return True, ""


# =============================================================================
# FLOW CLASSIFIER  — A–G logic UNCHANGED; H–K detection added
# =============================================================================

class FlowClassifier:
    """
    Classifies user input into FLOW_A through FLOW_K.

    Returns:
    {
        "flow"             : "A" | "B" | ... | "K",
        "source_types"     : ["s3", "azure_sql", ...],
        "ingestion_mode"   : "full" | "incremental",
        "schedule"         : "one_time" | "hourly" | "daily",
        "confidence"       : "high" | "medium" | "low",
        "reason"           : "one sentence",
        "confirmed_by"     : "llm" | "user_confirmed" | "user_manual",
        # Gold-only extras (H/I only):
        "gold_bronze_source" : "s3" | "sql" | "hybrid" | "databricks" | None,
        "gold_bronze_source_detail" : { ... }  # bucket/db counts
    }
    """

    SYSTEM_PROMPT = """
You are an expert data pipeline flow classifier.

Your ONLY job: read what the user wants and classify it into exactly ONE
of the supported pipeline flows below.

============================
SUPPORTED FLOWS  (A–G unchanged)
============================

FLOW A — S3 → Bronze
  User wants to LOAD / INGEST / MIGRATE / COPY raw data from AWS S3
  into a Databricks bronze layer. Data arrives as-is. No transformation.

FLOW B — Azure SQL → Bronze
  User wants to LOAD / INGEST / MIGRATE / COPY raw data from Azure SQL
  into a Databricks bronze layer. Data arrives as-is. No transformation.

FLOW C — Databricks Bronze → Silver  (existing bronze)
  User ALREADY HAS a bronze layer in Databricks.
  They want to TRANSFORM / CLEAN / PROCESS that data into a silver layer.
  Key signals: "already have", "existing bronze", "transform only",
               "just transform", "only silver", "bronze is ready".

FLOW D — S3 → Bronze → Silver  (full flow)
  User wants to:
    FIRST  load raw data from S3 into bronze
    THEN   transform that bronze data into a silver layer

FLOW E — Azure SQL → Bronze → Silver  (full flow)
  Same as FLOW D but source is Azure SQL.

FLOW F — Mixed (S3 + SQL) → Bronze
  User wants to load from multiple source types into bronze only.

FLOW G — Mixed (S3 + SQL) → Bronze → Silver
  User wants to load from multiple sources then transform to silver.

============================
GOLD LAYER FLOWS  (NEW: H–K)
============================

FLOW H — Bronze Source → Bronze → Silver → Gold Data Modeling
  Key signals: "gold layer", "data model", "star schema", "fact table",
               "dimension table", "model the data", "gold modeling".
  AND user mentions loading from a source first (S3, SQL, or any).
  The bronze source will be clarified interactively.

FLOW I — Bronze Source → Bronze → Silver → KPI Calculation
  Key signals: "KPI", "metrics", "aggregate", "business metrics",
               "gold KPI", "calculate KPIs", "KPI layer".
  AND user mentions loading from a source first.

FLOW J — Silver (existing) → Gold Data Modeling
  Key signals: "gold layer", "star schema", "data model", "fact", "dimension"
  AND user says "already have silver", "silver exists", "silver is ready",
  "just model", "only gold", "skip bronze".

FLOW K — Silver (existing) → KPI Calculation
  Key signals: "KPI", "metrics", "aggregate"
  AND user says silver already exists / they want KPIs from existing silver.

============================
ADDITIONAL RULES
============================

1. source_types = a list of strings representing the type for each source.
2. ingestion_mode: "full" (default) | "incremental"
3. schedule: "one_time" (default) | "hourly" | "daily"
4. confidence: "high" | "medium" | "low"

============================
OUTPUT FORMAT
============================

Return ONLY valid JSON. No markdown. No backticks. No extra text.

{
  "flow"           : "A"|"B"|"C"|"D"|"E"|"F"|"G"|"H"|"I"|"J"|"K",
  "source_types"   : [<string>, ...],
  "ingestion_mode" : "full" | "incremental",
  "schedule"       : "one_time" | "hourly" | "daily",
  "confidence"     : "high" | "medium" | "low",
  "reason"         : "<one sentence explaining your decision>"
}
"""

    MANUAL_MENU = (
    "\n"
    "══════════════════════════════════════════════════════════════\n"
    "        🚀 MEDALLION DATA PIPELINE ORCHESTRATOR 🚀\n"
    "══════════════════════════════════════════════════════════════\n"
    "\n"
    "🥉 BRONZE / SILVER FLOWS\n"
    "──────────────────────────────────────────────────────────────\n"
    "\n"
    "  [A] ☁️  S3                  ➜ Bronze\n"
    "  [B] 🗄️  Azure SQL           ➜ Bronze\n"
    "  [C] ⚡ Databricks           ➜ Silver (Existing Bronze)\n"
    "\n"
    "  [D] ☁️  S3                  ➜ Bronze ➜ Silver\n"
    "  [E] 🗄️  Azure SQL           ➜ Bronze ➜ Silver\n"
    "  [F] 🔀 Hybrid (S3 + SQL)    ➜ Bronze\n"
    "  [G] 🔀 Hybrid (S3 + SQL)    ➜ Bronze ➜ Silver\n"
    "\n"
    "🥇 GOLD LAYER FLOWS\n"
    "──────────────────────────────────────────────────────────────\n"
    "\n"
    "  [H] 🌟 Source ➜ Bronze ➜ Silver ➜ Gold Data Model\n"
    "  [I] 📊 Source ➜ Bronze ➜ Silver ➜ KPI Tables\n"
    "  [J] 🧩 Existing Silver ➜ Gold Data Model\n"
    "  [K] 📈 Existing Silver ➜ KPI Tables\n"
    "\n"
    "══════════════════════════════════════════════════════════════\n"
    "   👉 SELECT PIPELINE OPTION : [ A - K ]\n"
    "══════════════════════════════════════════════════════════════\n"
    )

    def __init__(self, llm: DatabricksLLMClient):
        self.llm = llm

    # ── PUBLIC ────────────────────────────────────────────────────────────────

    def classify(self, user_input: str) -> dict:
        logger.info("FlowClassifier: classifying user input.")
        try:
            result = self._call_llm(user_input)
            logger.info(
                "LLM → flow='%s' sources=%d confidence='%s' reason='%s'",
                result["flow"], len(result["source_types"]),
                result["confidence"], result["reason"],
            )
        except (LLMCallError, LLMParseError, FlowClassificationError) as exc:
            logger.warning("Flow classification failed (%s) — manual fallback.", exc)
            return self._manual_menu("I could not automatically determine your pipeline flow.")

        if result["confidence"] == "high":
            # For H/I flows, collect bronze source interactively after classification
            if result["flow"] in ("H", "I"):
                result = self._collect_gold_bronze_source(result)
            return result
        if result["confidence"] == "medium":
            return self._confirm(result)
        return self._manual_menu(
            f"I think this might be Flow {result['flow']} but I am not certain. "
            f"{result['reason']}"
        )

    # ── LLM CALL ──────────────────────────────────────────────────────────────

    def _call_llm(self, user_input: str) -> dict:
        prompt = (
            "Classify the following user request into the correct pipeline flow.\n"
            "Think carefully about the SOURCE of the data and whether transformation is needed.\n\n"
            f'User request:\n"""\n{user_input}\n"""\n\n'
            "Return only the JSON classification. No extra text."
        )
        raw    = self.llm.call(self.SYSTEM_PROMPT, prompt)
        result = extract_json(raw)
        return self._validate(result)

    def _validate(self, result: dict) -> dict:
        valid_flows = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"}
        valid_conf  = {"high", "medium", "low"}

        flow = str(result.get("flow", "")).upper().strip()
        conf = str(result.get("confidence", "")).lower().strip()

        if flow not in valid_flows:
            raise FlowClassificationError(f"Invalid flow '{flow}'")
        if conf not in valid_conf:
            raise FlowClassificationError(f"Invalid confidence '{conf}'")

        source_types = result.get("source_types", [])
        if not source_types:
            flow_char = str(result.get("flow", "A")).upper()
            source_types = [FLOW_SOURCE_TYPES.get(flow_char, ["s3"])[0]]

        ingestion_mode = result.get("ingestion_mode", "full").lower()
        if ingestion_mode not in VALID_MODES:
            ingestion_mode = "full"

        schedule = result.get("schedule", "one_time").lower()
        if schedule not in VALID_SCHEDULES:
            schedule = "one_time"

        return {
            "flow"               : flow,
            "source_types"       : source_types,
            "ingestion_mode"     : ingestion_mode,
            "schedule"           : schedule,
            "confidence"         : conf,
            "reason"             : str(result.get("reason", "")).strip(),
            "confirmed_by"       : "llm",
            # Gold extras — populated later for H/I
            "gold_bronze_source" : None,
            "gold_bronze_source_detail": {},
        }

    # ── MEDIUM CONFIDENCE — confirm Y/N ───────────────────────────────────────

    def _confirm(self, result: dict) -> dict:
        flow  = result["flow"]
        label = FLOW_LABELS.get(flow, flow)

        print(f"\n  🤔 I think you want: Flow {flow} — {label}")
        print(f"     Reason : {result['reason']}")
        print(f"     Sources detected : {', '.join(result['source_types'])}")
        print(f"\n  Is this correct?")
        print(f"  Y = Yes, proceed   |   N = Let me pick manually")
        print(f"  > ", end="")

        while True:
            a = input().strip().upper()
            if a == "Y":
                result["confirmed_by"] = "user_confirmed"
                if result["flow"] in ("H", "I"):
                    result = self._collect_gold_bronze_source(result)
                return result
            if a == "N":
                return self._manual_menu("No problem — let me ask directly.")
            print("  Please enter Y or N: ", end="")

    # ── LOW CONFIDENCE — A to K menu ─────────────────────────────────────────

    def _manual_menu(self, reason: str = "") -> dict:
        if reason:
            print(f"\n  ℹ️  {reason}")
        print(self.MANUAL_MENU)          # no f-string, no extra "\n  " prefix

        valid_choices = set(FLOW_LABELS.keys())

        while True:
            a = input().strip().upper()
            if a in valid_choices:
                source_types = []

                # ── Original A–G source collection (unchanged) ─────────────
                if a in ("A", "D"):
                    count = self._ask_count("S3 buckets")
                    source_types = ["s3"] * count
                elif a in ("B", "E"):
                    count = self._ask_count("databases")
                    source_types = ["azure_sql"] * count
                elif a in ("F", "G"):
                    s3_count  = self._ask_count("S3 buckets")
                    sql_count = self._ask_count("databases")
                    source_types = (["s3"] * s3_count) + (["azure_sql"] * sql_count)
                elif a == "C":
                    source_types = ["databricks_bronze"]

                # ── New Gold flows ─────────────────────────────────────────
                elif a in ("H", "I"):
                    # Bronze source selection — asked interactively
                    source_types = ["bronze_source"]  # placeholder; resolved below
                elif a in ("J", "K"):
                    source_types = ["databricks_silver"]

                logger.info("User manually selected flow: %s, sources: %s", a, source_types)

                result = {
                    "flow"               : a,
                    "source_types"       : source_types,
                    "ingestion_mode"     : "incremental" if a in ("C", "J", "K") else "full",
                    "schedule"           : "one_time",
                    "confidence"         : "high",
                    "reason"             : f"User manually selected Flow {a}.",
                    "confirmed_by"       : "user_manual",
                    "gold_bronze_source" : None,
                    "gold_bronze_source_detail": {},
                }

                # Collect bronze source details for H/I
                if a in ("H", "I"):
                    result = self._collect_gold_bronze_source(result)

                return result

            print("  Please enter A, B, C, D, E, F, G, H, I, J, or K: ", end="")

    def _ask_count(self, label: str) -> int:
        while True:
            try:
                raw   = input(f"    How many {label}? [default: 1]: ").strip()
                count = int(raw) if raw else 1
                if count >= 0:
                    return count
                print("    ⚠  Must be at least 0.")
            except ValueError:
                print("    ⚠  Please enter a number.")

    # ── Gold bronze source selection (H / I only) ─────────────────────────────

    def _collect_gold_bronze_source(self, result: dict) -> dict:
        """
        For flows H and I: ask the user which bronze source type they have,
        then ask how many buckets / databases depending on choice.

        Source routing:
          1 → S3         → will mirror Flow D (S3 → Bronze → Silver → Gold)
          2 → SQL        → will mirror Flow E (SQL → Bronze → Silver → Gold)
          3 → Hybrid     → will mirror Flow G (Mixed → Bronze → Silver → Gold)
          4 → Databricks → will mirror Flow C (existing bronze → Silver → Gold,
                           no raw ingest step)
        """
        print("\n  " + "─" * 60)
        print("  BRONZE SOURCE SELECTION  (for Gold pipeline)")
        print("  " + "─" * 60)
        print("  Where does your raw data live?\n")
        for key, label in GOLD_BRONZE_SOURCE_LABELS.items():
            print(f"    {key}) {label}")
        print()

        while True:
            choice = input("  Your choice (1 / 2 / 3 / 4): ").strip()
            if choice in GOLD_BRONZE_SOURCES:
                break
            print("  ⚠  Please enter 1, 2, 3, or 4.")

        src_type = GOLD_BRONZE_SOURCES[choice]  # "s3" | "sql" | "hybrid" | "databricks"
        detail   = {"type": src_type}
        source_types = []

        if src_type == "s3":
            n = self._ask_count("S3 buckets")
            source_types = ["s3"] * n
            detail["s3_bucket_count"] = n

        elif src_type == "sql":
            n = self._ask_count("SQL databases")
            source_types = ["azure_sql"] * n
            detail["sql_db_count"] = n

        elif src_type == "hybrid":
            s3_n  = self._ask_count("S3 buckets")
            sql_n = self._ask_count("SQL databases")
            source_types = (["s3"] * s3_n) + (["azure_sql"] * sql_n)
            detail["s3_bucket_count"]  = s3_n
            detail["sql_db_count"]     = sql_n

        elif src_type == "databricks":
            # Databricks = existing bronze already in place — no raw ingest
            source_types = ["databricks_bronze"]
            detail["databricks_bronze"] = True

        result["source_types"]              = source_types
        result["gold_bronze_source"]        = src_type
        result["gold_bronze_source_detail"] = detail

        routing_note = {
            "s3"        : "Flow D path  (S3 → Bronze → Silver → Gold)",
            "sql"       : "Flow E path  (SQL → Bronze → Silver → Gold)",
            "hybrid"    : "Flow G path  (Mixed → Bronze → Silver → Gold)",
            "databricks": "Flow C path  (existing Databricks bronze → Silver → Gold)",
        }.get(src_type, src_type)

        print(
            f"\n  ✅  Bronze source: {GOLD_BRONZE_SOURCE_LABELS[choice].split('→')[0].strip()}"
            f"\n      Routing      : {routing_note}"
            f"\n      Connections  : {len(source_types)}"
        )
        return result


# =============================================================================
# PLAN BUILDER
# =============================================================================

class PlanBuilder:
    """
    Builds the mca_plan dict from a flow classification result.
    A–G behaviour is identical to before.
    H–K add gold_pipeline entries.

    Key fix for H/I + databricks source:
      When gold_bronze_source == "databricks", there is no raw ingestion step.
      active_pipelines is set to silver_pipelines (mirrors Flow C behaviour)
      so the orchestrator doesn't try to validate databricks_bronze as an
      ingest source.
    """

    def build(self, session_id: str, classification: dict) -> dict:
        flow          = classification["flow"]
        source_types  = classification["source_types"]
        pipeline_type = FLOW_PIPELINE_TYPE[flow]

        # Gold bronze source (H/I only — None for all other flows)
        gold_bronze_source = classification.get("gold_bronze_source")

        # ── Bronze pipelines (flows A, B, D, E, F, G, H, I) ──────────────────
        # For H/I + databricks: skip bronze pipelines — data already exists
        bronze_pipelines = []
        if flow not in ("C", "J", "K"):
            if not (flow in ("H", "I") and gold_bronze_source == "databricks"):
                for idx, stype in enumerate(source_types, 1):
                    bronze_pipelines.append(
                        self._bronze_pipeline(session_id, idx, stype, classification)
                    )

        # ── Silver pipelines (flows C, D, E, G, H, I, J, K) ─────────────────
        silver_pipelines = []
        if flow in ("C", "D", "E", "G", "H", "I", "J", "K"):
            count = max(len(source_types), 1)
            for idx in range(1, count + 1):
                # bronze_ref only applies when there IS a preceding bronze step
                has_bronze_step = flow in ("D", "E", "G") or (
                    flow in ("H", "I") and gold_bronze_source != "databricks"
                )
                bronze_ref = (
                    f"{session_id}_bronze_{idx:03d}"
                    if has_bronze_step
                    else None
                )
                silver_pipelines.append(
                    self._silver_pipeline(session_id, idx, bronze_ref, flow)
                )

        # ── Gold pipelines (flows H, I, J, K) ────────────────────────────────
        gold_pipelines = []
        if flow in ("H", "I", "J", "K"):
            gold_pipelines.append(
                self._gold_pipeline(session_id, flow)
            )

        # Source connection specs — tells CRED what to ask for
        source_connections = self._source_connections(flow, source_types, gold_bronze_source)

        # Destination connection specs
        destination_connections = self._destination_connections(flow)

        # ── Active pipelines ──────────────────────────────────────────────────
        # "pipelines" is what the orchestrator reads for Step 2 (CRED).
        # Routing:
        #   C          → silver  (no bronze ingest)
        #   J, K       → silver  (silver is Phase 1 for J/K)
        #   H/I + dbx  → silver  (existing databricks bronze → mirrors Flow C)
        #   everything else → bronze
        if flow == "C":
            active_pipelines = silver_pipelines
        elif flow in ("J", "K"):
            active_pipelines = silver_pipelines
        elif flow in ("H", "I") and gold_bronze_source == "databricks":
            active_pipelines = silver_pipelines
        else:
            active_pipelines = bronze_pipelines

        plan = {
            "session_id"              : session_id,
            "created_at"              : datetime.now(timezone.utc).isoformat(),
            "flow_type"               : flow,
            "pipeline_type"           : pipeline_type,
            "flow"                    : FLOW_DESCRIPTION[flow],
            "classification"          : classification,
            "stage"                   : "awaiting_credentials",
            "source_connections"      : source_connections,
            "destination_connections" : destination_connections,
            "pipelines"               : active_pipelines,
        }

        if bronze_pipelines:
            plan["bronze_pipelines"] = bronze_pipelines
            plan["bronze_count"]     = len(bronze_pipelines)

        if silver_pipelines:
            plan["silver_pipelines"] = silver_pipelines
            plan["silver_count"]     = len(silver_pipelines)

        if gold_pipelines:
            plan["gold_pipelines"] = gold_pipelines
            plan["gold_count"]     = len(gold_pipelines)
            plan["gold_operation"] = (
                "data_modeling" if flow in ("H", "J") else "kpi_calculation"
            )

        # Gold source detail (H/I only)
        if flow in ("H", "I"):
            plan["gold_bronze_source"]        = gold_bronze_source
            plan["gold_bronze_source_detail"] = classification.get("gold_bronze_source_detail", {})

        plan["total_pipelines"] = (
            len(bronze_pipelines) + len(silver_pipelines) + len(gold_pipelines)
        )

        return plan

    # ── PIPELINE CONSTRUCTORS ─────────────────────────────────────────────────

    def _bronze_pipeline(
        self, session_id: str, idx: int, source_type: str, classification: dict
    ) -> dict:
        return {
            "pipeline_id"        : f"{session_id}_bronze_{idx:03d}",
            "layer"              : "bronze",
            "source_type"        : source_type,
            "ingestion_mode"     : classification.get("ingestion_mode", "full"),
            "schedule"           : classification.get("schedule", "one_time"),
            "execution_engine"   : "spark",
            "transform"          : False,
            "connection_template": self._connection_template(source_type),
            "destination"        : {
                "catalog": "bronze_catalog",
                "schema" : "raw_ingestion",
            },
            "status"     : "awaiting_credentials",
            "created_at" : datetime.now(timezone.utc).isoformat(),
        }

    def _silver_pipeline(
        self, session_id: str, idx: int, bronze_ref: str | None, flow: str
    ) -> dict:
        pipeline = {
            "pipeline_id"             : f"{session_id}_silver_{idx:03d}",
            "layer"                   : "silver",
            "source_type"             : "databricks_bronze",
            "ingestion_mode"          : "incremental",
            "schedule"                : "one_time",
            "execution_engine"        : "spark",
            "transform"               : True,
            "transformation_required" : True,
            "transformation_doc"      : None,
            "connection_template"     : self._connection_template("databricks_bronze"),
            "destination"             : {
                "catalog": "silver_catalog",
                "schema" : "clean_data",
            },
            "status"     : "awaiting_credentials",
            "created_at" : datetime.now(timezone.utc).isoformat(),
        }

        if bronze_ref:
            pipeline["bronze_source"] = {
                "pipeline_ref" : bronze_ref,
                "catalog"      : "bronze_catalog",
                "schema"       : "raw_ingestion",
                "table"        : None,
            }
            pipeline["status"] = "awaiting_bronze_completion"

        return pipeline

    def _gold_pipeline(self, session_id: str, flow: str) -> dict:
        operation = "data_modeling" if flow in ("H", "J") else "kpi_calculation"
        return {
            "pipeline_id"        : f"{session_id}_gold_001",
            "layer"              : "gold",
            "operation"          : operation,
            "source_type"        : "databricks_silver",
            "execution_engine"   : "spark",
            "connection_template": self._connection_template("databricks_silver"),
            "destination"        : {
                "catalog": "gold_catalog",
                "schema" : "gold_layer",
            },
            "status"     : "awaiting_silver_completion",
            "created_at" : datetime.now(timezone.utc).isoformat(),
        }

    # ── CONNECTION SPECS ──────────────────────────────────────────────────────

    def _source_connections(
        self, flow: str, source_types: list, gold_bronze_source: str | None = None
    ) -> list:
        layer_map = {"C": "bronze", "J": "silver", "K": "silver"}

        # H/I + databricks: the "source" is an existing Databricks bronze layer
        if flow in ("H", "I") and gold_bronze_source == "databricks":
            return [{"type": "databricks_bronze", "layer": "bronze", "index": 1}]

        return [
            {"type": stype, "layer": layer_map.get(flow), "index": i}
            for i, stype in enumerate(source_types, 1)
        ]

    def _destination_connections(self, flow: str) -> list:
        if flow in ("A", "B"):
            return [{"type": "databricks", "layer": "bronze"}]
        if flow == "C":
            return [{"type": "databricks", "layer": "silver"}]
        if flow in ("J", "K"):
            return [
                {"type": "databricks", "layer": "silver"},
                {"type": "databricks", "layer": "gold"},
            ]
        if flow in ("H", "I"):
            return [
                {"type": "databricks", "layer": "bronze"},
                {"type": "databricks", "layer": "silver"},
                {"type": "databricks", "layer": "gold"},
            ]
        # D, E, G, F
        return [
            {"type": "databricks", "layer": "bronze"},
            {"type": "databricks", "layer": "silver"},
        ]

    # ── CONNECTION TEMPLATES ──────────────────────────────────────────────────

    @staticmethod
    def _connection_template(source_type: str) -> dict:
        templates = {
            "s3": {
                "bucket"               : "",
                "region"               : "",
                "prefix"               : "",
                "file_format"          : "",
                "aws_access_key_id"    : "",
                "aws_secret_access_key": "",
            },
            "azure_sql": {
                "server"  : "",
                "database": "",
                "username": "",
                "password": "",
                "schema"  : "",
            },
            "databricks_bronze": {
                "workspace_url": "",
                "warehouse_id" : "",
                "token"        : "",
                "catalog"      : "",
                "schema"       : "",
                "table"        : "",
            },
            "databricks_silver": {
                "workspace_url": "",
                "warehouse_id" : "",
                "token"        : "",
                "catalog"      : "",
                "schema"       : "",
            },
            "postgresql": {
                "host"    : "",
                "port"    : "5432",
                "database": "",
                "username": "",
                "password": "",
                "schema"  : "",
            },
        }
        return templates.get(source_type, {})


# =============================================================================
# MASTER CONTROL AGENT
# =============================================================================

class MasterControlAgent:

    def __init__(self, llm_client: DatabricksLLMClient | None = None):
        self.llm          = llm_client or DatabricksLLMClient()
        self.intent_gate  = IntentGate(self.llm)
        self.flow_clf     = FlowClassifier(self.llm)
        self.plan_builder = PlanBuilder()

    def execute(self, user_input: str, output_dir: str = ".") -> tuple:
        """
        Main entry point. Called by orchestrator.py.

        Returns:
            (plan: dict, plan_path: str)   on success
            (None, None)                   if input is off-topic
        """
        if not user_input or not user_input.strip():
            raise ValueError("User input is empty.")

        # ── Step 0: Relevance gate ─────────────────────────────────────────────
        print("  🔍 Checking request relevance...")
        is_relevant, redirect = self.intent_gate.check(user_input)
        if not is_relevant:
            print(f"\n  🤖 {redirect}\n")
            return None, None

        # ── Step 1: Flow classification ────────────────────────────────────────
        print("  🧠 Understanding your pipeline flow...\n")
        classification = self.flow_clf.classify(user_input)
        flow           = classification["flow"]

        print(f"\n  ✅ Flow detected    : {flow} — {FLOW_LABELS[flow].strip()}")
        print(f"     Pipeline type   : {FLOW_PIPELINE_TYPE[flow].upper()}")
        print(f"     Source count    : {len(classification['source_types'])}")
        print(f"     Ingestion mode  : {classification['ingestion_mode']}")
        print(f"     Schedule        : {classification['schedule']}")
        print(f"     Reason          : {classification['reason']}")
        print(f"     Confirmed by    : {classification['confirmed_by']}")

        # Gold-specific detail
        if flow in ("H", "I") and classification.get("gold_bronze_source"):
            gbs = classification["gold_bronze_source"]
            routing = {
                "s3"        : "Flow D path  (S3 → Bronze → Silver → Gold)",
                "sql"       : "Flow E path  (SQL → Bronze → Silver → Gold)",
                "hybrid"    : "Flow G path  (Mixed → Bronze → Silver → Gold)",
                "databricks": "Flow C path  (existing bronze → Silver → Gold)",
            }.get(gbs, gbs)
            print(f"     Bronze source   : {gbs}")
            print(f"     Routing         : {routing}")
            detail = classification.get("gold_bronze_source_detail", {})
            if detail.get("s3_bucket_count"):
                print(f"     S3 buckets      : {detail['s3_bucket_count']}")
            if detail.get("sql_db_count"):
                print(f"     SQL databases   : {detail['sql_db_count']}")
        print()

        logger.info(
            "MCA: flow=%s pipeline_type=%s source_count=%d",
            flow, FLOW_PIPELINE_TYPE[flow], len(classification["source_types"]),
        )

        # ── Step 2: Build plan ─────────────────────────────────────────────────
        session_id = f"session_{uuid.uuid4().hex[:10]}"
        plan       = self.plan_builder.build(session_id, classification)

        self._print_plan_summary(plan)

        # ── Step 3: Save plan ──────────────────────────────────────────────────
        os.makedirs(output_dir, exist_ok=True)
        plan_path = os.path.join(output_dir, f"mca_plan_{session_id}.json")
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump(plan, f, indent=2, ensure_ascii=False)

        logger.info("MCA plan → %s", plan_path)
        print(f"  💾 Plan saved → {plan_path}\n")
        return plan, plan_path

    # ── SUMMARY PRINTER ───────────────────────────────────────────────────────

    @staticmethod
    def _print_plan_summary(plan: dict) -> None:
        print("  " + "─" * 60)
        print("  PLAN SUMMARY")
        print("  " + "─" * 60)
        print(f"  Session ID      : {plan['session_id']}")
        print(f"  Flow            : {plan['flow']}")
        print(f"  Pipeline type   : {plan['pipeline_type'].upper()}")
        print(f"  Total pipelines : {plan['total_pipelines']}")

        bronze = plan.get("bronze_count", 0)
        silver = plan.get("silver_count", 0)
        gold   = plan.get("gold_count",   0)

        if bronze:
            print(f"    Bronze        : {bronze}")
        if silver:
            print(f"    Silver        : {silver}")
        if gold:
            op = plan.get("gold_operation", "")
            print(f"    Gold          : {gold}  [{op}]")

        if plan.get("gold_bronze_source"):
            detail = plan.get("gold_bronze_source_detail", {})
            gbs    = plan["gold_bronze_source"]
            routing = {
                "s3"        : "Flow D path",
                "sql"       : "Flow E path",
                "hybrid"    : "Flow G path",
                "databricks": "Flow C path (no raw ingest)",
            }.get(gbs, gbs)
            print(f"\n  Bronze source   : {gbs}  ({routing})")
            if detail.get("s3_bucket_count"):
                print(f"    S3 buckets    : {detail['s3_bucket_count']}")
            if detail.get("sql_db_count"):
                print(f"    SQL databases : {detail['sql_db_count']}")

        print("\n  Connections required:")
        for conn in plan.get("source_connections", []):
            layer = f" [{conn['layer']}]" if conn.get("layer") else ""
            print(f"    Source {conn['index']}  : {conn['type']}{layer}")
        for conn in plan.get("destination_connections", []):
            print(f"    Destination   : {conn['type']} [{conn['layer']}]")

        print("  " + "─" * 60 + "\n")