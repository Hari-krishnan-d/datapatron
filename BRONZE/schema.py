
"""
schema_mapping.py — Schema Mapping Agent (Hybrid: Rules + LLM)
===============================================================
Step 6 of the orchestration pipeline. Called by orchestrator.py.

HOW IT WORKS:
  1. Rule-based inference runs first on all columns (fast, always works)
  2. Name-pattern overrides fix known cases without needing LLM:
       phone/mobile/tel  → STRING   (even if value looks like int)
       is_/has_/flag     → BOOLEAN  (even if value is 0 or 1)
       *_id / id         → BIGINT   (even if all rows are null)
       email/mail        → STRING
       *_date/*_time/_at → TIMESTAMP
       salary/balance/amount/price → DOUBLE
       age/score/count/qty → INT
  3. LLM is called ONLY for columns that are still ambiguous after
     rules + patterns — typically null columns with no clear name signal
  4. If LLM fails → keep rule/pattern result (never blocks the pipeline)

Output structure is identical to both rule-based and LLM versions.
"""

import json
import os
import math
import datetime
import logging
import sys
from typing import Any

logger = logging.getLogger("Schema.Mapping")

# ── LLM client ────────────────────────────────────────────────────────────────
try:
    from llm import DatabricksLLMClient, extract_json
except ImportError:
    _here = os.path.dirname(os.path.abspath(__file__))
    _root = os.path.dirname(_here)
    sys.path.insert(0, os.path.join(_root, "MCA"))
    from llm import DatabricksLLMClient, extract_json


# =============================================================================
# NAME PATTERN RULES
# Applied after value-based inference.
# These override the rule result when the column name gives a clear signal.
# =============================================================================

def _apply_name_patterns(col_name: str, current_type: str) -> tuple[str, bool]:
    """
    Check column name against known semantic patterns.

    Returns:
        (final_type, overridden)
        overridden=True  means pattern matched — LLM not needed
        overridden=False means no pattern matched — LLM may be needed
    """
    name = col_name.lower()

    # Phone → always STRING (values like 6453301881 look like int but must be STRING)
    if any(p in name for p in ("phone", "mobile", "tel", "fax", "contact_no")):
        return "STRING", True

    # Email → always STRING
    if any(p in name for p in ("email", "e_mail", "mail")):
        return "STRING", True

    # Boolean → is_ / has_ / flag
    if name.startswith("is_") or name.startswith("has_") or "flag" in name:
        return "BOOLEAN", True

    # ID columns → BIGINT
    if name.endswith("_id") or name == "id" or name.endswith("_key"):
        return "BIGINT", True

    # Date/time columns → TIMESTAMP
    if any(p in name for p in ("_date", "_time", "_at", "_on", "date_",
                                "time_", "dob", "birth", "created", "updated",
                                "modified", "joined", "opened")):
        return "TIMESTAMP", True

    # Monetary → DOUBLE
    if any(p in name for p in ("salary", "balance", "amount", "price",
                                "revenue", "cost", "fee", "wage", "pay",
                                "charge", "total")):
        return "DOUBLE", True

    # Count/score/age → INT
    if any(p in name for p in ("age", "score", "count", "qty", "quantity",
                                "rank", "num_", "_num", "number_of")):
        return "INT", True

    # Country/state codes → STRING
    if any(p in name for p in ("country", "nation", "state", "region",
                                "province", "country_code", "state_code")):
        return "STRING", True

    # Gender/category → STRING
    if any(p in name for p in ("gender", "sex", "category", "status",
                                "type", "method", "mode")):
        return "STRING", True

    return current_type, False


# =============================================================================
# LLM SYSTEM PROMPT
# Only called for genuinely ambiguous columns.
# =============================================================================
AMBIGUOUS_PROMPT = """
You are a senior data engineer. Assign the correct SQL datatype to each column.

These are columns that could not be typed by rules alone — typically because
all sample values were null and the column name gives no clear signal.

Input: list of {name, sample_values} objects.
Output: JSON array — one object per column, same order:
[{"name": "<exact name>", "type": "<SQL type>"}, ...]

Types allowed: STRING, BIGINT, INT, DOUBLE, BOOLEAN, DATE, TIMESTAMP

Rules:
1. "name" must be EXACTLY as given.
2. If all sample values are null, use column name to infer — default STRING.
3. Return ONLY the JSON array. No markdown.
"""


# =============================================================================
# SCHEMA MAPPING AGENT
# =============================================================================
class SchemaMappingAgent:

    def __init__(self, strategy_file: str, metadata_file: str, output_file: str = "schema_output.json"):
        self.strategy_file = strategy_file
        self.metadata_file = metadata_file
        self.output_file   = output_file

        # LLM is optional — if init fails, we fall back to rules only
        try:
            self.llm = DatabricksLLMClient()
        except Exception as e:
            logger.warning("LLM client unavailable (%s) — using rules only", e)
            self.llm = None

    def execute(self):
        strategy = self._load_json(self.strategy_file)
        metadata = self._load_json(self.metadata_file)

        schema_output = {
            "session_id": strategy.get("session_id"),
            "tables":     [],
        }

        processed_pipeline_ids = set()

        for pipeline in strategy.get("pipelines", []):
            pipeline_id = pipeline.get("pipeline_id", "")
            source_type = pipeline.get("source_type", "")

            if pipeline_id in processed_pipeline_ids:
                print(f"  WARNING Duplicate pipeline_id '{pipeline_id}' - skipped.")
                continue

            for table in pipeline.get("tables", []):
                table_entry = self._build_table_schema(
                    table       = table,
                    metadata    = metadata,
                    pipeline_id = pipeline_id,
                    source_type = source_type,
                )
                schema_output["tables"].append(table_entry)
                print(f"  OK Schema mapped : {table_entry['table_name']}  (pipeline: {pipeline_id})")

            processed_pipeline_ids.add(pipeline_id)

        total = len(schema_output["tables"])
        print(f"\n  OK Schema mapping complete - {total} table(s) -> {self.output_file}")
        self._save_json(schema_output)

    # =========================================================================
    # BUILD TABLE SCHEMA
    # =========================================================================
    def _build_table_schema(self, table, metadata, pipeline_id, source_type):
        raw_name         = table.get("table_name", "") or pipeline_id
        write_strategy   = table.get("write_strategy", "append")
        watermark_column = table.get("watermark_column")
        partition_by     = table.get("partition_by")
        load_type        = table.get("load_type", "full")

        table_name  = self._normalize_table_name(raw_name)
        sample_rows = self._find_sample_data(metadata, raw_name, pipeline_id)

        inferred_columns = self._hybrid_infer_schema(table_name, sample_rows)

        primary_key = None
        if write_strategy == "merge":
            primary_key = self._detect_primary_key(inferred_columns)

        ddl = self._generate_ddl(table_name, inferred_columns, partition_by)

        return {
            "table_name":       table_name,
            "source_name":      raw_name,
            "pipeline_id":      pipeline_id,
            "source_type":      source_type,
            "load_type":        load_type,
            "write_strategy":   write_strategy,
            "columns":          inferred_columns,
            "primary_key":      primary_key,
            "watermark_column": watermark_column,
            "partition_by":     partition_by,
            "ddl":              ddl,
        }

    # =========================================================================
    # HYBRID SCHEMA INFERENCE
    #
    # Step 1: Value-based rule for every column
    # Step 2: Name-pattern override (no LLM needed)
    # Step 3: LLM only for remaining ambiguous columns
    # Step 4: Fallback to STRING if LLM fails
    # =========================================================================
    def _hybrid_infer_schema(self, table_name: str, sample_rows: list) -> list:
        if not sample_rows:
            return []

        col_names   = list(sample_rows[0].keys())
        best_values = {col: None for col in col_names}

        # Find best non-null value per column across all sample rows
        for row in sample_rows:
            for col in col_names:
                v = row.get(col)
                if best_values[col] is None and v is not None:
                    if not (isinstance(v, float) and math.isnan(v)):
                        best_values[col] = v

        # Step 1 + 2: Rule-based + name pattern per column
        results       = {}   # col → {"name", "type"}
        ambiguous     = []   # columns still needing LLM

        for col in col_names:
            best_val = best_values[col]

            # Step 1 — value-based rule
            value_type = self._value_type(best_val)

            # Step 2 — name-pattern override
            final_type, overridden = _apply_name_patterns(col, value_type)

            if overridden:
                # Pattern gave a clear answer — no LLM needed
                results[col] = {"name": col, "type": final_type}
            elif best_val is not None:
                # Value exists and wasn't overridden — rule result is reliable
                results[col] = {"name": col, "type": final_type}
            else:
                # All rows null AND no name pattern matched — ambiguous
                # Queue for LLM
                ambiguous.append(col)
                results[col] = {"name": col, "type": "STRING"}  # temp fallback

        # Step 3 — LLM for ambiguous columns only
        if ambiguous and self.llm:
            print(f"  🧠 LLM resolving {len(ambiguous)} ambiguous column(s): {ambiguous}")
            llm_results = self._llm_resolve_ambiguous(table_name, ambiguous, sample_rows)
            for col, t in llm_results.items():
                results[col] = {"name": col, "type": t}
        elif ambiguous:
            print(f"  ℹ  {len(ambiguous)} ambiguous column(s) defaulted to STRING (LLM unavailable)")

        # Return in original column order
        return [results[col] for col in col_names]

    # =========================================================================
    # LLM — resolve only ambiguous columns
    # =========================================================================
    def _llm_resolve_ambiguous(self, table_name: str, ambiguous_cols: list,
                                sample_rows: list) -> dict:
        """
        Send only ambiguous columns to LLM.
        Returns {col_name: type} dict.
        """
        col_samples = []
        for col in ambiguous_cols:
            values = [row.get(col) for row in sample_rows]
            col_samples.append({
                "name":          col,
                "sample_values": values,
            })

        user_prompt = (
            f"Table: {table_name}\n\n"
            f"Ambiguous columns (all sample values were null):\n"
            f"{json.dumps(col_samples, indent=2)}\n\n"
            f"Return JSON array with name + type for each column."
        )

        try:
            raw    = self.llm.call(AMBIGUOUS_PROMPT, user_prompt)
            parsed = extract_json(raw)

            if not isinstance(parsed, list):
                raise ValueError("LLM did not return a list")

            return {
                c.get("name"): c.get("type", "STRING")
                for c in parsed
                if c.get("name") in ambiguous_cols
            }

        except Exception as exc:
            logger.warning("LLM ambiguous resolution failed: %s", exc)
            return {}  # fallback STRING already set

    # =========================================================================
    # VALUE-BASED TYPE (Step 1)
    # =========================================================================
    def _value_type(self, value: Any) -> str:
        if value is None:           return "STRING"
        if isinstance(value, bool): return "BOOLEAN"
        if isinstance(value, int):  return "BIGINT"
        if isinstance(value, float):
            if math.isnan(value):   return "STRING"
            return "DOUBLE"
        if isinstance(value, str):
            v = value.strip()
            if not v:               return "STRING"
            if self._is_timestamp(v): return "TIMESTAMP"
            if self._is_date(v):      return "DATE"
            return "STRING"
        return "STRING"

    def _is_date(self, value: str) -> bool:
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
                    "%d %b %Y", "%d %B %Y", "%b %d, %Y"):
            try:
                datetime.datetime.strptime(value.strip(), fmt)
                return True
            except Exception:
                continue
        return False

    def _is_timestamp(self, value: str) -> bool:
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%d-%m-%Y %H:%M", "%d/%m/%Y %H:%M:%S"):
            try:
                datetime.datetime.strptime(value.strip()[:19], fmt)
                return True
            except Exception:
                continue
        return False

    # =========================================================================
    # DDL GENERATION
    # Backtick-quoted column names — mixed-case works in Databricks.
    # =========================================================================
    def _generate_ddl(self, table_name, columns, partition_by):
        if not columns:
            return (
                f"CREATE TABLE IF NOT EXISTS {{catalog}}.{{schema}}.{table_name} (\n"
                f"    _placeholder STRING\n"
                f")\nUSING DELTA"
            )

        col_defs = ",\n    ".join(
            f"`{c['name']}` {c['type']}"
            for c in columns
        )

        ddl = (
            f"CREATE TABLE IF NOT EXISTS {{catalog}}.{{schema}}.{table_name} (\n"
            f"    {col_defs}\n"
            f")\nUSING DELTA"
        )

        if partition_by:
            parts = ", ".join(f"`{p}`" for p in partition_by)
            ddl  += f"\nPARTITIONED BY ({parts})"

        return ddl

    # =========================================================================
    # HELPERS
    # =========================================================================
    def _detect_primary_key(self, columns):
        for col in columns:
            name = col["name"].lower()
            if name.endswith("_id") or name == "id" or "key" in name:
                return col["name"]
        return columns[0]["name"] if columns else None

    def _find_sample_data(self, metadata, table_name, pipeline_id):
        for source in metadata.get("sources", []):
            sample_data = source.get("sample_data", {})
            if table_name and table_name in sample_data:
                rows = sample_data[table_name]
                if isinstance(rows, list): return rows
            if pipeline_id and pipeline_id in sample_data:
                rows = sample_data[pipeline_id]
                if isinstance(rows, list): return rows
            for key, rows in sample_data.items():
                if not isinstance(rows, list): continue
                if table_name and (table_name in key or key in table_name):
                    return rows
        return []

    def _normalize_table_name(self, raw_name):
        name = raw_name.strip()
        if "/" in name:
            name = name.split("/")[-1]
        if "." in name:
            parts = name.split(".")
            if parts[-1].lower() in ("csv", "parquet", "json", "orc", "avro"):
                name = ".".join(parts[:-1])
            if "." in name:
                name = name.split(".")[-1]
        return name.replace(" ", "_").replace("-", "_").lower()

    def _load_json(self, path):
        if not path or not os.path.exists(path):
            return {}
        with open(path, "r") as f:
            return json.load(f)

    def _save_json(self, data):
        with open(self.output_file, "w") as f:
            json.dump(data, f, indent=2)


# =============================================================================
# ENTRY POINT
# =============================================================================
def run_schema_mapping(strategy_path, metadata_path, output_dir="."):
    os.makedirs(output_dir, exist_ok=True)
    schema_path = os.path.join(output_dir, "schema_output.json")
    SchemaMappingAgent(strategy_path, metadata_path, output_file=schema_path).execute()
    with open(schema_path, "r") as f:
        schema = json.load(f)
    return schema, schema_path


if __name__ == "__main__":
    strategy_path = input("Enter Strategy JSON path : ").strip()
    metadata_path = input("Enter Metadata JSON path : ").strip()
    SchemaMappingAgent(strategy_path, metadata_path).execute()