# """
# silver_agent/silver_classifier.py — Column Classifier
# =======================================================
# Step S4b of the silver pipeline. Runs after silver_metadata.py.

# For every column in every bronze table, assigns one label:

#   STANDARDIZE_CATEGORICAL  → send to silver_mapping.py (value standardization)
#   CAST_NUMERIC             → type casting in execute.py (numeric stored as string)
#   CAST_TEMPORAL            → date/timestamp normalisation in execute.py
#   NORMALIZE_FORMAT         → simple rule-based fix (email, boolean, phone)
#   IDENTITY_COLUMN          → rename only, no transformation
#   FREE_TEXT                → skip standardization (names, descriptions, notes)
#   HIGH_CARDINALITY         → flag for human decision
#   SKIP                     → fully null, single value, or binary — no action

# THREE LAYERS:
#   Layer 1 — Rule engine   (instant, resolves ~75% of columns deterministically)
#   Layer 2 — LLM           (for ambiguous columns the rules can't decide)
#   Layer 3 — Human queue   (for columns where LLM confidence is low)

# INPUT:  silver_metadata.json
# OUTPUT: silver_classifier_output.json
# """

# import json
# import os
# import re
# import sys
# import logging
# from datetime import datetime, timezone

# logger = logging.getLogger("Silver.Classifier")

# try:
#     from llm import DatabricksLLMClient, extract_json
# except ImportError:
#     _here = os.path.dirname(os.path.abspath(__file__))
#     _root = os.path.dirname(_here)
#     sys.path.insert(0, os.path.join(_root, "MCA"))
#     from llm import DatabricksLLMClient, extract_json


# # =============================================================================
# # CONSTANTS
# # =============================================================================

# # All valid labels the classifier can assign
# LABELS = {
#     "STANDARDIZE_CATEGORICAL",
#     "CAST_NUMERIC",
#     "CAST_TEMPORAL",
#     "NORMALIZE_FORMAT",
#     "IDENTITY_COLUMN",
#     "FREE_TEXT",
#     "HIGH_CARDINALITY",
#     "SKIP",
# }

# # Column name suffixes that carry strong type priors
# _SUFFIX_ID        = ("_id", "_key", "_code", "_ref", "_num", "_no", "_pk", "_fk")
# _SUFFIX_TEMPORAL  = ("_at", "_date", "_dt", "_time", "_timestamp", "_created",
#                      "_updated", "_modified", "_dob", "_expiry", "_start", "_end")
# _SUFFIX_NUMERIC   = ("_count", "_qty", "_quantity", "_amount", "_balance",
#                      "_total", "_score", "_age", "_rate", "_pct", "_percent",
#                      "_price", "_cost", "_fee", "_lat", "_lon", "_latitude",
#                      "_longitude", "_weight", "_height", "_width")
# _SUFFIX_BOOL      = ("_flag", "_active", "_enabled", "_deleted", "_is_",
#                      "_has_", "_can_")
# _SUFFIX_FREETEXT  = ("_name", "_description", "_desc", "_notes", "_note",
#                      "_comment", "_comments", "_text", "_body", "_address",
#                      "_addr", "_message", "_summary", "_detail")
# _SUFFIX_CATEG     = ("_status", "_type", "_category", "_cat", "_class",
#                      "_level", "_grade", "_gender", "_country", "_region",
#                      "_state", "_dept", "_department", "_role", "_source")

# # Regex patterns for sample value analysis
# _PAT_INTEGER   = re.compile(r"^-?\d+$")
# _PAT_DECIMAL   = re.compile(r"^-?[\d,]+\.[\d]+$")
# _PAT_CURRENCY  = re.compile(r"^[£$€¥₹][\d,.]+$")
# _PAT_DATE      = re.compile(
#     r"^\d{4}-\d{2}-\d{2}$"
#     r"|^\d{2}[/\-]\d{2}[/\-]\d{4}$"
#     r"|^\d{4}[/\-]\d{2}[/\-]\d{2}$"
#     r"|^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$"
#     r"|^[A-Za-z]{3}\s+\d{1,2},?\s+\d{4}$"
# )
# _PAT_TIMESTAMP = re.compile(
#     r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"
#     r"|^\d{2}[/\-]\d{2}[/\-]\d{4}\s+\d{2}:\d{2}"
# )
# _PAT_EMAIL     = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
# _PAT_PHONE     = re.compile(r"^[+\d\s\-().]{7,20}$")
# _PAT_BOOL_VAL  = {"true","false","yes","no","y","n","1","0","t","f",
#                   "on","off","active","inactive","enabled","disabled"}

# # Numeric parse threshold: if this fraction of non-null samples look numeric,
# # treat the column as numeric — exclude from standardization
# NUMERIC_THRESHOLD = 0.85
# DATE_THRESHOLD    = 0.85
# BOOL_THRESHOLD    = 0.90


# # =============================================================================
# # HELPERS
# # =============================================================================

# def _clean_sample(v) -> str:
#     if v is None:
#         return ""
#     return str(v).strip()


# def _parse_rate(samples: list, pattern) -> float:
#     """Fraction of non-empty samples that match a compiled regex pattern."""
#     clean = [s for s in (_clean_sample(v) for v in samples) if s]
#     if not clean:
#         return 0.0
#     return sum(1 for s in clean if pattern.match(s)) / len(clean)


# def _is_numeric_value(s: str) -> bool:
#     s = s.replace(",", "").replace("$", "").replace("€", "").replace("£", "").strip()
#     try:
#         float(s)
#         return True
#     except ValueError:
#         return False


# def _numeric_rate(samples: list) -> float:
#     clean = [s for s in (_clean_sample(v) for v in samples) if s]
#     if not clean:
#         return 0.0
#     return sum(1 for s in clean if _is_numeric_value(s)) / len(clean)


# def _bool_rate(samples: list) -> float:
#     clean = [s.lower() for s in (_clean_sample(v) for v in samples) if s]
#     if not clean:
#         return 0.0
#     return sum(1 for s in clean if s in _PAT_BOOL_VAL) / len(clean)


# def _avg_length(samples: list) -> float:
#     clean = [s for s in (_clean_sample(v) for v in samples) if s]
#     if not clean:
#         return 0.0
#     return sum(len(s) for s in clean) / len(clean)


# def _cardinality_ratio(null_count: int, row_count: int, distinct_count: int) -> float:
#     non_null = max(row_count - null_count, 1)
#     return distinct_count / non_null if non_null > 0 else 0.0


# # =============================================================================
# # LAYER 1 — RULE ENGINE
# # Returns (label, confidence, reason)
# # confidence: "high" → skip LLM, "low" → send to LLM
# # =============================================================================

# def _rule_classify(col: dict, row_count: int) -> tuple:
#     """
#     Deterministic rule-based classification.

#     Returns (label, confidence, reason) where confidence is:
#       "high"  → certain, do not send to LLM
#       "medium"→ likely correct, send to LLM for confirmation only
#       "low"   → uncertain, send to LLM
#       "none"  → rule engine cannot decide, must use LLM
#     """
#     name         = col.get("name", "").lower().strip()
#     src_type     = col.get("type", "string").lower()
#     null_count   = col.get("null_count", 0)
#     null_pct     = col.get("null_pct", 0.0)
#     samples      = col.get("sample_values", [])

#     # ── Gate 1: Trivial columns ───────────────────────────────────────────────
#     non_null = row_count - null_count
#     if non_null == 0:
#         return "SKIP", "high", "column is fully null"

#     # ── Gate 2: Already a non-string type in bronze ───────────────────────────
#     if src_type in ("bigint","int","integer","long","double","float","decimal"):
#         return "CAST_NUMERIC", "high", f"bronze type is {src_type}"
#     if src_type in ("date","timestamp","datetime"):
#         return "CAST_TEMPORAL", "high", f"bronze type is {src_type}"
#     if src_type in ("boolean","bool"):
#         return "NORMALIZE_FORMAT", "high", "bronze type is boolean"

#     # ── Gate 3: Sample value analysis (strongest signal) ─────────────────────
#     num_rate  = _numeric_rate(samples)
#     date_rate = _parse_rate(samples, _PAT_DATE)
#     ts_rate   = _parse_rate(samples, _PAT_TIMESTAMP)
#     bool_rate = _bool_rate(samples)
#     avg_len   = _avg_length(samples)

#     if ts_rate >= DATE_THRESHOLD:
#         return "CAST_TEMPORAL", "high", f"timestamp pattern in {ts_rate:.0%} of samples"
#     if date_rate >= DATE_THRESHOLD:
#         return "CAST_TEMPORAL", "high", f"date pattern in {date_rate:.0%} of samples"
#     if num_rate >= NUMERIC_THRESHOLD:
#         return "CAST_NUMERIC", "high", f"{num_rate:.0%} of samples are numeric"
#     if bool_rate >= BOOL_THRESHOLD:
#         return "NORMALIZE_FORMAT", "high", f"{bool_rate:.0%} of samples are boolean-like"

#     # ── Gate 4: Column name suffix rules ─────────────────────────────────────
#     if any(name.endswith(s) or name.startswith(s.lstrip("_")) for s in _SUFFIX_TEMPORAL):
#         return "CAST_TEMPORAL", "medium", "column name implies temporal"

#     if any(name.endswith(s) for s in _SUFFIX_ID):
#         # IDs: if samples are purely numeric → CAST_NUMERIC, else IDENTITY
#         if num_rate > 0.5:
#             return "CAST_NUMERIC", "medium", "id column with numeric values"
#         return "IDENTITY_COLUMN", "medium", "id/key column — rename only"

#     if any(name.endswith(s) for s in _SUFFIX_NUMERIC):
#         return "CAST_NUMERIC", "medium", "column name implies numeric measurement"

#     if any(name.startswith(s.lstrip("_")) or name.endswith(s) for s in _SUFFIX_BOOL):
#         return "NORMALIZE_FORMAT", "medium", "boolean flag column by name"

#     if any(name.endswith(s) for s in _SUFFIX_FREETEXT):
#         return "FREE_TEXT", "medium", "free text column by name"

#     if any(name.endswith(s) for s in _SUFFIX_CATEG):
#         return "STANDARDIZE_CATEGORICAL", "medium", "categorical column by name"

#     # ── Gate 5: Average value length heuristic ────────────────────────────────
#     if avg_len > 60:
#         return "FREE_TEXT", "medium", f"avg value length {avg_len:.0f} chars — free text"

#     # ── Rule engine cannot decide ─────────────────────────────────────────────
#     return "UNKNOWN", "none", "rule engine inconclusive"


# # =============================================================================
# # LAYER 2 — LLM CLASSIFIER
# # Only called for columns where rule engine returned "none" confidence.
# # =============================================================================

# CLASSIFIER_SYSTEM = """You are a data engineer classifying bronze table columns for a Silver ETL pipeline.
# For each column, assign exactly one label from this list:

#   STANDARDIZE_CATEGORICAL  — low cardinality string with variants (status, type, gender, country)
#   CAST_NUMERIC             — number stored as string (amount, score, id with only digits)
#   CAST_TEMPORAL            — date or timestamp stored as string
#   NORMALIZE_FORMAT         — email, phone, boolean string (yes/no), simple formatting
#   IDENTITY_COLUMN          — primary/foreign key with alphanumeric IDs — rename only
#   FREE_TEXT                — names, descriptions, notes, addresses — do not standardize
#   HIGH_CARDINALITY         — too many distinct values to map manually (product names, cities)
#   SKIP                     — single value, fully null, or binary column

# Rules:
# - Numeric exclusion is hard: if 85%+ of samples look like numbers → CAST_NUMERIC
# - Do NOT assign STANDARDIZE_CATEGORICAL to numeric, temporal, free-text, or ID columns
# - confidence: high=certain, medium=likely, low=unsure

# Output ONLY a JSON array, one entry per column:
# [{"col_name":"<name>","label":"<LABEL>","confidence":"high|medium|low","reason":"<max 8 words>"}]"""


# def _llm_classify_batch(columns: list, table_name: str, llm) -> dict:
#     """
#     Send a batch of ambiguous columns to LLM for classification.
#     Returns dict: col_name → {label, confidence, reason}
#     """
#     try:
#         type_llm = DatabricksLLMClient(max_tokens=2000, timeout=60)
#     except TypeError:
#         type_llm = llm

#     lines = []
#     for col in columns:
#         samples   = col.get("sample_values", [])[:5]
#         samp_str  = ", ".join(f'"{s}"' for s in samples if s) or "no samples"
#         null_pct  = col.get("null_pct", 0)
#         lines.append(
#             f"{col['name']} | type={col.get('type','string')} "
#             f"| null={null_pct}% | samples=[{samp_str}]"
#         )

#     user = (
#         f"Table: {table_name}\n"
#         f"Format: col_name | type | null% | samples\n\n"
#         + "\n".join(lines)
#         + "\n\nClassify each column. Return JSON array only."
#     )

#     try:
#         raw    = type_llm.call(CLASSIFIER_SYSTEM, user)
#         parsed = extract_json(raw)
#         if isinstance(parsed, list):
#             return {r.get("col_name", ""): r for r in parsed if r.get("col_name")}
#     except Exception as exc:
#         logger.warning("LLM classifier batch failed for %s: %s", table_name, exc)

#     return {}


# # =============================================================================
# # LAYER 3 — HUMAN REVIEW QUEUE
# # For columns where LLM confidence is low or LLM and rules disagree.
# # =============================================================================

# def _human_review(col: dict, rule_label: str, llm_label: str,
#                   llm_confidence: str, llm_reason: str) -> str:
#     """
#     Show column details to user and ask for final label.
#     Returns the confirmed label.
#     """
#     name    = col.get("name", "")
#     samples = col.get("sample_values", [])[:8]
#     null_pct= col.get("null_pct", 0)

#     print(f"\n  {'─' * 65}")
#     print(f"  MANUAL REVIEW NEEDED: {name}")
#     print(f"  {'─' * 65}")
#     print(f"  Type      : {col.get('type','string')}")
#     print(f"  Null      : {null_pct}%")
#     print(f"  Samples   : {samples}")
#     print(f"  Rule says : {rule_label or 'inconclusive'}")
#     print(f"  LLM says  : {llm_label}  [{llm_confidence}]  — {llm_reason}")
#     print(f"\n  Available labels:")
#     for i, lbl in enumerate(sorted(LABELS), 1):
#         print(f"    [{i}] {lbl}")
#     print(f"    [Enter] Accept LLM suggestion: {llm_label}\n")

#     while True:
#         choice = input("  Your choice [1-8 or Enter]: ").strip()
#         if not choice:
#             print(f"  ✅ Accepted: {llm_label}")
#             return llm_label
#         try:
#             idx = int(choice) - 1
#             label = sorted(LABELS)[idx]
#             print(f"  ✅ Set to: {label}")
#             return label
#         except (ValueError, IndexError):
#             print("  ⚠  Enter a number 1-8 or press Enter.")


# # =============================================================================
# # MAIN CLASSIFICATION FUNCTION
# # =============================================================================

# def classify_table(table_profile: dict, llm) -> list:
#     """
#     Classify all columns in one table.

#     Returns list of column classification dicts:
#       {
#         source_column, source_type, label, confidence,
#         reason, layer,    ← "rule" | "llm" | "human"
#         null_count, null_pct, sample_values
#       }
#     """
#     table_name = table_profile.get("full_table", table_profile.get("table", ""))
#     row_count  = table_profile.get("row_count", 1)
#     columns    = table_profile.get("columns", [])

#     results       = []
#     llm_queue     = []     # columns rules couldn't resolve
#     rule_results  = {}     # col_name → (label, confidence, reason)

#     # ── Layer 1: Rules ────────────────────────────────────────────────────────
#     for col in columns:
#         label, conf, reason = _rule_classify(col, row_count)
#         if conf in ("high", "medium"):
#             rule_results[col["name"]] = (label, conf, reason, "rule")
#         else:
#             llm_queue.append(col)

#     print(f"  Rules : {len(rule_results)}/{len(columns)} resolved")
#     if llm_queue:
#         print(f"  LLM   : {len(llm_queue)} column(s) need LLM review")

#     # ── Layer 2: LLM for unresolved columns ───────────────────────────────────
#     llm_results = {}
#     if llm_queue:
#         BATCH = 10
#         batches = [llm_queue[i:i+BATCH] for i in range(0, len(llm_queue), BATCH)]
#         for batch_num, batch in enumerate(batches, 1):
#             print(f"  LLM batch {batch_num}/{len(batches)} ({len(batch)} cols)...")
#             llm_results.update(_llm_classify_batch(batch, table_name, llm))

#     # ── Layer 3: Human review for low-confidence LLM results ──────────────────
#     human_needed = []
#     for col in llm_queue:
#         name = col["name"]
#         llm_r = llm_results.get(name, {})
#         if not llm_r or llm_r.get("confidence", "low") == "low":
#             human_needed.append(col)

#     if human_needed:
#         print(f"\n  {len(human_needed)} column(s) need manual review:")
#         for col in human_needed:
#             name   = col["name"]
#             llm_r  = llm_results.get(name, {})
#             rule_l = rule_results.get(name, ("UNKNOWN", "none", "", "rule"))[0]
#             confirmed = _human_review(
#                 col,
#                 rule_label   = rule_l,
#                 llm_label    = llm_r.get("label", "SKIP"),
#                 llm_confidence = llm_r.get("confidence", "low"),
#                 llm_reason   = llm_r.get("reason", ""),
#             )
#             llm_results[name] = {
#                 "label"     : confirmed,
#                 "confidence": "high",
#                 "reason"    : "manually confirmed",
#                 "_layer"    : "human",
#             }

#     # ── Merge results ──────────────────────────────────────────────────────────
#     for col in columns:
#         name = col["name"]

#         if name in rule_results:
#             label, conf, reason, layer = rule_results[name]
#         elif name in llm_results:
#             r      = llm_results[name]
#             label  = r.get("label", "SKIP")
#             conf   = r.get("confidence", "medium")
#             reason = r.get("reason", "")
#             layer  = r.get("_layer", "llm")
#             # Validate label
#             if label not in LABELS:
#                 label = "SKIP"
#         else:
#             label, conf, reason, layer = "SKIP", "low", "no classification produced", "fallback"

#         results.append({
#             "source_column" : name,
#             "source_type"   : col.get("type", "string"),
#             "label"         : label,
#             "confidence"    : conf,
#             "reason"        : reason,
#             "layer"         : layer,
#             "null_count"    : col.get("null_count", 0),
#             "null_pct"      : col.get("null_pct", 0.0),
#             "sample_values" : col.get("sample_values", []),
#         })

#     return results


# # =============================================================================
# # DISPLAY
# # =============================================================================

# def _print_classification_summary(table_name: str, classifications: list) -> None:
#     label_groups = {}
#     for c in classifications:
#         label_groups.setdefault(c["label"], []).append(c["source_column"])

#     print(f"\n  {'─' * 70}")
#     print(f"  CLASSIFICATION RESULTS — {table_name}")
#     print(f"  {'─' * 70}")
#     print(f"  {'COLUMN':<30} {'LABEL':<28} {'LAYER':<6} REASON")
#     print(f"  {'─' * 70}")
#     for c in classifications:
#         reason = c["reason"][:28] if c["reason"] else ""
#         print(
#             f"  {c['source_column']:<30} "
#             f"{c['label']:<28} "
#             f"{c['layer']:<6} "
#             f"{reason}"
#         )
#     print(f"  {'─' * 70}")

#     for label in sorted(LABELS):
#         cols = label_groups.get(label, [])
#         if cols:
#             marker = " ← standardize" if label == "STANDARDIZE_CATEGORICAL" else ""
#             print(f"  {label}: {len(cols)}{marker}")


# # =============================================================================
# # MAIN ENTRY POINT
# # =============================================================================

# def run_silver_classifier(
#     metadata_path: str,
#     output_dir:    str = ".",
# ) -> tuple:
#     """
#     Main entry point. Called by orchestrator.py after silver_metadata.

#     Args:
#         metadata_path : Path to silver_metadata.json
#         output_dir    : Where to save silver_classifier_output.json

#     Returns:
#         (classifier_output: dict, output_path: str)
#     """
#     logger.info("Silver Classifier started.")

#     with open(metadata_path, "r", encoding="utf-8") as f:
#         metadata = json.load(f)

#     llm = DatabricksLLMClient()

#     print("\n" + "=" * 62)
#     print("  SILVER — COLUMN CLASSIFIER")
#     print("=" * 62)
#     print()
#     print("  Layer 1 — Rule engine   (cardinality, samples, col name)")
#     print("  Layer 2 — LLM           (for ambiguous columns only)")
#     print("  Layer 3 — Human review  (for low-confidence LLM results)")
#     print()
#     print("  Numeric columns are excluded from standardization.")
#     print("  Only STANDARDIZE_CATEGORICAL columns → Mapping Agent.\n")

#     output_tables = []

#     for source in metadata.get("sources", []):
#         for table_profile in source.get("tables", []):
#             full_name = table_profile.get("full_table", table_profile.get("table", ""))

#             print(f"\n  {'=' * 62}")
#             print(f"  Table: {full_name}")
#             print(f"  {'=' * 62}")

#             classifications = classify_table(table_profile, llm)
#             _print_classification_summary(full_name, classifications)

#             standardize_cols = [
#                 c for c in classifications
#                 if c["label"] == "STANDARDIZE_CATEGORICAL"
#             ]
#             print(f"\n  → {len(standardize_cols)} column(s) will go to Mapping Agent")

#             output_tables.append({
#                 "pipeline_id"    : source.get("pipeline_id", ""),
#                 "table"          : table_profile.get("table", ""),
#                 "full_table"     : full_name,
#                 "row_count"      : table_profile.get("row_count", 0),
#                 "classifications": classifications,
#             })

#     output = {
#         "session_id"  : metadata.get("session_id", ""),
#         "created_at"  : datetime.now(timezone.utc).isoformat(),
#         "tables"      : output_tables,
#     }

#     os.makedirs(output_dir, exist_ok=True)
#     out_path = os.path.join(output_dir, "silver_classifier_output.json")
#     with open(out_path, "w", encoding="utf-8") as f:
#         json.dump(output, f, indent=2, ensure_ascii=False)

#     total_cols     = sum(len(t["classifications"]) for t in output_tables)
#     std_cols       = sum(
#         sum(1 for c in t["classifications"] if c["label"] == "STANDARDIZE_CATEGORICAL")
#         for t in output_tables
#     )
#     numeric_cols   = sum(
#         sum(1 for c in t["classifications"] if c["label"] == "CAST_NUMERIC")
#         for t in output_tables
#     )

#     print(f"\n  {'─' * 55}")
#     print(f"  CLASSIFIER COMPLETE")
#     print(f"  Tables     : {len(output_tables)}")
#     print(f"  Columns    : {total_cols}")
#     print(f"  Standardize: {std_cols}  (→ Mapping Agent)")
#     print(f"  Numeric    : {numeric_cols}  (→ Type casting, excluded from std)")
#     print(f"  Saved   → {out_path}")
#     print(f"  {'─' * 55}\n")

#     return output, out_path


"""
silver_agent/silver_classifier.py — Column Classifier
=======================================================
Step S4b of the silver pipeline. Runs after silver_metadata.py.
 
For every column in every bronze table, assigns one label:
 
  STANDARDIZE_CATEGORICAL  → send to silver_mapping.py (value standardization)
  CAST_NUMERIC             → type casting in execute.py (numeric stored as string)
  CAST_TEMPORAL            → date/timestamp normalisation in execute.py
  NORMALIZE_FORMAT         → simple rule-based fix (email, boolean, phone)
  IDENTITY_COLUMN          → rename only, no transformation
  FREE_TEXT                → skip standardization (names, descriptions, notes)
  HIGH_CARDINALITY         → flag for human decision
  SKIP                     → fully null, single value, or binary — no action
 
THREE LAYERS:
  Layer 1 — Rule engine   (instant, resolves ~75% of columns deterministically)
  Layer 2 — LLM           (for ambiguous columns the rules can't decide)
  Layer 3 — Human queue   (DISABLED — LLM suggestion always auto-accepted)
 
INPUT:  silver_metadata.json
OUTPUT: silver_classifier_output.json
"""
 
import json
import os
import re
import sys
import logging
from datetime import datetime, timezone
 
logger = logging.getLogger("Silver.Classifier")
 
try:
    from llm import DatabricksLLMClient, extract_json
except ImportError:
    _here = os.path.dirname(os.path.abspath(__file__))
    _root = os.path.dirname(_here)
    sys.path.insert(0, os.path.join(_root, "MCA"))
    from llm import DatabricksLLMClient, extract_json
 
 
# =============================================================================
# CONSTANTS
# =============================================================================
 
LABELS = {
    "STANDARDIZE_CATEGORICAL",
    "CAST_NUMERIC",
    "CAST_TEMPORAL",
    "NORMALIZE_FORMAT",
    "IDENTITY_COLUMN",
    "FREE_TEXT",
    "HIGH_CARDINALITY",
    "SKIP",
}
 
_SUFFIX_ID        = ("_id", "_key", "_code", "_ref", "_num", "_no", "_pk", "_fk")
_SUFFIX_TEMPORAL  = ("_at", "_date", "_dt", "_time", "_timestamp", "_created",
                     "_updated", "_modified", "_dob", "_expiry", "_start", "_end")
_SUFFIX_NUMERIC   = ("_count", "_qty", "_quantity", "_amount", "_balance",
                     "_total", "_score", "_age", "_rate", "_pct", "_percent",
                     "_price", "_cost", "_fee", "_lat", "_lon", "_latitude",
                     "_longitude", "_weight", "_height", "_width")
_SUFFIX_BOOL      = ("_flag", "_active", "_enabled", "_deleted", "_is_",
                     "_has_", "_can_")
_SUFFIX_FREETEXT  = ("_name", "_description", "_desc", "_notes", "_note",
                     "_comment", "_comments", "_text", "_body", "_address",
                     "_addr", "_message", "_summary", "_detail")
_SUFFIX_CATEG     = ("_status", "_type", "_category", "_cat", "_class",
                     "_level", "_grade", "_gender", "_country", "_region",
                     "_state", "_dept", "_department", "_role", "_source")
 
_PAT_INTEGER   = re.compile(r"^-?\d+$")
_PAT_DECIMAL   = re.compile(r"^-?[\d,]+\.[\d]+$")
_PAT_CURRENCY  = re.compile(r"^[£$€¥₹][\d,.]+$")
_PAT_DATE      = re.compile(
    r"^\d{4}-\d{2}-\d{2}$"
    r"|^\d{2}[/\-]\d{2}[/\-]\d{4}$"
    r"|^\d{4}[/\-]\d{2}[/\-]\d{2}$"
    r"|^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$"
    r"|^[A-Za-z]{3}\s+\d{1,2},?\s+\d{4}$"
)
_PAT_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"
    r"|^\d{2}[/\-]\d{2}[/\-]\d{4}\s+\d{2}:\d{2}"
)
_PAT_EMAIL     = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_PAT_PHONE     = re.compile(r"^[+\d\s\-().]{7,20}$")
_PAT_BOOL_VAL  = {"true","false","yes","no","y","n","1","0","t","f",
                  "on","off","active","inactive","enabled","disabled"}
 
NUMERIC_THRESHOLD = 0.85
DATE_THRESHOLD    = 0.85
BOOL_THRESHOLD    = 0.90
 
 
# =============================================================================
# HELPERS
# =============================================================================
 
def _clean_sample(v) -> str:
    if v is None:
        return ""
    return str(v).strip()
 
 
def _parse_rate(samples: list, pattern) -> float:
    clean = [s for s in (_clean_sample(v) for v in samples) if s]
    if not clean:
        return 0.0
    return sum(1 for s in clean if pattern.match(s)) / len(clean)
 
 
def _is_numeric_value(s: str) -> bool:
    s = s.replace(",", "").replace("$", "").replace("€", "").replace("£", "").strip()
    try:
        float(s)
        return True
    except ValueError:
        return False
 
 
def _numeric_rate(samples: list) -> float:
    clean = [s for s in (_clean_sample(v) for v in samples) if s]
    if not clean:
        return 0.0
    return sum(1 for s in clean if _is_numeric_value(s)) / len(clean)
 
 
def _bool_rate(samples: list) -> float:
    clean = [s.lower() for s in (_clean_sample(v) for v in samples) if s]
    if not clean:
        return 0.0
    return sum(1 for s in clean if s in _PAT_BOOL_VAL) / len(clean)
 
 
def _avg_length(samples: list) -> float:
    clean = [s for s in (_clean_sample(v) for v in samples) if s]
    if not clean:
        return 0.0
    return sum(len(s) for s in clean) / len(clean)
 
 
def _cardinality_ratio(null_count: int, row_count: int, distinct_count: int) -> float:
    non_null = max(row_count - null_count, 1)
    return distinct_count / non_null if non_null > 0 else 0.0
 
 
# =============================================================================
# LAYER 1 — RULE ENGINE
# =============================================================================
 
def _rule_classify(col: dict, row_count: int) -> tuple:
    name         = col.get("name", "").lower().strip()
    src_type     = col.get("type", "string").lower()
    null_count   = col.get("null_count", 0)
    null_pct     = col.get("null_pct", 0.0)
    samples      = col.get("sample_values", [])
 
    non_null = row_count - null_count
    if non_null == 0:
        return "SKIP", "high", "column is fully null"
 
    if src_type in ("bigint","int","integer","long","double","float","decimal"):
        return "CAST_NUMERIC", "high", f"bronze type is {src_type}"
    if src_type in ("date","timestamp","datetime"):
        return "CAST_TEMPORAL", "high", f"bronze type is {src_type}"
    if src_type in ("boolean","bool"):
        return "NORMALIZE_FORMAT", "high", "bronze type is boolean"
 
    num_rate  = _numeric_rate(samples)
    date_rate = _parse_rate(samples, _PAT_DATE)
    ts_rate   = _parse_rate(samples, _PAT_TIMESTAMP)
    bool_rate = _bool_rate(samples)
    avg_len   = _avg_length(samples)
 
    if ts_rate >= DATE_THRESHOLD:
        return "CAST_TEMPORAL", "high", f"timestamp pattern in {ts_rate:.0%} of samples"
    if date_rate >= DATE_THRESHOLD:
        return "CAST_TEMPORAL", "high", f"date pattern in {date_rate:.0%} of samples"
    if num_rate >= NUMERIC_THRESHOLD:
        return "CAST_NUMERIC", "high", f"{num_rate:.0%} of samples are numeric"
    if bool_rate >= BOOL_THRESHOLD:
        return "NORMALIZE_FORMAT", "high", f"{bool_rate:.0%} of samples are boolean-like"
 
    if any(name.endswith(s) or name.startswith(s.lstrip("_")) for s in _SUFFIX_TEMPORAL):
        return "CAST_TEMPORAL", "medium", "column name implies temporal"
 
    if any(name.endswith(s) for s in _SUFFIX_ID):
        if num_rate > 0.5:
            return "CAST_NUMERIC", "medium", "id column with numeric values"
        return "IDENTITY_COLUMN", "medium", "id/key column — rename only"
 
    if any(name.endswith(s) for s in _SUFFIX_NUMERIC):
        return "CAST_NUMERIC", "medium", "column name implies numeric measurement"
 
    if any(name.startswith(s.lstrip("_")) or name.endswith(s) for s in _SUFFIX_BOOL):
        return "NORMALIZE_FORMAT", "medium", "boolean flag column by name"
 
    if any(name.endswith(s) for s in _SUFFIX_FREETEXT):
        return "FREE_TEXT", "medium", "free text column by name"
 
    if any(name.endswith(s) for s in _SUFFIX_CATEG):
        return "STANDARDIZE_CATEGORICAL", "medium", "categorical column by name"
 
    if avg_len > 60:
        return "FREE_TEXT", "medium", f"avg value length {avg_len:.0f} chars — free text"
 
    # Pass all ambiguous STRING columns to mapping agent.
    # silver_mapping.py has its own gates (numeric, datetime, alphanumeric)
    # that will correctly filter out anything not worth standardizing.
    return "STANDARDIZE_CATEGORICAL", "medium", "ambiguous — mapping agent will gate"
 
 
# =============================================================================
# LAYER 2 — LLM CLASSIFIER
# =============================================================================
 
CLASSIFIER_SYSTEM = """You are a data engineer classifying bronze table columns for a Silver ETL pipeline.
For each column, assign exactly one label from this list:
 
  STANDARDIZE_CATEGORICAL  — low cardinality string with variants (status, type, gender, country)
  CAST_NUMERIC             — number stored as string (amount, score, id with only digits)
  CAST_TEMPORAL            — date or timestamp stored as string
  NORMALIZE_FORMAT         — email, phone, boolean string (yes/no), simple formatting
  IDENTITY_COLUMN          — primary/foreign key with alphanumeric IDs — rename only
  FREE_TEXT                — names, descriptions, notes, addresses — do not standardize
  HIGH_CARDINALITY         — too many distinct values to map manually (product names, cities)
  SKIP                     — single value, fully null, or binary column
 
Rules:
- Numeric exclusion is hard: if 85%+ of samples look like numbers → CAST_NUMERIC
- Do NOT assign STANDARDIZE_CATEGORICAL to numeric, temporal, free-text, or ID columns
- confidence: high=certain, medium=likely, low=unsure
 
Output ONLY a JSON array, one entry per column:
[{"col_name":"<name>","label":"<LABEL>","confidence":"high|medium|low","reason":"<max 8 words>"}]"""
 
 
def _llm_classify_batch(columns: list, table_name: str, llm) -> dict:
    try:
        type_llm = DatabricksLLMClient(max_tokens=2000, timeout=60)
    except TypeError:
        type_llm = llm
 
    lines = []
    for col in columns:
        samples   = col.get("sample_values", [])[:5]
        samp_str  = ", ".join(f'"{s}"' for s in samples if s) or "no samples"
        null_pct  = col.get("null_pct", 0)
        lines.append(
            f"{col['name']} | type={col.get('type','string')} "
            f"| null={null_pct}% | samples=[{samp_str}]"
        )
 
    user = (
        f"Table: {table_name}\n"
        f"Format: col_name | type | null% | samples\n\n"
        + "\n".join(lines)
        + "\n\nClassify each column. Return JSON array only."
    )
 
    try:
        raw    = type_llm.call(CLASSIFIER_SYSTEM, user)
        parsed = extract_json(raw)
        if isinstance(parsed, list):
            return {r.get("col_name", ""): r for r in parsed if r.get("col_name")}
    except Exception as exc:
        logger.warning("LLM classifier batch failed for %s: %s", table_name, exc)
 
    return {}
 
 
# =============================================================================
# LAYER 3 — AUTO-ACCEPT (no human prompt)
# LLM suggestion is always accepted automatically.
# Low-confidence LLM results default to SKIP if no label provided.
# =============================================================================
 
def _auto_accept(col: dict, llm_label: str, llm_confidence: str,
                 llm_reason: str) -> str:
    """
    Auto-accept the LLM suggestion without prompting the user.
    Falls back to SKIP if LLM provided no label.
    """
    accepted = llm_label if llm_label in LABELS else "SKIP"
    print(f"  AutoAccept: {col.get('name','')} → {accepted}  [{llm_confidence}]")
    return accepted
 
 
# =============================================================================
# MAIN CLASSIFICATION FUNCTION
# =============================================================================
 
def classify_table(table_profile: dict, llm) -> list:
    table_name = table_profile.get("full_table", table_profile.get("table", ""))
    row_count  = table_profile.get("row_count", 1)
    columns    = table_profile.get("columns", [])
 
    results       = []
    llm_queue     = []
    rule_results  = {}
 
    # ── Layer 1: Rules ────────────────────────────────────────────────────────
    for col in columns:
        label, conf, reason = _rule_classify(col, row_count)
        if conf in ("high", "medium"):
            rule_results[col["name"]] = (label, conf, reason, "rule")
        else:
            llm_queue.append(col)
 
    print(f"  Rules : {len(rule_results)}/{len(columns)} resolved")
    if llm_queue:
        print(f"  LLM   : {len(llm_queue)} column(s) need LLM review")
 
    # ── Layer 2 & 3: LLM and human review disabled ────────────────────────────
    # Rule engine now routes all ambiguous STRING columns to STANDARDIZE_CATEGORICAL.
    # silver_mapping.py gates (numeric, datetime, alphanumeric, cardinality) handle
    # the filtering — no LLM classification or human prompt needed.
    llm_results = {}
    if llm_queue:
        # Fallback: if any column still reaches here, auto-route to mapping agent
        for col in llm_queue:
            llm_results[col["name"]] = {
                "label"     : "STANDARDIZE_CATEGORICAL",
                "confidence": "medium",
                "reason"    : "ambiguous — routed to mapping agent",
                "_layer"    : "rule",
            }
 
    # ── Merge results ──────────────────────────────────────────────────────────
    for col in columns:
        name = col["name"]
 
        if name in rule_results:
            label, conf, reason, layer = rule_results[name]
        elif name in llm_results:
            r      = llm_results[name]
            label  = r.get("label", "SKIP")
            conf   = r.get("confidence", "medium")
            reason = r.get("reason", "")
            layer  = r.get("_layer", "llm")
            if label not in LABELS:
                label = "SKIP"
        else:
            label, conf, reason, layer = "SKIP", "low", "no classification produced", "fallback"
 
        results.append({
            "source_column" : name,
            "source_type"   : col.get("type", "string"),
            "label"         : label,
            "confidence"    : conf,
            "reason"        : reason,
            "layer"         : layer,
            "null_count"    : col.get("null_count", 0),
            "null_pct"      : col.get("null_pct", 0.0),
            "sample_values" : col.get("sample_values", []),
        })
 
    return results
 
 
# =============================================================================
# DISPLAY
# =============================================================================
 
def _print_classification_summary(table_name: str, classifications: list) -> None:
    label_groups = {}
    for c in classifications:
        label_groups.setdefault(c["label"], []).append(c["source_column"])
 
    print(f"\n  {'─' * 70}")
    print(f"  CLASSIFICATION RESULTS — {table_name}")
    print(f"  {'─' * 70}")
    print(f"  {'COLUMN':<30} {'LABEL':<28} {'LAYER':<6} REASON")
    print(f"  {'─' * 70}")
    for c in classifications:
        reason = c["reason"][:28] if c["reason"] else ""
        print(
            f"  {c['source_column']:<30} "
            f"{c['label']:<28} "
            f"{c['layer']:<6} "
            f"{reason}"
        )
    print(f"  {'─' * 70}")
 
    for label in sorted(LABELS):
        cols = label_groups.get(label, [])
        if cols:
            marker = " ← standardize" if label == "STANDARDIZE_CATEGORICAL" else ""
            print(f"  {label}: {len(cols)}{marker}")
 
 
# =============================================================================
# MAIN ENTRY POINT
# =============================================================================
 
def run_silver_classifier(
    metadata_path: str,
    output_dir:    str = ".",
) -> tuple:
    logger.info("Silver Classifier started.")
 
    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)
 
    llm = DatabricksLLMClient()
 
    print("\n" + "=" * 62)
    print("  SILVER — COLUMN CLASSIFIER")
    print("=" * 62)
    print()
    print("  Layer 1 — Rule engine   (cardinality, samples, col name)")
    print("  Layer 2 — Direct route  (ambiguous STRING → mapping agent)")
    print("  No LLM classification, no human review — mapping agent gates all.")
    print()
    print("  Numeric columns are excluded from standardization.")
    print("  Only STANDARDIZE_CATEGORICAL columns → Mapping Agent.\n")
 
    output_tables = []
 
    for source in metadata.get("sources", []):
        for table_profile in source.get("tables", []):
            full_name = table_profile.get("full_table", table_profile.get("table", ""))
 
            print(f"\n  {'=' * 62}")
            print(f"  Table: {full_name}")
            print(f"  {'=' * 62}")
 
            classifications = classify_table(table_profile, llm)
            _print_classification_summary(full_name, classifications)
 
            standardize_cols = [
                c for c in classifications
                if c["label"] == "STANDARDIZE_CATEGORICAL"
            ]
            print(f"\n  → {len(standardize_cols)} column(s) will go to Mapping Agent")
 
            output_tables.append({
                "pipeline_id"    : source.get("pipeline_id", ""),
                "table"          : table_profile.get("table", ""),
                "full_table"     : full_name,
                "row_count"      : table_profile.get("row_count", 0),
                "classifications": classifications,
            })
 
    output = {
        "session_id"  : metadata.get("session_id", ""),
        "created_at"  : datetime.now(timezone.utc).isoformat(),
        "tables"      : output_tables,
    }
 
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "silver_classifier_output.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
 
    total_cols     = sum(len(t["classifications"]) for t in output_tables)
    std_cols       = sum(
        sum(1 for c in t["classifications"] if c["label"] == "STANDARDIZE_CATEGORICAL")
        for t in output_tables
    )
    numeric_cols   = sum(
        sum(1 for c in t["classifications"] if c["label"] == "CAST_NUMERIC")
        for t in output_tables
    )
 
    print(f"\n  {'─' * 55}")
    print(f"  CLASSIFIER COMPLETE")
    print(f"  Tables     : {len(output_tables)}")
    print(f"  Columns    : {total_cols}")
    print(f"  Standardize: {std_cols}  (→ Mapping Agent)")
    print(f"  Numeric    : {numeric_cols}  (→ Type casting, excluded from std)")
    print(f"  Saved   → {out_path}")
    print(f"  {'─' * 55}\n")
 
    return output, out_path