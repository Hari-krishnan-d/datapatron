"""
silver_agent/silver_transform.py — Silver Transform Agent
===========================================================
Step S5 of the silver pipeline. Called by orchestrator.py.

Asks user to choose:
  [A] Basic transformation  — LLM auto-suggests renames + type casts, user approves
  [B] Mapping document      — User provides Excel/CSV/JSON file,
                              LLM understands it and builds rules

Both paths produce:  transform_plan.json

FLOW PER TABLE:
  Step 1 — Column renames   (LLM suggests, user approves)
  Step 2 — Type casting     (LLM reads sample_values from silver_metadata.json,
                              recommends Spark SQL types, user approves/edits)
  Step 3 — Operations block (production null handling rules)

HOW SAMPLE DATA FLOWS:
  silver_metadata.json
    └── sources[].tables[].columns[].sample_values   ← 5 real values per column
    └── sources[].tables[].sample                    ← 5 full row dicts

  _build_column_renames()
    reads:  table_profile["columns"][i]["sample_values"]
    copies: sample_values onto each proposed column dict
    so they survive the rename step intact

  _get_type_recommendations()
    reads:  col["sample_values"]  on each renamed column dict
    sends:  to LLM as  source | silver | current_type | samples: ["v1","v2",...]
    LLM sees real values and recommends correct types

  _apply_type_recommendations()
    shows recommendations to user 
    user: [A] approve all / [E] edit individually / [S] skip

NULL HANDLING RULES (production):
  - If the ENTIRE row is null (every column) → DROP the row automatically
  - If only SOME columns are null (partial)  → KEEP the row as-is
    Individual per-column strategies (fill_mean, forward_fill, etc.)
    are applied on top by execute.py without dropping partial-null rows.
  - Replace placeholder strings (N/A, DK, -, NULL, etc.) with NULL in-place
  - drop_duplicates only drops rows where ALL column values are identical
"""

import json
import os
import re
import sys
import logging
from datetime import datetime, timezone

logger = logging.getLogger("Silver.Transform")

# Placeholder strings that silver_execution.py will replace with NULL
# NOTE: Empty string ("") is intentionally NOT included.
#       Empty strings in bronze are kept as-is in silver.
NULL_PLACEHOLDERS = [
    "N/A", "NA", "n/a", "na",
    "None", "none", "NONE",
    "NULL", "null", "Null",
    "NaN", "nan",
    "NIL", "nil",
    "DK", "dk",
    "TBD", "tbd",
    "-", "--", "---",
    "?", "unknown", "Unknown", "UNKNOWN",
]

# Values that count as "no data" when evaluating sample values for type decisions
_EMPTY_SAMPLE_VALUES = {
    "", "null", "NULL", "None", "none", "NONE",
    "N/A", "NA", "n/a", "na",
    "DK", "dk", "TBD", "tbd",
    "-", "--",
}

# Valid Spark SQL types the user can choose from
VALID_SPARK_TYPES = [
    "STRING", "BIGINT", "INT", "DOUBLE", "FLOAT", "DECIMAL(18,2)",
    "BOOLEAN", "DATE", "TIMESTAMP", "BINARY",
]

# ── LLM client ────────────────────────────────────────────────────────────────
try:
    from llm import DatabricksLLMClient, extract_json
except ImportError:
    _here = os.path.dirname(os.path.abspath(__file__))
    _root = os.path.dirname(_here)
    sys.path.insert(0, os.path.join(_root, "MCA"))
    from llm import DatabricksLLMClient, extract_json


# =============================================================================
# HELPERS
# =============================================================================

def _ask(prompt: str, default: str = "", required: bool = True) -> str:
    display = f" [{default}]" if default else ""
    while True:
        value = input(f"  {prompt}{display}: ").strip()
        if not value and default:
            return default
        if value:
            return value
        if not required:
            return ""
        print("  ⚠  Required field.")


def _standardize(name: str) -> str:
    """Convert column name to lowercase_with_underscores."""
    name = name.strip().lower()
    name = re.sub(r"[\s\-]+", "_", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


def _clean_samples(raw_samples: list) -> list:
    """
    Filter sample values to only non-null, non-placeholder real values.
    These are what the LLM uses to infer types — placeholders would confuse it.
    """
    cleaned = []
    for v in raw_samples:
        if v is None:
            continue
        s = str(v).strip()
        if s and s not in _EMPTY_SAMPLE_VALUES:
            cleaned.append(s)
    return cleaned[:5]


def _read_file_content(file_path: str) -> str:
    """Read mapping document content. Supports CSV, JSON, Excel."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".json":
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    if ext == ".csv":
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
    if ext in (".xlsx", ".xls"):
        try:
            import pandas as pd
            df = pd.read_excel(file_path)
            return df.to_csv(index=False)
        except ImportError:
            raise RuntimeError(
                "pandas/openpyxl required to read Excel files.\n"
                "Install with: pip install pandas openpyxl"
            )
    raise ValueError(
        f"Unsupported file type: '{ext}'. Supported: .json, .csv, .xlsx, .xls"
    )


def _production_operations() -> dict:
    """Return the production-safe operations block.

    NULL HANDLING RULE (enforced by silver_execution.py):
      - Whole row all-null  → DROPPED automatically (drop_all_null_rows step)
      - Partial-null row    → KEPT; individual column strategies apply on top
    """
    return {
        "remove_nulls"     : False,
        "drop_duplicates"  : True,
        "standardize_names": True,
        "cast_types"       : True,
        "null_handling"    : {
            "replace_placeholders"     : True,
            "drop_if_all_columns_null" : True,   # enforced in drop_all_null_rows()
            "keep_partial_nulls"       : True,   # partial-null rows are never dropped
            "placeholder_values"       : NULL_PLACEHOLDERS,
        },
    }


# =============================================================================
# STEP 1 — COLUMN RENAMES
# LLM suggests business-friendly names, user approves.
# sample_values are carried forward on each column dict for Step 2.
# =============================================================================

def _build_column_renames(table_profile: dict, llm: DatabricksLLMClient) -> list:
    """
    For each column: auto-standardize name, ask LLM for a cleaner suggestion,
    show full list to user for approval.

    IMPORTANT: sample_values from silver_metadata.json are attached to each
    proposed column dict so they survive into Step 2 (type casting).

    Returns list of column dicts:
      {
        source_column, target_column,
        source_type, target_type,
        sample_values,          ← carried from silver_metadata.json
        null_count,
        transformation,
        cast_recommended, cast_reason
      }
    """
    columns    = table_profile.get("columns", [])
    table_name = table_profile.get("table", "")

    if not columns:
        return []

    # LLM: suggest business-friendly renames
    system = (
        "You are a data engineer helping rename database columns to clean, "
        "business-friendly names.\n"
        "Rules:\n"
        "1. Output ONLY valid JSON — no markdown, no explanation.\n"
        "2. Names must be lowercase_with_underscores.\n"
        "3. Expand abbreviations where obvious:\n"
        "   qty → quantity, dt → date, amt → amount, desc → description,\n"
        "   id → id, num → number, cd → code, nm → name.\n"
        "4. If the name is already clean, keep it as-is.\n"
        "5. Return an array: "
        '[{"source": "<original>", "suggested": "<clean_name>"}]'
    )
    col_list = "\n".join(f"  - {c['name']} ({c['type']})" for c in columns)
    user = (
        f"Table: {table_name}\n"
        f"Columns:\n{col_list}\n\n"
        f"Suggest clean names. Return JSON array only."
    )

    suggestions = {}
    try:
        raw    = llm.call(system, user)
        parsed = extract_json(raw)
        if isinstance(parsed, list):
            for item in parsed:
                suggestions[item.get("source", "")] = item.get("suggested", "")
    except Exception as exc:
        logger.warning("LLM rename suggestion failed (%s) — using standardize fallback.", exc)

    # Build proposed column list.
    # KEY FIX: copy sample_values from the original column metadata here
    # so they are available when _get_type_recommendations() runs in Step 2.
    proposed = []
    for col in columns:
        src  = col["name"]
        std  = _standardize(src)
        sugg = suggestions.get(src, std)
        sugg = _standardize(sugg) if sugg else std

        # Pull sample_values directly from silver_metadata.json column entry
        raw_samples    = col.get("sample_values", [])
        clean_samples  = _clean_samples(raw_samples)

        proposed.append({
            "source_column"   : src,
            "target_column"   : sugg,
            "source_type"     : col.get("type", "STRING"),
            "target_type"     : col.get("type", "STRING"),  # overwritten in Step 2
            "null_count"      : col.get("null_count", 0),
            "null_pct"        : col.get("null_pct", 0.0),
            "sample_values"   : clean_samples,              # carries into Step 2
            "transformation"  : "rename" if sugg != src.lower() else "standardize",
            "cast_recommended": False,
            "cast_reason"     : None,
        })

    # Show rename table to user
    print(f"\n  {'─' * 65}")
    print(f"  TABLE: {table_profile.get('full_table', table_name)}")
    print(f"  STEP 1 — Column Renames")
    print(f"  {'─' * 65}")
    print(f"  {'#':<4} {'SOURCE COLUMN':<30} {'→  SILVER COLUMN':<30} TYPE")
    print(f"  {'─' * 65}")
    for i, col in enumerate(proposed, 1):
        changed = "✏" if col["source_column"].lower() != col["target_column"] else " "
        print(
            f"  {i:<4} {col['source_column']:<30} "
            f"{changed}  {col['target_column']:<28} {col['source_type']}"
        )
    print(f"  {'─' * 65}")
    print(f"\n  [Y] Approve all  |  [E] Edit a column name  |  [S] Skip renames\n")

    while True:
        choice = input("  Your choice [Y/E/S]: ").strip().upper()

        if choice == "Y":
            print("  ✅ Column renames approved.\n")
            return proposed

        elif choice == "S":
            for col in proposed:
                col["target_column"]  = _standardize(col["source_column"])
                col["transformation"] = "standardize"
            print("  ✅ Using standardized originals.\n")
            return proposed

        elif choice == "E":
            while True:
                try:
                    num = int(input(f"  Enter column number to edit (1-{len(proposed)}): "))
                    if 1 <= num <= len(proposed):
                        col      = proposed[num - 1]
                        new_name = input(
                            f"  New name for '{col['source_column']}' "
                            f"[current: {col['target_column']}]: "
                        ).strip()
                        if new_name:
                            col["target_column"]  = _standardize(new_name)
                            col["transformation"] = "rename"
                        print(f"  ✅ Updated → {col['target_column']}")
                        another = input("  Edit another? [Y/N]: ").strip().upper()
                        if another != "Y":
                            break
                    else:
                        print(f"  ⚠  Enter a number between 1 and {len(proposed)}.")
                except ValueError:
                    print("  ⚠  Please enter a number.")

            print(f"\n  Updated column map:")
            for i, col in enumerate(proposed, 1):
                print(f"  {i}. {col['source_column']} → {col['target_column']}")
            confirm = input("\n  Approve this map? [Y/N]: ").strip().upper()
            if confirm == "Y":
                print("  ✅ Column renames approved.\n")
                return proposed

        else:
            print("  ⚠  Enter Y, E, or S.")


# =============================================================================
# STEP 2 — TYPE CASTING RECOMMENDATIONS
# LLM receives column name + current type + real sample values from metadata.
# Recommends correct Spark SQL types. User approves / edits / skips.
# =============================================================================

# =============================================================================
# STEP 2 — TYPE CASTING: RULE-BASED + LLM HYBRID
#
# Layer 1 — Rule engine (instant, no LLM call):
#   Checks sample values first (regex patterns for dates, numbers, booleans),
#   then falls back to column name suffix rules (_id → BIGINT, _date → DATE…).
#   Any column resolved with HIGH confidence by rules skips the LLM entirely.
#
# Layer 2 — LLM (only for columns rules couldn't confidently resolve):
#   Columns that are ambiguous or have no sample data go to LLM in batches of 8.
#   LLM receives: source name | silver name | current type | sample values.
#
# Result: rules handle the obvious cases fast and reliably; LLM handles the
# nuanced cases where real data patterns matter.
# =============================================================================

import re as _re

# ── Sample value patterns ─────────────────────────────────────────────────────
_PAT_INTEGER   = _re.compile(r"^-?\d+$")
_PAT_DECIMAL   = _re.compile(r"^-?\d+\.\d+$")
_PAT_MONEY     = _re.compile(r"^-?\d+\.\d{2}$")
_PAT_DATE      = _re.compile(
    r"^\d{4}-\d{2}-\d{2}$"                          # 2024-01-15
    r"|^\d{2}-\d{2}-\d{4}$"                          # 26-11-2003
    r"|^\d{2}/\d{2}/\d{4}$"                          # 08/02/1971
       r"|^\d{1,2} [A-Za-z]{3} \d{4}$"                  # 26 Feb 2023
    r"|^[A-Za-z]{3} \d{1,2},? \d{4}$"                # Feb 26 2023
    r"|^\d{1,2}-[A-Za-z]{3}-\d{4}$"                  # 26-Feb-2023
)
_PAT_TIMESTAMP = _re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}"             # 2024-01-15 10:30 / T10:30
    r"|^\d{2}-\d{2}-\d{4} \d{2}:\d{2}"               # 15-01-2024 08:00
    r"|^\d{2}/\d{2}/\d{4} \d{2}:\d{2}"               # 08/02/1971 10:30
    r"|^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"         # ISO 8601 Full
)
_PAT_BOOL_TRUE  = {"true", "yes", "y", "1", "on",  "active",   "enabled"}
_PAT_BOOL_FALSE = {"false","no",  "n", "0", "off", "inactive", "disabled"}

# ── Column name suffix → type rules ──────────────────────────────────────────
_SUFFIX_BIGINT = (
    "_id", "_key", "_no", "_num", "_count", "_qty",
    "_quantity", "_age", "_score", "_rank", "_year",
    "_total_count", "_number",
)
_SUFFIX_DOUBLE = (
    "_rate", "_pct", "_ratio", "_percent",
    "_price", "_cost", "_fee", "_tax",
    "_lat", "_lon", "_latitude", "_longitude",
    "_weight", "_height", "_width", "_length",
)
_SUFFIX_DECIMAL = (
    "_amount", "_balance", "_total", "_revenue",
    "_salary", "_wage", "_budget",
)
_SUFFIX_DATE = (
    "_date", "_dt", "_day", "_dob",
    "_birth", "_expiry", "_expiration",
    "_start_date", "_end_date",
)
_SUFFIX_TIMESTAMP = (
    "_at", "_time", "_timestamp", "_datetime",
    "_created", "_updated", "_modified",
    "_processed", "_inserted",
)
_PREFIX_BOOL = ("is_", "has_", "can_", "was_", "did_", "will_")
_EXACT_BOOL  = {"active", "enabled", "disabled", "flagged", "verified",
                "deleted", "archived", "published", "visible"}


def _rule_based_type(col_name: str, samples: list) -> tuple:
    """
    Layer 1: Determine Spark SQL type using rules only.

    Returns (recommended_type, confidence, reason) where confidence is:
      "high"   → rule is certain, skip LLM
      "medium" → rule has a reasonable guess, LLM should confirm
      "none"   → rule cannot decide, send to LLM
    """
    name_lower = col_name.lower().strip()

    # ── Sample-value analysis (highest confidence) ────────────────────────────
    if samples:
        clean = [s for s in samples if s and s.strip()]

        if clean:
            # All samples match timestamp pattern?
            if all(_PAT_TIMESTAMP.match(s) for s in clean):
                return "TIMESTAMP", "high", "all samples are timestamps"

            # All samples match date pattern?
            if all(_PAT_DATE.match(s) for s in clean):
                return "DATE", "high", "all samples are dates"

            # All samples are boolean-like?
            all_vals = {s.lower() for s in clean}
            if all_vals.issubset(_PAT_BOOL_TRUE | _PAT_BOOL_FALSE):
                return "BOOLEAN", "high", "only true/false values"

            # All samples are integers?
            if all(_PAT_INTEGER.match(s) for s in clean):
                # _id/_key columns → BIGINT even if values look like integers
                if any(name_lower.endswith(s) for s in _SUFFIX_BIGINT):
                    return "BIGINT", "high", "numeric id/key column"
                return "BIGINT", "high", "all samples are integers"

            # All samples are money (exactly 2 decimal places)?
            if all(_PAT_MONEY.match(s) for s in clean):
                return "DECIMAL(18,2)", "high", "2 decimal places in samples"

            # All samples are decimal numbers?
            if all(_PAT_DECIMAL.match(s) for s in clean):
                return "DOUBLE", "high", "all samples are decimals"

    # ── Column name suffix rules (medium confidence — no samples available) ──
    # Only applied when samples are empty or all were placeholders

    if any(name_lower.endswith(s) for s in _SUFFIX_TIMESTAMP):
        return "TIMESTAMP", "medium", "column name implies timestamp"

    if any(name_lower.endswith(s) for s in _SUFFIX_DATE):
        return "DATE", "medium", "column name implies date"

    if any(name_lower.endswith(s) for s in _SUFFIX_BIGINT):
        return "BIGINT", "medium", "column name implies integer id/count"

    if any(name_lower.endswith(s) for s in _SUFFIX_DECIMAL):
        return "DECIMAL(18,2)", "medium", "column name implies currency amount"

    if any(name_lower.endswith(s) for s in _SUFFIX_DOUBLE):
        return "DOUBLE", "medium", "column name implies decimal ratio/rate"

    if any(name_lower.startswith(p) for p in _PREFIX_BOOL):
        return "BOOLEAN", "medium", "is_/has_/can_ prefix implies boolean"

    if name_lower in _EXACT_BOOL:
        return "BOOLEAN", "medium", "column name is boolean flag"

    # ── Cannot decide — send to LLM ───────────────────────────────────────────
    return "STRING", "none", ""


# LLM system prompt — only receives columns rules couldn't resolve
TYPE_RECOMMENDATION_SYSTEM = """Recommend Spark SQL types for columns the rule engine couldn't confidently decide.
Output ONLY a JSON array, nothing else.

You receive columns with sample values. Use samples as primary evidence.
If samples are 'none', infer from column name.

Type rules:
- Whole numbers ("17874","42") → BIGINT
- 2 decimal places consistently ("1200.00","750.00") → DECIMAL(18,2)
- Decimals ("3.14","19.99") → DOUBLE
- Date patterns ("2024-01-15","08/02/1971","26 Feb 2023") → DATE
- Datetime with time ("2024-01-15 10:30:00") → TIMESTAMP
- Only true/false or Y/N or 0/1 → BOOLEAN
- Text, names, codes, phone, email, address → STRING
- Uncertain → STRING

Output format (reason max 6 words):
[{"source_column":"<src>","silver_column":"<silver>","current_type":"<t>","recommended_type":"<TYPE>","change_recommended":true,"confidence":"high","reason":"<max 6 words>"}]"""


def _extract_partial_json_array(raw: str) -> list:
    """
    Recover as many valid JSON objects as possible from a truncated JSON array.
    """
    try:
        result = json.loads(raw)
        if isinstance(result, list):
            return result
    except Exception:
        pass

    cleaned = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
    try:
        result = json.loads(cleaned)
        if isinstance(result, list):
            return result
    except Exception:
        pass

    recovered = []
    depth     = 0
    start     = None
    for i, ch in enumerate(cleaned):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                fragment = cleaned[start : i + 1]
                try:
                    obj = json.loads(fragment)
                    if isinstance(obj, dict) and "source_column" in obj:
                        recovered.append(obj)
                except Exception:
                    pass
                start = None
    return recovered


def _get_type_recommendations(columns: list, table_name: str, llm: DatabricksLLMClient) -> list:
    """
    Two-layer type detection:

    Layer 1 — Rule engine runs on every column first.
      HIGH confidence → recommendation stored, column skipped from LLM.
      MEDIUM confidence → recommendation stored, column skipped from LLM.
      NONE → column queued for LLM.

    Layer 2 — LLM called only for columns rules couldn't resolve.
      Sent in batches of 8. Receives sample values for evidence.

    Final list merges both layers, preserving original column order.
    """
    BATCH_SIZE = 8

    try:
        type_llm = DatabricksLLMClient(max_tokens=3000, timeout=60)
    except TypeError:
        type_llm = llm

    rule_results  = {}   # source_column → recommendation dict (from rules)
    llm_queue     = []   # columns to send to LLM

    # ── Layer 1: Rule engine ──────────────────────────────────────────────────
    rule_resolved = 0
    for col in columns:
        src     = col["source_column"]
        silver  = col["target_column"]
        samples = col.get("sample_values", [])

        rec_type, confidence, reason = _rule_based_type(silver, samples)

        if confidence in ("high", "medium"):
            change = rec_type != col["source_type"].upper()
            rule_results[src] = {
                "source_column"     : src,
                "silver_column"     : silver,
                "current_type"      : col["source_type"],
                "recommended_type"  : rec_type,
                "change_recommended": change,
                "confidence"        : confidence,
                "reason"            : reason,
                "_source"           : "rule",
            }
            rule_resolved += 1
        else:
            llm_queue.append(col)

    print(f"  📐 Rules resolved {rule_resolved}/{len(columns)} columns — "
          f"{len(llm_queue)} sent to LLM.")

    # ── Layer 2: LLM for unresolved columns ───────────────────────────────────
    llm_results = {}
    if llm_queue:
        batches = [llm_queue[i : i + BATCH_SIZE] for i in range(0, len(llm_queue), BATCH_SIZE)]

        for batch_num, batch in enumerate(batches, 1):
            print(f"  🤖 LLM batch {batch_num}/{len(batches)} ({len(batch)} columns)...")

            col_lines = []
            for col in batch:
                samples    = col.get("sample_values", [])
                sample_str = ", ".join(f'"{v}"' for v in samples) if samples else "none"
                col_lines.append(
                    f"{col['source_column']}|{col['target_column']}|{col['source_type']}|[{sample_str}]"
                )

            user = (
                f"Table: {table_name}\n"
                f"Format: source|silver|current_type|[samples]\n\n"
                + "\n".join(col_lines)
                + f"\n\nReturn JSON array with exactly {len(batch)} entries. "
                f"Use short reason (6 words max). Only JSON, nothing else."
            )

            try:
                raw    = type_llm.call(TYPE_RECOMMENDATION_SYSTEM, user)
                result = _extract_partial_json_array(raw)
                if result:
                    for r in result:
                        r["_source"] = "llm"
                        llm_results[r.get("source_column", "")] = r
                    logger.info(
                        "LLM batch %d/%d: %d/%d for %s",
                        batch_num, len(batches), len(result), len(batch), table_name
                    )
            except Exception as exc:
                logger.warning("LLM batch %d/%d failed for %s: %s",
                               batch_num, len(batches), table_name, exc)

    # ── Merge: preserve original column order ─────────────────────────────────
    all_recommendations = []
    for col in columns:
        src = col["source_column"]
        rec = rule_results.get(src) or llm_results.get(src)
        if rec:
            all_recommendations.append(rec)
        else:
            # Fallback: no rule, no LLM result — keep as STRING
            all_recommendations.append({
                "source_column"     : src,
                "silver_column"     : col["target_column"],
                "current_type"      : col["source_type"],
                "recommended_type"  : col["source_type"],
                "change_recommended": False,
                "confidence"        : "low",
                "reason"            : "no rule or LLM result",
                "_source"           : "fallback",
            })

    return all_recommendations


def _display_type_recommendations(recommendations: list) -> None:
    """Print a clear table of type recommendations showing rule vs LLM source."""
    changes    = [r for r in recommendations if r.get("change_recommended", False)]
    no_changes = len(recommendations) - len(changes)
    rule_count = sum(1 for r in recommendations if r.get("_source") == "rule")
    llm_count  = sum(1 for r in recommendations if r.get("_source") == "llm")

    print(f"\n  {'─' * 78}")
    print(f"  STEP 2 — Type Casting Recommendations  (📐 rules + 🤖 LLM)")
    print(f"  {'─' * 78}")
    print(f"  Columns analysed : {len(recommendations)}")
    print(f"  Resolved by rules: {rule_count}   By LLM: {llm_count}")
    print(f"  Changes suggested: {len(changes)}   No change: {no_changes}")

    if not changes:
        print(f"\n  ✅ All current types look correct.")
        print(f"  {'─' * 78}\n")
        return

    print(f"\n  {'#':<4} {'SILVER COLUMN':<26} {'FROM':<12} {'→  TO':<16} {'CONF':<7} {'SRC':<5} REASON")
    print(f"  {'─' * 78}")
    idx = 1
    for r in recommendations:
        if not r.get("change_recommended", False):
            continue
        conf   = r.get("confidence", "").upper()[:3]
        reason = r.get("reason", "")
        src    = "📐" if r.get("_source") == "rule" else "🤖"
        if len(reason) > 32:
            reason = reason[:29] + "..."
        print(
            f"  {idx:<4} {r.get('silver_column',''):<26} "
            f"{r.get('current_type',''):<12} → "
            f"{r.get('recommended_type',''):<14} "
            f"[{conf}]  {src}   {reason}"
        )
        idx += 1

    print(f"  {'─' * 78}")


def _apply_type_recommendations(columns: list, table_name: str, llm: DatabricksLLMClient) -> list:
    """
    Step 2: Get LLM type recommendations using sample_values already on each column,
    show recommendations to user, let them approve / edit / skip.

    Updates target_type and transformation on each column.
    Returns the updated columns list.
    """
    print(f"\n  🔍 Analysing column types using sample data from metadata...")

    # Debug: show what sample data we have before sending to LLM
    sample_summary = []
    for col in columns:
        sv = col.get("sample_values", [])
        if sv:
            sample_summary.append(f"{col['source_column']}: {sv}")

    if sample_summary:
        print(f"  Sample data loaded for {len(sample_summary)} column(s).")
    else:
        print(f"  ⚠  No sample values found in metadata.")
        print(f"     LLM will infer types from column names only.")

    recommendations = _get_type_recommendations(columns, table_name, llm)

    if not recommendations:
        print(f"  ⚠  LLM did not return type recommendations. Keeping current types.\n")
        return columns

    _display_type_recommendations(recommendations)

    changes = [r for r in recommendations if r.get("change_recommended", False)]
    if not changes:
        print(f"\n  No type changes recommended — moving on.\n")
        return columns

    print(f"\n  Options:")
    print(f"  [A] Approve ALL recommended type changes")
    print(f"  [E] Edit — review and decide each change individually")
    print(f"  [S] Skip — keep all current types as-is\n")

    while True:
        choice = input("  Your choice [A/E/S]: ").strip().upper()

        # ── Approve all ────────────────────────────────────────────────────────
        if choice == "A":
            # Index recommendations by source_column for fast lookup
            rec_map = {r.get("source_column", ""): r for r in recommendations}
            applied = 0
            for col in columns:
                rec = rec_map.get(col["source_column"])
                if rec and rec.get("change_recommended", False):
                    col["target_type"]     = rec.get("recommended_type", col["source_type"])
                    col["cast_recommended"]= True
                    col["cast_reason"]     = rec.get("reason", "")
                    if "cast" not in col["transformation"]:
                        col["transformation"] += " + cast"
                    applied += 1
            print(f"  ✅ Applied {applied} type cast(s).\n")
            return columns

        # ── Edit individually ──────────────────────────────────────────────────
        elif choice == "E":
            rec_map = {r.get("source_column", ""): r for r in recommendations}
            applied = 0
            skipped = 0

            for col in columns:
                rec = rec_map.get(col["source_column"])
                if not rec or not rec.get("change_recommended", False):
                    continue

                samples = col.get("sample_values", [])
                sample_display = ", ".join(f'"{v}"' for v in samples) if samples else "none"

                print(f"\n  {'─' * 65}")
                print(f"  Column    : {col['target_column']}  (source: {col['source_column']})")
                print(f"  Samples   : [{sample_display}]")
                print(f"  Current   : {col['source_type']}")
                print(f"  Suggested : {rec.get('recommended_type', '')}  [{rec.get('confidence','').upper()}]")
                print(f"  Reason    : {rec.get('reason', '')}")
                print(f"\n  [Y] Apply suggested type")
                print(f"  [C] Choose a different type manually")
                print(f"  [N] Keep current type ({col['source_type']})\n")

                while True:
                    sub = input("  Your choice [Y/C/N]: ").strip().upper()

                    if sub == "Y":
                        col["target_type"]     = rec.get("recommended_type", col["source_type"])
                        col["cast_recommended"]= True
                        col["cast_reason"]     = rec.get("reason", "")
                        if "cast" not in col["transformation"]:
                            col["transformation"] += " + cast"
                        print(f"  ✅ {col['target_column']}: {col['source_type']} → {col['target_type']}")
                        applied += 1
                        break

                    elif sub == "C":
                        print(f"\n  Available Spark SQL types:")
                        for j, t in enumerate(VALID_SPARK_TYPES, 1):
                            print(f"    [{j}] {t}")
                        print(f"  Or type a custom type (e.g. DECIMAL(10,4))\n")
                        raw_input = input("  Enter number or custom type: ").strip()
                        try:
                            idx = int(raw_input) - 1
                            chosen_type = (
                                VALID_SPARK_TYPES[idx]
                                if 0 <= idx < len(VALID_SPARK_TYPES)
                                else None
                            )
                            if chosen_type is None:
                                print("  ⚠  Number out of range.")
                                continue
                        except ValueError:
                            chosen_type = raw_input.upper()

                        col["target_type"]     = chosen_type
                        col["cast_recommended"]= True
                        col["cast_reason"]     = f"Manually set to {chosen_type}"
                        if "cast" not in col["transformation"]:
                            col["transformation"] += " + cast"
                        print(f"  ✅ {col['target_column']}: {col['source_type']} → {col['target_type']}")
                        applied += 1
                        break

                    elif sub == "N":
                        print(f"  ⏭  Keeping {col['source_type']} for {col['target_column']}")
                        skipped += 1
                        break

                    else:
                        print("  ⚠  Enter Y, C, or N.")

            print(f"\n  ✅ Type casting review complete: {applied} applied, {skipped} skipped.\n")
            return columns

        # ── Skip ───────────────────────────────────────────────────────────────
        elif choice == "S":
            print("  ✅ Keeping all current types. No casts applied.\n")
            return columns

        else:
            print("  ⚠  Enter A, E, or S.")


# =============================================================================
# STEP 3 — NULL STRATEGY COLLECTION
# For every column that has nulls, ask the user what to do.
# Decision is stored on col["null_handling"] in transform_plan.json.
# execute.py reads this and applies it mechanically — no decisions there.
# =============================================================================

NUMERIC_TYPES = {
    "BIGINT", "INT", "INTEGER", "LONG",
    "DOUBLE", "FLOAT", "DECIMAL", "DECIMAL(18,2)",
}


def _collect_null_strategies(columns: list) -> list:
    """
    For every column with null_count > 0, ask the user how to handle nulls.
    Stores the decision in col["null_handling"] — a dict that execute.py reads.

    Strategies available:
      Numeric columns : fill_mean / fill_min / fill_max / fill_constant /
                        forward_fill / backward_fill / drop_rows / keep
      String columns  : fill_constant / forward_fill / backward_fill /
                        drop_rows / keep

    "keep" means: leave nulls as-is (no action in execute.py).
    "drop_rows" means: drop any row where this column is null.

    Output added to each column:
      "null_handling": {
          "strategy":   "fill_mean" | "fill_constant" | "forward_fill" | ...
          "fill_value": "0"          # only for fill_constant
      }
    """
    null_cols = [c for c in columns if c.get("null_count", 0) > 0]

    if not null_cols:
        print(f"\n  ✅ No null columns — skipping null strategy step.\n")
        return columns

    print(f"\n  {'=' * 65}")
    print(f"  STEP 3 — Null Handling Strategy")
    print(f"  {len(null_cols)} column(s) have null values.")
    print(f"  For each column choose how execute.py should handle nulls.")
    print(f"  {'=' * 65}")

    for col in columns:
        if col.get("null_count", 0) == 0:
            col["null_handling"] = {"strategy": "keep"}
            continue

        col_name   = col.get("target_column", col.get("source_column", ""))
        null_count = col.get("null_count", 0)
        null_pct   = col.get("null_pct", 0.0)
        tgt_type   = str(col.get("target_type", "STRING")).upper().split("(")[0].strip()
        is_numeric = tgt_type in NUMERIC_TYPES

        print(f"\n  ┌─ Column  : {col_name}")
        print(f"  │  Nulls   : {null_count:,}  ({null_pct}%)")
        print(f"  │  Type    : {tgt_type}")

        if is_numeric:
            print(f"  │  Options :")
            print(f"  │    [1] Fill with MEAN")
            print(f"  │    [2] Fill with MEDIAN")
            print(f"  │    [3] Fill with MIN")
            print(f"  │    [4] Fill with MAX")
            print(f"  │    [5] Fill with a constant value")
            print(f"  │    [6] Forward fill  (carry previous row's value)")
            print(f"  │    [7] Backward fill (carry next row's value)")
            print(f"  │    [8] Drop rows where this column is null")
            print(f"  │    [9] Keep nulls as-is")
            valid = {"1","2","3","4","5","6","7","8","9"}
        else:
            print(f"  │  Options :")
            print(f"  │    [1] Fill with a constant string")
            print(f"  │    [2] Forward fill  (carry previous row's value)")
            print(f"  │    [3] Backward fill (carry next row's value)")
            print(f"  │    [4] Drop rows where this column is null")
            print(f"  │    [5] Keep nulls as-is")
            valid = {"1","2","3","4","5"}

        while True:
            choice = input(f"  └─ Your choice: ").strip()
            if choice not in valid:
                print(f"     ⚠  Enter one of: {', '.join(sorted(valid))}")
                continue

            if is_numeric:
                if   choice == "1": col["null_handling"] = {"strategy": "fill_mean"}
                elif choice == "2": col["null_handling"] = {"strategy": "fill_median"}
                elif choice == "3": col["null_handling"] = {"strategy": "fill_min"}
                elif choice == "4": col["null_handling"] = {"strategy": "fill_max"}
                elif choice == "5":
                    v = input("     Constant value: ").strip()
                    col["null_handling"] = {"strategy": "fill_constant", "fill_value": v}
                elif choice == "6": col["null_handling"] = {"strategy": "forward_fill"}
                elif choice == "7": col["null_handling"] = {"strategy": "backward_fill"}
                elif choice == "8": col["null_handling"] = {"strategy": "drop_rows"}
                elif choice == "9": col["null_handling"] = {"strategy": "keep"}
            else:
                if   choice == "1":
                    v = input("     Constant string: ").strip()
                    col["null_handling"] = {"strategy": "fill_constant", "fill_value": v}
                elif choice == "2": col["null_handling"] = {"strategy": "forward_fill"}
                elif choice == "3": col["null_handling"] = {"strategy": "backward_fill"}
                elif choice == "4": col["null_handling"] = {"strategy": "drop_rows"}
                elif choice == "5": col["null_handling"] = {"strategy": "keep"}

            strat = col["null_handling"]["strategy"]
            print(f"  ✅ {col_name} → {strat}")
            break

    print()
    return columns


# =============================================================================
# PATH A — BASIC TRANSFORMATION
# Step 1 → Step 2 → Step 3 → Operations
# =============================================================================

def _path_a(metadata: dict, llm: DatabricksLLMClient) -> list:
    """Run Path A for all tables in metadata."""
    table_transforms = []

    for source in metadata.get("sources", []):
        for table_profile in source.get("tables", []):
            full_name  = table_profile.get("full_table", table_profile.get("table", ""))
            table_name = table_profile.get("table", "")

            print(f"\n  {'=' * 65}")
            print(f"  Processing table: {full_name}")
            print(f"  {'=' * 65}")

            # Step 1: Column renames — sample_values carried forward on each col
            columns = _build_column_renames(table_profile, llm)

            # Step 2: Type casting — LLM uses col["sample_values"] directly
            columns = _apply_type_recommendations(columns, full_name, llm)

            # Step 3: Null strategy — ask user how to handle each null column
            columns = _collect_null_strategies(columns)

            # Remove sample_values from output plan (not needed downstream)
            for col in columns:
                col.pop("sample_values", None)

            table_transforms.append({
                "pipeline_id": source.get("pipeline_id", ""),
                "table"      : table_name,
                "full_table" : full_name,
                "row_count"  : table_profile.get("row_count", 0),
                "operations" : _production_operations(),
                "columns"    : columns,
            })

    return table_transforms


# =============================================================================
# PATH B — MAPPING DOCUMENT
# LLM reads mapping → type casting layer runs on top with sample values.
# =============================================================================

def _path_b(metadata: dict, llm: DatabricksLLMClient) -> list:
    """Run Path B: user provides a mapping file, LLM interprets it."""
    while True:
        file_path = input(
            "\n  Enter path to your mapping document (.xlsx / .csv / .json):\n  > "
        ).strip().strip('"')
        if not file_path:
            print("  ⚠  File path cannot be empty.")
            continue
        if not os.path.exists(file_path):
            print(f"  ⚠  File not found: {file_path}")
            continue
        break

    try:
        content = _read_file_content(file_path)
        print(f"  ✅ Loaded: {os.path.basename(file_path)}")
    except Exception as exc:
        raise RuntimeError(f"Could not read mapping file: {exc}")

    # Build bronze column context for LLM
    bronze_context_parts = []
    for source in metadata.get("sources", []):
        for tbl in source.get("tables", []):
            cols = ", ".join(
                f"{c['name']} ({c['type']})"
                for c in tbl.get("columns", [])
            )
            bronze_context_parts.append(
                f"Table: {tbl.get('full_table', tbl.get('table', ''))}\n"
                f"Columns: {cols}"
            )
    bronze_context = "\n\n".join(bronze_context_parts)

    system = """
You are a data engineering expert interpreting a mapping document.
Your job:
1. Understand what the document is saying regardless of its column names.
2. Match each mapping entry to the correct bronze source column.
3. Produce a JSON array — one entry per source column.

Rules:
- target_column must be lowercase_with_underscores.
- transformation: "rename", "cast", "rename+cast", "drop", or "keep".
- If the document specifies a type change, reflect it in target_type.
- If a bronze column is NOT in the mapping doc, include it with transformation="keep".
- Return ONLY valid JSON array. No markdown. No explanation.

Output format:
[
  {
    "source_column"  : "<bronze column name>",
    "target_column"  : "<silver column name>",
    "source_type"    : "<bronze type>",
    "target_type"    : "<silver type>",
    "transformation" : "<what to do>",
    "cast_recommended": false,
    "cast_reason"    : null
  }
]
"""
    user = (
        f"Bronze table columns:\n{bronze_context}\n\n"
        f"Mapping document content:\n{content}\n\n"
        f"Produce the column transformation plan as a JSON array."
    )

    print("\n  🧠 LLM is interpreting your mapping document...")

    try:
        raw    = llm.call(system, user)
        parsed = extract_json(raw)
        if not isinstance(parsed, list):
            raise ValueError("LLM did not return a list.")
    except Exception as exc:
        raise RuntimeError(
            f"LLM could not interpret the mapping document: {exc}\nTry Path A instead."
        )

    print(f"\n  {'─' * 65}")
    print(f"  LLM INTERPRETED MAPPING")
    print(f"  {'─' * 65}")
    print(f"  {'SOURCE COLUMN':<30} {'→  SILVER COLUMN':<30} RULE")
    print(f"  {'─' * 65}")
    for row in parsed:
        print(
            f"  {row.get('source_column',''):<30} "
            f"   {row.get('target_column',''):<30} "
            f"{row.get('transformation','')}"
        )
    print(f"  {'─' * 65}")

    confirm = input("\n  Does this look correct? [Y = approve / N = abort]: ").strip().upper()
    if confirm != "Y":
        raise RuntimeError("Mapping rejected by user.")
    print("  ✅ Mapping document approved.\n")

    table_transforms = []
    for source in metadata.get("sources", []):
        for tbl in source.get("tables", []):

            # Build lookup of sample_values from silver_metadata.json
            # so we can attach them to mapped columns for type casting
            sample_lookup = {
                c["name"]: _clean_samples(c.get("sample_values", []))
                for c in tbl.get("columns", [])
            }

            tbl_col_names = {c["name"] for c in tbl.get("columns", [])}
            tbl_map = [row for row in parsed if row.get("source_column", "") in tbl_col_names]

            # Add keep entries for any unmapped columns
            mapped_sources = {r["source_column"] for r in tbl_map}
            for col in tbl.get("columns", []):
                if col["name"] not in mapped_sources:
                    tbl_map.append({
                        "source_column"   : col["name"],
                        "target_column"   : _standardize(col["name"]),
                        "source_type"     : col.get("type", "STRING"),
                        "target_type"     : col.get("type", "STRING"),
                        "transformation"  : "keep",
                        "cast_recommended": False,
                        "cast_reason"     : None,
                    })

            # Attach sample_values from metadata to each mapped column
            for col in tbl_map:
                src = col.get("source_column", "")
                col["sample_values"] = sample_lookup.get(src, [])

            full_name = tbl.get("full_table", tbl.get("table", ""))
            print(f"\n  {'=' * 65}")
            print(f"  Type Casting Review: {full_name}")
            print(f"  {'=' * 65}")

            # Type casting on top of mapping, using sample_values
            tbl_map = _apply_type_recommendations(tbl_map, full_name, llm)

            # Null strategy — ask user per null column
            tbl_map = _collect_null_strategies(tbl_map)

            # Remove sample_values from output
            for col in tbl_map:
                col.pop("sample_values", None)

            table_transforms.append({
                "pipeline_id"  : source.get("pipeline_id", ""),
                "table"        : tbl.get("table", ""),
                "full_table"   : full_name,
                "row_count"    : tbl.get("row_count", 0),
                "mapping_file" : os.path.basename(file_path),
                "operations"   : _production_operations(),
                "columns"      : tbl_map,
            })

    return table_transforms


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_silver_transform(
    metadata_path: str,
    output_dir:    str = ".",
) -> tuple:
    """
    Main entry point. Called by orchestrator.py.

    Args:
        metadata_path : Path to silver_metadata.json
        output_dir    : Where to save transform_plan.json

    Returns:
        (transform_plan: dict, transform_plan_path: str)
    """
    logger.info("Silver Transform Agent started.")

    with open(metadata_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    llm = DatabricksLLMClient()

    print("\n" + "=" * 62)
    print("  SILVER — TRANSFORMATION PLAN")
    print("=" * 62)
    print()
    print("  Step 1 — Column renames  (LLM suggests, you approve)")
    print("  Step 2 — Type casting    (LLM reads sample data from metadata,")
    print("                            recommends types, you decide per column)\n")
    print("  Production null rules:")
    print("  • Entire row all-null → DROPPED automatically")
    print("  • Partial-null row    → KEPT as-is (only some columns null)")
    print("  • DK / N/A / -        → replaced with NULL in-place\n")
    print("  [A] Basic transformation  — LLM suggests renames + type casts")
    print("  [B] Mapping document      — provide .xlsx / .csv / .json\n")

    while True:
        choice = input("  Your choice [A / B]: ").strip().upper()
        if choice in ("A", "B"):
            break
        print("  ⚠  Please enter A or B.")

    print()

    try:
        if choice == "A":
            print("  ▶  Path A: Basic transformation\n")
            table_transforms = _path_a(metadata, llm)
        else:
            print("  ▶  Path B: Mapping document\n")
            table_transforms = _path_b(metadata, llm)
    except RuntimeError as exc:
        raise RuntimeError(f"Transform agent failed: {exc}")

    # ── Type cast summary ─────────────────────────────────────────────────────
    total_casts = sum(
        sum(1 for col in t.get("columns", []) if col.get("cast_recommended"))
        for t in table_transforms
    )
    if total_casts:
        print(f"  {'─' * 55}")
        print(f"  TYPE CASTS APPLIED: {total_casts} total")
        for tbl in table_transforms:
            casts = [col for col in tbl.get("columns", []) if col.get("cast_recommended")]
            if casts:
                print(f"\n  {tbl['full_table']}:")
                for col in casts:
                    print(
                        f"    {col['target_column']:<30} "
                        f"{col['source_type']} → {col['target_type']}"
                    )
        print(f"  {'─' * 55}\n")

    # ── Build and save plan ───────────────────────────────────────────────────
    plan = {
        "session_id"    : metadata.get("session_id", ""),
        "created_at"    : datetime.now(timezone.utc).isoformat(),
        "transform_path": choice,
        "tables"        : table_transforms,
    }

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "transform_plan.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)

    logger.info("Transform plan → %s", out_path)

    total_cols = sum(len(t.get("columns", [])) for t in table_transforms)
    print(f"  {'─' * 55}")
    print(f"  TRANSFORM PLAN COMPLETE")
    print(f"  Path      : {choice}")
    print(f"  Tables    : {len(table_transforms)}")
    print(f"  Columns   : {total_cols}")
    print(f"  Type casts: {total_casts}")
    print(f"  Saved  → {out_path}")
    print(f"  {'─' * 55}\n")

    return plan, out_path
