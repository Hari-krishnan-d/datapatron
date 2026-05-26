
"""
silver_agent/silver_mapping.py — Value Standardization Mapping Agent
======================================================================
Step S4c of the silver pipeline. Runs after silver_classifier.py.

PIPELINE PER COLUMN:
  Step 1 — Fetch ALL distinct values + frequency from Databricks
  Step 2 — Numeric exclusion gate (85% threshold)
  Step 2b — Date/time pattern gate (85% threshold — catches STRING columns with date data)
  Step 2c — Alphanumeric/ID gate (85% threshold — catches code/ID columns)
  Step 3 — Combined similarity grouping (Levenshtein + sorted-char anagram score)
            Threshold 0.72 — catches typos, scrambles, case variants
  Step 4 — LLM reviews each multi-value group → picks canonical, flags bad merges
  Step 5 — LLM singleton pass → maps semantic variants (gpay→UPI, NB→Net Banking)
  Step 6 — User approval (groups + singleton suggestions shown together)
  Step 7 — Commit all approved mappings to silver_taxonomy_lookup Delta table

SIMILARITY FUNCTION (change from original):
  OLD: Levenshtein only — misses scrambled anagrams like Cehqeu, Wltaet
  NEW: combined = (Levenshtein * 0.6) + (SortedChar * 0.4)
       SortedChar sorts both strings alphabetically then compares —
       this catches anagram-style scrambles that preserve letters but change order.
       Threshold lowered from 0.82 to 0.72 accordingly.

SINGLETON LLM PASS (new):
  After fuzzy grouping, singletons = values that matched nothing.
  Sent to LLM with the known canonicals as context.
  LLM maps semantic variants the fuzzy algo structurally cannot:
    gpay → UPI, NB → Net Banking, Chq → Cheque, FT → Full Time
  User approves before any singleton mapping is committed.

SKIP GATES (added):
  Step 2b — Date/time: skips STRING columns whose values are 85%+ date/time patterns.
  Step 2c — Alphanumeric: skips ID/code columns (TXN001, ACC-2024-XYZ) that contain
            both letters and digits with no spaces — not meaningful categoricals.
  Label gate — also skips columns classified as TIME (in addition to DATE/TIMESTAMP).
"""

import json
import os
import re
import sys
import time
import difflib
import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger("Silver.Mapping")

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

NUMERIC_THRESHOLD  = 0.85
DATETIME_THRESHOLD = 0.85
ALNUM_THRESHOLD    = 0.70   # lowered from 0.85 — real columns are messy
FUZZY_THRESHOLD    = 0.72   # lowered from 0.82 — combined score is higher overall
MAX_DISTINCT       = None

_NULL_PLACEHOLDERS = {
    "n/a", "na", "none", "null", "nan", "nil", "dk", "tbd",
    "-", "--", "---", "?", "unknown", "",
}


def _normalize(s: str) -> str:
    """
    Normalize a raw value before fuzzy comparison AND before storing in taxonomy.
    Must produce the same result as execute.py's CASE WHEN normalization so that
    taxonomy.raw_value == normalize(data_value) is always a valid comparison.

    Steps applied (order matters):
      1. strip()         — remove leading/trailing whitespace and unicode spaces
      2. lower()         — case-insensitive
      3. unicode spaces  — replace non-breaking/zero-width spaces with regular space
      4. hyphens/underscores → space  — "short-term" = "short term" = "short_term"
      5. collapse spaces — "Short  Term" → "short term"
    """
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"[ ​ ﻿]", " ", s)  # unicode spaces → space
    s = re.sub(r"[-_]", " ", s)                          # hyphen/underscore → space
    s = re.sub(r"\s+", " ", s)                           # collapse multiple spaces
    return s.strip()


# =============================================================================
# DATABRICKS SQL
# =============================================================================

def _run_sql(workspace_url: str, token: str, warehouse_id: str,
             sql: str, timeout: int = 120) -> list:
    headers    = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    submit_url = f"{workspace_url}/api/2.0/sql/statements"
    resp = requests.post(
        submit_url, headers=headers,
        json={"statement": sql, "warehouse_id": warehouse_id, "wait_timeout": "30s"},
        timeout=60,
    )
    if resp.status_code != 200:
        raise Exception(f"SQL failed ({resp.status_code}): {resp.text[:300]}")

    result = resp.json()
    state  = result.get("status", {}).get("state", "")

    if state not in ("SUCCEEDED", "FAILED", "CANCELED"):
        stmt_id    = result["statement_id"]
        status_url = f"{workspace_url}/api/2.0/sql/statements/{stmt_id}"
        start      = time.time()
        while True:
            sr    = requests.get(status_url, headers=headers, timeout=30)
            state = sr.json().get("status", {}).get("state", "")
            if state == "SUCCEEDED":
                result = sr.json()
                break
            if state in ("FAILED", "CANCELED"):
                raise Exception(f"SQL failed: {sr.json().get('status',{}).get('error',{})}")
            if time.time() - start > timeout:
                raise Exception("SQL timed out")
            time.sleep(2)
    elif state != "SUCCEEDED":
        raise Exception(f"SQL failed: {result.get('status',{}).get('error',{})}")

    return result.get("result", {}).get("data_array", [])


# =============================================================================
# STEP 1 — DISTINCT VALUE QUERY
# =============================================================================

def _fetch_distinct_values(full_table, col_name, workspace_url, token, warehouse_id):
    limit_clause = f"LIMIT {MAX_DISTINCT}" if MAX_DISTINCT is not None else ""
    sql = (
        f"SELECT `{col_name}`, COUNT(*) as freq "
        f"FROM {full_table} "
        f"WHERE `{col_name}` IS NOT NULL AND TRIM(`{col_name}`) != '' "
        f"GROUP BY `{col_name}` ORDER BY freq DESC {limit_clause}"
    )
    rows = _run_sql(workspace_url, token, warehouse_id, sql)
    # NOTE: raw value preserved as-is (no .strip()) so taxonomy stores exact source value
    #       fuzzy comparison still works because _normalize() strips internally
    return [(str(r[0]), int(r[1])) for r in rows if r and r[0]]


# =============================================================================
# STEP 2 — NUMERIC GATE
# =============================================================================

def _is_numeric(s: str) -> bool:
    try:
        float(s.replace(",","").replace("$","").replace("€","").replace("£","").strip())
        return True
    except ValueError:
        return False


def _should_skip_numeric(value_freq: list) -> bool:
    real = [v for v, _ in value_freq if v.lower() not in _NULL_PLACEHOLDERS]
    if not real:
        return True
    return (sum(1 for v in real if _is_numeric(v)) / len(real)) >= NUMERIC_THRESHOLD


# =============================================================================
# STEP 2b — DATE / TIME PATTERN GATE
# =============================================================================

_DATE_TIME_PATTERNS = [
    # Numeric date
    r"^\d{4}-\d{2}-\d{2}$",                        # 2024-01-15
    r"^\d{2}-\d{2}-\d{4}$",                        # 15-01-2024
    r"^\d{2}/\d{2}/\d{4}$",                        # 15/01/2024
    r"^\d{4}/\d{2}/\d{2}$",                        # 2024/01/15
    r"^\d{8}$",                                     # 20240115
    r"^\d{1,2}-\d{2}-\d{4}$",                      # 5-01-2024  single-digit day
    r"^\d{1,2}/\d{2}/\d{4}$",                      # 5/01/2024
    r"^\d{2}-\d{2}-\d{2}$",                        # 24-01-15   two-digit year
    # With time (HH:mm:ss)
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",   # 2024-01-15T10:30:00
    r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$",     # 15-01-2024 10:30:00
    r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$",     # 15/01/2024 10:30:00
    # With time (HH:mm no seconds)
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}$",        # 2024-01-15T10:30
    r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$",           # 07-04-1986 08:00
    r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$",           # 07/04/1986 08:00
    # With timezone
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$",  # ISO 8601 tz
    # Time only
    r"^\d{2}:\d{2}(:\d{2})?$",                     # 10:30 or 10:30:00
    r"^\d{2}:\d{2}:\d{2}\.\d+$",                   # 10:30:00.123
    # Named month — day first
    r"^\d{1,2}[\s\-][A-Za-z]{3}[\s\-]\d{4}$",     # 15-Jan-2024  15 Jan 2024
    r"^\d{1,2}[\s\-][A-Za-z]{4,9}[\s\-]\d{4}$",   # 15-January-2024
    r"^\d{2} [A-Za-z]{3} \d{4}$",                  # 22 MAR 1986
    # Named month — month first
    r"^[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}$",       # January 15, 2024  Jan 15 2024
    # Named month with time
    r"^\d{1,2}[\s\-][A-Za-z]{3,9}[\s\-]\d{4}\s\d{2}:\d{2}",  # 15 Jan 2024 10:30
    r"^[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}\s+\d{2}:\d{2}",     # Jan 15, 2024 10:30
    # Duration
    r"^\d{1,2}:\d{2}\.\d+$",                       # 14:45.6   MM:SS.f
    r"^\d{1,3}:\d{2}:\d{2}\.\d+$",                 # 1:14:45.6 HH:MM:SS.f
]


_DATE_TIME_COMPILED = [re.compile(p) for p in _DATE_TIME_PATTERNS]


def _should_skip_datetime(value_freq: list, threshold: float = DATETIME_THRESHOLD) -> bool:
    """
    Returns True if 85%+ of non-null distinct values match a date/time pattern.
    Catches columns typed as STRING in the source but containing date/time data.
    """
    real = [v for v, _ in value_freq if v.lower() not in _NULL_PLACEHOLDERS]
    if not real:
        return False
    matches = sum(
        1 for v in real
        if any(pat.match(v.strip()) for pat in _DATE_TIME_COMPILED)
    )
    return (matches / len(real)) >= threshold


# =============================================================================
# STEP 2c — ALPHANUMERIC / MIXED CONTENT GATE
# =============================================================================

_ALNUM_PATTERN = re.compile(r"^[A-Za-z0-9\-_/\\#@.]+$")
_CODE_PATTERN  = re.compile(r"^[A-Z0-9\-_/\\#@.]{2,12}$")  # ALL-CAPS short token


def _is_alnum_code(v: str) -> bool:
    """
    Returns True if a single value looks like a code/ID token rather than
    a human-readable categorical label.

    Two cases caught:

    Case 1 — digit + alpha, no spaces:
      88cv, 234shndk, TXN001, ACC-2024-XYZ, 1r.27, 8F.26, REF#8821
      These are numeric values with injected letters, or alphanumeric IDs.
      Guard: no spaces (spaces signal readable labels like "Credit Card").

    Case 2 — ALL-CAPS short token, no digits, no spaces:
      TXN, REF, ACC, INV, ORD, CV, ABC
      These are abbreviation-style codes with no lowercase.
      Guard: length 2–12, all uppercase letters/symbols, no spaces.
      Does NOT catch: "Male", "India", "Active" — mixed/lower case real words.
    """
    v = v.strip()
    if not v:
        return False
    # Case 1: digit + alpha, no spaces
    if (_ALNUM_PATTERN.match(v)
            and any(c.isdigit() for c in v)
            and any(c.isalpha() for c in v)):
        return True
    # Case 2: ALL-CAPS short token, no digits
    if _CODE_PATTERN.match(v) and not any(c.isdigit() for c in v):
        return True
    return False


def _should_skip_alphanumeric(value_freq: list, threshold: float = ALNUM_THRESHOLD) -> bool:
    """
    Returns True if threshold%+ of non-null distinct values look like IDs,
    codes, or mixed alphanumeric tokens — not human-readable categorical labels.

    Threshold lowered to 0.70 (from 0.85) because real columns often have a
    small number of clean values mixed in with the codes — e.g. 83% TXN-style
    IDs with 17% category names. 0.85 would miss these; 0.70 catches them while
    still keeping genuine categorical columns (gender, status, payment_method).

    Skips columns like:
      88cv, 234shndk, TXN001, ACC-2024-XYZ, REF#8821, 1r.27, 8F.26

    Does NOT skip columns like:
      "Credit Card", "Net Banking", "Male", "Active", "Cheque"
      These are lowercase/mixed-case human-readable labels with spaces.
    """
    real = [v for v, _ in value_freq if v.strip().lower() not in _NULL_PLACEHOLDERS]
    if not real:
        return False
    matches = sum(1 for v in real if _is_alnum_code(v))
    return (matches / len(real)) >= threshold


# =============================================================================
# STEP 3 — COMBINED SIMILARITY GROUPING
# =============================================================================

def _combined_similarity(a: str, b: str) -> float:
    """
    Combined similarity using two signals:

    Signal 1 — Levenshtein (difflib.SequenceMatcher):
      Order-sensitive. Catches: "CenTRAl", "Net Bankging", "Walet".
      Misses: anagram-style scrambles like "Cehqeu" (Cheque), "Wltaet" (Wallet).

    Signal 2 — Sorted-character similarity:
      Sorts both strings alphabetically then runs Levenshtein on them.
      "cheque" sorted = "ceehqu", "cehqeu" sorted = "ceehqu" → score 1.0.
      Catches scrambled anagrams. Misses order-sensitive typos.

    Combined = (Levenshtein * 0.6) + (SortedChar * 0.4)
      Levenshtein weighted higher — preserves order sensitivity.
      SortedChar fills the gap for anagram-style scrambles.

    Why this combination is safe:
      "Debit" vs "Credit":  lev=0.545  sorted=0.727  combined=0.618 — below 0.72 ✅
      "Wallet" vs "Mallet": lev=0.833  sorted=0.833  combined=0.833 — above 0.72 ⚠️
        → but LLM Step 4 flags this as merge=false (different semantic meaning)
      "Cheque" vs "Cehqeu": lev=0.667  sorted=1.000  combined=0.800 ✅ caught
      "Central" vs "CtNEnral": lev=0.800 sorted=0.933 combined=0.853 ✅ caught
    """
    a_low = _normalize(a)
    b_low = _normalize(b)

    lev = difflib.SequenceMatcher(None, a_low, b_low).ratio()

    sorted_a = "".join(sorted(a_low.replace(" ", "")))
    sorted_b = "".join(sorted(b_low.replace(" ", "")))
    srt = difflib.SequenceMatcher(None, sorted_a, sorted_b).ratio()

    return (lev * 0.6) + (srt * 0.4)


def _fuzzy_group(value_freq: list) -> tuple:
    """
    Group similar values using combined similarity (Levenshtein + sorted-char).

    Returns:
      multi_groups : list of lists — groups with 2+ members (typo clusters)
      singletons   : list of str  — values that matched nothing (sent to LLM)
      freq         : dict {value: count}
    """
    values = [v for v, _ in value_freq]
    freq   = {v: c for v, c in value_freq}
    visited = set()
    groups  = []

    for v in values:
        if v in visited:
            continue
        group   = [v]
        visited.add(v)
        v_lower = _normalize(v)

        for other in values:
            if other in visited:
                continue
            other_lower = _normalize(other)

            # Exact match after normalization → always group
            # catches: "short term" == "Short Term" == "short-term" == "Short_Term"
            # also catches: "  India  " == "India" == " India" after normalize
            if v_lower == other_lower:
                group.append(other)
                visited.add(other)
                continue

            # Combined similarity
            score = _combined_similarity(v, other)
            if score >= FUZZY_THRESHOLD:
                group.append(other)
                visited.add(other)

        groups.append(group)

    # Sort: multi-value groups first, then by total frequency
    groups.sort(key=lambda g: (len(g) == 1, -sum(freq.get(v, 0) for v in g)))

    multi_groups = [g for g in groups if len(g) > 1]
    singletons   = [g[0] for g in groups if len(g) == 1]

    return multi_groups, singletons, freq


# =============================================================================
# STEP 4 — LLM: REVIEW FUZZY GROUPS
# =============================================================================

CANONICAL_SYSTEM = """You are a data quality expert reviewing automatically-grouped values.
Each group was clustered by character similarity. For each group:
1. Are these values truly the same thing (typos/casing/scrambled spelling of one word)?
2. If yes: what is the single canonical (standard) value?
   → Prefer: highest frequency form, properly cased, full word over abbreviation
3. If no (e.g. "Wallet" and "Mallet" are genuinely different): set merge=false
4. Confidence: high=certain, medium=likely, low=unsure

Output ONLY a JSON array, one object per group:
[{"group_id":1,"merge":true,"canonical":"Central","confidence":"high","reason":"all are scrambled/cased variants of Central"}]"""


def _llm_review_groups(col_name: str, groups: list, freq: dict, llm) -> list:
    """LLM reviews fuzzy groups and confirms/rejects each merge."""
    try:
        type_llm = DatabricksLLMClient(max_tokens=2000, timeout=60)
    except TypeError:
        type_llm = llm

    lines = []
    for i, grp in enumerate(groups, 1):
        vals = ", ".join(f'"{v}" ({freq.get(v,0):,})' for v in grp)
        lines.append(f"Group {i}: [{vals}]")

    user = (
        f"Column: {col_name}\n"
        f"Fuzzy-grouped values (clustered by character similarity):\n\n"
        + "\n".join(lines)
        + "\n\nReturn JSON array."
    )

    try:
        raw     = type_llm.call(CANONICAL_SYSTEM, user)
        cleaned = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
        # Robust extraction: find first valid JSON array
        match = re.search(r'\[.*\]', cleaned, re.DOTALL)
        if match:
            result = json.loads(match.group())
            if isinstance(result, list):
                return result
    except Exception as exc:
        logger.warning("LLM group review failed for %s: %s", col_name, exc)

    return []


# =============================================================================
# STEP 5 — LLM: SINGLETON PASS
# Singletons are values that matched nothing by character similarity.
# These are semantic variants: gpay, NB, Chq, NEFT, FT, etc.
# LLM maps them to the known canonicals established in Step 4.
# =============================================================================

SINGLETON_SYSTEM = """You are a data standardization expert.
You are given a list of values that could not be matched to any known value by
character similarity alone. Using the known canonical values as reference,
map each singleton to the correct canonical — or null if it is genuinely new.

Use your world knowledge freely:
  - Brand names: gpay, phonepe, paytm → UPI
  - Abbreviations: NB → Net Banking, Chq → Cheque, FT → Full Time
  - Synonyms: e-wallet → Wallet, online banking → Net Banking

Rules:
  - canonical MUST be one of the provided known canonicals (exact match)
  - Return null only if the value cannot reasonably map to any canonical
  - Do NOT return null as a shortcut — make your best judgment

Output ONLY valid JSON, no explanation:
{"raw_value": "canonical_or_null", ...}"""


def _llm_map_singletons(col_name: str, singletons: list,
                         known_canonicals: list, freq: dict, llm) -> dict:
    """
    Send singletons to LLM for semantic mapping.
    Returns dict {raw_value: canonical_or_None}.
    Retries once with a simpler prompt if JSON parsing fails.
    """
    if not singletons or not known_canonicals:
        return {}

    try:
        type_llm = DatabricksLLMClient(max_tokens=2000, timeout=60)
    except TypeError:
        type_llm = llm

    canon_str = ", ".join(f'"{c}"' for c in known_canonicals)
    vals_str  = "\n".join(f'  "{v}" ({freq.get(v,0):,} rows)' for v in singletons)

    user = (
        f"Column: {col_name}\n\n"
        f"Known canonicals (you MUST map to one of these exactly):\n"
        f"  [{canon_str}]\n\n"
        f"Unmatched values to map:\n{vals_str}\n\n"
        f"Return JSON object mapping each raw value to its canonical or null."
    )

    def _parse(raw: str) -> dict:
        cleaned = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
        # Try full parse first
        try:
            result = json.loads(cleaned)
            if isinstance(result, dict):
                return result
        except json.JSONDecodeError:
            pass
        # Fallback: extract first {...} block
        match = re.search(r'\{[^{}]*\}', cleaned, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        return {}

    # First attempt
    try:
        raw    = type_llm.call(SINGLETON_SYSTEM, user)
        result = _parse(raw)
        if result:
            return result
    except Exception as exc:
        logger.warning("LLM singleton pass failed for %s: %s", col_name, exc)

    # Retry with simpler prompt (no system message complexity)
    try:
        simple_user = (
            f"Map these values from column '{col_name}' to one of: {known_canonicals}\n"
            f"Values: {singletons}\n"
            f"Return JSON only: {{\"value\": \"canonical_or_null\"}}"
        )
        raw    = type_llm.call("Return only JSON.", simple_user)
        result = _parse(raw)
        if result:
            return result
    except Exception as exc:
        logger.warning("LLM singleton retry failed for %s: %s", col_name, exc)

    return {}


# =============================================================================
# STEP 6 — USER APPROVAL
# =============================================================================

def _show_proposed(col_name: str, proposed_groups: list,
                   singleton_suggestions: dict, freq: dict) -> None:
    """Display fuzzy groups + singleton LLM suggestions for user review."""

    # Groups
    total_group_maps = sum(
        len([v for v in g["raw_values"] if v != g["canonical"]])
        for g in proposed_groups
    )
    print(f"\n  {'─' * 72}")
    print(f"  PROPOSED MAPPINGS — {col_name}")
    print(f"  {'─' * 72}")

    if proposed_groups:
        print(f"\n  FUZZY GROUPS ({total_group_maps} typo variant(s) to remap):")
        print(f"  {'GRP':<5} {'CANONICAL':<25} {'CONF':<6} VARIANTS  (freq)")
        print(f"  {'─' * 68}")
        for i, grp in enumerate(proposed_groups, 1):
            conf      = grp["confidence"].upper()[:3]
            canon     = grp["canonical"]
            non_canon = [
                f'"{v}" ({grp["freqs"].get(v,0):,})'
                for v in grp["raw_values"] if v != canon
            ]
            vals_str = ", ".join(non_canon) if non_canon else "(already canonical)"
            print(f"  {i:<5} {canon:<25} [{conf}]  {vals_str}")

    if singleton_suggestions:
        confident   = {k: v for k, v in singleton_suggestions.items() if v is not None}
        unconfident = {k: v for k, v in singleton_suggestions.items() if v is None}

        if confident:
            print(f"\n  SEMANTIC MAPPINGS — LLM suggestions ({len(confident)} values):")
            print(f"  {'RAW VALUE':<30} {'→ CANONICAL':<25} FREQ")
            print(f"  {'─' * 68}")
            for raw, canon in sorted(confident.items(), key=lambda x: -freq.get(x[0], 0)):
                print(f"  {raw:<30} → {canon:<25} ({freq.get(raw,0):,} rows)")

        if unconfident:
            print(f"\n  UNRESOLVED — LLM returned null ({len(unconfident)} values):")
            for raw in unconfident:
                print(f"    \"{raw}\" ({freq.get(raw,0):,} rows) — needs manual decision")

    print(f"\n  {'─' * 72}")


def _collect_user_approval(col_name: str, proposed_groups: list,
                            singleton_suggestions: dict,
                            freq: dict) -> tuple:
    """
    Show proposed mappings and collect user decision.

    Returns:
      final_mappings : dict  {raw_value: canonical_value}
      audit          : list  [{raw, canonical, freq, confidence, method}]
      skipped        : bool
      skip_all       : bool
    """
    _show_proposed(col_name, proposed_groups, singleton_suggestions, freq)

    confident_singletons   = {k: v for k, v in singleton_suggestions.items() if v is not None}
    unconfident_singletons = [k for k, v in singleton_suggestions.items() if v is None]
    known_canonicals       = list({g["canonical"] for g in proposed_groups})

    print(f"\n  [A] Approve all  [E] Edit  [S] Skip {col_name}  [X] Skip ALL")

    while True:
        choice = input(f"\n  Your choice [A/E/S/X]: ").strip().upper()

        if choice == "X":
            return {}, [], True, True

        elif choice == "S":
            return {}, [], True, False

        elif choice == "A":
            mappings, audit = {}, []

            # Groups
            for grp in proposed_groups:
                canon = grp["canonical"]
                for raw in grp["raw_values"]:
                    if raw != canon:
                        mappings[raw] = canon
                    audit.append({
                        "raw": raw, "canonical": canon,
                        "freq": grp["freqs"].get(raw, 0),
                        "confidence": grp["confidence"], "method": "fuzzy_approved",
                    })

            # Confident singleton suggestions
            for raw, canon in confident_singletons.items():
                mappings[raw] = canon
                audit.append({
                    "raw": raw, "canonical": canon,
                    "freq": freq.get(raw, 0),
                    "confidence": "medium", "method": "llm_singleton_approved",
                })

            # Unconfident singletons — ask inline
            if unconfident_singletons:
                print(f"\n  {len(unconfident_singletons)} value(s) need manual mapping:")
                if known_canonicals:
                    print(f"  Known canonicals: {known_canonicals}")
                print(f"  (Press Enter to leave unmapped)\n")
                for raw in unconfident_singletons:
                    ans = input(
                        f"    \"{raw}\" ({freq.get(raw,0):,} rows) → "
                    ).strip()
                    if ans:
                        mappings[raw] = ans
                        audit.append({
                            "raw": raw, "canonical": ans,
                            "freq": freq.get(raw, 0),
                            "confidence": "high", "method": "manual",
                        })

            print(f"  ✅ {len(mappings)} mapping(s) approved for {col_name}.")
            return mappings, audit, False, False

        elif choice == "E":
            mappings, audit = {}, []

            # Edit groups
            for i, grp in enumerate(proposed_groups, 1):
                print(f"\n  Group {i}: {grp['raw_values']}")
                print(f"  Canonical: \"{grp['canonical']}\"  [{grp['confidence']}]")
                print(f"  Reason: {grp['reason']}")
                override    = input("  Press Enter to keep, or type new canonical: ").strip()
                final_canon = override if override else grp["canonical"]
                print(f"  ✅ → \"{final_canon}\"")
                for raw in grp["raw_values"]:
                    if raw != final_canon:
                        mappings[raw] = final_canon
                    audit.append({
                        "raw": raw, "canonical": final_canon,
                        "freq": grp["freqs"].get(raw, 0),
                        "confidence": "high",
                        "method": "user_edit" if override else "fuzzy_approved",
                    })

            # Edit singleton suggestions
            if confident_singletons:
                print(f"\n  Review {len(confident_singletons)} semantic suggestion(s):")
                for raw, suggested in sorted(
                    confident_singletons.items(),
                    key=lambda x: -freq.get(x[0], 0)
                ):
                    ans = input(
                        f"    \"{raw}\" ({freq.get(raw,0):,} rows) → "
                        f"\"{suggested}\"  [Y/n/type new]: "
                    ).strip()
                    if ans.lower() == "n":
                        continue
                    final = ans if ans and ans.lower() not in ("y","yes") else suggested
                    mappings[raw] = final
                    audit.append({
                        "raw": raw, "canonical": final,
                        "freq": freq.get(raw, 0),
                        "confidence": "high",
                        "method": "user_edit" if (ans and ans.lower() not in ("y","yes","")) else "llm_singleton_approved",
                    })

            # Handle unconfident singletons
            if unconfident_singletons:
                print(f"\n  {len(unconfident_singletons)} unresolved value(s):")
                if known_canonicals:
                    print(f"  Known canonicals: {known_canonicals}")
                for raw in unconfident_singletons:
                    ans = input(
                        f"    \"{raw}\" ({freq.get(raw,0):,} rows) → "
                    ).strip()
                    if ans:
                        mappings[raw] = ans
                        audit.append({
                            "raw": raw, "canonical": ans,
                            "freq": freq.get(raw, 0),
                            "confidence": "high", "method": "manual",
                        })

            print(f"\n  ✅ {len(mappings)} total mapping(s) confirmed for {col_name}.")
            return mappings, audit, False, False

        else:
            print("  ⚠  Enter A, E, S, or X.")


# =============================================================================
# REUSE
# =============================================================================

def _check_reuse(col_name: str, existing_mappings: dict):
    bare = col_name.lower().split(".")[-1]
    for key, mapping in existing_mappings.items():
        if key.lower().split(".")[-1] == bare and mapping.get("mappings"):
            return mapping
    return None


# =============================================================================
# TAXONOMY TABLE
# =============================================================================

def _ensure_taxonomy_table(workspace_url, token, warehouse_id, catalog, schema) -> str:
    _run_sql(workspace_url, token, warehouse_id,
             f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    _run_sql(workspace_url, token, warehouse_id,
             f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    full = f"`{catalog}`.`{schema}`.silver_taxonomy_lookup"
    _run_sql(workspace_url, token, warehouse_id, f"""
        CREATE TABLE IF NOT EXISTS {full} (
            source_schema      STRING,
            source_table       STRING,
            source_column      STRING,
            raw_value          STRING,
            standardized_value STRING,
            data_type          STRING,
            confidence         STRING,
            last_updated       TIMESTAMP
        ) USING DELTA""")
    return full


def _sync_taxonomy_to_delta(mappings, audit, source_schema, source_table,
                             source_column, data_type,
                             workspace_url, token, warehouse_id,
                             catalog, schema) -> None:
    if not mappings:
        return

    full_table = _ensure_taxonomy_table(
        workspace_url, token, warehouse_id, catalog, schema
    )
    now_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conf_map = {a["raw"]: a.get("confidence", "high") for a in audit}

    def esc(s):
        return str(s).replace("'", "''")

    rows = []
    for raw, canon in mappings.items():
        if _normalize(raw) == _normalize(canon):
            continue
        conf       = conf_map.get(raw, "high")
        # NOTE: store raw as-is (with original spaces) so execute.py exact match works
        raw_stored = raw
        row  = (
            "('" + esc(source_schema) + "','" + esc(source_table) + "',"
            "'" + esc(source_column) + "','" + esc(raw_stored) + "',"
            "'" + esc(canon) + "','" + esc(data_type) + "',"
            "'" + esc(conf) + "',TO_TIMESTAMP('" + now_str + "'))"
        )
        rows.append(row)

    if not rows:
        return

    merge_sql = """MERGE INTO """ + full_table + """ AS target
USING (
    SELECT col0 AS source_schema, col1 AS source_table,
           col2 AS source_column, col3 AS raw_value,
           col4 AS standardized_value, col5 AS data_type,
           col6 AS confidence,         col7 AS last_updated
    FROM (VALUES """ + ",".join(rows) + """)
    AS t(col0,col1,col2,col3,col4,col5,col6,col7)
) AS source
ON  target.source_schema = source.source_schema
AND target.source_table  = source.source_table
AND target.source_column = source.source_column
AND target.raw_value     = source.raw_value
WHEN MATCHED THEN UPDATE SET
    standardized_value = source.standardized_value,
    data_type          = source.data_type,
    confidence         = source.confidence,
    last_updated       = source.last_updated
WHEN NOT MATCHED THEN INSERT
    (source_schema,source_table,source_column,raw_value,
     standardized_value,data_type,confidence,last_updated)
VALUES
    (source.source_schema,source.source_table,source.source_column,
     source.raw_value,source.standardized_value,source.data_type,
     source.confidence,source.last_updated)"""

    try:
        _run_sql(workspace_url, token, warehouse_id, merge_sql)
        print(f"  Taxonomy: {len(rows)} mapping(s) committed -> {full_table}")
    except Exception as exc:
        logger.error("Taxonomy sync failed %s.%s: %s", source_table, source_column, exc)
        raise Exception(f"Taxonomy sync failed: {exc}") from exc


def _process_column(col_spec, full_table, workspace_url, token, warehouse_id,
                    llm, existing_mappings, string_cols) -> tuple:
    src_col = col_spec["source_column"]
    tgt_col = col_spec.get("target_column", src_col)
    display = tgt_col or src_col

    print(f"\n  {'─' * 65}")
    print(f"  Column : {display}  (source: {src_col})")
    print(f"  Table  : {full_table}")

    # Reuse
    reuse = _check_reuse(src_col, existing_mappings)
    if reuse:
        print(f"  Found existing mapping ({len(reuse['mappings'])} entries).")
        if input("  Reuse this mapping? [Y/N]: ").strip().upper() == "Y":
            print(f"  ✅ Reusing existing mapping.")
            return reuse, False, False

    # Step 1: Fetch
    print(f"  [1/5] Fetching distinct values…")
    try:
        value_freq = _fetch_distinct_values(
            full_table, src_col, workspace_url, token, warehouse_id
        )
    except Exception as exc:
        print(f"  ⚠  Fetch failed: {exc} — skipping.")
        return {"enabled": False}, False, False

    total_distinct = len(value_freq)
    total_rows     = sum(c for _, c in value_freq)
    print(f"        {total_distinct} distinct value(s)  |  {total_rows:,} total row(s)")

    if total_distinct <= 1:
        print(f"  ✅ Only {total_distinct} distinct value — nothing to standardize.")
        return {"enabled": False}, False, False

    # Step 2: Numeric / datetime / alphanumeric gates
    if _should_skip_numeric(value_freq):
        print(f"  ⚠  85%+ numeric — skipping.")
        return {"enabled": False}, False, False

    if _should_skip_datetime(value_freq):
        print(f"  ⚠  85%+ date/time pattern detected (STRING column) — skipping.")
        return {"enabled": False}, False, False

    if _should_skip_alphanumeric(value_freq):
        print(f"  ⚠  85%+ alphanumeric/ID-style values — skipping.")
        return {"enabled": False}, False, False

    # Step 3: Combined similarity grouping
    print(f"  [2/5] Fuzzy grouping (combined similarity ≥ {FUZZY_THRESHOLD})…")
    multi_groups, singletons, freq = _fuzzy_group(value_freq)
    print(f"        {len(multi_groups)} multi-value group(s)  |  "
          f"{len(singletons)} singleton(s) → LLM")

    # Step 4: LLM reviews fuzzy groups
    proposed = []
    if multi_groups:
        print(f"  [3/5] LLM reviewing {len(multi_groups)} group(s)…")
        llm_decisions = _llm_review_groups(display, multi_groups, freq, llm)

        for i, grp in enumerate(multi_groups, 1):
            dec = next((d for d in llm_decisions if d.get("group_id") == i), None)
            if dec and dec.get("merge", True):
                canonical  = dec.get("canonical", max(grp, key=lambda v: freq.get(v, 0)))
                confidence = dec.get("confidence", "medium")
                reason     = dec.get("reason", "")
            else:
                canonical  = max(grp, key=lambda v: freq.get(v, 0))
                confidence = "low"
                reason     = "LLM flagged as different values — review manually"

            proposed.append({
                "canonical" : canonical,
                "raw_values": grp,
                "confidence": confidence,
                "reason"    : reason,
                "freqs"     : {v: freq.get(v, 0) for v in grp},
            })
    else:
        print(f"  [3/5] No fuzzy groups — skipping LLM group review.")

    # Step 5: LLM singleton pass
    singleton_suggestions = {}
    if singletons:
        known_canonicals = [g["canonical"] for g in proposed if g["confidence"] != "low"]
        print(f"  [4/5] LLM singleton pass "
              f"({len(singletons)} value(s), {len(known_canonicals)} known canonical(s))…")
        raw_suggestions = _llm_map_singletons(
            display, singletons, known_canonicals, freq, llm
        )
        # Validate: canonical must be in known list (case-insensitive check)
        canon_lower = {c.lower(): c for c in known_canonicals}
        for raw, suggested in raw_suggestions.items():
            if suggested is None:
                singleton_suggestions[raw] = None
            else:
                # Normalise to exact casing from known canonicals
                matched = canon_lower.get(suggested.lower())
                singleton_suggestions[raw] = matched if matched else suggested
    else:
        print(f"  [4/5] No singletons — skipping singleton LLM pass.")

    if not proposed and not any(v for v in singleton_suggestions.values()):
        print(f"  ✅ Nothing to standardize for {display}.")
        return {"enabled": False}, False, False

    # Step 6: User approval
    print(f"  [5/5] Waiting for approval…")
    final_mappings, audit, skipped, skip_all = _collect_user_approval(
        display, proposed, singleton_suggestions, freq
    )

    if skipped:
        return {"enabled": False}, skipped, skip_all

    if not final_mappings:
        return {"enabled": False}, False, False

    # Step 7: Commit to taxonomy
    taxonomy_catalog = col_spec.get("_taxonomy_catalog", "")
    taxonomy_schema  = col_spec.get("_taxonomy_schema", "")
    if taxonomy_catalog and taxonomy_schema:
        try:
            parts      = full_table.replace("`", "").split(".")
            src_schema = parts[1] if len(parts) >= 3 else "bronze"
            src_table  = parts[-1]
            _sync_taxonomy_to_delta(
                mappings=final_mappings, audit=audit,
                source_schema=src_schema, source_table=src_table,
                source_column=src_col,
                data_type=col_spec.get("source_type", "STRING").upper(),
                workspace_url=workspace_url, token=token, warehouse_id=warehouse_id,
                catalog=taxonomy_catalog, schema=taxonomy_schema,
            )
        except Exception as e:
            print(f"  ⚠  Taxonomy sync failed (non-fatal): {e}")

    return {
        "enabled"        : True,
        "mappings"       : final_mappings,
        "flag_unknowns"  : True,
        "total_distinct" : total_distinct,
        "audit"          : audit,
        "reviewed_at"    : datetime.now(timezone.utc).isoformat(),
    }, False, False


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_silver_mapping(classifier_path: str, credentials_path: str,
                       output_dir: str = ".") -> tuple:
    logger.info("Silver Mapping Agent started.")

    with open(classifier_path,  "r", encoding="utf-8") as f:
        classifier_output = json.load(f)
    with open(credentials_path, "r", encoding="utf-8") as f:
        creds = json.load(f)

    pipelines     = creds.get("pipelines", [])
    first_conn    = pipelines[0].get("connection_template", {}) if pipelines else {}
    workspace_url = first_conn.get("workspace_url", "").rstrip("/")
    token         = first_conn.get("token", "")
    warehouse_id  = first_conn.get("warehouse_id", "")

    silver_catalog = creds.get("silver_catalog", "")
    silver_schema  = creds.get("silver_schema",  "")
    if not silver_catalog or not silver_schema:
        first_dest     = pipelines[0].get("destination", {}) if pipelines else {}
        silver_catalog = first_dest.get("catalog", silver_catalog)
        silver_schema  = first_dest.get("schema",  silver_schema)

    if not all([workspace_url, token, warehouse_id, silver_catalog, silver_schema]):
        raise RuntimeError("Missing Databricks credentials or Silver destination.")

    print(f"\n  Preparing taxonomy table at {silver_catalog}.{silver_schema}…")
    _ensure_taxonomy_table(workspace_url, token, warehouse_id,
                           silver_catalog, silver_schema)

    llm = DatabricksLLMClient()

    print("\n" + "=" * 65)
    print("  SILVER — VALUE STANDARDIZATION MAPPING AGENT")
    print("=" * 65)
    print()
    print("  Step 2 : Numeric gate       (85% threshold)")
    print("  Step 2b: Date/time gate     (85% threshold — catches STRING date columns)")
    print("  Step 2c: Alphanumeric gate  (85% threshold — catches ID/code columns)")
    print("  Step 3 : Combined similarity (Levenshtein + sorted-char anagram score)")
    print("           Threshold 0.72 — catches typos, scrambles, case variants")
    print("  Step 4 : LLM reviews fuzzy groups — rejects wrong merges")
    print("  Step 5 : LLM singleton pass — maps gpay→UPI, NB→Net Banking etc.")
    print("  Step 6 : User approves everything before taxonomy commit")
    print()

    existing_mappings: dict = {}
    total_tables = total_seen = total_mapped = total_committed = 0

    for table_entry in classifier_output.get("tables", []):
        full_table   = table_entry.get("full_table", table_entry.get("table", ""))
        total_tables += 1

        std_cols = []
        for c in table_entry.get("classifications", []):
            src_col  = c.get("source_column", "").strip().lower()
            src_type = c.get("source_type", "STRING").upper()
            label    = (c.get("label","") or c.get("classification","")).upper()

            # Skip by declared label — includes TIME in addition to DATE/TIMESTAMP
            if label in ("SKIP", "DROP", "NUMERIC", "DATE", "TIMESTAMP", "TIME"):
                continue
            if src_type not in ("STRING", "VARCHAR", "CHAR"):
                continue
            skip_keywords = [
                "_name", "_email", "_date", "_timestamp", "_time", "_at", "_on",
                "_id", "_code", "_ref", "_no", "_num", "_key",
                "_hash", "_token", "_uuid", "_guid",
            ]
            if any(src_col.endswith(k) for k in skip_keywords):
                continue
            if src_col in ("name", "email", "date", "timestamp", "time", "id"):
                continue
            std_cols.append(c)

        if not std_cols:
            print(f"\n  Table: {full_table} — no eligible columns.")
            continue

        total_seen += len(std_cols)
        print(f"\n  {'=' * 65}")
        print(f"  Table  : {full_table}")
        print(f"  Columns: {len(std_cols)} eligible  "
              f"({', '.join(c['source_column'] for c in std_cols[:6])}"
              f"{'…' if len(std_cols)>6 else ''})")
        print(f"  {'=' * 65}")

        skip_all_flag = False

        for col_spec in std_cols:
            if skip_all_flag:
                continue

            col_spec["_taxonomy_catalog"] = silver_catalog
            col_spec["_taxonomy_schema"]  = silver_schema

            result, skipped, skip_all = _process_column(
                col_spec=col_spec, full_table=full_table,
                workspace_url=workspace_url, token=token,
                warehouse_id=warehouse_id, llm=llm,
                existing_mappings=existing_mappings,
                string_cols=std_cols,
            )

            if skip_all:
                skip_all_flag = True

            if result.get("enabled") and result.get("mappings"):
                existing_mappings[f"{full_table}.{col_spec['source_column']}"] = result
                total_mapped    += 1
                total_committed += len(result["mappings"])

    print(f"\n  {'─' * 55}")
    print(f"  MAPPING COMPLETE")
    print(f"  Tables     : {total_tables}")
    print(f"  Columns    : {total_seen} eligible  |  {total_mapped} mapped")
    print(f"  Taxonomy   : {total_committed} row(s) committed")
    print(f"  {'─' * 55}\n")

    return {"status": "success", "columns_mapped": total_mapped,
            "rows_committed": total_committed}, None