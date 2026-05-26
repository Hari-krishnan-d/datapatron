"""
bk_analyzer.py — Business Key Gap Analyzer  (Rules + LLM hybrid)
=================================================================
UPDATED — v2

Two-pass analysis:

  Pass 1 — Rule engine
    Detects entities from column names.
    Flags a column as "already a proper surrogate BK" ONLY when its
    sample values are plain integers (1, 2, 42).
    Codes like ORD-0001 / PROD-501 / STR-01 are natural keys, NOT
    surrogates — the entity still needs a generated BK.

  Pass 2 — LLM review
    Sends full column list + sample values + Pass-1 findings to the LLM.
    LLM can confirm, add missed entities, or correct already_has_id.

Fixes applied in v2
--------------------
FIX-1  _col_matches_entity() was too greedy — third condition
       `c.startswith(prefix)` swallowed prefixes like "store" matching
       "stored_at" or "storage_type". Now uses only exact match or
       `startswith(prefix + "_")`.

FIX-2  _is_surrogate_id() previously accepted prefixed codes like
       ORD-0001 / cus_042 as already-a-surrogate. This caused false
       negatives (skipping BK generation when the column held a natural
       key). Now ONLY plain integers are treated as proper surrogates.
       Prefixed codes are always treated as natural keys needing a new BK.

FIX-3  LLM prompt now receives an explicit `confirmed_surrogates` list
       (columns the rule engine already verified as plain-integer
       surrogates). The LLM is instructed NOT to flag these as missing,
       preventing it from contradicting the rule engine.

FIX-4  _merge_results() previously allowed the LLM to silently override
       the rule engine's already_has_id=True finding. The rule engine
       has access to real sample values; the LLM guesses from names.
       Rule now wins on already_has_id=True — LLM override is logged
       as a warning and ignored.

Key notes
---------
Regex patterns use prefix matching (r"customer") not word-boundary
matching (r"\\bcustomer\\b") because underscore is a word character in
Python regex, so "customer_name" has NO word boundary after "customer".
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass

from openai import OpenAI


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BKSuggestion:
    entity          : str
    bk_column_name  : str
    bk_prefix       : str
    source_table    : str
    natural_key_col : str
    supporting_cols : list
    reason          : str
    already_has_id  : bool = False
    detected_by     : str  = "rule"
    natural_key_conflict : str = ""   # non-empty = blocked; value = reason

    @property
    def value_format(self):
        return f"{self.bk_prefix}_001, {self.bk_prefix}_002, ..."


# ─────────────────────────────────────────────────────────────────────────────
# Entity catalogue
# NOTE: patterns use simple prefix/substring matching, NOT \b word boundaries,
#       because underscore is a word char — "customer_name" has no \b after
#       "customer" before "_". We match column.lower().startswith or "in" checks.
# ─────────────────────────────────────────────────────────────────────────────

_ENTITY_CATALOGUE = [
    {
        "entity":            "customer",
        "bk_prefix":         "cus",
        "prefixes":          ["customer", "client", "buyer"],
        "also_matches":      ["user_name", "user_email"],
        "natural_key_hints": ["customer_email", "email", "customer_name",
                              "username", "phone", "mobile", "login"],
        "id_cols":           ["customer_id", "client_id", "buyer_id"],
    },
    {
        "entity":            "order",
        "bk_prefix":         "ord",
        "prefixes":          ["order", "purchase", "transaction", "sale"],
        "also_matches":      [],
        "natural_key_hints": ["order_id", "order_number", "order_no",
                              "transaction_ref", "invoice_number"],
        "id_cols":           ["order_id", "purchase_id", "transaction_id", "sale_id"],
    },
    {
        "entity":            "product",
        "bk_prefix":         "prd",
        "prefixes":          ["product", "item", "sku", "article"],
        "also_matches":      [],
        "natural_key_hints": ["product_id", "sku", "barcode", "product_code",
                              "product_name"],
        "id_cols":           ["product_id", "item_id", "sku_id"],
    },
    {
        "entity":            "store",
        "bk_prefix":         "str",
        "prefixes":          ["store", "branch", "outlet", "shop"],
        "also_matches":      [],
        "natural_key_hints": ["store_id", "store_code", "branch_code"],
        "id_cols":           ["store_id", "branch_id", "outlet_id"],
    },
    {
        "entity":            "warehouse",
        "bk_prefix":         "whs",
        "prefixes":          ["warehouse", "fulfil", "depot"],
        "also_matches":      [],
        "natural_key_hints": ["warehouse_id", "warehouse_code", "depot_code"],
        "id_cols":           ["warehouse_id", "depot_id"],
    },
    {
        "entity":            "payment",
        "bk_prefix":         "pay",
        "prefixes":          ["payment", "invoice", "receipt", "billing"],
        "also_matches":      [],
        "natural_key_hints": ["payment_ref", "payment_number", "invoice_number",
                              "payment_method", "payment_type", "payment_mode"],
        "id_cols":           ["payment_id", "invoice_id", "receipt_id"],
    },
    {
        "entity":            "employee",
        "bk_prefix":         "emp",
        "prefixes":          ["employee", "staff", "worker", "agent"],
        "also_matches":      [],
        "natural_key_hints": ["employee_number", "staff_number", "badge"],
        "id_cols":           ["employee_id", "staff_id", "worker_id", "agent_id"],
    },
    {
        "entity":            "supplier",
        "bk_prefix":         "sup",
        "prefixes":          ["supplier", "vendor", "manufacturer"],
        "also_matches":      [],
        "natural_key_hints": ["vendor_code", "supplier_code", "tax_id"],
        "id_cols":           ["supplier_id", "vendor_id"],
    },
    {
        "entity":            "category",
        "bk_prefix":         "cat",
        "prefixes":          ["category", "department", "segment"],
        "also_matches":      ["product_category"],
        "natural_key_hints": ["category_code", "department_code", "category_name",
                              "product_category"],
        "id_cols":           ["category_id", "department_id"],
    },
    {
        "entity":            "region",
        "bk_prefix":         "reg",
        "prefixes":          ["region", "territory", "zone", "district"],
        "also_matches":      ["shipping_region"],
        "natural_key_hints": ["region_code", "territory_code", "shipping_region"],
        "id_cols":           ["region_id", "territory_id"],
    },
    {
        "entity":            "shipment",
        "bk_prefix":         "shp",
        "prefixes":          ["shipment", "parcel", "tracking"],
        "also_matches":      [],
        "natural_key_hints": ["tracking_number", "shipment_number", "waybill", "shipment_ref"],
        "id_cols":           ["shipment_id", "delivery_id"],
        "require_hint_match": True,
    },
    {
        "entity":            "player",
        "bk_prefix":         "pla",
        "prefixes":          ["player", "athlete", "participant"],
        "also_matches":      [],
        "natural_key_hints": ["player_name", "first_name", "last_name",
                              "player_email", "player_code"],
        "id_cols":           ["player_id", "athlete_id"],
    },
    {
        "entity":            "match",
        "bk_prefix":         "mch",
        "prefixes":          ["match", "game", "fixture"],
        "also_matches":      [],
        "natural_key_hints": ["match_number", "match_code", "fixture_number",
                              "tournament", "venue"],
        "id_cols":           ["match_id", "game_id", "fixture_id"],
        "require_hint_match": True,
    },
    {
        "entity":            "team",
        "bk_prefix":         "tem",
        "prefixes":          ["team", "club", "squad"],
        "also_matches":      [],
        "natural_key_hints": ["team_name", "club_name", "team_code"],
        "id_cols":           ["team_id", "club_id"],
    },
    {
        "entity":            "tournament",
        "bk_prefix":         "trn",
        "prefixes":          ["tournament", "league", "competition", "series"],
        "also_matches":      [],
        "natural_key_hints": ["tournament", "tournament_name", "league_name",
                              "competition_name"],
        "id_cols":           ["tournament_id", "league_id", "competition_id"],
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# FIX-1: Column matching — strict prefix-based (no greedy startswith)
# ─────────────────────────────────────────────────────────────────────────────

def _col_matches_entity(col: str, entry: dict) -> bool:
    """
    True when a column belongs to this entity.

    FIX-1: Previously the third condition `c.startswith(prefix)` was too
    greedy — "store" would match "stored_at", "storage_type" etc.
    Now we only accept:
      - exact match (col == prefix)
      - col starts with prefix + "_"  (e.g. "store_id", "store_name")
      - exact match in also_matches list
    """
    c = col.lower()
    for prefix in entry["prefixes"]:
        if c == prefix or c.startswith(prefix + "_"):
            return True
    for exact in entry.get("also_matches", []):
        if c == exact.lower():
            return True
    return False


def _entity_columns(col_names: list, entry: dict) -> list:
    return [c for c in col_names if _col_matches_entity(c, entry)]


def _find_natural_key(
    col_names: list,
    hints: list,
    entity: str = "",
) -> str | None:
    """
    Find the best natural key column for an entity.

    Priority 1 — exact match against entity-specific hints.
    Priority 2 — substring match, but column must contain entity name
                 (prevents customer_email bleeding into payment/shipment).
    Priority 3 — conservative suffix fallback restricted to non-email
                 suffixes (_number, _no, _code, _ref).
    """
    lower_cols = {c.lower(): c for c in col_names}

    # Priority 1 — exact hint match
    for hint in hints:
        if hint.lower() in lower_cols:
            return lower_cols[hint.lower()]

    # Priority 2 — substring match anchored to entity name
    for hint in hints:
        for lc, orig in lower_cols.items():
            if hint.lower() in lc:
                if not entity or entity.lower() in lc:
                    return orig

    # Priority 3 — safe suffix fallback (_email excluded for non-customer)
    safe_suffixes = ("_number", "_no", "_code", "_ref")
    for suffix in safe_suffixes:
        for lc, orig in lower_cols.items():
            if lc.endswith(suffix):
                return orig

    # Last resort — _name only when entity token present
    for lc, orig in lower_cols.items():
        if lc.endswith("_name") and (not entity or entity.lower() in lc):
            return orig

    return None


# ─────────────────────────────────────────────────────────────────────────────
# FIX-2: Surrogate detection — plain integers ONLY
# ─────────────────────────────────────────────────────────────────────────────

_SURROGATE_INT_RE = re.compile(r"^\d+$")

# FIX-2: _SURROGATE_CODED_RE removed.
# Prefixed codes (ORD-0001, cus_042, pla_001) are natural keys, NOT surrogates.
# Accepting them as already_has_id=True caused the BK generator to skip columns
# that genuinely needed a new surrogate generated on top of them.
# Only plain integers (1, 42, 1001) count as proper surrogate keys.


def _sample_values(table_meta: dict, column: str) -> list:
    return [str(r[column]) for r in table_meta.get("sample_data", [])
            if column in r and r[column] is not None][:5]


def _is_surrogate_id(samples: list) -> bool:
    """
    FIX-2: Return True ONLY when every sample value is a plain integer.

    Prefixed codes like ORD-0001, PROD-501, STR-01, cus_042, pla_001 are
    treated as natural keys — the entity still needs a generated surrogate BK.
    This prevents false negatives where existing prefixed codes caused the
    system to skip BK generation entirely.
    """
    if not samples:
        return False
    return all(
        _SURROGATE_INT_RE.match(s.strip())
        for s in samples
        if s.strip()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pass 1 — Rule engine
# ─────────────────────────────────────────────────────────────────────────────

def _col_names(table_meta: dict) -> list:
    return [c["column"] for c in table_meta.get("columns", [])]


def _rule_pass(full_table: str, table_meta: dict, col_names: list) -> list:
    suggestions = []
    seen        = set()
    lower_set   = {c.lower() for c in col_names}

    for entry in _ENTITY_CATALOGUE:
        entity      = entry["entity"]
        bk_col_name = f"{entity}_id"

        if entity in seen:
            continue

        related_cols = _entity_columns(col_names, entry)
        if not related_cols:
            continue

        seen.add(entity)

        id_col     = next((c for c in col_names
                           if c.lower() in {x.lower() for x in entry["id_cols"]}), None)
        if id_col:
            samples    = _sample_values(table_meta, id_col)
            already_id = _is_surrogate_id(samples)
            if already_id:
                reason = (f"[RULE] Surrogate '{id_col}' present "
                          f"(plain integer values: {samples[:2]}).")
            else:
                reason = (f"[RULE] '{id_col}' found but values {samples[:2]} "
                          f"are not plain integers → surrogate needed.")
        else:
            already_id = False
            reason     = (f"[RULE] {len(related_cols)} {entity}-related col(s): "
                          f"{related_cols[:4]}{'...' if len(related_cols) > 4 else ''} "
                          f"but no {bk_col_name}.")

        natural_key = _find_natural_key(col_names, entry["natural_key_hints"], entity=entity)

        if natural_key is None and entry.get("require_hint_match"):
            continue

        if natural_key is None:
            natural_key = related_cols[0]

        suggestions.append(BKSuggestion(
            entity          = entity,
            bk_column_name  = bk_col_name,
            bk_prefix       = entry["bk_prefix"],
            source_table    = full_table,
            natural_key_col = natural_key,
            supporting_cols = related_cols,
            reason          = reason,
            already_has_id  = already_id,
            detected_by     = "rule",
        ))

    return suggestions


# ─────────────────────────────────────────────────────────────────────────────
# Pass 2 — LLM review
# FIX-3: confirmed_surrogates list sent to LLM; LLM told not to flag them.
# ─────────────────────────────────────────────────────────────────────────────

_LLM_SYSTEM = """You are a data engineering expert specializing in business key analysis.

You receive:
1. Column names and sample values from a Silver layer table.
2. A preliminary rule-based analysis.
3. A list of columns the rule engine has already confirmed as proper surrogates
   (plain integer values). Do NOT flag any of these as missing a business key.

Your tasks:
- Confirm or correct the rule findings.
- Add any entity the rules missed. Examples:
    * "customer" if customer_name / customer_email exist but no customer_id
    * "product" if product_name / product_category exist but no surrogate product_id
    * "region" if shipping_region exists but no region_id
    * "category" if product_category exists but no category_id
    * "player" if first_name / last_name / player columns exist but no player_id
    * "tournament" if tournament column exists but no tournament_id
- For each entity needing a BK, choose the single best natural key column
  (most unique and stable — prefer email > name, code > label).
- already_has_id = true ONLY when sample values are plain integers (1, 42, 1001).
  Prefixed codes like ORD-0001, PROD-501, STR-01, cus_042, pla_001 are natural
  keys — set already_has_id = false for these; a surrogate still needs to be
  generated on top of them.
- IMPORTANT: If a column name appears in the confirmed_surrogates list provided,
  you MUST set already_has_id = true for that entity. Do not override the rule
  engine's finding for confirmed surrogates.
- For natural_key_col: choose a column that belongs to the same entity domain.
  Never assign customer_email as the natural key for payment, shipment, or any
  non-customer entity.

Respond ONLY with a valid JSON array. No preamble, no markdown.
Schema per element:
{
  "entity":          "customer",
  "bk_column_name":  "customer_id",
  "bk_prefix":       "cus",
  "natural_key_col": "customer_email",
  "already_has_id":  false,
  "reason":          "customer_name and customer_email present but no customer_id"
}"""


def _llm_pass(table_short, col_names, table_meta, rule_results,
              full_table, hostname, access_token, model_endpoint, max_retries=3):

    client = OpenAI(
        api_key  = access_token,
        base_url = f"https://{hostname}/serving-endpoints",
    )

    col_lines = [f"  {c}: {_sample_values(table_meta, c)}" for c in col_names]
    rule_summary = [
        {"entity": s.entity, "already_has_id": s.already_has_id,
         "natural_key_col": s.natural_key_col, "reason": s.reason}
        for s in rule_results
    ]

    # FIX-3: Build confirmed_surrogates list from rule results
    confirmed_surrogates = [
        s.bk_column_name
        for s in rule_results
        if s.already_has_id
    ]

    prompt = (
        f"Table: {table_short}\n\n"
        f"Columns and sample values:\n" + "\n".join(col_lines) + "\n\n"
        f"Rule-based findings:\n{json.dumps(rule_summary, indent=2)}\n\n"
        f"Confirmed surrogates (rule engine verified — do NOT flag as missing):\n"
        f"{json.dumps(confirmed_surrogates)}\n\n"
        "Return the complete list of entities that need a surrogate business key. "
        "Include both confirmed and newly discovered entities. "
        "Set already_has_id=true ONLY when sample values are plain integers, "
        "OR when the entity appears in the confirmed_surrogates list above."
    )

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model       = model_endpoint,
                messages    = [
                    {"role": "system", "content": _LLM_SYSTEM},
                    {"role": "user",   "content": prompt},
                ],
                temperature = 0,
                max_tokens  = 1024,
            )
            raw = response.choices[0].message.content.strip()
            raw = re.sub(r"^```[a-z]*\n?", "", raw)
            raw = re.sub(r"\n?```$",       "", raw)

            items = json.loads(raw)
            if not isinstance(items, list):
                raise ValueError("LLM did not return a JSON array")

            llm_suggestions = []
            lower_col_map   = {c.lower(): c for c in col_names}

            for item in items:
                entity = str(item.get("entity", "")).strip().lower()

                if not entity:
                    continue

                bk_col = str(item.get("bk_column_name", "")).strip()
                if not bk_col:
                    bk_col = f"{entity}_id"

                bk_prefix = str(item.get("bk_prefix", "")).strip()
                if not bk_prefix:
                    words = entity.split("_")
                    if len(words) >= 2:
                        bk_prefix = "".join(w[:2] for w in words[:2])[:3]
                    else:
                        bk_prefix = entity[:3]

                nat_key = str(item.get("natural_key_col", "")).strip()
                has_id  = bool(item.get("already_has_id", False))
                reason  = f"[LLM] {item.get('reason', f'{entity} needs business key')}"

                if not nat_key:
                    for col in col_names:
                        if entity in col.lower():
                            nat_key = col
                            break
                    if not nat_key and col_names:
                        nat_key = col_names[0]

                if not nat_key:
                    continue

                if nat_key not in col_names:
                    nat_key = lower_col_map.get(nat_key.lower()) or col_names[0]

                supporting = [c for c in col_names if entity in c.lower()] or [nat_key]

                llm_suggestions.append(BKSuggestion(
                    entity          = entity,
                    bk_column_name  = bk_col,
                    bk_prefix       = bk_prefix,
                    source_table    = full_table,
                    natural_key_col = nat_key,
                    supporting_cols = supporting,
                    reason          = reason,
                    already_has_id  = has_id,
                    detected_by     = "llm",
                ))

            return llm_suggestions

        except Exception as exc:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                print(f"  [LLM]  Failed after {max_retries} attempts: {exc}")
                print(f"  [LLM]  Falling back to rule-only results.")

    return []


# ─────────────────────────────────────────────────────────────────────────────
# Merge
# FIX-4: Rule engine wins on already_has_id=True — LLM override is logged
#        as a warning and ignored instead of silently overwriting.
# ─────────────────────────────────────────────────────────────────────────────

def _merge_results(rule_results, llm_results):
    merged = {s.entity: s for s in rule_results}

    for ls in llm_results:
        # Ensure bk_column_name is not None or empty
        if not ls.bk_column_name or ls.bk_column_name == "None":
            ls.bk_column_name = f"{ls.entity}_id"

        # Ensure bk_prefix is not None or empty
        if not ls.bk_prefix or ls.bk_prefix == "None":
            words = ls.entity.split("_")
            if len(words) >= 2:
                ls.bk_prefix = "".join(w[:2] for w in words[:2])[:3]
            else:
                ls.bk_prefix = ls.entity[:3]

        if ls.entity in merged:
            rs = merged[ls.entity]

            if rs.already_has_id and not ls.already_has_id:
                # FIX-4: Rule engine confirmed a surrogate (plain integers).
                # LLM disagrees — log a warning and keep rule engine's finding.
                print(
                    f"  [WARN]  LLM disagrees with rule engine on '{rs.entity}': "
                    f"rule says surrogate exists (plain integers), LLM says missing. "
                    f"Rule engine wins — no BK will be generated for '{rs.entity}'."
                )
                # Do NOT override rs.already_has_id — rule engine is authoritative
                # Update natural_key and reason as informational only
                rs.reason     = rs.reason + "  |  [LLM overridden — rule wins]"
                rs.detected_by = "rule+llm(rule_wins)"

            elif not rs.already_has_id and not ls.already_has_id:
                # Both agree BK is missing — LLM refines the natural key choice
                rs.natural_key_col = ls.natural_key_col
                rs.reason          = rs.reason + "  |  " + ls.reason
                rs.detected_by     = "rule+llm"

                # Update bk_column_name and bk_prefix if they were missing
                if not rs.bk_column_name or rs.bk_column_name == "None":
                    rs.bk_column_name = ls.bk_column_name
                if not rs.bk_prefix or rs.bk_prefix == "None":
                    rs.bk_prefix = ls.bk_prefix

            # Case: rs.already_has_id=False, ls.already_has_id=True
            # LLM thinks it's fine but rule said missing — rule wins (missing).
            # No action needed; rs.already_has_id stays False.

        else:
            # New entity discovered by LLM — add it
            ls.detected_by = "llm"
            merged[ls.entity] = ls

    return list(merged.values())


# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight column existence check
# ─────────────────────────────────────────────────────────────────────────────

def _check_existing_columns(col_names: list, suggestions: list) -> list:
    """
    Cross-check BK suggestions against actual Silver column names.

    If a suggested bk_column_name already exists in the Silver table,
    mark it as already_has_id=True and skip generation — regardless of
    what the LLM said. This prevents false positives where the LLM flags
    an existing column as missing.

    Returns the updated suggestions list.
    """
    col_name_set = {c.lower() for c in col_names}

    for s in suggestions:
        if s.already_has_id:
            continue
        if s.bk_column_name.lower() in col_name_set:
            print(
                f"  [CHECK] '{s.bk_column_name}' already exists in Silver columns "
                f"— marking as already_has_id=True (skipping generation)."
            )
            s.already_has_id = True
            s.reason         = s.reason + "  |  [CHECK] Column exists in Silver — no generation needed."

    return suggestions

def _build_embedded_columns_set(model: dict) -> set:
    """
    Returns all column names already embedded as descriptive attributes
    inside any dim table. These don't need a new business key — they are
    already represented in the schema inside another dimension.
    Works for any domain automatically.
    """
    embedded = set()
    for dim in model.get("dimensions", []):
        for col in dim.get("columns", []):
            if isinstance(col, dict):
                if col.get("role") in ("descriptive", "business_key", "unknown"):
                    embedded.add(col.get("column", "").lower())
    return embedded

# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def analyze_missing_business_keys(silver_metadata, creds=None, model=None):
    """
    Two-pass BK analysis: rules first, LLM refinement second.

    Parameters
    ----------
    silver_metadata : dict from fetch_metadata_grouped
    creds           : Databricks credentials dict (enables LLM pass).
                      If None or incomplete, only rule pass runs.

    Pass order
    ----------
    1. Rule engine  — detects entities, checks plain-integer surrogates.
    2. Pre-flight   — cross-checks bk_column_name against actual Silver cols
                      (catches LLM false positives like player_id already existing).
    3. LLM review   — refines natural key choices, finds missed entities.
    4. Merge        — rule engine wins on already_has_id=True conflicts.
    5. Dedup        — one natural key column per BK (first claimant wins).
    """
    use_llm = (
        creds is not None
        and creds.get("hostname")
        and creds.get("access_token")
        and creds.get("model_endpoint")
    )

    all_suggestions = []

    for full_table, table_meta in silver_metadata.items():
        col_names   = _col_names(table_meta)
        if not col_names:
            continue
        table_short = full_table.split(".")[-1]

        # Pass 1 — rules
        rule_results  = _rule_pass(full_table, table_meta, col_names)
        missing_count = sum(1 for s in rule_results if not s.already_has_id)
        print(f"  [RULE]  {table_short}: "
              f"{len(rule_results)} entity/entities detected  "
              f"({missing_count} missing BK)")

        # Pre-flight — check actual column existence before LLM call
        rule_results = _check_existing_columns(col_names, rule_results)

        # Pass 2 — LLM
        if use_llm:
            print(f"  [LLM ]  {table_short}: sending to LLM for review...")
            llm_results = _llm_pass(
                table_short    = table_short,
                col_names      = col_names,
                table_meta     = table_meta,
                rule_results   = rule_results,
                full_table     = full_table,
                hostname       = creds["hostname"],
                access_token   = creds["access_token"],
                model_endpoint = creds["model_endpoint"],
            )
            merged = _merge_results(rule_results, llm_results)

            # Post-merge pre-flight check (catches LLM-injected entities)
            merged = _check_existing_columns(col_names, merged)

            mc     = sum(1 for s in merged if not s.already_has_id)
            print(f"  [DONE]  {table_short}: "
                  f"{len(merged)} total entities, {mc} need BK generation")
        else:
            print(f"  [RULE]  {table_short}: LLM skipped (no credentials)")
            merged = rule_results

        all_suggestions.extend(merged)

    # ── Deduplication pass ─────────────────────────────────────────────────
    # A natural key column may only be claimed by ONE business key.
    # First claimant keeps it; subsequent ones are flagged as CONFLICT.
    claimed: dict = {}   # (table, natural_key_col) → bk_column_name

    for s in all_suggestions:
        if s.already_has_id or not s.natural_key_col:
            continue
        claim_key = (s.source_table, s.natural_key_col.lower())
        if claim_key in claimed:
            s.natural_key_conflict = (
                f"Natural key '{s.natural_key_col}' is already used by "
                f"'{claimed[claim_key]}' — cannot generate '{s.bk_column_name}' "
                f"from the same column."
            )
        else:
            claimed[claim_key] = s.bk_column_name

    if model is not None:
        embedded_cols = _build_embedded_columns_set(model)
        dim_names = {
            d["name"].replace("dim_", "").lower()
            for d in model.get("dimensions", [])
        }
        guarded = []
        for s in all_suggestions:
            if s.already_has_id:
                guarded.append(s)
                continue
            has_dim  = s.entity.lower() in dim_names
            is_embed = s.natural_key_col.lower() in embedded_cols
            if has_dim and not is_embed:
                guarded.append(s)
            else:
                print(f"  [SKIP]  '{s.bk_column_name}' skipped — "
                      f"column already embedded in an existing dim table.")
        all_suggestions = guarded

    return all_suggestions


def print_bk_audit_report(suggestions):
    existing  = [s for s in suggestions if s.already_has_id]
    conflicts = [s for s in suggestions if not s.already_has_id and s.natural_key_conflict]
    missing   = [s for s in suggestions if not s.already_has_id and not s.natural_key_conflict]

    col_w = 40
    print(f"\n  {'Entity / Table':<{col_w}}  {'Detected By':<12}  {'Status'}")
    print(f"  {'-' * col_w}  {'-' * 12}  {'-' * 58}")

    for s in sorted(existing, key=lambda x: (x.source_table, x.entity)):
        short_table = s.source_table.split(".")[-1]
        label       = f"{s.entity}  [{short_table}]"
        print(f"  {label:<{col_w}}  {s.detected_by:<12}  "
              f"✅  {s.bk_column_name} — surrogate already present")

    for s in sorted(missing, key=lambda x: (x.source_table, x.entity)):
        short_table = s.source_table.split(".")[-1]
        label       = f"{s.entity}  [{short_table}]"
        print(
            f"  {label:<{col_w}}  {s.detected_by:<12}  "
            f"⚠   Missing {s.bk_column_name}"
            f"  ->  natural key: {s.natural_key_col}"
            f"  ->  format: {s.value_format}"
        )

    for s in sorted(conflicts, key=lambda x: (x.source_table, x.entity)):
        short_table = s.source_table.split(".")[-1]
        label       = f"{s.entity}  [{short_table}]"
        print(
            f"  {label:<{col_w}}  {s.detected_by:<12}  "
            f"❌  CONFLICT — {s.natural_key_conflict}"
        )

    print()
    if missing or conflicts:
        print(f"  Summary: {len(missing)} business key(s) missing, "
              f"{len(existing)} already have surrogates"
              + (f", {len(conflicts)} conflict(s) blocked." if conflicts else "."))
    else:
        print("  All entity domains have proper surrogate business keys.")
    print()