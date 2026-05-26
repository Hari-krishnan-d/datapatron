"""
pk_null_analyzer.py — Primary Key & Foreign Key NULL Analyzer
=============================================================
Scans ALL id columns (primary_key AND foreign_key roles) across
Gold fact/dim tables and detects NULL values.

Two-pass analysis:
  Pass 1 — Rule engine
    Detects PK/FK columns from Gold model metadata.
    Counts NULL values in each column via Databricks SQL.

  Pass 2 — LLM review
    For each table with NULLs, LLM identifies the best unique "anchor"
    column (or COMPOSITE anchor columns) to derive the generated ID from.

    COMPOSITE ANCHOR DETECTION (v3)
    --------------------------------
    When the LLM detects that a single anchor column is NOT granular
    enough to uniquely identify each row for a given PK column (e.g.
    'email' repeats across many orders so it cannot be the sole anchor
    for order_id), it returns a list of columns in anchor_cols instead
    of a single anchor_col string.

    The filler concatenates composite anchor columns into a single
    synthetic key:  CONCAT_WS('||', col1, col2, col3)
    and uses that as the lookup key in its CASE WHEN expression —
    the same logic as for a single anchor, just on the concat string.

    The existing anchor_col (string) field is set to the first element
    of anchor_cols for backward compatibility with the report printer
    and the approval-gate display.  anchor_cols (list) carries the
    full composite definition through to the filler.

Fix summary
-----------
  1. _extract_id_columns → returns BOTH primary_key AND foreign_key cols.
  2. PKNullSuggestion gains column_role ("primary_key" | "foreign_key").
  3. analyze_pk_nulls iterates over ALL tables in gold_metadata.
  4. Print output labels each column with its role.
  5. [FIX 2B] _best_anchor() pre-filters date/timestamp columns.
  6. [FIX 2B] _LLM_SYSTEM prompt: no-date-anchor is a HARD RULE.
  7. [FIX 2B] Post-LLM guard rejects any date anchor that slips through.
  8. [v3] LLM now returns anchor_cols (list) for composite anchors.
         PKNullSuggestion.anchor_cols carries the full composite list.
         anchor_col (str) is kept as the primary / display column.
  9. [v3] NLP editor in enriched_agent supports editing anchor_cols.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Optional

from openai import OpenAI


# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PKNullSuggestion:
    """Represents a PK or FK column that contains NULL values needing fill."""
    table_name        : str
    full_table        : str
    pk_column         : str
    null_count        : int
    total_rows        : int
    anchor_col        : str        # primary anchor (first element of anchor_cols)
    bk_prefix         : str
    entity            : str
    column_role       : str = "primary_key"   # "primary_key" | "foreign_key"
    # v3 — composite anchor support
    # When len(anchor_cols) > 1 the filler uses CONCAT_WS('||', *anchor_cols)
    # as the synthetic lookup key.  When len == 1 it behaves exactly as before.
    anchor_cols       : list[str] = field(default_factory=list)
    supporting_cols   : list[str] = field(default_factory=list)
    reason            : str = ""
    detected_by       : str = "rule"
    null_anchor_count : int = 0
    fillable_count    : int = 0

    def __post_init__(self):
        # Ensure anchor_cols is always populated and in sync with anchor_col
        if not self.anchor_cols and self.anchor_col:
            self.anchor_cols = [self.anchor_col]
        elif self.anchor_cols and not self.anchor_col:
            self.anchor_col = self.anchor_cols[0]

    @property
    def is_composite_anchor(self) -> bool:
        return len(self.anchor_cols) > 1

    @property
    def anchor_display(self) -> str:
        """Human-readable anchor string for reports and the approval gate."""
        if self.is_composite_anchor:
            return "CONCAT(" + ", ".join(self.anchor_cols) + ")"
        return self.anchor_col

    @property
    def null_pct(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return round(self.null_count / self.total_rows * 100, 2)

    @property
    def anchor_null_pct(self) -> float:
        if self.null_count == 0:
            return 0.0
        return round(self.null_anchor_count / self.null_count * 100, 2)

    @property
    def is_fully_unrecoverable(self) -> bool:
        return self.null_anchor_count > 0 and self.null_anchor_count >= self.null_count

    @property
    def is_partially_unrecoverable(self) -> bool:
        return 0 < self.null_anchor_count < self.null_count

    @property
    def example_ids(self) -> str:
        return f"{self.bk_prefix}_001, {self.bk_prefix}_002, ..."


# ─────────────────────────────────────────────────────────────────────────────
# Entity catalogue
# ─────────────────────────────────────────────────────────────────────────────

_ENTITY_CATALOGUE = [
    {
        "entity":   "customer",
        "prefix":   "cus",
        "id_hints": ["customer_id", "client_id", "buyer_id"],
        "anchor_hints": ["customer_email", "email", "customer_name", "username",
                         "phone", "mobile", "login"],
    },
    {
        "entity":   "order",
        "prefix":   "ord",
        "id_hints": ["order_id", "purchase_id", "transaction_id", "sale_id"],
        "anchor_hints": ["order_number", "order_no", "order_ref",
                         "transaction_ref", "invoice_number"],
    },
    {
        "entity":   "product",
        "prefix":   "prd",
        "id_hints": ["product_id", "item_id", "sku_id"],
        "anchor_hints": ["sku", "barcode", "product_code", "product_name"],
    },
    {
        "entity":   "store",
        "prefix":   "str",
        "id_hints": ["store_id", "branch_id", "outlet_id"],
        "anchor_hints": ["store_code", "branch_code", "store_name"],
    },
    {
        "entity":   "employee",
        "prefix":   "emp",
        "id_hints": ["employee_id", "staff_id", "worker_id", "agent_id"],
        "anchor_hints": ["employee_number", "staff_number", "badge",
                         "employee_email", "email"],
    },
    {
        "entity":   "supplier",
        "prefix":   "sup",
        "id_hints": ["supplier_id", "vendor_id"],
        "anchor_hints": ["vendor_code", "supplier_code", "tax_id"],
    },
    {
        "entity":   "payment",
        "prefix":   "pay",
        "id_hints": ["payment_id", "invoice_id", "receipt_id"],
        "anchor_hints": ["payment_ref", "payment_number", "invoice_number"],
    },
    {
        "entity":   "category",
        "prefix":   "cat",
        "id_hints": ["category_id", "department_id"],
        "anchor_hints": ["category_code", "department_code", "category_name"],
    },
    {
        "entity":   "region",
        "prefix":   "reg",
        "id_hints": ["region_id", "territory_id"],
        "anchor_hints": ["region_code", "territory_code", "shipping_region"],
    },
    {
        "entity":   "shipment",
        "prefix":   "shp",
        "id_hints": ["shipment_id", "delivery_id"],
        "anchor_hints": ["tracking_number", "shipment_number", "waybill",
                         "shipment_ref"],
    },
    {
        "entity":   "warehouse",
        "prefix":   "whs",
        "id_hints": ["warehouse_id", "depot_id"],
        "anchor_hints": ["warehouse_code", "depot_code", "warehouse_name"],
    },
    {
        "entity":   "player",
        "prefix":   "plr",
        "id_hints": ["player_id"],
        "anchor_hints": ["player_name", "first_name", "last_name", "nationality"],
    },
    {
        "entity":   "team",
        "prefix":   "tem",
        "id_hints": ["team_id"],
        "anchor_hints": ["team_name", "team_code"],
    },
    {
        "entity":   "match",
        "prefix":   "mch",
        "id_hints": ["match_id"],
        "anchor_hints": ["match_number", "match_ref", "venue", "tournament"],
    },
    {
        "entity":   "date",
        "prefix":   "dte",
        "id_hints": ["date_id"],
        "anchor_hints": ["date", "full_date", "calendar_date"],
    },
    {
        "entity":   "tournament",
        "prefix":   "trn",
        "id_hints": ["tournament_id"],
        "anchor_hints": ["tournament_name", "tournament_code", "tournament"],
    },
]

NATURAL_KEY_HINTS = [
    "email", "phone", "mobile",
    "account_number", "account_no", "account_code",
    "order_number", "order_no", "invoice_number",
    "employee_number", "staff_number", "badge_number",
    "product_code", "sku", "barcode",
    "pan", "tax_id", "national_id", "passport",
    "registration_number", "license_number",
    "username", "login",
    "reference", "ref_number", "tracking_number",
    "name", "full_name", "customer_name",
    "city", "location", "address",
    "code", "label", "description",
]


# ─────────────────────────────────────────────────────────────────────────────
# Date-column detector
# ─────────────────────────────────────────────────────────────────────────────

_DATE_COL_PATTERN = re.compile(
    r"(^date$|_date$|_dt$|^dt$|_timestamp$|^timestamp$|_time$|^time$|_day$|^day$)",
    re.IGNORECASE,
)

def _is_date_column_name(col_name: str) -> bool:
    return bool(_DATE_COL_PATTERN.search(col_name))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_entity(pk_col: str) -> tuple[str, str]:
    lower = pk_col.lower()
    for entry in _ENTITY_CATALOGUE:
        for hint in entry["id_hints"]:
            if lower == hint.lower():
                return entry["entity"], entry["prefix"]
    entity = re.sub(r"_id$", "", lower)
    prefix = entity[:3]
    return entity, prefix


def _best_anchor(col_names: list[str], entity: str) -> str | None:
    """
    Rule-based single anchor selection.  Date columns are always excluded.
    Returns None when no suitable anchor exists.
    """
    non_date_cols = [c for c in col_names if not _is_date_column_name(c)]
    if not non_date_cols:
        return None

    lower_map = {c.lower(): c for c in non_date_cols}

    # Priority 1 — catalogue hints
    for entry in _ENTITY_CATALOGUE:
        if entry["entity"] == entity:
            for hint in entry["anchor_hints"]:
                if hint.lower() in lower_map:
                    return lower_map[hint.lower()]
            break

    # Priority 2 — entity name in column + useful suffix
    useful_suffixes = ("_email", "_number", "_no", "_code", "_ref", "_name")
    for suffix in useful_suffixes:
        for lc, orig in lower_map.items():
            if entity in lc and lc.endswith(suffix):
                return orig

    # Priority 3 — any column with unique-looking suffix
    for suffix in useful_suffixes:
        for lc, orig in lower_map.items():
            if lc.endswith(suffix):
                return orig

    # Priority 4 — NATURAL_KEY_HINTS ordered match
    for hint in NATURAL_KEY_HINTS:
        if hint in lower_map:
            return lower_map[hint]
        for lc, orig in lower_map.items():
            if hint in lc:
                return orig

    return non_date_cols[0] if non_date_cols else None


def _extract_id_columns(table_meta: dict) -> list[tuple[str, str]]:
    """
    Extract all PK and FK columns. Returns list of (column_name, role).
    """
    id_cols: list[tuple[str, str]] = []
    columns = table_meta.get("columns", [])

    for col in columns:
        role = col.get("role", "").lower()
        if "primary_key" in role:
            id_cols.append((col["column"], "primary_key"))
        elif "foreign_key" in role:
            id_cols.append((col["column"], "foreign_key"))

    if not id_cols:
        for col in columns:
            col_name = col["column"].lower()
            if col_name.endswith("_id") or col_name.endswith("_key"):
                id_cols.append((col["column"], "primary_key"))

    return id_cols


def _count_nulls(conn, full_table: str, col: str) -> tuple[int, int]:
    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT COUNT(*) as total, "
            f"SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END) as nulls "
            f"FROM {full_table}"
        )
        row   = cur.fetchone()
        total = row[0] or 0
        nulls = row[1] or 0
        return int(nulls), int(total)
    finally:
        cur.close()


# ─────────────────────────────────────────────────────────────────────────────
# LLM system prompt — v3: composite anchor support added
# ─────────────────────────────────────────────────────────────────────────────

_LLM_SYSTEM = """You are a data engineering expert specializing in data quality.

You receive a table name, its columns with sample values, and a list of PK/FK
columns that contain NULL values needing generated identifiers.

Your task:
For each column with NULLs, identify the best "anchor" — one OR MORE columns
whose COMBINATION uniquely identifies each row for that specific entity.

COMPOSITE ANCHOR RULE (critical):
----------------------------------
If a single column is NOT granular enough to uniquely identify rows for a
given PK column, return multiple columns in anchor_cols.

Example:
  - customer_id  → anchor_cols: ["email"]
      (email uniquely identifies a customer)
  - order_id     → anchor_cols: ["email", "product_name", "quantity"]
      (email alone repeats across orders; the combination is unique per order)

Look at the sample data carefully. If you see the same anchor value appearing
multiple times (indicating a one-to-many relationship), you MUST add more
columns to anchor_cols to make the combination unique for that PK column.

HARD RULES — absolute, cannot be overridden:
1. NEVER include any date/timestamp column in anchor_cols. These are non-unique.
   Banned examples: transaction_date, order_date, created_at, updated_at,
   event_time, date_id, dt, timestamp, any column ending in _date, _dt,
   _time, _timestamp, or _day.
2. Prefer: email > unique code/number > phone > name > other descriptive cols.
3. The anchor must belong to the same entity domain as the PK/FK.
4. If NO valid non-date anchor exists, return anchor_cols: [] and explain
   in reason that the rows will be dropped as unrecoverable.

Respond ONLY with a valid JSON array. No preamble, no markdown fences.
Schema per element:
{
  "pk_column":   "order_id",
  "anchor_cols": ["email", "product_name", "quantity"],
  "anchor_col":  "email",
  "entity":      "order",
  "bk_prefix":   "ord",
  "reason":      "email repeats per customer; email+product_name+quantity is unique per order"
}

anchor_col must always equal anchor_cols[0] (the primary/display anchor).
"""


# ─────────────────────────────────────────────────────────────────────────────
# LLM pass
# ─────────────────────────────────────────────────────────────────────────────

def _llm_anchor_pass(
    table_short    : str,
    col_names      : list[str],
    table_meta     : dict,
    pk_nulls       : list[dict],
    full_table     : str,
    hostname       : str,
    access_token   : str,
    model_endpoint : str,
    max_retries    : int = 3,
) -> list[dict]:
    """
    Ask LLM to identify the best anchor column(s) for each PK/FK with NULLs.
    Returns list of dicts: {pk_column, anchor_cols, anchor_col, entity,
                             bk_prefix, reason}
    """
    client = OpenAI(
        api_key  = access_token,
        base_url = f"https://{hostname}/serving-endpoints",
    )

    sample_lines = []
    for c in col_names:
        samples = [
            str(r.get(c, "")) for r in table_meta.get("sample_data", [])
            if r.get(c) is not None
        ][:5]
        sample_lines.append(f"  {c}: {samples}")

    prompt = (
        f"Table: {table_short}\n\n"
        f"Columns and sample values:\n" + "\n".join(sample_lines) + "\n\n"
        f"PK/FK columns with NULLs:\n"
        + "\n".join(
            f"  {p['pk_col']} ({p['role']}):  "
            f"{p['null_count']} NULLs out of {p['total']} rows"
            for p in pk_nulls
        )
        + "\n\nFor each column, identify the anchor column(s) needed to "
          "UNIQUELY identify each row for that PK/FK. Use composite anchors "
          "when a single column repeats across rows for the same entity."
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
            if isinstance(items, list):
                return items
        except Exception as exc:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                print(f"  [LLM]  Anchor pass failed: {exc} — using rule fallback.")
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Main analyzer
# ─────────────────────────────────────────────────────────────────────────────

def analyze_pk_nulls(
    gold_metadata  : dict,
    creds          : dict,
    catalog        : str,
    source_schema  : str,
) -> list[PKNullSuggestion]:
    """
    Scan ALL fact/dim tables in gold_metadata for PK AND FK columns with NULLs.
    Returns list[PKNullSuggestion] — one entry per (table, column) with NULLs.
    Each suggestion carries anchor_cols (list) for composite anchor support.
    """
    from databricks import sql as _dbsql

    use_llm = (
        creds.get("hostname")
        and creds.get("access_token")
        and creds.get("model_endpoint")
    )

    conn = _dbsql.connect(
        server_hostname = creds["hostname"],
        http_path       = creds["sql_warehouse_id"],
        access_token    = creds["access_token"],
    )

    all_suggestions: list[PKNullSuggestion] = []

    try:
        for table_key, table_meta in gold_metadata.items():
            parts       = table_key.replace("`", "").split(".")
            table_short = parts[-1]
            full_table  = f"`{catalog}`.`{source_schema}`.`{table_short}`"
            col_names   = [c["column"] for c in table_meta.get("columns", [])]

            if not col_names:
                continue

            id_cols = _extract_id_columns(table_meta)
            if not id_cols:
                print(f"  [SCAN]  {table_short}: no PK/FK columns found — skipping.")
                continue

            pk_count = sum(1 for _, r in id_cols if r == "primary_key")
            fk_count = sum(1 for _, r in id_cols if r == "foreign_key")
            role_summary = []
            if pk_count:
                role_summary.append(f"{pk_count} PK")
            if fk_count:
                role_summary.append(f"{fk_count} FK")
            print(
                f"  [SCAN]  {table_short}: checking {len(id_cols)} column(s)"
                f" ({', '.join(role_summary)})…"
            )

            cols_with_nulls = []
            for col_name, col_role in id_cols:
                try:
                    null_count, total_rows = _count_nulls(conn, full_table, col_name)
                    role_tag = "PK" if col_role == "primary_key" else "FK"
                    if null_count > 0:
                        null_pct = round(null_count / max(total_rows, 1) * 100, 1)
                        print(
                            f"           ⚠  [{role_tag}] {col_name}: "
                            f"{null_count:,} NULLs / {total_rows:,} rows ({null_pct}%)"
                        )
                        if null_pct > 80:
                            print(
                                f"           ⚠  WARNING: {null_pct}% NULL rate "
                                f"suggests a pipeline issue."
                            )
                        cols_with_nulls.append({
                            "pk_col":     col_name,
                            "role":       col_role,
                            "null_count": null_count,
                            "total":      total_rows,
                        })
                    else:
                        print(f"           ✅  [{role_tag}] {col_name}: no NULLs")
                except Exception as exc:
                    print(f"           ⚠  Could not check {col_name}: {exc}")

            if not cols_with_nulls:
                continue

            # ── LLM pass — identify best anchor column(s) ─────────────────
            llm_anchors: dict[str, dict] = {}
            if use_llm:
                print(f"  [LLM ]  {table_short}: finding best anchor column(s)…")
                llm_results = _llm_anchor_pass(
                    table_short    = table_short,
                    col_names      = col_names,
                    table_meta     = table_meta,
                    pk_nulls       = cols_with_nulls,
                    full_table     = full_table,
                    hostname       = creds["hostname"],
                    access_token   = creds["access_token"],
                    model_endpoint = creds["model_endpoint"],
                )
                for item in llm_results:
                    pk = item.get("pk_column", "")
                    if pk:
                        # Normalise: ensure anchor_cols is always a list
                        if "anchor_cols" not in item or not item["anchor_cols"]:
                            single = item.get("anchor_col", "")
                            item["anchor_cols"] = [single] if single else []
                        llm_anchors[pk] = item

            # ── Build suggestions ──────────────────────────────────────────
            for col_info in cols_with_nulls:
                col_name   = col_info["pk_col"]
                col_role   = col_info["role"]
                null_count = col_info["null_count"]
                total_rows = col_info["total"]

                if col_name in llm_anchors:
                    llm_item    = llm_anchors[col_name]
                    entity      = llm_item.get("entity", "") or _resolve_entity(col_name)[0]
                    bk_prefix   = llm_item.get("bk_prefix", "") or _resolve_entity(col_name)[1]
                    anchor_cols = llm_item.get("anchor_cols", [])
                    anchor_col  = llm_item.get("anchor_col", "") or (anchor_cols[0] if anchor_cols else "")
                    reason      = f"[LLM] {llm_item.get('reason', '')}"
                    detected    = "rule+llm"

                    # ── Post-LLM guard: strip any date columns from anchor_cols
                    clean_anchor_cols = [
                        c for c in anchor_cols
                        if not _is_date_column_name(c)
                    ]
                    if len(clean_anchor_cols) < len(anchor_cols):
                        stripped = set(anchor_cols) - set(clean_anchor_cols)
                        print(
                            f"  [WARN]  {table_short}.{col_name}: LLM included date "
                            f"column(s) in anchor — stripped: {sorted(stripped)}"
                        )
                    anchor_cols = clean_anchor_cols

                    # If ALL anchors were date columns, fall back to rule
                    if not anchor_cols:
                        print(
                            f"  [WARN]  {table_short}.{col_name}: no valid anchor "
                            f"after date-stripping — falling back to rule engine."
                        )
                        fallback = _best_anchor(col_names, entity) or (col_names[0] if col_names else "")
                        anchor_cols = [fallback] if fallback else []

                    anchor_col = anchor_cols[0] if anchor_cols else ""

                else:
                    entity, bk_prefix = _resolve_entity(col_name)
                    fallback_anchor   = _best_anchor(col_names, entity) or (col_names[0] if col_names else "")
                    anchor_cols       = [fallback_anchor] if fallback_anchor else []
                    anchor_col        = fallback_anchor
                    reason            = (
                        f"[RULE] {col_name} ({col_role}) has {null_count} NULLs"
                    )
                    detected          = "rule"

                # Validate all anchor cols exist in the table
                valid_anchor_cols = [c for c in anchor_cols if c in col_names]
                if len(valid_anchor_cols) < len(anchor_cols):
                    missing = set(anchor_cols) - set(valid_anchor_cols)
                    print(
                        f"  [WARN]  {table_short}.{col_name}: anchor column(s) "
                        f"not found in table metadata — dropping: {sorted(missing)}"
                    )
                    anchor_cols = valid_anchor_cols
                    anchor_col  = anchor_cols[0] if anchor_cols else ""

                if not anchor_cols:
                    anchor_col  = col_names[0] if col_names else ""
                    anchor_cols = [anchor_col]

                is_composite = len(anchor_cols) > 1
                if is_composite:
                    print(
                        f"  [INFO]  {table_short}.{col_name}: composite anchor detected "
                        f"→ CONCAT({', '.join(anchor_cols)})"
                    )

                supporting = [
                    c for c in col_names
                    if entity in c.lower() and c != col_name
                ]

                all_suggestions.append(PKNullSuggestion(
                    table_name      = table_short,
                    full_table      = full_table,
                    pk_column       = col_name,
                    null_count      = null_count,
                    total_rows      = total_rows,
                    anchor_col      = anchor_col,
                    anchor_cols     = anchor_cols,
                    bk_prefix       = bk_prefix,
                    entity          = entity,
                    column_role     = col_role,
                    supporting_cols = supporting,
                    reason          = reason,
                    detected_by     = detected,
                ))

    finally:
        conn.close()

    return all_suggestions


# ─────────────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────────────

def print_pk_null_report(suggestions: list[PKNullSuggestion]) -> None:
    if not suggestions:
        print("\n  ✅  No NULL values found in any PK or FK column.")
        return

    col_w  = 30
    pk_w   = 24
    role_w = 12

    print()
    print(
        f"  {'Table':<{col_w}}  {'Column':<{pk_w}}  {'Role':<{role_w}}"
        f"  {'NULLs':>8}  {'Total':>8}  {'NULL %':>7}  Anchor"
    )
    print(
        f"  {'-'*col_w}  {'-'*pk_w}  {'-'*role_w}"
        f"  {'-'*8}  {'-'*8}  {'-'*7}  {'-'*34}"
    )

    for s in sorted(suggestions, key=lambda x: (x.table_name, x.column_role, x.pk_column)):
        # Show composite indicator in anchor display
        anchor_str = s.anchor_display
        if s.is_composite_anchor:
            anchor_str = f"⊕ {anchor_str}"   # ⊕ signals composite
        print(
            f"  {s.table_name:<{col_w}}  {s.pk_column:<{pk_w}}"
            f"  {s.column_role:<{role_w}}"
            f"  {s.null_count:>8,}  {s.total_rows:>8,}  {s.null_pct:>6.1f}%"
            f"  {anchor_str}"
        )

    pk_issues = [s for s in suggestions if s.column_role == "primary_key"]
    fk_issues = [s for s in suggestions if s.column_role == "foreign_key"]
    composite  = [s for s in suggestions if s.is_composite_anchor]
    tables     = {s.table_name for s in suggestions}

    print()
    print(
        f"  Summary: {len(suggestions)} column(s) with NULLs across "
        f"{len(tables)} table(s)."
    )
    if pk_issues:
        print(f"    PK NULLs  : {len(pk_issues)} column(s) → generate surrogate IDs")
    if fk_issues:
        print(f"    FK NULLs  : {len(fk_issues)} column(s) → insert -1 sentinel in dim")
    if composite:
        print(
            f"    Composite : {len(composite)} column(s) use multi-column anchors "
            f"(⊕) — CONCAT used for uniqueness"
        )
    print()