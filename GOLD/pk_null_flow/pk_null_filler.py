"""
pk_null_filler.py — PK / FK NULL Filler
========================================

Fills NULL values in PK and FK columns using anchor-based surrogate ID
generation.

COMPOSITE ANCHOR SUPPORT (v3)
------------------------------
When PKNullSuggestion.anchor_cols contains more than one column, the filler
builds a SYNTHETIC KEY by concatenating all anchor columns:

    synthetic_key = CONCAT_WS('||', col1, col2, col3)

This synthetic key is used as the lookup value everywhere a single
anchor_col was previously used:

  1. Fetching distinct anchor values from the source table:
       SELECT DISTINCT CONCAT_WS('||', `email`, `product_name`, `quantity`)
       FROM source WHERE pk_col IS NULL AND <all anchors NOT NULL>

  2. Fetching existing anchor→pk mappings (for reuse):
       SELECT DISTINCT CONCAT_WS('||', `email`, `product_name`, `quantity`),
                       CAST(pk_col AS STRING)
       FROM source WHERE pk_col IS NOT NULL

  3. The CASE WHEN SQL expression in the final SELECT:
       CASE
         WHEN pk_col IS NOT NULL THEN CAST(pk_col AS STRING)
         WHEN CONCAT_WS('||', `email`, `product_name`, `quantity`) = 'a@b.com||Widget||2'
              THEN 'ord_001'
         ...
         ELSE NULL
       END AS pk_col

  4. The WHERE clause that drops unrecoverable rows:
       NOT (pk_col IS NULL AND
            (CONCAT_WS('||', ...) IS NULL OR CONCAT_WS('||', ...) = ''))

For single-anchor suggestions (anchor_cols has exactly one element) the
behaviour is IDENTICAL to v2 — CONCAT_WS of one column equals the column
itself, but the code uses the column directly for readability/performance.

NULL handling for composite anchors
-------------------------------------
A row is "unrecoverable" when pk_col IS NULL AND every anchor column is
also NULL (i.e. the synthetic key cannot be formed).  The existing
_handle_unrecoverable_rows / _audit_partial_anchor_nulls helpers are
extended to build the IS NULL condition across all anchor columns using AND.

FIX NOTES
---------
* v2: staging table swap prevents source-overwrites during same-table writes.
* v3: composite anchor support; single-anchor path unchanged.
* v4: fixed UNRESOLVED_COLUMN error — SELECT DISTINCT now aliases the
      synthetic key expression as `__sk__` and ORDER BY references that
      alias instead of repeating the raw expression, which caused Databricks
      to mis-parse column names inside CONCAT_WS as bare column references.
"""

from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
from databricks import sql as _dbsql

from GOLD.pk_null_flow.pk_null_analyzer import PKNullSuggestion


# ─────────────────────────────────────────────────────────────────────────────
# Result structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PKNullColumnResult:
    table_name          : str
    pk_column           : str
    anchor_col          : str          # primary anchor (display)
    anchor_cols         : list[str]    # full composite list
    bk_prefix           : str
    null_count          : int
    offset              : int
    status              : str          # "filled" | "dropped" | "skipped" | "failed"
    note                : str = ""
    dropped_json_path   : str = ""
    null_anchor_count   : int = 0
    fillable_count      : int = 0
    column_role         : str = "primary_key"


@dataclass
class PKNullFillResult:
    source_table   : str
    target_table   : str
    column_results : list[PKNullColumnResult] = field(default_factory=list)
    status         : str = "pending"
    error          : str = ""


# ─────────────────────────────────────────────────────────────────────────────
# SQL helpers
# ─────────────────────────────────────────────────────────────────────────────

def _sql_str(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


_DATE_COL_PATTERN = re.compile(
    r"(^date$|_date$|_dt$|^dt$|_timestamp$|^timestamp$|_time$|^time$|_day$|^day$)",
    re.IGNORECASE,
)

def _is_date_column_name(col_name: str) -> bool:
    return bool(_DATE_COL_PATTERN.search(col_name))


def _synthetic_key_expr(anchor_cols: list[str]) -> str:
    """
    Build a SQL expression that produces a single synthetic key string
    from one or more anchor columns.

    Single anchor  → CAST(`col` AS STRING)          — no separator overhead
    Composite      → CONCAT_WS('||', `c1`, `c2`, …) — NULL-safe concatenation
                     CONCAT_WS skips NULL parts so partial NULLs produce a
                     non-empty string; the unrecoverable-row guard checks all
                     parts separately.
    """
    if len(anchor_cols) == 1:
        return f"CAST(`{anchor_cols[0]}` AS STRING)"
    cols_sql = ", ".join(f"`{c}`" for c in anchor_cols)
    return f"CONCAT_WS('||', {cols_sql})"


def _all_anchors_null_expr(anchor_cols: list[str]) -> str:
    """
    SQL condition that is TRUE when ALL anchor columns are NULL or empty.
    Used to identify rows that are truly unrecoverable (cannot form any key).

    Single anchor  → (`col` IS NULL OR CAST(`col` AS STRING) = '')
    Composite      → every column must be NULL/empty simultaneously
    """
    parts = [
        f"(`{c}` IS NULL OR CAST(`{c}` AS STRING) = '')"
        for c in anchor_cols
    ]
    return "(" + " AND ".join(parts) + ")"


def _any_anchor_notnull_expr(anchor_cols: list[str]) -> str:
    """
    SQL condition that is TRUE when AT LEAST ONE anchor column is not NULL/empty.
    Used to find fillable rows (have enough data to form the synthetic key).
    """
    parts = [
        f"(`{c}` IS NOT NULL AND CAST(`{c}` AS STRING) <> '')"
        for c in anchor_cols
    ]
    return "(" + " OR ".join(parts) + ")"


# ─────────────────────────────────────────────────────────────────────────────
# Display helpers
# ─────────────────────────────────────────────────────────────────────────────

def _format_dropped_rows_clean(
    df          : pd.DataFrame,
    pk_col      : str,
    anchor_cols : list[str],
) -> None:
    seen = set()
    display_cols = []

    for c in [pk_col] + anchor_cols:
        if c in df.columns and c not in seen:
            seen.add(c)
            display_cols.append(c)
    for c in df.columns:
        if c not in seen:
            seen.add(c)
            display_cols.append(c)

    anchor_set = set(anchor_cols)
    for i, (_, row) in enumerate(df.iterrows(), 1):
        print(f"     ┌─ Row {i} {'─' * 40}")
        for col in display_cols:
            val = str(row.get(col, "")) if row.get(col) is not None else "None"
            tag = ""
            if col == pk_col:
                tag = "  ← PK (NULL)"
            elif col in anchor_set:
                tag = "  ← anchor (NULL)"
            print(f"     │  {col:<28} {val:<20}{tag}")
        print(f"     └{'─' * 47}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Unrecoverable-row handlers (composite-aware)
# ─────────────────────────────────────────────────────────────────────────────

def _handle_unrecoverable_rows(
    conn        ,
    full_source : str,
    pk_col      : str,
    anchor_cols : list[str],
    output_dir  : str,
    table_name  : str,
) -> tuple[int, str]:
    """
    Fetch and save rows where pk IS NULL AND all anchors are NULL/empty.
    These rows cannot be assigned any ID and are excluded from the output.
    """
    all_null_cond = _all_anchors_null_expr(anchor_cols)
    anchor_display = (
        anchor_cols[0] if len(anchor_cols) == 1
        else "CONCAT(" + ", ".join(anchor_cols) + ")"
    )

    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT * FROM {full_source} "
            f"WHERE `{pk_col}` IS NULL AND {all_null_cond}"
        )
        rows    = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    finally:
        cur.close()

    dropped_count = len(rows)
    if dropped_count == 0:
        return 0, ""

    df = pd.DataFrame(rows, columns=columns)
    os.makedirs(output_dir, exist_ok=True)
    json_path = os.path.join(
        output_dir, f"{table_name}_{pk_col}_unrecoverable_nulls.json"
    )
    df.to_json(json_path, orient="records", indent=2, date_format="iso")

    print(f"\n  ⚠  DROPPED ROWS — '{table_name}.{pk_col}'")
    print(f"     Both '{pk_col}' and anchor ({anchor_display}) are NULL → cannot assign a PK.")
    print(f"     {dropped_count} row(s) excluded from enriched output.")
    print(f"     Saved to: {json_path}\n")

    _format_dropped_rows_clean(df, pk_col, anchor_cols)
    return dropped_count, json_path


def _audit_partial_anchor_nulls(
    conn        ,
    full_source : str,
    pk_col      : str,
    anchor_cols : list[str],
    output_dir  : str,
    table_name  : str,
) -> tuple[int, int, str]:
    """
    Count and report rows where:
      - pk IS NULL AND all anchors are NULL/empty  → unrecoverable (drop)
      - pk IS NULL AND at least one anchor has a value → fillable

    Returns (null_anchor_count, fillable_count, json_path).
    """
    all_null_cond    = _all_anchors_null_expr(anchor_cols)
    any_notnull_cond = _any_anchor_notnull_expr(anchor_cols)
    anchor_display   = (
        anchor_cols[0] if len(anchor_cols) == 1
        else "CONCAT(" + ", ".join(anchor_cols) + ")"
    )

    cur = conn.cursor()
    try:
        cur.execute(
            f"SELECT COUNT(*) FROM {full_source} "
            f"WHERE `{pk_col}` IS NULL AND {all_null_cond}"
        )
        null_anchor_count = int(cur.fetchone()[0] or 0)

        cur.execute(
            f"SELECT COUNT(*) FROM {full_source} "
            f"WHERE `{pk_col}` IS NULL AND {any_notnull_cond}"
        )
        fillable_count = int(cur.fetchone()[0] or 0)
    finally:
        cur.close()

    json_path = ""

    if null_anchor_count > 0:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT * FROM {full_source} "
                f"WHERE `{pk_col}` IS NULL AND {all_null_cond}"
            )
            rows    = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
        finally:
            cur.close()

        df = pd.DataFrame(rows, columns=columns)
        os.makedirs(output_dir, exist_ok=True)
        json_path = os.path.join(
            output_dir, f"{table_name}_{pk_col}_unrecoverable_nulls.json"
        )
        df.to_json(json_path, orient="records", indent=2, date_format="iso")

        print()
        print(f"  {'─' * 60}")
        print(f"  ⚠  PARTIAL NULL ANCHOR DETECTED — '{table_name}.{pk_col}'")
        print(f"  {'─' * 60}")
        print(f"     Anchor                   : {anchor_display}")
        print(f"     Total NULL-pk rows       : {null_anchor_count + fillable_count:,}")
        print(f"     ✅  Fillable (anchor OK)  : {fillable_count:,}  → IDs will be generated")
        print(f"     ❌  Droppable (no anchor) : {null_anchor_count:,} → excluded from output")
        print()
        print(f"     Dropped rows (both '{pk_col}' and anchor are NULL):")
        print()
        _format_dropped_rows_clean(df, pk_col, anchor_cols)
        print(f"     Drop report saved to: {json_path}")
        print(f"  {'─' * 60}")
        print()

    return null_anchor_count, fillable_count, json_path


# ─────────────────────────────────────────────────────────────────────────────
# PKNullFiller
# ─────────────────────────────────────────────────────────────────────────────

class PKNullFiller:

    _PREFIXED_NUM_RE = re.compile(r"^[A-Za-z]{2,6}[-_](\d+)$")
    _PLAIN_INT_RE    = re.compile(r"^\d+$")

    def __init__(self, hostname: str, sql_warehouse_id: str, access_token: str):
        self._hostname         = hostname
        self._sql_warehouse_id = sql_warehouse_id
        self._access_token     = access_token

    def _connect(self):
        return _dbsql.connect(
            server_hostname = self._hostname,
            http_path       = self._sql_warehouse_id,
            access_token    = self._access_token,
        )

    @staticmethod
    def _pad_width(total_count: int) -> int:
        if total_count <= 0:
            return 3
        digits = math.floor(math.log10(total_count)) + 1
        return max(3, digits)

    def _fetch_existing_values(self, conn, full_table: str, pk_col: str) -> set[str]:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT DISTINCT CAST(`{pk_col}` AS STRING) "
                f"FROM {full_table} WHERE `{pk_col}` IS NOT NULL"
            )
            return {row[0] for row in cur.fetchall() if row[0] is not None}
        finally:
            cur.close()

    def _fetch_existing_anchor_mappings(
        self,
        conn,
        full_table  : str,
        pk_col      : str,
        anchor_cols : list[str],
    ) -> dict[str, str]:
        """
        Fetch existing synthetic_key → pk_value mappings for rows that
        already have a non-NULL pk.  Used to reuse existing IDs when the
        same anchor combination appears in both NULL and non-NULL pk rows.
        """
        key_expr = _synthetic_key_expr(anchor_cols)
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT DISTINCT {key_expr} AS __sk__, "
                f"CAST(`{pk_col}` AS STRING) "
                f"FROM {full_table} "
                f"WHERE `{pk_col}` IS NOT NULL"
                + (
                    f" AND {_any_anchor_notnull_expr(anchor_cols)}"
                    if len(anchor_cols) > 1 else
                    f" AND `{anchor_cols[0]}` IS NOT NULL"
                )
            )
            return {
                row[0]: row[1]
                for row in cur.fetchall()
                if row[0] is not None and row[1] is not None
            }
        finally:
            cur.close()

    def _safe_offset(self, existing_values: set[str]) -> int:
        max_num = 0
        for val in existing_values:
            v = val.strip()
            if self._PLAIN_INT_RE.match(v):
                max_num = max(max_num, int(v))
                continue
            m = self._PREFIXED_NUM_RE.match(v)
            if m:
                max_num = max(max_num, int(m.group(1)))
        return max_num + 1

    def _generate_ids(
        self,
        null_count      : int,
        prefix          : str,
        existing_values : set[str],
        offset          : int,
        pad_width       : int,
    ) -> list[str]:
        ids      : list[str] = []
        counter  = offset
        attempts = 0
        max_attempts = null_count * 10 + 1000

        while len(ids) < null_count and attempts < max_attempts:
            candidate = f"{prefix}_{str(counter).zfill(pad_width)}"
            if candidate not in existing_values:
                ids.append(candidate)
                existing_values.add(candidate)
            counter  += 1
            attempts += 1

        if len(ids) < null_count:
            raise RuntimeError(
                f"Only generated {len(ids)}/{null_count} collision-free IDs "
                f"for prefix '{prefix}' after {attempts} attempts."
            )
        return ids

    def _ensure_schema(self, conn, catalog: str, schema: str) -> None:
        cur = conn.cursor()
        try:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        finally:
            cur.close()

    def _get_table_columns(self, conn, full_table: str) -> list[str]:
        cur = conn.cursor()
        try:
            cur.execute(f"DESCRIBE TABLE {full_table}")
            return [
                row[0] for row in cur.fetchall()
                if not row[0].startswith("#") and row[0].strip()
            ]
        finally:
            cur.close()

    # ── MAIN FILL METHOD ──────────────────────────────────────────────────────

    def fill(
        self,
        catalog        : str,
        source_schema  : str,
        target_schema  : str,
        source_table   : str,
        suggestions    : list[PKNullSuggestion],
        source_catalog : str = "",
        target_catalog : str = "",
        output_dir     : str = "output",
        dim_catalog    : str = "",
        dim_schema     : str = "",
    ) -> PKNullFillResult:

        eff_src_cat = source_catalog if source_catalog else catalog
        eff_tgt_cat = target_catalog if target_catalog else catalog

        full_source = f"`{eff_src_cat}`.`{source_schema}`.`{source_table}`"
        full_target = f"`{eff_tgt_cat}`.`{target_schema}`.`{source_table}`"

        result = PKNullFillResult(source_table=full_source, target_table=full_target)

        conn = self._connect()
        try:
            self._ensure_schema(conn, eff_tgt_cat, target_schema)

            col_results   : list[PKNullColumnResult] = []
            # fill_mappings: (pk_col, anchor_cols, synthetic_key→id dict)
            fill_mappings : list[tuple[str, list[str], dict[str, str]]] = []

            for s in suggestions:
                pk_col      = s.pk_column
                # v3: use anchor_cols list; fall back to [anchor_col] for old data
                anchor_cols = s.anchor_cols if s.anchor_cols else [s.anchor_col]
                prefix      = s.bk_prefix
                null_count  = s.null_count
                is_composite = len(anchor_cols) > 1

                anchor_display = (
                    anchor_cols[0] if not is_composite
                    else "CONCAT(" + ", ".join(anchor_cols) + ")"
                )

                # ── Reject date-only anchors ──────────────────────────────────
                date_anchors = [c for c in anchor_cols if _is_date_column_name(c)]
                if date_anchors and len(date_anchors) == len(anchor_cols):
                    print(
                        f"  [WARN]  {source_table}.{pk_col}: "
                        f"all anchor column(s) are date columns {date_anchors} — "
                        f"dates are non-unique and MUST NOT be used as anchors.\n"
                        f"          Please re-run the analyzer with a better anchor."
                    )
                    col_results.append(PKNullColumnResult(
                        table_name  = source_table,
                        pk_column   = pk_col,
                        anchor_col  = anchor_cols[0],
                        anchor_cols = anchor_cols,
                        bk_prefix   = prefix,
                        null_count  = null_count,
                        offset      = 0,
                        status      = "skipped",
                        column_role = s.column_role,
                        note        = (
                            f"All anchor column(s) {anchor_cols} are date columns — "
                            f"non-unique. Supply a better anchor and re-run."
                        ),
                    ))
                    continue

                # Strip any individual date anchors from a composite list
                if date_anchors and is_composite:
                    anchor_cols = [c for c in anchor_cols if not _is_date_column_name(c)]
                    print(
                        f"  [INFO]  {source_table}.{pk_col}: "
                        f"stripped date anchor(s) from composite: {date_anchors}. "
                        f"Remaining: {anchor_cols}"
                    )
                    is_composite   = len(anchor_cols) > 1
                    anchor_display = (
                        anchor_cols[0] if not is_composite
                        else "CONCAT(" + ", ".join(anchor_cols) + ")"
                    )

                # ── SQL expressions for this anchor set ───────────────────────
                key_expr         = _synthetic_key_expr(anchor_cols)
                all_null_cond    = _all_anchors_null_expr(anchor_cols)
                any_notnull_cond = _any_anchor_notnull_expr(anchor_cols)

                # ── 1. Fetch existing non-null pk values ──────────────────────
                print(
                    f"    [FILL]  {source_table}.{pk_col}: "
                    f"collecting existing values…"
                    + (f"  [composite anchor: {anchor_display}]" if is_composite else "")
                )
                existing = self._fetch_existing_values(conn, full_source, pk_col)

                offset    = self._safe_offset(existing)
                pad_width = self._pad_width(len(existing) + null_count)

                # ── 2. Fetch distinct synthetic keys for NULL-pk rows ─────────
                # FIX v4: alias the key expression as __sk__ so that ORDER BY
                # can reference the alias instead of repeating the raw
                # CONCAT_WS(...) expression — Databricks was mis-parsing column
                # names inside the expression as bare unresolved column refs.
                print(
                    f"    [FILL]  {source_table}.{pk_col}: "
                    f"reading anchor values…"
                )
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"SELECT DISTINCT {key_expr} AS __sk__ "
                        f"FROM {full_source} "
                        f"WHERE `{pk_col}` IS NULL "
                        f"  AND {any_notnull_cond} "
                        f"ORDER BY __sk__"
                    )
                    distinct_anchors = [
                        row[0] for row in cur.fetchall() if row[0] is not None
                    ]
                finally:
                    cur.close()

                # ── ALL anchors NULL → drop every NULL-pk row ─────────────────
                if not distinct_anchors:
                    dropped_count, json_path = _handle_unrecoverable_rows(
                        conn        = conn,
                        full_source = full_source,
                        pk_col      = pk_col,
                        anchor_cols = anchor_cols,
                        output_dir  = output_dir,
                        table_name  = source_table,
                    )
                    col_results.append(PKNullColumnResult(
                        table_name        = source_table,
                        pk_column         = pk_col,
                        anchor_col        = anchor_cols[0],
                        anchor_cols       = anchor_cols,
                        bk_prefix         = prefix,
                        null_count        = null_count,
                        offset            = 0,
                        status            = "dropped",
                        column_role       = s.column_role,
                        null_anchor_count = dropped_count,
                        fillable_count    = 0,
                        note              = (
                            f"All {null_count} NULL-pk rows also have NULL anchor "
                            f"({anchor_display}). "
                            f"{dropped_count} row(s) excluded from enriched output."
                        ),
                        dropped_json_path = json_path,
                    ))
                    fill_mappings.append((pk_col, anchor_cols, {}))
                    continue

                # ── SOME anchors NULL → partial audit ─────────────────────────
                print(
                    f"    [FILL]  {source_table}.{pk_col}: "
                    f"auditing anchor nulls (partial-NULL check)…"
                )
                null_anchor_count, fillable_count, partial_json_path = (
                    _audit_partial_anchor_nulls(
                        conn        = conn,
                        full_source = full_source,
                        pk_col      = pk_col,
                        anchor_cols = anchor_cols,
                        output_dir  = output_dir,
                        table_name  = source_table,
                    )
                )

                # ── 3. Fetch existing anchor→pk mappings ──────────────────────
                print(
                    f"    [FILL]  {source_table}.{pk_col}: "
                    f"fetching existing anchor mappings…"
                )
                existing_anchor_mappings = self._fetch_existing_anchor_mappings(
                    conn        = conn,
                    full_table  = full_source,
                    pk_col      = pk_col,
                    anchor_cols = anchor_cols,
                )

                anchors_needing_new = [
                    a for a in distinct_anchors if a not in existing_anchor_mappings
                ]

                # ── 4. Generate collision-safe IDs ────────────────────────────
                generated_ids = self._generate_ids(
                    null_count      = len(anchors_needing_new),
                    prefix          = prefix,
                    existing_values = existing,
                    offset          = offset,
                    pad_width       = pad_width,
                )

                # Build synthetic_key → id mapping
                anchor_to_id : dict[str, str] = {}
                new_idx = 0
                for a in distinct_anchors:
                    if a in existing_anchor_mappings:
                        anchor_to_id[a] = existing_anchor_mappings[a]
                    else:
                        anchor_to_id[a] = generated_ids[new_idx]
                        new_idx += 1

                fill_mappings.append((pk_col, anchor_cols, anchor_to_id))

                partial_note = ""
                if null_anchor_count > 0:
                    partial_note = (
                        f" | ⚠ {null_anchor_count} row(s) with NULL anchor dropped"
                    )

                anchor_label = (
                    f"anchor: {anchor_display}"
                    if not is_composite
                    else f"composite anchor: {anchor_display}"
                )
                print(
                    f"    [FILL]  {source_table}.{pk_col}: "
                    f"{len(generated_ids)} new IDs generated "
                    f"({anchor_label}), "
                    f"{len(distinct_anchors) - len(generated_ids)} reused. "
                    f"Sample: {generated_ids[:3] if generated_ids else []}"
                    f"{partial_note}"
                )

                col_results.append(PKNullColumnResult(
                    table_name        = source_table,
                    pk_column         = pk_col,
                    anchor_col        = anchor_cols[0],
                    anchor_cols       = anchor_cols,
                    bk_prefix         = prefix,
                    null_count        = null_count,
                    offset            = offset,
                    status            = "filled",
                    column_role       = s.column_role,
                    null_anchor_count = null_anchor_count,
                    fillable_count    = fillable_count,
                    note              = (
                        f"{len(generated_ids)} IDs generated from {anchor_display}; "
                        f"offset {offset}; format {prefix}_{'0'*(pad_width-1)}X"
                        + (
                            f"; {null_anchor_count} row(s) with NULL anchor excluded"
                            if null_anchor_count > 0 else ""
                        )
                    ),
                    dropped_json_path = partial_json_path,
                ))

            result.column_results = col_results

            filled  = [r for r in col_results if r.status == "filled"]
            dropped = [r for r in col_results if r.status == "dropped"]
            if not filled and not dropped:
                result.status = "failed"
                result.error  = "No PK columns could be processed."
                return result

            # ── 5. Build SELECT expressions ───────────────────────────────────
            all_cols = self._get_table_columns(conn, full_source)

            select_exprs : list[str] = []
            # Map pk_col → (anchor_cols, synthetic_key→id dict)
            pk_fill_map  : dict[str, tuple[list[str], dict[str, str]]] = {
                m[0]: (m[1], m[2]) for m in fill_mappings
            }

            for col in all_cols:
                if col in pk_fill_map:
                    fill_anchor_cols, id_map = pk_fill_map[col]
                    if not id_map:
                        # Fully dropped — keep as-is; WHERE removes unrecoverable rows
                        select_exprs.append(f"`{col}`")
                        continue

                    # Build the synthetic key expression for the CASE WHEN branches
                    key_expr = _synthetic_key_expr(fill_anchor_cols)

                    branches = "\n          ".join(
                        f"WHEN {key_expr} = {_sql_str(av)} THEN {_sql_str(nid)}"
                        for av, nid in id_map.items()
                    )
                    expr = (
                        f"CASE\n"
                        f"          WHEN `{col}` IS NOT NULL THEN CAST(`{col}` AS STRING)\n"
                        f"          {branches}\n"
                        f"          ELSE NULL\n"
                        f"        END AS `{col}`"
                    )
                    select_exprs.append(expr)
                else:
                    select_exprs.append(f"`{col}`")

            # ── WHERE: exclude truly unrecoverable rows ───────────────────────
            where_clauses = []
            for pk_col_w, anchor_cols_w, id_map in fill_mappings:
                cr = next(
                    (r for r in col_results if r.pk_column == pk_col_w), None
                )
                has_unrecoverable = (not id_map) or (cr and cr.null_anchor_count > 0)
                if has_unrecoverable:
                    all_null_cond = _all_anchors_null_expr(anchor_cols_w)
                    where_clauses.append(
                        f"NOT (`{pk_col_w}` IS NULL AND {all_null_cond})"
                    )

            where_sql = ""
            if where_clauses:
                where_sql = "\nWHERE " + "\n  AND ".join(where_clauses)

            select_cols_sql = ",\n        ".join(select_exprs)
            final_select = (
                f"SELECT\n        {select_cols_sql}\n"
                f"FROM {full_source}"
                f"{where_sql}"
            )

            print(f"\n    [DDL ]  Generated SELECT:\n{final_select}\n")

            # ── 6. Write via staging table (safe for same-table overwrites) ───
            staging_table = f"{source_table}__pk_fill_staging"
            full_staging  = f"`{eff_tgt_cat}`.`{target_schema}`.`{staging_table}`"

            print(f"    [DDL ]  Writing enriched table to {full_target}…")

            # Step 1 — Build staging table
            cur = conn.cursor()
            try:
                cur.execute(
                    f"CREATE OR REPLACE TABLE {full_staging}\n"
                    f"AS {final_select}"
                )
            finally:
                cur.close()

            # Step 2 — Verify staging row count
            cur = conn.cursor()
            try:
                cur.execute(f"SELECT COUNT(*) FROM {full_staging}")
                staging_count = int(cur.fetchone()[0] or 0)
            finally:
                cur.close()

            if staging_count == 0:
                cur = conn.cursor()
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {full_staging}")
                finally:
                    cur.close()
                raise RuntimeError(
                    f"Staging table {full_staging} produced 0 rows — "
                    f"source table may be empty or WHERE clause is too restrictive. "
                    f"Original table left untouched."
                )

            # Step 3 — Atomic swap: DROP target, RENAME staging → target
            cur = conn.cursor()
            try:
                cur.execute(f"DROP TABLE IF EXISTS {full_target}")
            finally:
                cur.close()

            cur = conn.cursor()
            try:
                cur.execute(
                    f"ALTER TABLE {full_staging} "
                    f"RENAME TO `{eff_tgt_cat}`.`{target_schema}`.`{source_table}`"
                )
            finally:
                cur.close()

            # Step 4 — Final verification
            cur = conn.cursor()
            try:
                cur.execute(f"SELECT COUNT(*) FROM {full_target}")
                written_count = int(cur.fetchone()[0] or 0)
                print(f"    [DDL ]  ✅ {written_count:,} row(s) written to {full_target}")
            finally:
                cur.close()

            result.status = "enriched"

        except Exception as exc:
            # Best-effort cleanup of any leftover staging table
            try:
                cur = conn.cursor()
                cur.execute(
                    f"DROP TABLE IF EXISTS "
                    f"`{eff_tgt_cat}`.`{target_schema}`.`{source_table}__pk_fill_staging`"
                )
                cur.close()
            except Exception:
                pass

            result.status = "failed"
            result.error  = str(exc)
            for cr in result.column_results:
                if cr.status == "filled":
                    cr.status = "failed"
                    cr.note   = str(exc)
        finally:
            conn.close()

        return result