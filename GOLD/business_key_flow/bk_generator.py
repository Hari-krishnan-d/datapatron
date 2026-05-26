"""
bk_generator.py — Business Key Generator
=========================================
Takes approved BKSuggestion objects and:

  1. Connects to Databricks (Silver source table).
  2. For each entity, determines which column provides the "natural key"
     (e.g. email → customer, order_number → order).
  3. Builds a DENSE_RANK-based mapping so every unique natural-key value
     gets a deterministic, zero-padded surrogate:
       cus_001, cus_002 … cus_NNN
       ord_001, ord_002 … ord_NNN
  4. Adds the new _id column(s) to the Silver table, writing the enriched
     result into the caller-specified target schema.

The source table is NEVER modified. A new enriched copy is written to
  {catalog}.{target_schema}.{table_name}

Value format
------------
  <prefix>_<zero-padded-integer>
  e.g.  cus_001  cus_042  cus_1000
  Padding width auto-scales to the number of distinct values:
    ≤ 999   → 3 digits   (cus_001)
    ≤ 9999  → 4 digits   (cus_0001)
    ≥ 10000 → 5 digits
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Optional

from databricks import sql as _dbsql

from GOLD.business_key_flow.bk_analyzer import BKSuggestion


# ─────────────────────────────────────────────────────────────────────────────
# Result structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BKColumnResult:
    """Outcome of generating one business-key column."""
    entity          : str
    bk_column_name  : str
    natural_key_col : str
    bk_prefix       : str
    distinct_values : int
    status          : str            # "generated" | "skipped" | "already_exists"
    note            : str = ""


@dataclass
class BKGenerationResult:
    """Aggregate outcome returned to the orchestrator."""
    source_table    : str
    target_table    : str
    column_results  : list[BKColumnResult] = field(default_factory=list)
    status          : str = "pending"   # "enriched" | "failed"
    error           : str = ""


# ─────────────────────────────────────────────────────────────────────────────
# BKGenerator
# ─────────────────────────────────────────────────────────────────────────────

class BKGenerator:
    """
    Generates business key columns and writes an enriched Silver table to
    the specified target schema in Databricks.
    """

    def __init__(
        self,
        hostname:         str,
        sql_warehouse_id: str,
        access_token:     str,
    ):
        self._hostname         = hostname
        self._sql_warehouse_id = sql_warehouse_id
        self._access_token     = access_token

    # ── Connection helper ─────────────────────────────────────────────────────

    def _connect(self):
        return _dbsql.connect(
            server_hostname = self._hostname,
            http_path       = self._sql_warehouse_id,
            access_token    = self._access_token,
        )

    # ── Value format helpers ──────────────────────────────────────────────────

    @staticmethod
    def _pad_width(distinct_count: int) -> int:
        """Return zero-padding width based on distinct value count."""
        if distinct_count <= 0:
            return 3
        digits = math.floor(math.log10(distinct_count)) + 1
        return max(3, digits)

    @staticmethod
    def _format_expr(prefix: str, pad_width: int, rank_expr: str) -> str:
        """
        Build a SQL expression that produces  prefix_001 style values.

        Uses CONCAT + LPAD so the output is a STRING column.
        Example:
            CONCAT('cus_', LPAD(CAST(DENSE_RANK() OVER (...) AS STRING), 3, '0'))
        """
        return (
            f"CONCAT('{prefix}_', LPAD(CAST({rank_expr} AS STRING), {pad_width}, '0'))"
        )

    # ── Distinct value count ──────────────────────────────────────────────────

    def _count_distinct(self, conn, full_table: str, column: str) -> int:
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(DISTINCT `{column}`) FROM {full_table}")
            return cur.fetchone()[0] or 0
        finally:
            cur.close()

    # ── Column existence check ────────────────────────────────────────────────

    def _column_exists(self, conn, full_table: str, column: str) -> bool:
        parts   = full_table.split(".")
        catalog = parts[0]
        schema  = parts[1]
        table   = parts[2]
        cur     = conn.cursor()
        try:
            cur.execute(f"""
                SELECT COUNT(*)
                FROM `{catalog}`.information_schema.columns
                WHERE table_schema  = '{schema}'
                  AND table_name    = '{table}'
                  AND column_name   = '{column}'
            """)
            return (cur.fetchone()[0] or 0) > 0
        finally:
            cur.close()

    # ── Schema / table creation ───────────────────────────────────────────────

    def _ensure_schema(self, conn, catalog: str, schema: str) -> None:
        cur = conn.cursor()
        try:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        finally:
            cur.close()

    # ── Core generation ───────────────────────────────────────────────────────

    def generate(
        self,
        catalog        : str,
        source_schema  : str,
        target_schema  : str,
        source_table   : str,           # short name, e.g. "orders_silver"
        suggestions    : list[BKSuggestion],
        source_catalog : str = "",     # catalog where Silver source lives;
                                        # defaults to `catalog` when omitted
        target_catalog : str = "",     # catalog where enriched table is written;
                                        # defaults to `catalog` when omitted
    ) -> BKGenerationResult:
        """
        Build an enriched copy of source_table in target_schema, adding one
        new _id column per approved BKSuggestion.

        Parameters
        ----------
        catalog        : Fallback catalog for both source and target when
                         source_catalog / target_catalog are not supplied.
                         Kept for backward compatibility.
        source_schema  : Schema where the Silver source table lives.
        target_schema  : Schema where the enriched table will be written.
        source_table   : Short table name (no catalog/schema prefix).
        suggestions    : Approved BKSuggestion objects for this table.
        source_catalog : Catalog that holds the Silver source table.
                         Pass this explicitly when the user chose a different
                         target catalog — otherwise full_source would be built
                         with the target catalog and the SELECT would fail with
                         TABLE_OR_VIEW_NOT_FOUND.
        target_catalog : Catalog where the enriched table is written.
                         Defaults to `catalog` when omitted.

        Returns
        -------
        BKGenerationResult
        """
        # Resolve effective catalogs — explicit args win over the legacy `catalog`
        eff_source_catalog = source_catalog if source_catalog else catalog
        eff_target_catalog = target_catalog if target_catalog else catalog

        full_source = f"`{eff_source_catalog}`.`{source_schema}`.`{source_table}`"
        full_target = f"`{eff_target_catalog}`.`{target_schema}`.`{source_table}`"

        result = BKGenerationResult(
            source_table = full_source,
            target_table = full_target,
        )

        conn = self._connect()
        try:
            self._ensure_schema(conn, eff_target_catalog, target_schema)

            col_results: list[BKColumnResult] = []
            select_expressions: list[str] = ["*"]   # start with all source cols

            for s in suggestions:
                # Skip if BK column already exists in source
                if self._column_exists(conn, full_source.replace("`", ""), s.bk_column_name):
                    col_results.append(BKColumnResult(
                        entity          = s.entity,
                        bk_column_name  = s.bk_column_name,
                        natural_key_col = s.natural_key_col,
                        bk_prefix       = s.bk_prefix,
                        distinct_values = 0,
                        status          = "already_exists",
                        note            = f"{s.bk_column_name} already present in source",
                    ))
                    continue

                # Count distinct natural-key values to determine padding width
                distinct_count = self._count_distinct(
                    conn,
                    full_source.replace("`", ""),
                    s.natural_key_col,
                )

                if distinct_count == 0:
                    col_results.append(BKColumnResult(
                        entity          = s.entity,
                        bk_column_name  = s.bk_column_name,
                        natural_key_col = s.natural_key_col,
                        bk_prefix       = s.bk_prefix,
                        distinct_values = 0,
                        status          = "skipped",
                        note            = f"No distinct values in {s.natural_key_col}",
                    ))
                    continue

                pad_width  = self._pad_width(distinct_count)
                rank_expr  = (
                    f"DENSE_RANK() OVER (ORDER BY `{s.natural_key_col}`)"
                )
                val_expr   = self._format_expr(s.bk_prefix, pad_width, rank_expr)
                select_expressions.append(f"{val_expr} AS `{s.bk_column_name}`")

                col_results.append(BKColumnResult(
                    entity          = s.entity,
                    bk_column_name  = s.bk_column_name,
                    natural_key_col = s.natural_key_col,
                    bk_prefix       = s.bk_prefix,
                    distinct_values = distinct_count,
                    status          = "generated",
                    note            = (
                        f"Derived from {s.natural_key_col} "
                        f"({distinct_count} distinct values, "
                        f"format: {s.bk_prefix}_{'0' * (pad_width - 1)}1)"
                    ),
                ))

            result.column_results = col_results

            generated = [r for r in col_results if r.status == "generated"]
            if not generated:
                result.status = "failed"
                result.error  = "No business key columns could be generated."
                return result

            # Build and execute CREATE TABLE … AS SELECT
            select_sql = ", ".join(select_expressions)
            ddl = (
                f"CREATE OR REPLACE TABLE {full_target}\n"
                f"AS SELECT {select_sql}\n"
                f"FROM {full_source}"
            )

            # print(f"\n  Executing DDL:\n  {ddl[:300]}{'…' if len(ddl) > 300 else ''}\n")

            cur = conn.cursor()
            try:
                cur.execute(ddl)
            finally:
                cur.close()

            result.status = "enriched"

        except Exception as exc:
            result.status = "failed"
            result.error  = str(exc)
        finally:
            conn.close()

        return result