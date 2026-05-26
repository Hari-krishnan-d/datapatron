"""
silver_agent/silver_advanced_transform.py
==========================================
Three-stage JSON pipeline:

  STAGE 1 — template_rules.json
    Keyword-classify every row.
    Simple rows → predefined SQL expression (zero LLM).
    Saved as template_rules.json.

  STAGE 2 — complex_rules.json
    Complex rows → ONE batch LLM call → SQL expressions.
    Saved as complex_rules.json.
    Reusable: if same pattern seen again, read from file.

  STAGE 3 — silver_migration_plan.json (sql block)
    Merge template + complex rules in original row order.
    Assemble one complete SQL SELECT statement.
    User approves.
    Saved into the table's sql_transform block.

silver_execution.py:
    Reads silver_migration_plan.json
    Runs spark.sql(plan["sql"])
    Zero LLM. Same output every run.
"""

import json
import os
import sys
import re
import logging
from datetime import datetime, timezone

logger = logging.getLogger("Silver.AdvancedTransform")

# ── LLM client ────────────────────────────────────────────────────────────────
try:
    from llm import DatabricksLLMClient, extract_json
except ImportError:
    _here = os.path.dirname(os.path.abspath(__file__))
    _root = os.path.dirname(_here)
    sys.path.insert(0, os.path.join(_root, "MCA"))
    from llm import DatabricksLLMClient, extract_json


# =============================================================================
# PREDEFINED TEMPLATE PATTERNS
# =============================================================================

class Templates:
    @staticmethod
    def direct_load(src, tgt, **_):
        return f"src.`{src}` AS {tgt}"

    @staticmethod
    def rename(src, tgt, **_):
        return f"src.`{src}` AS {tgt}"

    @staticmethod
    def cast(src, tgt, target_type="STRING", **_):
        t = target_type.upper() if target_type else "STRING"
        return f"CAST(src.`{src}` AS {t}) AS {tgt}"

    @staticmethod
    def trim_clean(src, tgt, target_type="", **_):
        expr = f"NULLIF(TRIM(src.`{src}`), '')"
        if target_type and target_type.upper() not in ("STRING","VARCHAR","TEXT",""):
            expr = f"CAST({expr} AS {target_type.upper()})"
        return f"{expr} AS {tgt}"

    @staticmethod
    def standardize(src, tgt, **_):
        return f"UPPER(NULLIF(TRIM(src.`{src}`), '')) AS {tgt}"

    @staticmethod
    def cast_numeric(src, tgt, target_type="DECIMAL(18,2)", **_):    
        t = target_type.upper() if target_type else "DECIMAL(18,2)"
        return f"CAST(NULLIF(TRIM(src.`{src}`), '') AS {t}) AS {tgt}"


TEMPLATE_MAP = {
    "direct_load"    : Templates.direct_load,
    "direct"         : Templates.direct_load,
    "load"           : Templates.direct_load,
    "rename"         : Templates.rename,
    "cast"           : Templates.cast,
    "trim_clean"     : Templates.trim_clean,
    "trim"           : Templates.trim_clean,
    "clean"          : Templates.trim_clean,
    "standardize"    : Templates.standardize,
    "upper"          : Templates.standardize,
    "cast_numeric"   : Templates.cast_numeric,
    "convert_numeric": Templates.cast_numeric,
    "numeric"        : Templates.cast_numeric,
    "convert"        : Templates.cast_numeric,
}

KEYWORD_RULES = [
    (["surrogate", "foreign key", "fk ", "lookup", "dimension"], "foreign_key"),
    (["case when", "conditional", "flag", "if ", "when "],       "conditional"),
    (["multiply", "divide", "concat", "calculate", "formula",
      "arithmetic", "ratio", "percentage", " * ", " / ", " + "], "calculation"),
    (["upper", "lower", "standardize", "validate allowed",
      "allowed codes"],                                           "standardize"),
    (["trim", "clean", "null check", "nullif", "basic valid"],   "trim_clean"),
    (["convert to numeric", "numeric", "decimal", "cast numeric"],"cast_numeric"),
    (["cast", "convert", "change type", "data type"],            "cast"),
    (["rename", "different name"],                               "rename"),
    (["direct load", "as-is", "as is", "without modification",
      "load as-is", "load as is"],                              "direct_load"),
]

COMPLEX_TYPES = {"foreign_key", "conditional", "calculation", "custom"}


# =============================================================================
# HELPERS
# =============================================================================

def _classify(transformation: str, detail: str) -> str:
    text = (transformation + " " + detail).lower()
    for keywords, ttype in KEYWORD_RULES:
        if any(kw in text for kw in keywords):
            return ttype
    return None


def _infer_type(detail: str, transformation: str) -> str:
    text = (detail + " " + transformation).lower()
    if "decimal" in text:
        m = re.search(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", text)
        return f"DECIMAL({m.group(1)},{m.group(2)})" if m else "DECIMAL(18,2)"
    if "integer" in text or "int " in text: return "BIGINT"
    if "timestamp" in text or "datetime" in text: return "TIMESTAMP"
    if "date" in text and "time" not in text and "update" not in text: return "DATE"
    return "STRING"


def _extract(row: dict) -> dict:
    def g(*keys):
        for k in keys:
            v = row.get(k, "")
            if v: return str(v).strip()
        return ""
    return {
        "source_column"     : g("Source Column","source_column","SourceColumn","From","src_col"),
        "destination_column": g("Destination Column","destination_column","DestinationColumn","To","Target Column","dst_col"),
        "transformation"    : g("Transformation","transformation","Transform","Type"),
        "detail"            : g("Detailed Transformation Logic","detail","Description","Logic","Notes"),
        "destination_table" : g("Destination Table","destination_table","Target Table","TargetTable"),
        "source_table"      : g("Source Table","source_table","SourceTable"),
    }


def _read_file(file_path: str) -> list:
    ext = os.path.splitext(file_path)[1].lower()
    try:
        import pandas as pd
        if ext == ".csv":
            df = pd.read_csv(file_path)
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(file_path)
        elif ext == ".json":
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else [data]
        else:
            raise ValueError(f"Unsupported: '{ext}'")
        df.columns = [c.strip() for c in df.columns]
        return df.fillna("").to_dict(orient="records")
    except ImportError:
        raise RuntimeError("pandas required: pip install pandas openpyxl")


# =============================================================================
# STAGE 1 — TEMPLATE RULES JSON
# Keyword-classify every row. Simple rows → SQL expression instantly.
# Complex rows → flagged for Stage 2.
# =============================================================================

def _build_template_rules(rows: list, source_table: str, output_dir: str) -> dict:
    """
    Classify every row by keyword.
    Build SQL expression for all simple rows using predefined templates.

    Saves: template_rules.json
    Returns: {
        "simple":  {source_col: {sql_expr, ttype, target_col}},
        "complex": [{source_column, destination_column, transformation, detail, ttype}],
        "order":   [source_col, ...]   ← original row order for final assembly
    }
    """
    simple  = {}
    complex_rows = []
    order   = []
    skipped = []

    print(f"\n  {'─' * 60}")
    print(f"  STAGE 1 — Template Classification")
    print(f"  {'─' * 60}")
    print(f"  {'SOURCE':<24} {'TARGET':<24} {'TYPE'}")
    print(f"  {'─' * 60}")

    for raw_row in rows:
        row   = _extract(raw_row)
        row["source_table"] = source_table
        src   = row["source_column"]
        tgt   = row["destination_column"]
        trans = row["transformation"]
        det   = row["detail"]

        if not src or not tgt:
            skipped.append(src or "(empty)")
            continue

        ttype = _classify(trans, det) or "direct_load"
        order.append(src)

        if ttype not in COMPLEX_TYPES:
            fn       = TEMPLATE_MAP.get(ttype, Templates.direct_load)
            tgt_type = _infer_type(det, trans)
            sql_expr = fn(src=src, tgt=tgt, target_type=tgt_type)
            simple[src] = {
                "sql_expr"  : sql_expr,
                "ttype"     : ttype,
                "target_col": tgt,
            }
            print(f"  {src:<24} {tgt:<24} template:{ttype}")
        else:
            complex_rows.append({
                "source_column"     : src,
                "destination_column": tgt,
                "transformation"    : trans,
                "detail"            : det,
                "ttype"             : ttype,
            })
            print(f"  {src:<24} {tgt:<24} → complex ({ttype})")

    print(f"  {'─' * 60}")
    print(f"  Simple : {len(simple)}   Complex : {len(complex_rows)}   Skipped : {len(skipped)}\n")

    result = {
        "source_table": source_table,
        "created_at"  : datetime.now(timezone.utc).isoformat(),
        "simple"      : simple,
        "complex"     : complex_rows,
        "order"       : order,
    }

    # Save Stage 1 output
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "template_rules.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"  💾 template_rules.json → {path}")

    return result


# =============================================================================
# STAGE 2 — COMPLEX RULES JSON
# One batch LLM call for all complex rows.
# Result saved as complex_rules.json.
# Reusable: same source_col + ttype = same key, never re-generated.
# =============================================================================

BATCH_SYSTEM = """
You are a Spark SQL expert. Generate SQL expressions for multiple columns.

Source table alias: src
Dimension table aliases: use short names (cust, prod, dept, etc.)

For each row in the input, return one SQL expression + alias.

Rules:
1. Format: <expression> AS <target_column>  — no SELECT, no FROM, no semicolons
2. foreign_key  : COALESCE(<dim>.<surrogate_key>, -1) AS <target>
3. conditional  : CASE WHEN ... THEN ... ELSE ... END AS <target>
4. calculation  : <arithmetic> AS <target>
5. Handle nulls — use NULLIF, COALESCE, TRIM where needed
6. For foreign keys, return the JOIN clause needed
7. Return ONLY a valid JSON array. No markdown. No explanation.

Output format:
[
  {
    "source_column": "<same as input>",
    "sql_expr"     : "<complete expression> AS <alias>",
    "join_clause"  : "<LEFT JOIN ...>" or null
  }
]
"""


def _build_complex_rules(
    complex_rows: list,
    source_table: str,
    output_dir  : str,
    llm         : DatabricksLLMClient,
) -> dict:
    """
    Send ALL complex rows to LLM in ONE batch call.
    Saves result as complex_rules.json.

    Returns: {source_col: {sql_expr, join_clause, ttype}}
    """
    if not complex_rows:
        print(f"  No complex rows — skipping Stage 2.\n")
        return {}

    print(f"  {'─' * 60}")
    print(f"  STAGE 2 — Complex Rules  ({len(complex_rows)} rows → 1 LLM call)")
    print(f"  {'─' * 60}")
    print(f"  🧠 Sending batch to LLM...")

    user = (
        f"Source table : {source_table}  (alias: src)\n\n"
        f"Generate SQL expressions for ALL these rows:\n"
        f"{json.dumps(complex_rows, indent=2)}\n\n"
        f"Return a JSON array — one object per row."
    )

    try:
        raw     = llm.call(BATCH_SYSTEM, user)
        results = extract_json(raw)
        if not isinstance(results, list):
            raise ValueError("LLM did not return a list")
    except Exception as exc:
        logger.warning("Batch LLM failed: %s — using direct_load fallback", exc)
        print(f"  ⚠  LLM failed ({exc}) — falling back to direct_load for complex rows.")
        results = [
            {
                "source_column": r["source_column"],
                "sql_expr"     : f"src.`{r['source_column']}` AS {r['destination_column']}",
                "join_clause"  : None,
            }
            for r in complex_rows
        ]

    # Build lookup
    lookup = {}
    ttype_map = {r["source_column"]: r["ttype"] for r in complex_rows}

    for item in results:
        src = item.get("source_column", "")
        if not src:
            continue
        lookup[src] = {
            "sql_expr"   : item.get("sql_expr", f"src.`{src}` AS {src}"),
            "join_clause": item.get("join_clause"),
            "ttype"      : ttype_map.get(src, "custom"),
        }
        print(f"  ✅ {src:<24} → {item.get('sql_expr','')[:50]}...")

    print(f"\n  {len(lookup)} complex expressions generated.\n")

    # Save Stage 2 output
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "complex_rules.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "source_table": source_table,
            "created_at"  : datetime.now(timezone.utc).isoformat(),
            "rules"       : lookup,
        }, f, indent=2, ensure_ascii=False)
    print(f"  💾 complex_rules.json → {path}")

    return lookup


# =============================================================================
# STAGE 3 — ASSEMBLE FINAL SQL
# Merge template + complex in original row order.
# Build complete SELECT statement.
# User approves → saved into silver_migration_plan.json sql_transform block.
# =============================================================================

def _assemble_sql(
    template_rules: dict,
    complex_rules : dict,
    row_order     : list,
    source_table  : str,
    join_clauses  : list,
) -> str:
    """
    Merge template and complex rules in original row order.
    Build complete SELECT statement.
    """
    exprs = []
    for src in row_order:
        if src in template_rules["simple"]:
            exprs.append(template_rules["simple"][src]["sql_expr"])
        elif src in complex_rules:
            exprs.append(complex_rules[src]["sql_expr"])
        # else: skipped row, don't include

    col_list   = ",\n    ".join(exprs)
    join_block = ("\n" + "\n".join(join_clauses)) if join_clauses else ""

    return (
        f"SELECT\n"
        f"    {col_list}\n"
        f"FROM {source_table} src"
        f"{join_block}"
    )


def _save_final(
    sql         : str,
    template_rules: dict,
    complex_rules : dict,
    join_clauses  : list,
    output_dir  : str,
) -> str:
    """Save the assembled SQL plan as final_transform.json."""
    plan = {
        "created_at"     : datetime.now(timezone.utc).isoformat(),
        "source_table"   : template_rules.get("source_table", ""),
        "total_simple"   : len(template_rules.get("simple", {})),
        "total_complex"  : len(complex_rules),
        "total_columns"  : len(template_rules.get("order", [])),
        "join_clauses"   : join_clauses,
        "sql"            : sql,
    }
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "final_transform.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(plan, f, indent=2, ensure_ascii=False)
    print(f"  💾 final_transform.json → {path}")
    return path


def _review_sql(sql: str, simple_count: int, complex_count: int) -> str:
    """Show full SQL. User types YES or pastes correction."""
    print(f"\n  {'═' * 62}")
    print(f"  STAGE 3 — Final SQL  ({simple_count} template + {complex_count} LLM columns)")
    print(f"  {'═' * 62}\n")
    print(sql)
    print(f"\n  {'─' * 62}")
    print(f"  Type  YES         → approve and save")
    print(f"  Paste correction  → paste SQL then type END on a new line")
    print(f"  {'─' * 62}\n")

    while True:
        first = input("  > ").strip()
        if first.upper() == "YES":
            print("  ✅ SQL approved.\n")
            return sql
        lines = [first]
        print("  (paste SQL, type END when done)")
        while True:
            line = input()
            if line.strip().upper() == "END":
                break
            lines.append(line)
        corrected = "\n".join(lines).strip()
        if corrected:
            print("  ✅ Using your corrected SQL.\n")
            return corrected
        print("  ⚠  Nothing entered. Type YES or paste SQL.")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def collect_advanced_transforms(
    table_transforms    : list,
    metadata            : dict,
    llm                 : DatabricksLLMClient,
    existing_doc_content: str | None = None,
    output_dir          : str        = ".",
) -> list:
    """
    Called from silver_transform.py after basic transforms are planned.

    Three-stage pipeline:
      Stage 1  template_rules.json  — keyword classify, predefined SQL
      Stage 2  complex_rules.json   — 1 batch LLM call
      Stage 3  final_transform.json — assemble + user approval

    Adds sql_transform block to each table in table_transforms.
    """
    print(f"\n  {'─' * 62}")
    print(f"  ADVANCED TRANSFORMATION — MAPPING DOCUMENT")
    print(f"  {'─' * 62}\n")

    has_doc = input("  Process a mapping document? [Y / N]: ").strip().upper()
    if has_doc != "Y":
        print("\n  ℹ️  Skipping.\n")
        return table_transforms

    # ── Get mapping file ──────────────────────────────────────────────────────
    while True:
        file_path = input(
            "\n  Path to mapping document (.xlsx / .csv / .json):\n  > "
        ).strip().strip('"')
        if not file_path:
            print("  ⚠  Cannot be empty.")
            continue
        if not os.path.exists(file_path):
            print(f"  ⚠  Not found: {file_path}")
            continue
        break

    try:
        rows = _read_file(file_path)
        print(f"\n  ✅ Loaded {len(rows)} rows from {os.path.basename(file_path)}\n")
    except Exception as exc:
        print(f"  ❌ Could not read: {exc}")
        return table_transforms

    # ── Group rows by destination table ───────────────────────────────────────
    dest_groups: dict = {}
    for row in rows:
        r   = _extract(row)
        dst = r["destination_table"] or "output"
        dest_groups.setdefault(dst, []).append(row)

    print(f"  Destination tables: {list(dest_groups.keys())}\n")

    # ── Process each pipeline table ───────────────────────────────────────────
    for tbl in table_transforms:
        source_table = tbl.get("full_table", tbl.get("table", ""))
        tbl_output   = os.path.join(output_dir, tbl.get("table", "transform"))

        # Match destination group
        dest_rows = None
        for dst_name, dst_rows_candidate in dest_groups.items():
            norm_dst = dst_name.lower().replace(" ", "_").replace("-", "_")
            norm_src = tbl.get("table", "").lower()
            if norm_dst in norm_src or norm_src in norm_dst or norm_dst == norm_src:
                dest_rows = dst_rows_candidate
                print(f"  Matched '{dst_name}' → {source_table}\n")
                break

        if dest_rows is None:
            dest_rows = rows
            print(f"  No exact match — using all {len(rows)} rows for {source_table}\n")

        # ── STAGE 1: template_rules.json ──────────────────────────────────────
        template_rules = _build_template_rules(dest_rows, source_table, tbl_output)

        # ── STAGE 2: complex_rules.json ───────────────────────────────────────
        complex_rules = _build_complex_rules(
            complex_rows = template_rules["complex"],
            source_table = source_table,
            output_dir   = tbl_output,
            llm          = llm,
        )

        # Collect join clauses from complex rules
        join_clauses = [
            v["join_clause"]
            for v in complex_rules.values()
            if v.get("join_clause")
        ]

        # ── STAGE 3: assemble + user approval ─────────────────────────────────
        full_sql = _assemble_sql(
            template_rules = template_rules,
            complex_rules  = complex_rules,
            row_order      = template_rules["order"],
            source_table   = source_table,
            join_clauses   = join_clauses,
        )

        approved_sql = _review_sql(
            sql           = full_sql,
            simple_count  = len(template_rules["simple"]),
            complex_count = len(complex_rules),
        )

        # ── Save final_transform.json ──────────────────────────────────────────
        _save_final(
            sql            = approved_sql,
            template_rules = template_rules,
            complex_rules  = complex_rules,
            join_clauses   = join_clauses,
            output_dir     = tbl_output,
        )

        # ── Attach to table transform ──────────────────────────────────────────
        tbl["sql_transform"] = {
            "enabled"       : True,
            "sql"           : approved_sql,
            "source_table"  : source_table,
            "join_clauses"  : join_clauses,
            "total_columns" : len(template_rules["order"]),
            "template_count": len(template_rules["simple"]),
            "complex_count" : len(complex_rules),
            "files": {
                "template_rules"  : os.path.join(tbl_output, "template_rules.json"),
                "complex_rules"   : os.path.join(tbl_output, "complex_rules.json"),
                "final_transform" : os.path.join(tbl_output, "final_transform.json"),
            },
        }

        print(
            f"  ✅ Pipeline complete for {source_table}\n"
            f"     Stage 1  template_rules.json  : {len(template_rules['simple'])} columns\n"
            f"     Stage 2  complex_rules.json   : {len(complex_rules)} columns  (1 LLM call)\n"
            f"     Stage 3  final_transform.json : {len(template_rules['order'])} columns total\n"
        )

    return table_transforms