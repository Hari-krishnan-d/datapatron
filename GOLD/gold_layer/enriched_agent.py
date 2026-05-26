"""
enriched_agent.py — Unified Silver Enrichment Agent
=====================================================
Combines the Business Key (BK) workflow and the PK NULL fill workflow
into a single, sequenced pipeline.

Pipeline flow
-------------
  PHASE 1 — BK Analysis & Generation
    1a. BKAnalyzer   scans Silver metadata for missing business keys
        based on the star-schema model's flagged dimensions.
    1b. User reviews the BK suggestions and approves / edits / skips.
    1c. BKGenerator  writes enriched Silver tables (with new _id cols)
        into the target catalog / schema.

  ── CHECKPOINT ──────────────────────────────────────────────────────
      The user presses Enter to confirm the enriched tables look
      correct before the PK NULL phase begins.

  PHASE 2 — PK NULL Analysis & Fill
    2a. PKNullAnalyzer scans the enriched tables (or original Silver
        if no enrichment was produced) for NULL values in PK/FK cols.
    2b. User reviews the NULL suggestions via A / E / S gate:
          A — Approve all and proceed
          E — Edit via NLP chat (change anchor col, prefix, etc.)
          S — Skip: choose ALL or select specific tables/columns to skip
    2c. PKNullFiller  overwrites the enriched tables with NULL-filled
        versions (only for approved suggestions).

Key design rules
----------------
* The ORIGINAL Silver tables are NEVER modified.
* If Phase 1 produces enriched tables  → Phase 2 reads/writes there.
* If Phase 1 produces NO enriched tables → Phase 2 reads/writes the
  original Silver catalog + schema so nothing is silently skipped.
* Both phases present a human-approval checkpoint before writing
  anything to Databricks.

FIX NOTES (v2)
--------------
* _synthesise_suggestions: source_table is now resolved by scanning
  tables_metadata for whichever Silver table actually CONTAINS the
  descriptive columns belonging to the dim (e.g. dim_location's
  city/state/country columns come from the 'customer' table in Silver,
  not from a non-existent 'location' table).  Falls back to the entity
  name only if no match is found.
* Checkpoint is always shown after BK phase completes (whether enriched
  tables were produced or not) so the user can verify before PK phase.
* PK phase correctly uses effective_catalog/schema derived from BK
  phase outcome.

FIX NOTES (v3)
--------------
* PK NULL phase now has a full A / E / S approval gate (mirrors BK gate).
* S (Skip) now asks: skip ALL suggestions, or pick specific ones to skip
  interactively — remaining suggestions continue through the fill phase.
* New _nlp_edit_pk_suggestions: NLP chat editor for PK suggestions,
  allowing the user to change anchor_col, bk_prefix, column_role, etc.
* New _pk_approval_gate: orchestrates the A / E / S loop for PK phase.
* New _selective_skip_pk: presents a numbered menu to pick which
  table/column suggestions to skip; returns only the kept suggestions.
"""

from __future__ import annotations

import os
import sys
import json
import importlib.util
from dataclasses import dataclass, field
from typing import Optional

from GOLD.pk_null_flow.pk_null_editor import run_nlp_editor


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic import helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_module(module_name: str, file_path: str):
    """Import a Python file as a module by absolute path."""
    if module_name in sys.modules:
        return sys.modules[module_name]
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"\n  ⛔ Module not found: {file_path}\n"
            f"  Ensure '{os.path.basename(file_path)}' exists at that path."
        )
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod  = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return mod


# ─────────────────────────────────────────────────────────────────────────────
# Configuration dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AgentConfig:
    """All Databricks + location settings for the enrichment pipeline."""
    hostname         : str
    sql_warehouse_id : str
    access_token     : str
    source_catalog   : str          # Silver source catalog
    source_schema    : str          # Silver source schema
    target_catalog   : str          # Where enriched tables land
    target_schema    : str          # Where enriched tables land
    model_endpoint   : str = ""     # LLM endpoint for analyzer passes
    dropped_rows_dir : str = "output/dropped_null_audit"

    # Resolved at runtime by _resolve_module_paths()
    bk_analyzer_path  : str = ""
    bk_generator_path : str = ""
    pk_analyzer_path  : str = ""
    pk_filler_path    : str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EnrichmentPipelineResult:
    """Returned to the orchestrator after the full enrichment run."""
    target_catalog  : str = ""
    target_schema   : str = ""

    bk_suggestions    : list = field(default_factory=list)
    generation_results: list = field(default_factory=list)

    pk_suggestions  : list = field(default_factory=list)
    pk_fill_results : list = field(default_factory=list)

    enriched_tables : list[str] = field(default_factory=list)

    status : str = "pending"
    error  : str = ""


# ─────────────────────────────────────────────────────────────────────────────
# EnrichedAgent
# ─────────────────────────────────────────────────────────────────────────────

class EnrichedAgent:
    """
    Orchestrates the full BK → checkpoint → PK NULL enrichment pipeline.

    Parameters
    ----------
    config : AgentConfig
        All credentials and location settings.
    root_dir : str | None
        Project root directory.  Defaults to two levels above this file.
    """

    def __init__(self, config: AgentConfig, root_dir: str | None = None):
        self.config = config
        self._root  = root_dir or os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        self._resolve_module_paths()

    # ── Module path resolution ─────────────────────────────────────────────

    def _resolve_module_paths(self) -> None:
        bk_flow = os.path.join(self._root, "GOLD", "business_key_flow")
        pk_flow = os.path.join(self._root, "GOLD", "pk_null_flow")

        if not self.config.bk_analyzer_path:
            self.config.bk_analyzer_path  = os.path.join(bk_flow, "bk_analyzer.py")
        if not self.config.bk_generator_path:
            self.config.bk_generator_path = os.path.join(bk_flow, "bk_generator.py")
        if not self.config.pk_analyzer_path:
            self.config.pk_analyzer_path  = os.path.join(pk_flow, "pk_null_analyzer.py")
        if not self.config.pk_filler_path:
            self.config.pk_filler_path    = os.path.join(pk_flow, "pk_null_filler.py")

    # ── Module loaders ────────────────────────────────────────────────────

    def _bk_analyzer_mod(self):
        return _load_module("bk_analyzer_mod",      self.config.bk_analyzer_path)

    def _bk_generator_mod(self):
        return _load_module("bk_generator_mod",     self.config.bk_generator_path)

    def _pk_analyzer_mod(self):
        return _load_module("pk_null_analyzer_mod", self.config.pk_analyzer_path)

    def _pk_filler_mod(self):
        return _load_module("pk_null_filler_mod",   self.config.pk_filler_path)

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _creds(config: AgentConfig) -> dict:
        return {
            "hostname"        : config.hostname,
            "sql_warehouse_id": config.sql_warehouse_id,
            "access_token"    : config.access_token,
            "model_endpoint"  : config.model_endpoint,
        }

    @staticmethod
    def _checkpoint(message: str) -> None:
        """Pause execution and wait for the user to press Enter."""
        print()
        print("=" * 62)
        print("  ⏸  CHECKPOINT")
        print("=" * 62)
        print(f"\n  {message}\n")
        input("  ▶  Press Enter to continue... ")
        print()

    @staticmethod
    def _yesno(prompt: str) -> bool:
        while True:
            ans = input(f"  {prompt} (yes / no): ").strip().lower()
            if ans in ("y", "yes"):
                return True
            if ans in ("n", "no"):
                return False
            print("  ⚠  Please enter yes or no.")

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 1 helpers — BK synthesis
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _pick_natural_key(dim: dict, table_meta: dict) -> str:
        """
        Choose the best natural-key column for a dim that has no catalogue
        entry.  Priority:
          1. Descriptive column ending in _name or _code
          2. Descriptive column ending in _type, _method, or _category
          3. First descriptive column overall
          4. pk_temp column (last resort — DENSE_RANK on itself)
        """
        descriptive = [
            col["column"] for col in dim.get("columns", [])
            if isinstance(col, dict) and col.get("role") == "descriptive"
        ]
        if table_meta:
            silver_cols = [c["column"] for c in table_meta.get("columns", [])]
            descriptive = [c for c in descriptive if c in silver_cols] or descriptive

        for c in descriptive:
            if c.lower().endswith(("_name", "_code")):
                return c
        for c in descriptive:
            if c.lower().endswith(("_type", "_method", "_category")):
                return c
        if descriptive:
            return descriptive[0]

        for col in dim.get("columns", []):
            if isinstance(col, dict) and col.get("role") == "primary_key_temp":
                return col["column"]
        return ""

    @staticmethod
    def _make_bk_prefix(entity_name: str) -> str:
        parts = entity_name.split("_")
        if len(parts) >= 2:
            return (parts[0][:2] + parts[1][:1]).lower()
        return entity_name[:3].lower()

    # ── KEY FIX: resolve source table from Silver metadata ────────────────
    def _resolve_source_table_for_dim(
        self,
        dim            : dict,
        tables_metadata: dict,
    ) -> tuple[str, dict]:
        """
        Find which Silver table actually contains the descriptive columns
        that belong to this dim.
        """
        entity      = dim.get("name", "").replace("dim_", "").lower()
        dim_columns = {
            col["column"].lower()
            for col in dim.get("columns", [])
            if isinstance(col, dict) and col.get("role") == "descriptive"
        }

        # Pass 1 — exact entity name match
        for fqn, meta in tables_metadata.items():
            short = fqn.split(".")[-1].lower()
            if short == entity:
                return fqn, meta

        # Pass 2 — find the Silver table that contains the most dim columns
        best_fqn   = ""
        best_meta  = {}
        best_score = 0

        for fqn, meta in tables_metadata.items():
            silver_col_names = {
                c["column"].lower()
                for c in meta.get("columns", [])
            }
            overlap = len(dim_columns & silver_col_names)
            if overlap > best_score:
                best_score = overlap
                best_fqn   = fqn
                best_meta  = meta

        if best_fqn and best_score > 0:
            print(
                f"  [SYNTH] '{dim.get('name')}': no dedicated Silver table found — "
                f"using '{best_fqn.split('.')[-1]}' "
                f"({best_score}/{len(dim_columns)} columns matched)"
            )
            return best_fqn, best_meta

        # Pass 3 — fallback
        fallback_fqn = (
            f"{self.config.source_catalog}"
            f".{self.config.source_schema}"
            f".{entity}"
        )
        print(
            f"  [WARN]  '{dim.get('name')}': could not match any Silver table — "
            f"falling back to '{fallback_fqn}' (may not exist)."
        )
        return fallback_fqn, {}

    def _synthesise_suggestions(
        self,
        flagged_dims   : list,
        tables_metadata: dict,
        BKSuggestion,
    ) -> list:
        suggestions = []

        for dim in flagged_dims:
            dim_name  = dim.get("name", "")
            entity    = dim_name.replace("dim_", "")
            bk_col    = f"{entity}_id"
            bk_prefix = self._make_bk_prefix(entity)

            source_table, table_meta = self._resolve_source_table_for_dim(
                dim             = dim,
                tables_metadata = tables_metadata,
            )

            natural_key = self._pick_natural_key(dim, table_meta)
            if not natural_key:
                print(
                    f"  [SKIP]  Cannot synthesise BK for '{dim_name}' "
                    f"— no usable natural key column found."
                )
                continue

            s = BKSuggestion(
                entity          = entity,
                bk_column_name  = bk_col,
                bk_prefix       = bk_prefix,
                source_table    = source_table,
                natural_key_col = natural_key,
                supporting_cols = [],
                reason          = (
                    f"[SYNTH] '{dim_name}' has primary_key_temp — "
                    f"generating {bk_col} from '{natural_key}'"
                ),
                already_has_id  = False,
                detected_by     = "synth",
            )
            suggestions.append(s)
            print(
                f"  [SYNTH] '{dim_name}': will generate "
                f"'{bk_col}' from '{natural_key}' "
                f"(prefix: {bk_prefix})"
            )

        return suggestions

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 1 — Business Key Analysis + Generation
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _display_bk_suggestions(suggestions: list) -> None:
        col_w = 42
        by_w  = 12
        print(f"\n  {'Entity / Table':<{col_w}} {'Detected By':<{by_w}} Status")
        print(f"  {'-'*col_w}  {'-'*by_w}  {'-'*54}")
        for s in suggestions:
            entity  = f"{s.entity}  [{s.source_table.split('.')[-1]}]"
            det_by  = getattr(s, "detected_by", "rule")
            bk_col  = getattr(s, "bk_column_name", f"{s.entity}_id")
            nk      = getattr(s, "natural_key_col", "?")
            prefix  = getattr(s, "bk_prefix", s.entity[:3])
            status  = (
                f"⚠   Missing {bk_col}  ->  natural key: {nk}  ->  "
                f"format: {prefix}_001, {prefix}_002, ..."
            )
            print(f"  {entity:<{col_w}}  {det_by:<{by_w}}  {status}")
        missing = len(suggestions)
        print(f"\n  Summary: {missing} business key(s) missing, 0 already have surrogates.")

    def _nlp_edit_bk_suggestions(self, suggestions: list) -> list:
        """
        NLP chat editor for BK suggestions.
        Returns the (possibly modified) suggestions list.
        """
        import copy, json, re, urllib.request, urllib.error

        cfg = self.config

        SYSTEM_PROMPT = """You are a data-modelling assistant editing a list of Business Key suggestions.
The user will give you the current list as JSON and a plain-English instruction.
Return ONLY the updated list as a single valid JSON array — no markdown fences, no explanation.

Each item has this shape:
{
  "entity":          "<string>",
  "bk_column_name":  "<string>",
  "bk_prefix":       "<3-letter prefix>",
  "source_table":    "<fully qualified table name>",
  "natural_key_col": "<column name>",
  "detected_by":     "rule" | "synth"
}

Instructions you must handle:
- "change <entity>'s natural key to <col>"       -> update natural_key_col
- "remove <entity>"                              -> remove that item
- "change <entity>'s prefix to <pre>"           -> update bk_prefix and bk_column_name prefix
- "rename <entity> to <name>"                   -> update entity and bk_column_name
- "add entity <name> from table <t> using <col> with prefix <pre>" -> append new item
"""

        def _serialize(sugg_list: list) -> list:
            out = []
            for s in sugg_list:
                out.append({
                    "entity"         : getattr(s, "entity",          ""),
                    "bk_column_name" : getattr(s, "bk_column_name",  f"{s.entity}_id"),
                    "bk_prefix"      : getattr(s, "bk_prefix",       s.entity[:3]),
                    "source_table"   : getattr(s, "source_table",    ""),
                    "natural_key_col": getattr(s, "natural_key_col", ""),
                    "detected_by"    : getattr(s, "detected_by",     "rule"),
                })
            return out

        def _deserialize(raw_list: list, original: list) -> list:
            if not original:
                return raw_list
            SuggClass = type(original[0])
            result = []
            for item in raw_list:
                try:
                    s = SuggClass(
                        entity          = item.get("entity", ""),
                        bk_column_name  = item.get("bk_column_name", ""),
                        bk_prefix       = item.get("bk_prefix", ""),
                        source_table    = item.get("source_table", ""),
                        natural_key_col = item.get("natural_key_col", ""),
                        supporting_cols = [],
                        reason          = f"[EDITED] {item.get('entity','')}",
                        already_has_id  = False,
                        detected_by     = item.get("detected_by", "rule"),
                    )
                    result.append(s)
                except Exception:
                    result.append(item)
            return result

        def _call_llm(current_serial: list, instruction: str) -> list:
            payload = {
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": (
                        f"Current suggestions:\n"
                        f"{json.dumps(current_serial, indent=2)}\n\n"
                        f"Instruction: {instruction}"
                    )},
                ],
                "max_tokens": 2048,
            }
            url = f"https://{cfg.hostname}/serving-endpoints/{cfg.model_endpoint}/invocations"
            req = urllib.request.Request(
                url,
                data    = json.dumps(payload).encode(),
                headers = {
                    "Content-Type" : "application/json",
                    "Authorization": f"Bearer {cfg.access_token}",
                },
                method = "POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=120) as resp:
                    raw = json.loads(resp.read().decode())
            except urllib.error.HTTPError as e:
                raise RuntimeError(f"LLM HTTP {e.code}: {e.read().decode()}") from e

            text = ""
            choices = raw.get("choices", [])
            if choices:
                text = choices[0].get("message", {}).get("content", "") or choices[0].get("text", "")
            if not text:
                text = raw.get("content", "") or str(raw)

            text = __import__("re").sub(r"^```(?:json)?\s*", "", text.strip(), flags=__import__("re").IGNORECASE)
            text = __import__("re").sub(r"\s*```$",          "", text.strip())
            return json.loads(text)

        print("\n" + "=" * 62)
        print("  BUSINESS KEY EDITOR  —  NLP Chat Mode")
        print("=" * 62)
        print("  Describe your changes in plain English.")
        print("  Examples:")
        print('    "change employee\'s natural key to employee_name"')
        print('    "remove the category entity"')
        print('    "change location\'s prefix to lct"')
        print('    "add entity product from table orders using product_name with prefix prd"')
        print()
        print("  Type  done / continue  (or press Enter)  to finish editing.")
        print("-" * 62)

        current    = copy.deepcopy(suggestions)
        edit_count = 0

        while True:
            try:
                instruction = input(f"\n  Edit #{edit_count + 1} → ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\n  (Editor interrupted — keeping last saved suggestions.)")
                break

            if instruction.lower() in ("", "done", "continue", "exit", "quit"):
                if edit_count == 0:
                    print("  No changes made.")
                else:
                    print(f"  Exiting editor — {edit_count} edit(s) applied.")
                break

            print("  ⏳ Applying edit...")
            try:
                serialized  = _serialize(current)
                updated_raw = _call_llm(serialized, instruction)
            except json.JSONDecodeError as e:
                print(f"  ⚠  LLM returned invalid JSON — edit not applied. ({e})")
                continue
            except Exception as e:
                print(f"  ⚠  Edit failed: {e}")
                continue

            if not isinstance(updated_raw, list):
                print("  ⚠  LLM response is not a list — edit not applied.")
                continue

            current    = _deserialize(updated_raw, suggestions)
            edit_count += 1
            print("\n  ✅  Edit applied. Updated suggestions:")
            self._display_bk_suggestions(current)

            try:
                more = input(
                    "\n  Make another change? (yes / no  or press Enter to finish): "
                ).strip().lower()
            except (EOFError, KeyboardInterrupt):
                break

            if more not in ("yes", "y"):
                print(f"  Exiting editor — {edit_count} edit(s) applied.")
                break

        return current

    def _run_bk_phase(
        self,
        tables_metadata : dict,
        flagged_dims    : list,
        result          : EnrichmentPipelineResult,
    ) -> dict[str, list]:
        cfg = self.config

        print("\n" + "=" * 62)
        print("  ENRICHMENT PHASE 1 — BUSINESS KEY ANALYSIS")
        print("=" * 62)

        bk_mod = self._bk_analyzer_mod()
        creds  = self._creds(cfg)

        all_suggestions = bk_mod.analyze_missing_business_keys(
            silver_metadata = tables_metadata,
            creds           = creds,
        )

        flagged_entities: set[str] = set()
        for dim in flagged_dims:
            raw = dim.get("name", "").replace("dim_", "").lower()
            flagged_entities.add(raw)

        catalogue_matched: list = [
            s for s in all_suggestions
            if (not s.already_has_id)
            and (
                s.entity.lower() in flagged_entities
                or any(
                    d.get("name", "").replace("dim_", "").lower()
                    in s.source_table.lower()
                    for d in flagged_dims
                )
            )
        ]

        catalogue_covered_dims = {s.entity.lower() for s in catalogue_matched}

        uncovered_dims = [
            d for d in flagged_dims
            if d.get("name", "").replace("dim_", "").lower()
            not in catalogue_covered_dims
        ]

        synth_suggestions: list = []
        if uncovered_dims:
            print(
                f"\n  ℹ  {len(uncovered_dims)} dim(s) not in BK catalogue — "
                f"synthesising suggestions directly:"
            )
            BKSuggestion      = bk_mod.BKSuggestion
            synth_suggestions = self._synthesise_suggestions(
                flagged_dims    = uncovered_dims,
                tables_metadata = tables_metadata,
                BKSuggestion    = BKSuggestion,
            )

        suggestions_for_flagged = catalogue_matched + synth_suggestions
        result.bk_suggestions   = suggestions_for_flagged

        if not suggestions_for_flagged:
            print("\n  ✅  No missing business keys found for the flagged dimensions.")
            return {}

        print()
        bk_mod.print_bk_audit_report(suggestions_for_flagged)

        approved = self._bk_approval_gate(suggestions_for_flagged)
        if approved is None:
            return {}
        suggestions_for_flagged = approved
        result.bk_suggestions   = approved

        print("\n" + "=" * 62)
        print("  ENRICHMENT PHASE 1 — BUSINESS KEY GENERATION")
        print("=" * 62)

        gen_mod   = self._bk_generator_mod()
        generator = gen_mod.BKGenerator(
            hostname         = cfg.hostname,
            sql_warehouse_id = cfg.sql_warehouse_id,
            access_token     = cfg.access_token,
        )

        by_table: dict[str, list] = {}
        for s in suggestions_for_flagged:
            tbl_short = s.source_table.split(".")[-1]
            by_table.setdefault(tbl_short, []).append(s)

        generation_results = []
        approved_by_table  = {}

        for tbl_short, table_suggestions in by_table.items():
            print(f"\n  ▶  Generating BKs for table: {tbl_short}")
            print(f"       Source: {cfg.source_catalog}.{cfg.source_schema}.{tbl_short}")
            print(f"       Target: {cfg.target_catalog}.{cfg.target_schema}.{tbl_short}")

            gen_result = generator.generate(
                catalog        = cfg.source_catalog,
                source_schema  = cfg.source_schema,
                target_schema  = cfg.target_schema,
                source_table   = tbl_short,
                suggestions    = table_suggestions,
                source_catalog = cfg.source_catalog,
                target_catalog = cfg.target_catalog,
            )

            generation_results.append(gen_result)

            if gen_result.status == "enriched":
                print(
                    f"  ✅  BK enrichment complete for '{tbl_short}' → "
                    f"{gen_result.target_table}"
                )
                result.enriched_tables.append(tbl_short)
                approved_by_table[tbl_short] = table_suggestions
            else:
                print(
                    f"  ❌  BK generation failed for '{tbl_short}': "
                    f"{gen_result.error}"
                )

        result.generation_results = generation_results
        return approved_by_table

    def _bk_approval_gate(self, suggestions: list) -> list | None:
        """
        A — Approve and generate
        E — Edit via NLP, then re-ask
        S — Skip BK generation entirely

        Returns approved suggestions list, or None if skipped.
        """
        while True:
            print("\n" + "=" * 62)
            print("  BUSINESS KEY REVIEW & APPROVAL")
            print("=" * 62)
            self._display_bk_suggestions(suggestions)
            print()
            print("    A — Approve    (generate the business keys above)")
            print("    E — Edit       (modify using natural language)")
            print("    S — Skip       (proceed without generating any BKs)")
            print()

            choice = input("  Your choice (A / E / S): ").strip().upper()

            if choice == "A":
                print("\n  ✅  Approved. Generating business keys...\n")
                return suggestions

            if choice == "S":
                print("\n  ⚠  BK generation skipped by user.\n")
                return None

            if choice == "E":
                edited = self._nlp_edit_bk_suggestions(suggestions)

                print("\n" + "=" * 62)
                print("  UPDATED BUSINESS KEY SUGGESTIONS")
                print("=" * 62)
                self._display_bk_suggestions(edited)
                print()
                print("    A — Approve and generate")
                print("    E — Make more changes")
                print("    S — Skip BK generation")
                print()

                inner = input("  Your choice (A / E / S): ").strip().upper()

                if inner == "A":
                    print("\n  ✅  Approved. Generating business keys...\n")
                    return edited
                if inner == "S":
                    print("\n  ⚠  BK generation skipped.\n")
                    return None
                if inner == "E":
                    suggestions = edited
                    continue

                print("  ⚠  Please enter A, E, or S.")
                suggestions = edited
                continue

            print("  ⚠  Please enter A, E, or S.")

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 2 — PK NULL Analysis + Fill  (NEW: A / E / S approval gate)
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _display_pk_suggestions(suggestions: list) -> None:
        """Print a formatted table of PK NULL suggestions."""
        col_w  = 26
        col_w2 = 22
        col_w3 = 22
        print(
            f"\n  {'#':<4} {'Table':<{col_w}} {'PK / FK Column':<{col_w2}} "
            f"{'Anchor Col':<{col_w3}} {'Role':<18} NULLs"
        )
        print(
            f"  {'-'*4}  {'-'*col_w}  {'-'*col_w2}  {'-'*col_w3}  {'-'*18}  {'-'*8}"
        )
        for i, s in enumerate(suggestions, start=1):
            tbl    = getattr(s, "table_name",  "?")
            pk_col = getattr(s, "pk_column",   "?")
            anchor = getattr(s, "anchor_display", None) or getattr(s, "anchor_col", "?")
            role   = getattr(s, "column_role", "primary_key")
            nulls  = getattr(s, "null_count",  0)
            prefix = getattr(s, "bk_prefix",   "")
            prefix_str = f"  (prefix: {prefix})" if prefix else ""
            print(
                f"  {i:<4} {tbl:<{col_w}}  {pk_col:<{col_w2}}  "
                f"{anchor:<{col_w3}}  {role:<18}  {nulls}{prefix_str}"
            )
        print(
            f"\n  Summary: {len(suggestions)} NULL PK/FK column(s) flagged across "
            f"{len({getattr(s,'table_name','') for s in suggestions})} table(s)."
        )

    @staticmethod
    def _selective_skip_pk(suggestions: list) -> list:
        """
        Present a numbered list of PK suggestions and let the user choose
        which ones to SKIP.  Returns only the suggestions the user wants
        to keep (i.e. those NOT skipped).

        The user can enter:
          - individual numbers:  1 3 5
          - ranges:              2-4
          - "all"                (skip everything — returns empty list)
          - blank / Enter        (keep everything — returns full list)
        """
        if not suggestions:
            return suggestions

        print("\n" + "=" * 62)
        print("  PK NULL SELECTIVE SKIP")
        print("=" * 62)
        print("  Enter the number(s) of the suggestion(s) you want to SKIP.")
        print("  Separate multiple numbers with spaces.  Ranges are OK (e.g. 2-4).")
        print('  Type  "all"  to skip everything, or press Enter to keep all.')
        print("-" * 62)

        # Re-display the numbered list for easy reference
        for i, s in enumerate(suggestions, start=1):
            tbl    = getattr(s, "table_name",  "?")
            pk_col = getattr(s, "pk_column",   "?")
            anchor = getattr(s, "anchor_display", None) or getattr(s, "anchor_col", "?")
            nulls  = getattr(s, "null_count",  0)
            print(f"  [{i:>2}]  {tbl}.{pk_col}  (anchor: {anchor}, nulls: {nulls})")

        print()

        while True:
            try:
                raw = input("  Skip which items? → ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\n  (Interrupted — keeping all suggestions.)")
                return suggestions

            if raw == "":
                print("  No items skipped — keeping all suggestions.")
                return suggestions

            if raw.lower() == "all":
                print("  ⚠  All PK NULL suggestions skipped.")
                return []

            # Parse individual numbers and ranges
            skip_indices: set[int] = set()
            tokens = raw.replace(",", " ").split()
            parse_error = False
            for token in tokens:
                if "-" in token:
                    parts = token.split("-", 1)
                    try:
                        lo, hi = int(parts[0]), int(parts[1])
                        skip_indices.update(range(lo, hi + 1))
                    except ValueError:
                        print(f"  ⚠  Cannot parse range '{token}' — try again.")
                        parse_error = True
                        break
                else:
                    try:
                        skip_indices.add(int(token))
                    except ValueError:
                        print(f"  ⚠  '{token}' is not a valid number — try again.")
                        parse_error = True
                        break

            if parse_error:
                continue

            valid_range = set(range(1, len(suggestions) + 1))
            out_of_range = skip_indices - valid_range
            if out_of_range:
                print(
                    f"  ⚠  Number(s) out of range: "
                    f"{sorted(out_of_range)} — valid range is 1–{len(suggestions)}."
                )
                continue

            kept    = [s for i, s in enumerate(suggestions, start=1) if i not in skip_indices]
            skipped = [s for i, s in enumerate(suggestions, start=1) if i in skip_indices]

            skipped_names = ", ".join(
                f"{getattr(s,'table_name','?')}.{getattr(s,'pk_column','?')}"
                for s in skipped
            )
            print(f"\n  Skipping {len(skipped)} item(s): {skipped_names}")

            if kept:
                print(f"  Keeping  {len(kept)} item(s) for NULL fill.")
            else:
                print("  ⚠  All suggestions were skipped — nothing will be filled.")

            return kept

    def _nlp_edit_pk_suggestions(self, suggestions: list) -> list:
        """
        NLP chat editor for PK NULL suggestions.
        Delegates to run_nlp_editor which applies edits IN-PLACE on the
        existing PKNullSuggestion objects — only the fields explicitly
        mentioned in the edit are mutated; everything else is preserved.
        """
        return run_nlp_editor(suggestions, self._display_pk_suggestions)

    def _pk_approval_gate(self, suggestions: list) -> list | None:
        """
        A / E / S approval gate for PK NULL suggestions — mirrors _bk_approval_gate.

        A — Approve all and proceed to fill
        E — Edit via NLP chat (anchor col, prefix, role, etc.), then re-ask
        S — Skip:
              ALL      → return None  (fill phase is entirely bypassed)
              SPECIFIC → let user pick which suggestions to skip;
                         returns the remaining (non-skipped) suggestions,
                         or None if everything was skipped

        Returns
        -------
        list  — the (possibly edited / filtered) suggestions to fill, or
        None  — if the user chose to skip all suggestions.
        """
        while True:
            print("\n" + "=" * 62)
            print("  PK NULL REVIEW & APPROVAL")
            print("=" * 62)
            self._display_pk_suggestions(suggestions)
            print()
            print("    A — Approve    (fill all NULL values listed above)")
            print("    E — Edit       (modify using natural language)")
            print("    S — Skip       (skip all or choose specific items to skip)")
            print()

            choice = input("  Your choice (A / E / S): ").strip().upper()

            # ── Approve ───────────────────────────────────────────────────
            if choice == "A":
                print("\n  ✅  Approved. Proceeding with PK NULL fill...\n")
                return suggestions

            # ── Skip ──────────────────────────────────────────────────────
            if choice == "S":
                print()
                print("    1 — Skip ALL    (bypass NULL fill entirely)")
                print("    2 — Skip SPECIFIC items (choose which ones to skip)")
                print()
                sub = input("  Your choice (1 / 2): ").strip()

                if sub == "1":
                    print("\n  ⚠  All PK NULL suggestions skipped by user.\n")
                    return None

                if sub == "2":
                    kept = self._selective_skip_pk(suggestions)
                    if not kept:
                        print("\n  ⚠  All suggestions were skipped — nothing to fill.\n")
                        return None
                    # Ask the user to confirm the kept suggestions before proceeding
                    print("\n  Suggestions remaining after selective skip:")
                    self._display_pk_suggestions(kept)
                    print()
                    confirm = input(
                        "  Proceed with filling the remaining suggestion(s)? (yes / no): "
                    ).strip().lower()
                    if confirm in ("y", "yes"):
                        print("\n  ✅  Proceeding with partial PK NULL fill...\n")
                        return kept
                    else:
                        print("\n  ⚠  Fill cancelled — looping back to approval gate.\n")
                        # Loop back so the user can re-choose A / E / S
                        continue

                print("  ⚠  Please enter 1 or 2.")
                continue

            # ── Edit ──────────────────────────────────────────────────────
            if choice == "E":
                edited = self._nlp_edit_pk_suggestions(suggestions)

                print("\n" + "=" * 62)
                print("  UPDATED PK NULL SUGGESTIONS")
                print("=" * 62)
                self._display_pk_suggestions(edited)
                print()
                print("    A — Approve and fill")
                print("    E — Make more changes")
                print("    S — Skip (all or specific)")
                print()

                inner = input("  Your choice (A / E / S): ").strip().upper()

                if inner == "A":
                    print("\n  ✅  Approved. Proceeding with PK NULL fill...\n")
                    return edited

                if inner == "S":
                    # Reuse the same skip sub-menu with the edited list
                    suggestions = edited
                    # Re-enter the outer loop so the S branch runs again cleanly
                    # (set choice implicitly by continuing)
                    print()
                    print("    1 — Skip ALL")
                    print("    2 — Skip SPECIFIC items")
                    print()
                    sub = input("  Your choice (1 / 2): ").strip()

                    if sub == "1":
                        print("\n  ⚠  All PK NULL suggestions skipped.\n")
                        return None

                    if sub == "2":
                        kept = self._selective_skip_pk(edited)
                        if not kept:
                            print("\n  ⚠  All suggestions skipped — nothing to fill.\n")
                            return None
                        print("\n  Suggestions remaining after selective skip:")
                        self._display_pk_suggestions(kept)
                        print()
                        confirm = input(
                            "  Proceed with filling the remaining suggestion(s)? (yes / no): "
                        ).strip().lower()
                        if confirm in ("y", "yes"):
                            print("\n  ✅  Proceeding with partial PK NULL fill...\n")
                            return kept
                        else:
                            suggestions = edited
                            continue

                    print("  ⚠  Please enter 1 or 2.")
                    suggestions = edited
                    continue

                if inner == "E":
                    suggestions = edited
                    continue

                print("  ⚠  Please enter A, E, or S.")
                suggestions = edited
                continue

            print("  ⚠  Please enter A, E, or S.")

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 2 helpers
    # ─────────────────────────────────────────────────────────────────────

    def _build_pk_metadata(
        self,
        tables_metadata   : dict,
        enriched_tables   : list[str],
        effective_catalog : str,
        effective_schema  : str,
    ) -> dict:
        pk_metadata : dict = {}
        enriched_set = set(enriched_tables)

        for fqn, meta in tables_metadata.items():
            tbl_short = fqn.split(".")[-1]
            if tbl_short in enriched_set:
                new_key = (
                    f"`{effective_catalog}`"
                    f".`{effective_schema}`"
                    f".`{tbl_short}`"
                )
            else:
                new_key = fqn
            pk_metadata[new_key] = meta

        return pk_metadata

    def _run_pk_phase(
        self,
        tables_metadata   : dict,
        effective_catalog : str,
        effective_schema  : str,
        result            : EnrichmentPipelineResult,
    ) -> None:
        """
        Scan for PK/FK NULLs, present A / E / S approval gate, then fill.
        """
        cfg = self.config

        print("\n" + "=" * 62)
        print("  ENRICHMENT PHASE 2 — PK / FK NULL ANALYSIS")
        print("=" * 62)

        pk_meta = self._build_pk_metadata(
            tables_metadata   = tables_metadata,
            enriched_tables   = result.enriched_tables,
            effective_catalog = effective_catalog,
            effective_schema  = effective_schema,
        )

        pk_mod = self._pk_analyzer_mod()
        creds  = self._creds(cfg)

        pk_suggestions = pk_mod.analyze_pk_nulls(
            gold_metadata = pk_meta,
            catalog       = effective_catalog,
            source_schema = effective_schema,
            creds         = creds,
        )

        result.pk_suggestions = pk_suggestions

        if not pk_suggestions:
            print("\n  ✅  No NULL values found in any PK or FK column.")
            return

        print()
        pk_mod.print_pk_null_report(pk_suggestions)

        # ── A / E / S gate ────────────────────────────────────────────────
        approved_suggestions = self._pk_approval_gate(pk_suggestions)

        if approved_suggestions is None:
            print("\n  ⚠  PK NULL fill skipped by user.")
            return

        # Update result to reflect only the approved subset
        result.pk_suggestions = approved_suggestions

        print("\n" + "=" * 62)
        print("  ENRICHMENT PHASE 2 — PK / FK NULL FILL")
        print("=" * 62)

        fill_mod = self._pk_filler_mod()
        filler   = fill_mod.PKNullFiller(
            hostname         = cfg.hostname,
            sql_warehouse_id = cfg.sql_warehouse_id,
            access_token     = cfg.access_token,
        )

        by_table: dict[str, list] = {}
        for s in approved_suggestions:
            by_table.setdefault(s.table_name, []).append(s)

        fill_results = []

        for tbl_short, tbl_suggestions in by_table.items():
            if tbl_short in result.enriched_tables:
                read_catalog = cfg.target_catalog
                read_schema  = cfg.target_schema
            else:
                read_catalog = cfg.source_catalog
                read_schema  = cfg.source_schema

            print(
                f"\n  ▶  Filling PK NULLs for table: {tbl_short}\n"
                f"       Source: {read_catalog}.{read_schema}.{tbl_short}\n"
                f"       Target: {cfg.target_catalog}.{cfg.target_schema}.{tbl_short}"
            )

            fill_result = filler.fill(
                catalog        = read_catalog,
                source_schema  = read_schema,
                target_schema  = cfg.target_schema,
                source_table   = tbl_short,
                suggestions    = tbl_suggestions,
                source_catalog = read_catalog,
                target_catalog = cfg.target_catalog,
                output_dir     = cfg.dropped_rows_dir,
            )

            fill_results.append(fill_result)

            if fill_result.status == "enriched":
                print(
                    f"  ✅  PK NULL fill complete for '{tbl_short}' → "
                    f"{fill_result.target_table}"
                )
                if tbl_short not in result.enriched_tables:
                    result.enriched_tables.append(tbl_short)
            else:
                print(
                    f"  ❌  PK NULL fill failed for '{tbl_short}': "
                    f"{fill_result.error}"
                )

        result.pk_fill_results = fill_results

    # ─────────────────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ─────────────────────────────────────────────────────────────────────

    def run_pipeline_with_context(
        self,
        tables_metadata : dict,
        flagged_dims    : list,
    ) -> dict:
        """
        Execute the full enrichment pipeline.

        Parameters
        ----------
        tables_metadata : dict
            Silver metadata from gold_metadata.fetch_metadata_grouped().
        flagged_dims : list
            Dimension dicts from the star schema model that have
            role == "primary_key_temp" columns (need BK generation).

        Returns
        -------
        dict compatible with the orchestrator's enrichment_result usage.
        """
        cfg    = self.config
        result = EnrichmentPipelineResult(
            target_catalog = cfg.target_catalog,
            target_schema  = cfg.target_schema,
        )

        try:
            # ── PHASE 1 — BK ────────────────────────────────────────────
            if flagged_dims:
                self._run_bk_phase(
                    tables_metadata = tables_metadata,
                    flagged_dims    = flagged_dims,
                    result          = result,
                )
            else:
                print(
                    "\n  ℹ  No flagged dimensions supplied — "
                    "skipping BK generation phase."
                )

            # ── CHECKPOINT ──────────────────────────────────────────────
            if result.enriched_tables:
                checkpoint_msg = (
                    f"Business key enrichment wrote {len(result.enriched_tables)} "
                    f"table(s) to:\n"
                    f"  {cfg.target_catalog}.{cfg.target_schema}\n\n"
                    f"  Tables enriched:\n"
                    + "\n".join(
                        f"    • {cfg.target_catalog}.{cfg.target_schema}.{t}"
                        for t in result.enriched_tables
                    )
                    + "\n\n"
                    "  Please verify the enriched tables in Databricks look\n"
                    "  correct before the PK NULL fill phase runs."
                )
                effective_catalog = cfg.target_catalog
                effective_schema  = cfg.target_schema
            else:
                checkpoint_msg = (
                    "BK phase produced no enriched tables.\n\n"
                    "  The PK NULL scan will run against the ORIGINAL\n"
                    f"  Silver tables in:\n"
                    f"    {cfg.source_catalog}.{cfg.source_schema}\n\n"
                    "  No Silver data will be modified — only the target\n"
                    f"  schema ({cfg.target_catalog}.{cfg.target_schema})\n"
                    "  will receive PK-filled copies."
                )
                effective_catalog = cfg.source_catalog
                effective_schema  = cfg.source_schema

            self._checkpoint(checkpoint_msg)

            # ── PHASE 2 — PK NULL ───────────────────────────────────────
            self._run_pk_phase(
                tables_metadata   = tables_metadata,
                effective_catalog = effective_catalog,
                effective_schema  = effective_schema,
                result            = result,
            )

            # ── CHECKPOINT — post PK NULL fill ──────────────────────────
            pk_filled = [
                r for r in result.pk_fill_results
                if getattr(r, "status", "") == "enriched"
                or (isinstance(r, dict) and r.get("status") == "enriched")
            ]
            pk_failed = [
                r for r in result.pk_fill_results
                if getattr(r, "status", "") == "failed"
                or (isinstance(r, dict) and r.get("status") == "failed")
            ]

            if result.pk_fill_results:
                def _tbl_name(r):
                    return (
                        r.get("source_table", "") if isinstance(r, dict)
                        else getattr(r, "source_table", "")
                    )
                def _tbl_status(r):
                    return (
                        r.get("status", "") if isinstance(r, dict)
                        else getattr(r, "status", "")
                    )

                summary_lines = "\n".join(
                    f"    {'✅' if _tbl_status(r) == 'enriched' else '❌'}  "
                    f"{cfg.target_catalog}.{cfg.target_schema}."
                    f"{_tbl_name(r).split('.')[-1].strip('`')}"
                    f"  [{_tbl_status(r)}]"
                    for r in result.pk_fill_results
                )

                if pk_failed:
                    pk_cp_msg = (
                        f"PK NULL fill phase complete — "
                        f"{len(pk_filled)} succeeded, {len(pk_failed)} failed.\n\n"
                        f"  Results per table:\n{summary_lines}\n\n"
                        f"  ⚠  Some tables had errors — review the output above\n"
                        f"  before allowing DM-5 Gold deployment to proceed."
                    )
                else:
                    pk_cp_msg = (
                        f"PK NULL fill phase complete — "
                        f"{len(pk_filled)} table(s) written to:\n"
                        f"  {cfg.target_catalog}.{cfg.target_schema}\n\n"
                        f"  Results per table:\n{summary_lines}\n\n"
                        f"  Please verify the final enriched tables in Databricks\n"
                        f"  look correct before DM-5 deploys them to the Gold layer."
                    )
            else:
                pk_cp_msg = (
                    "PK NULL fill phase produced no changes\n"
                    "  (no NULLs were found, all were skipped, or user cancelled).\n\n"
                    f"  Final enriched data is in:\n"
                    f"    {cfg.target_catalog}.{cfg.target_schema}\n\n"
                    "  Press Enter to hand off to Gold DM-5 deployment."
                )

            self._checkpoint(pk_cp_msg)

            # ── Final status ─────────────────────────────────────────────
            result.status = (
                "complete" if result.enriched_tables else "skipped"
            )

        except Exception as exc:
            import traceback
            print(f"\n  ❌  Enrichment pipeline error: {exc}")
            traceback.print_exc()
            result.status = "failed"
            result.error  = str(exc)

        return self._to_dict(result)

    # ─────────────────────────────────────────────────────────────────────
    # Serialise result for the orchestrator
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _to_dict(result: EnrichmentPipelineResult) -> dict:
        def _col_result_dict(cr) -> dict:
            return {
                "entity"         : getattr(cr, "entity",          ""),
                "bk_column_name" : getattr(cr, "bk_column_name",  ""),
                "natural_key_col": getattr(cr, "natural_key_col",  ""),
                "bk_prefix"      : getattr(cr, "bk_prefix",       ""),
                "distinct_values": getattr(cr, "distinct_values",  0),
                "status"         : getattr(cr, "status",           ""),
                "note"           : getattr(cr, "note",             ""),
            }

        def _gen_result_dict(gr) -> dict:
            return {
                "source_table": getattr(gr, "source_table", ""),
                "target_table": getattr(gr, "target_table", ""),
                "status"      : getattr(gr, "status",       ""),
                "error"       : getattr(gr, "error",        ""),
                "columns"     : [
                    _col_result_dict(cr)
                    for cr in getattr(gr, "column_results", [])
                ],
            }

        def _pk_col_result_dict(cr) -> dict:
            return {
                "table_name" : getattr(cr, "table_name",  ""),
                "pk_column"  : getattr(cr, "pk_column",   ""),
                "anchor_col" : getattr(cr, "anchor_col",  ""),
                "bk_prefix"  : getattr(cr, "bk_prefix",  ""),
                "null_count" : getattr(cr, "null_count",  0),
                "status"     : getattr(cr, "status",      ""),
                "column_role": getattr(cr, "column_role", "primary_key"),
                "note"       : getattr(cr, "note",        ""),
            }

        def _pk_fill_result_dict(fr) -> dict:
            return {
                "source_table": getattr(fr, "source_table", ""),
                "target_table": getattr(fr, "target_table", ""),
                "status"      : getattr(fr, "status",       ""),
                "error"       : getattr(fr, "error",        ""),
                "columns"     : [
                    _pk_col_result_dict(cr)
                    for cr in getattr(fr, "column_results", [])
                ],
            }

        return {
            "target_catalog"    : result.target_catalog,
            "target_schema"     : result.target_schema,
            "enriched_tables"   : result.enriched_tables,
            "generation_results": [_gen_result_dict(gr) for gr in result.generation_results],
            "pk_fill_results"   : [_pk_fill_result_dict(fr) for fr in result.pk_fill_results],
            "status"            : result.status,
            "error"             : result.error,
        }