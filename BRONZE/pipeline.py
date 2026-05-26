
"""
pipeline.py — Pipeline Strategy Agent
============================================
Step 5 of the orchestration pipeline. Called by orchestrator.py.

  Input : mca_plan JSON  +  metadata JSON
  Output: strategy.json

Key rules:
  - REST API  → always incremental, engine=python
                → injects watermark_table spec into strategy.json
                  so Schema Agent (Step 6) and Table Creation Agent (Step 7)
                  physically create _rest_api_watermark alongside destination tables
  - S3 / Azure SQL / PostgreSQL → full or incremental, engine=spark
  - Pipelines matched to metadata sources BY INDEX
"""

import json
import os
from typing import Dict, Any, List, Optional

WATERMARK_KEYWORDS = [
    "timestamp", "updated_at", "modified_at", "last_modified",
    "created_at", "insert_date", "last_update", "event_time",
    "record_time", "time", "date",
]

# Reserved table name — recognised by Schema Agent and Table Creation Agent
WATERMARK_TABLE_NAME = "_rest_api_watermark"


class PipelineStrategyAgent:

    def __init__(self, mca_file: str, metadata_file: str, output_file: str = "strategy.json"):
        self.mca_file      = mca_file
        self.metadata_file = metadata_file
        self.output_file   = output_file

    # ------------------------------------------------------------------
    # ENTRY POINT
    # ------------------------------------------------------------------
    def execute(self):
        mca_config = self._load_json(self.mca_file)
        metadata   = self._load_json(self.metadata_file)
        sources    = metadata.get("sources", [])

        strategy_output = {
            "session_id": mca_config.get("session_id"),
            "pipelines":  [],
        }

        has_rest_api = False

        for idx, pipeline in enumerate(mca_config.get("pipelines", [])):
            source_meta = sources[idx] if idx < len(sources) else {}
            strategy_output["pipelines"].append(
                self._build_pipeline_strategy(pipeline, source_meta)
            )
            if pipeline["source_type"] == "rest_api":
                has_rest_api = True

        # ── Inject watermark table spec if any REST API pipeline exists ──
        # Schema Agent reads this to generate DDL.
        # Table Creation Agent reads this to CREATE the table in Databricks
        # and seed one row per pipeline BEFORE the executor ever runs.
        if has_rest_api:
            strategy_output["watermark_table"] = self._watermark_table_spec()
            print(f"  ✓ Watermark table spec injected → {WATERMARK_TABLE_NAME}")

        self._save_json(strategy_output)
        print(f"  ✓ Strategy written → {self.output_file}")

    # ------------------------------------------------------------------
    # WATERMARK TABLE SPEC  (fixed schema — never inferred from sample data)
    # ------------------------------------------------------------------
    def _watermark_table_spec(self) -> dict:
        """
        Fixed-schema spec for _rest_api_watermark.

        Columns:
            api_id               STRING  PK — one row per API endpoint
            table_name           STRING  — destination table this row tracks
            last_processed_time  STRING  — last successfully fetched watermark timestamp
            last_updated_date    STRING  — when this row was last written
            pipeline_id          STRING  — owning pipeline_id
        """
        return {
            "table_name":        WATERMARK_TABLE_NAME,
            "is_watermark_table": True,
            "write_strategy":    "merge",
            "columns": [
                {"name": "api_id",              "type": "STRING"},
                {"name": "table_name",          "type": "STRING"},
                {"name": "last_processed_time", "type": "STRING"},
                {"name": "last_updated_date",   "type": "STRING"},
                {"name": "pipeline_id",         "type": "STRING"},
            ],
        }

    # ------------------------------------------------------------------
    # PIPELINE STRATEGY BUILDER
    # ------------------------------------------------------------------
    def _build_pipeline_strategy(self, pipeline: dict, source_meta: dict) -> dict:
        source_type    = pipeline["source_type"]
        is_rest_api    = source_type == "rest_api"
        ingestion_mode = "incremental" if is_rest_api else pipeline.get("ingestion_mode", "full")
        engine         = "python"      if is_rest_api else "spark"

        return {
            "pipeline_id":                 pipeline["pipeline_id"],
            "source_type":                 source_type,
            "engine":                      engine,
            "ingestion_mode":              ingestion_mode,
            "schedule":                    pipeline.get("schedule", "one_time"),
            "requires_watermark_tracking": is_rest_api,
            "tables": self._build_tables_strategy(
                source_type, ingestion_mode, pipeline, source_meta
            ),
        }

    # ------------------------------------------------------------------
    # TABLE STRATEGY DISPATCHER
    # ------------------------------------------------------------------
    def _build_tables_strategy(self, source_type, ingestion_mode, pipeline, source_meta):
        if source_type == "rest_api":
            return self._build_rest_api_tables(pipeline, source_meta)
        return self._build_db_tables(source_meta, ingestion_mode)

    # ------------------------------------------------------------------
    # REST API TABLE ENTRIES
    # ------------------------------------------------------------------
    def _build_rest_api_tables(self, pipeline: dict, source_meta: dict) -> list:
        sample_data     = source_meta.get("sample_data", {})
        pipeline_id     = pipeline["pipeline_id"]
        wm_block        = pipeline.get("watermark", {})

        api_id            = wm_block.get("api_id")            or pipeline_id
        lag_minutes       = wm_block.get("lag_minutes",       5)
        api_buffer_days   = wm_block.get("api_buffer_days",   1)
        precision_filter  = wm_block.get("precision_filter",  True)
        default_watermark = wm_block.get("default_watermark", "1900-01-01 00:00:00")

        watermark_col = source_meta.get("detected_watermark_column")
        tables        = []

        if isinstance(sample_data, dict):
            for table_name, sample_rows in sample_data.items():
                if not isinstance(sample_rows, list) or not sample_rows:
                    continue
                if not isinstance(sample_rows[0], dict):
                    continue
                if not watermark_col:
                    watermark_col = self._detect_wm_col(sample_rows)
                tables.append(self._rest_table_entry(
                    table_name, api_id, watermark_col,
                    lag_minutes, api_buffer_days, precision_filter,
                    default_watermark, len(sample_rows),
                ))

        if not tables:
            tables.append(self._rest_table_entry(
                "api_result", api_id, watermark_col,
                lag_minutes, api_buffer_days, precision_filter,
                default_watermark, 0,
            ))

        return tables

    def _rest_table_entry(
        self, table_name, api_id, watermark_col,
        lag_minutes, api_buffer_days, precision_filter,
        default_watermark, row_count,
    ) -> dict:
        return {
            "table_name":        table_name,
            "write_strategy":    "merge",
            "load_type":         "incremental",
            "watermark_column":  watermark_col,
            "api_id":            api_id,
            "api_id_column":     "api_id",
            "partition_by":      None,
            "parallelism":       self._parallelism(row_count),
            "lag_minutes":       lag_minutes,
            "api_buffer_days":   api_buffer_days,
            "precision_filter":  precision_filter,
            "default_watermark": default_watermark,
        }

    # ------------------------------------------------------------------
    # DB / S3 TABLE ENTRIES
    # ------------------------------------------------------------------
    def _build_db_tables(self, source_meta: dict, ingestion_mode: str) -> list:
        tables      = []
        sample_data = source_meta.get("sample_data", {})
        if not isinstance(sample_data, dict):
            return tables

        for table_name, sample_rows in sample_data.items():
            if not isinstance(sample_rows, list) or not sample_rows:
                continue
            if not isinstance(sample_rows[0], dict):
                continue

            wm_col         = self._detect_wm_col(sample_rows)
            write_strategy = "append" if ingestion_mode == "full" else ("merge" if wm_col else "append")

            tables.append({
                "table_name":       table_name,
                "write_strategy":   write_strategy,
                "watermark_column": wm_col,
                "partition_by":     self._partition(sample_rows, len(sample_rows)),
                "parallelism":      self._parallelism(len(sample_rows)),
            })

        return tables

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------
    def _detect_wm_col(self, sample_rows) -> Optional[str]:
        if not sample_rows or not isinstance(sample_rows[0], dict):
            return None
        for col in sample_rows[0].keys():
            if any(kw in col.lower() for kw in WATERMARK_KEYWORDS):
                return col
        return None

    def _partition(self, sample_rows, row_count) -> Optional[List[str]]:
        if not sample_rows or not isinstance(sample_rows[0], dict):
            return None
        for col in sample_rows[0].keys():
            if "date" in col.lower() and row_count > 1000:
                return [col]
        return None

    def _parallelism(self, row_count: int) -> int:
        if row_count < 1000:   return 2
        if row_count < 10000:  return 4
        if row_count < 100000: return 8
        return 16

    def _load_json(self, path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_json(self, data):
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)


# ======================================================================
# PROGRAMMATIC ENTRY
# ======================================================================
def run_pipeline_strategy(mca_path: str, metadata_path: str, output_dir: str = ".") -> tuple:
    os.makedirs(output_dir, exist_ok=True)
    strategy_path = os.path.join(output_dir, "strategy.json")
    PipelineStrategyAgent(mca_path, metadata_path, output_file=strategy_path).execute()
    with open(strategy_path, "r", encoding="utf-8") as f:
        strategy = json.load(f)
    return strategy, strategy_path


if __name__ == "__main__":
    mca_path      = input("MCA JSON path      : ").strip()
    metadata_path = input("Metadata JSON path : ").strip()
    output_dir    = input("Output dir [.]     : ").strip() or "."
    strategy, path = run_pipeline_strategy(mca_path, metadata_path, output_dir)
    print(f"\nStrategy saved → {path}")
    print(json.dumps(strategy, indent=2))