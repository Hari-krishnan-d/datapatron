
"""
MCA/rest_handler.py — REST API Source Handler
Imported by new_mca.py.
Exposes: build_pipeline(), build_connection_template(), split_entries()

Rules for REST API:
  - ingestion_mode is ALWAYS "incremental"
  - Every pipeline carries watermark metadata so the executor knows to:
      1. Create/read a watermark table keyed on api_id
      2. Pull only data after last watermark_value using a windowed approach:
           - Broad API window  : (watermark - buffer_days)  →  (now + buffer_days)
           - Precise filter    : watermark < row_ts <= (now - lag_minutes)
      3. Update watermark_value ONLY after successful write
"""

from datetime import datetime, timezone

EXECUTION_ENGINE = "python"


def build_connection_template() -> dict:
    return {
        "endpoint":    "",
        "auth_type":   "",   # none | api_key | bearer | basic
        "api_key":     "",
        "header_name": "",
    }


def build_pipeline(session_id: str, idx: int, source: dict) -> dict:
    """
    Build a REST API pipeline entry.

    Key differences from other source handlers:
    - ingestion_mode is ALWAYS "incremental" regardless of what the user said.
    - watermark block is always present with window configuration:

        Watermark table schema (auto-created by executor on first run):
          api_id          STRING  — unique key, one row per API endpoint
          watermark_value STRING  — last successfully fetched timestamp
          pipeline_id     STRING  — which pipeline owns this row
          updated_at      STRING  — when this row was last updated

        On each scheduled run:
          1. Read watermark_value for this api_id
          2. Compute broad API window:
               api_from = watermark_value - api_buffer_days
               api_to   = now            + api_buffer_days
          3. Call API with api_from / api_to as query params
          4. Apply precise filter:
               keep rows where: watermark_value < row_ts <= (now - lag_minutes)
          5. Write filtered rows to destination (MERGE)
          6. Update watermark_value = (now - lag_minutes)  ← only on success
    """

    ingestion_mode = "incremental"
    pipeline_id    = f"{session_id}_pipeline_{idx:03d}"
    api_id         = source.get("api_id") or pipeline_id

    return {
        "pipeline_id":      pipeline_id,
        "source_type":      "rest_api",
        "ingestion_mode":   ingestion_mode,
        "schedule":         source.get("schedule", "hourly").lower(),
        "execution_engine": EXECUTION_ENGINE,

        "connection_template": build_connection_template(),

        # ── Watermark block ─────────────────────────────────────────────
        # Tells the executor agent how to track state and fetch windows.
        #
        # lag_minutes (int):
        #   Trailing edge buffer. The executor fetches up to (now - lag_minutes)
        #   so that records still being written to the API are not missed.
        #   Example: lag=5 at 06:00 → fetch up to 05:55.
        #
        # api_buffer_days (int):
        #   Broad window buffer added around the precise window when calling
        #   the API. Accounts for APIs that require a wider date range query.
        #   Example: buffer=1 → call API from (watermark-1day) to (now+1day),
        #   then filter precisely in-memory.
        #
        # precision_filter (bool):
        #   If True, apply the precise timestamp filter AFTER the broad API
        #   call to keep only the exact window rows.
        "watermark": {
            "api_id":               api_id,
            "watermark_column":     "watermark_value",
            "api_id_column":        "api_id",
            "requires_watermark_tracking": True,
            "default_watermark":    "1900-01-01 00:00:00",   # seed for first run
            "lag_minutes":          5,                         # now - 5min trailing edge
            "api_buffer_days":      1,                         # broad window ± 1 day
            "precision_filter":     True,                      # filter precisely after fetch
        },

        "status":     "awaiting_credentials",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def split_entries(source: dict) -> list:
    return [source]