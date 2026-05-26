"""
MCA/s3.py — AWS S3 Source Handler
====================================
Imported by new_mca.py.
Exposes: build_pipeline(), build_connection_template(), split_entries()
"""

from datetime import datetime, timezone

EXECUTION_ENGINE = "spark"


def build_connection_template() -> dict:
    """Empty S3 credential template — filled by cred.py (Step 2)."""
    return {
        "bucket":                "",
        "region":                "",
        "prefix":                "",
        "file_format":           "",   # csv | json | parquet
        "aws_access_key_id":     "",
        "aws_secret_access_key": "",
    }


def build_pipeline(session_id: str, idx: int, source: dict) -> dict:
    return {
        "pipeline_id":         f"{session_id}_pipeline_{idx:03d}",
        "source_type":         "s3",
        "ingestion_mode":      source.get("ingestion_mode", "full").lower(),
        "schedule":            source.get("schedule", "one_time").lower(),
        "execution_engine":    EXECUTION_ENGINE,
        "connection_template": build_connection_template(),
        "status":              "awaiting_credentials",
        "created_at":          datetime.now(timezone.utc).isoformat(),
    }


def split_entries(source: dict) -> list:
    """Expand into one entry per bucket if LLM returned a list."""
    bucket_hint = source.get("bucket_hint", "")
    if isinstance(bucket_hint, list):
        return [{**source, "bucket_hint": b} for b in bucket_hint]
    return [source]