"""
MCA/pg.py — PostgreSQL Source Handler
========================================
Imported by new_mca.py.
Exposes: build_pipeline(), build_connection_template(), split_entries()

Splits by schema (unlike Azure SQL which splits by database).
"""

from datetime import datetime, timezone

EXECUTION_ENGINE = "spark"


def build_connection_template() -> dict:
    """Empty PostgreSQL credential template — filled by cred.py (Step 2)."""
    return {
        "host":     "",
        "port":     "",
        "database": "",
        "schema":   "",
        "username": "",
        "password": "",
    }


def build_pipeline(session_id: str, idx: int, source: dict) -> dict:
    return {
        "pipeline_id":         f"{session_id}_pipeline_{idx:03d}",
        "source_type":         "postgresql",
        "ingestion_mode":      source.get("ingestion_mode", "full").lower(),
        "schedule":            source.get("schedule", "one_time").lower(),
        "execution_engine":    EXECUTION_ENGINE,
        "connection_template": build_connection_template(),
        "status":              "awaiting_credentials",
        "created_at":          datetime.now(timezone.utc).isoformat(),
    }


def split_entries(source: dict) -> list:
    """Expand into one entry per schema if LLM returned a list."""
    schema = source.get("schema", "")
    if isinstance(schema, list):
        return [{**source, "schema": s} for s in schema]
    return [source]