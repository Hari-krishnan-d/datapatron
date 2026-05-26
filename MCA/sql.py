"""
MCA/sql.py — Azure SQL Source Handler
========================================
Imported by new_mca.py.
Exposes: build_pipeline(), build_connection_template(), split_entries()

Splits by database. Fields: server, database, username, password.
"""

from datetime import datetime, timezone

EXECUTION_ENGINE = "spark"


def build_connection_template() -> dict:
    """Empty Azure SQL credential template — filled by cred.py (Step 2)."""
    return {
        "server":   "",
        "database": "",
        "username": "",
        "password": "",
    }


def build_pipeline(session_id: str, idx: int, source: dict) -> dict:
    return {
        "pipeline_id":         f"{session_id}_pipeline_{idx:03d}",
        "source_type":         "azure_sql",
        "ingestion_mode":      source.get("ingestion_mode", "full").lower(),
        "schedule":            source.get("schedule", "one_time").lower(),
        "execution_engine":    EXECUTION_ENGINE,
        "connection_template": build_connection_template(),
        "status":              "awaiting_credentials",
        "created_at":          datetime.now(timezone.utc).isoformat(),
    }


def split_entries(source: dict) -> list:
    """Expand into one entry per database if LLM returned a list."""
    database = source.get("database", "")
    if isinstance(database, list):
        return [{**source, "database": db} for db in database]
    return [source]