"""
CRED/pg.py — PostgreSQL Credential Collector
==============================================
Handles interactive prompting and validation for PostgreSQL pipelines.
Imported by cred.py.
"""

import getpass

REQUIRED_FIELDS  = ["host", "port", "database", "schema", "username", "password"]
SENSITIVE_FIELDS = {"password"}


def collect(pipeline_id: str, conn: dict) -> dict:
    """
    Interactively prompt the user for all empty PostgreSQL credential fields.
    Returns the fully populated connection dict.
    """
    print(f"\n  [PostgreSQL] Connection details for: {pipeline_id}\n")

    if not conn.get("host"):
        conn["host"] = _ask("Host  (e.g. db.example.com)")

    if not conn.get("port"):
        conn["port"] = _ask("Port", default="5432")

    if not conn.get("database"):
        conn["database"] = _ask("Database name")

    if not conn.get("schema"):
        conn["schema"] = _ask("Schema name")

    if not conn.get("username"):
        conn["username"] = _ask("Username")

    if not conn.get("password"):
        conn["password"] = _ask_secret("Password")

    return conn


def validate(conn: dict, pipeline_id: str) -> list:
    """Return list of missing required field names."""
    return [f for f in REQUIRED_FIELDS if not conn.get(f)]


# =============================================================================
# INPUT HELPERS
# =============================================================================

def _ask(prompt: str, default: str = "") -> str:
    while True:
        display = f"    {prompt}" + (f" (default: {default})" if default else "") + ": "
        value   = input(display).strip()
        if value:
            return value
        if default:
            return default
        print("    ⚠  Required field — please enter a value.")


def _ask_secret(prompt: str) -> str:
    while True:
        value = getpass.getpass(f"    {prompt}: ").strip()
        if value:
            return value
        print("    ⚠  Required field.")