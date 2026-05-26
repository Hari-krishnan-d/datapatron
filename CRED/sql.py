"""
CRED/sql.py — Azure SQL Credential Collector
==============================================
Handles interactive prompting and validation for Azure SQL pipelines.
Imported by cred.py.
"""

import getpass

REQUIRED_FIELDS  = ["server", "database", "username", "password"]
SENSITIVE_FIELDS = {"password"}


def collect(pipeline_id: str, conn: dict) -> dict:
    """
    Interactively prompt the user for all empty Azure SQL credential fields.
    Returns the fully populated connection dict.
    """
    print(f"\n  [Azure SQL] Connection details for: {pipeline_id}\n")

    if not conn.get("server"):
        conn["server"] = _ask("Server  (e.g. myserver.database.windows.net)")

    if not conn.get("database"):
        conn["database"] = _ask("Database name")

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

def _ask(prompt: str) -> str:
    while True:
        value = input(f"    {prompt}: ").strip()
        if value:
            return value
        print("    ⚠  Required field — please enter a value.")


def _ask_secret(prompt: str) -> str:
    while True:
        value = getpass.getpass(f"    {prompt}: ").strip()
        if value:
            return value
        print("    ⚠  Required field.")