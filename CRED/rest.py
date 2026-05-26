"""
CRED/rest.py — REST API Credential Collector
==============================================
Handles interactive prompting and validation for REST API pipelines.
Imported by cred.py.
"""

REQUIRED_FIELDS  = ["endpoint", "auth_type"]
SENSITIVE_FIELDS = {"api_key"}

AUTH_TYPES = ["none", "api_key", "bearer", "basic"]

_HEADER_HINT = "e.g. X-API-Key, Authorization, X-Auth-Token, Api-Key"


def collect(pipeline_id: str, conn: dict) -> dict:
    """
    Interactively prompt the user for all empty REST API credential fields.
    Returns the fully populated connection dict.
    """
    print(f"\n  [REST API] Connection details for: {pipeline_id}\n")

    if not conn.get("endpoint"):
        conn["endpoint"] = _ask("Endpoint URL  (e.g. https://api.example.com/v1/data)")

    if not conn.get("auth_type"):
        conn["auth_type"] = _ask_choice("Auth type", AUTH_TYPES)

    if conn["auth_type"] == "api_key":
        if not conn.get("header_name"):
            conn["header_name"] = _ask(
                f"Header name for the API key  ({_HEADER_HINT})"
            )
        if not conn.get("api_key"):
            # now visible while typing
            conn["api_key"] = _ask_secret("API key value")

    elif conn["auth_type"] == "bearer":
        if not conn.get("api_key"):
            conn["api_key"] = _ask_secret("Bearer token")
        conn["header_name"] = "Authorization"

    elif conn["auth_type"] == "basic":
        if not conn.get("api_key"):
            conn["api_key"] = _ask_secret("Base64 credentials (user:password)")
        conn["header_name"] = "Authorization"

    return conn


def validate(conn: dict, pipeline_id: str) -> list:
    """Return list of missing required field names."""
    missing = [f for f in REQUIRED_FIELDS if not conn.get(f)]

    auth = conn.get("auth_type", "none")
    if auth != "none":
        if not conn.get("api_key"):
            missing.append("api_key")
        if auth == "api_key" and not conn.get("header_name"):
            missing.append("header_name")

    return missing


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
    """
    Visible input version (NOT masked).
    You requested API key to be visible while typing.
    """
    while True:
        value = input(f"    {prompt}: ").strip()
        if value:
            return value
        print("    ⚠  Required field.")


def _ask_choice(prompt: str, options: list) -> str:
    while True:
        value = input(f"    {prompt}  [{' / '.join(options)}]: ").strip().lower()
        if value in options:
            return value
        print(f"    ⚠  Must be one of: {', '.join(options)}")
