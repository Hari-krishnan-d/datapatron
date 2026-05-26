"""
CRED/s3.py — AWS S3 Credential Collector
==========================================
Handles interactive prompting and validation for S3 pipelines.
Imported by cred.py.
"""

import getpass

REQUIRED_FIELDS = ["bucket", "region", "file_format", "aws_access_key_id", "aws_secret_access_key"]

SENSITIVE_FIELDS = {"aws_secret_access_key"}


def collect(pipeline_id: str, conn: dict) -> dict:
    """
    Interactively prompt the user for all empty S3 credential fields.
    Returns the fully populated connection dict.
    """
    print(f"\n  [S3] Connection details for: {pipeline_id}\n")

    if not conn.get("bucket"):
        conn["bucket"] = _ask("Bucket name")

    if not conn.get("region"):
        conn["region"] = _ask("AWS region  (e.g. us-east-1)")

    if not conn.get("prefix"):
        conn["prefix"] = _ask("Key prefix  (leave blank for root)", required=False)

    if not conn.get("file_format"):
        conn["file_format"] = _ask_choice(
            "File format", ["csv", "json", "parquet"]
        )

    if not conn.get("aws_access_key_id"):
        conn["aws_access_key_id"] = _ask("AWS Access Key ID")

    if not conn.get("aws_secret_access_key"):
        conn["aws_secret_access_key"] = _ask_secret("AWS Secret Access Key")

    return conn


def validate(conn: dict, pipeline_id: str) -> list:
    """Return list of missing required field names."""
    return [f for f in REQUIRED_FIELDS if not conn.get(f)]


# =============================================================================
# SHARED INPUT HELPERS (local to this module)
# =============================================================================

def _ask(prompt: str, required: bool = True) -> str:
    while True:
        value = input(f"    {prompt}: ").strip()
        if value:
            return value
        if not required:
            return ""
        print("    ⚠  Required field — please enter a value.")


def _ask_secret(prompt: str) -> str:
    while True:
        value = getpass.getpass(f"    {prompt}: ").strip()
        if value:
            return value
        print("    ⚠  Required field.")


def _ask_choice(prompt: str, options: list) -> str:
    while True:
        value = input(f"    {prompt}  [{' / '.join(options)}]: ").strip().lower()
        if value in options:
            return value
        print(f"    ⚠  Must be one of: {', '.join(options)}")