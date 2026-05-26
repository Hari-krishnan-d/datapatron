"""
validate_connections.py — Connection Validator
===============================================
Step 3 of the orchestration pipeline. Called by orchestrator.py.

Auth type handling:
  - api_key  → uses header_name as the header key, api_key as value
  - bearer   → Authorization: Bearer <token>
  - basic    → Authorization: Basic <base64>
  - none     → no auth headers

PARTIAL VALIDATION:
  - If some pipelines pass and some fail, user is prompted to either:
      1. Proceed with only the passing pipelines
      2. Terminate and restart from the beginning
"""

import json
import os
import requests
import psycopg2
import pyodbc
import boto3


# ============================================================
# DRIVER DETECTOR
# ============================================================

def get_sql_driver():
    """Detect best installed SQL Server ODBC driver automatically."""
    drivers = pyodbc.drivers()
    preferred_order = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server",
    ]
    for d in preferred_order:
        if d in drivers:
            return d
    raise Exception(
        "No SQL Server ODBC driver installed. "
        "Install Microsoft ODBC Driver 18 for SQL Server."
    )


# ============================================================
# MAIN VALIDATION FUNCTION
# ============================================================

def validate_connections(json_file: str) -> list:
    """Validate all pipeline connections in the given credentials JSON file."""
    with open(json_file, "r") as f:
        config = json.load(f)

    results = []

    for pipeline in config.get("pipelines", []):
        source = pipeline["source_type"]
        conn   = pipeline["connection_template"]

        print("\n" + "=" * 60)
        print(f"Validating: {pipeline['pipeline_id']} ({source})")

        try:
            if source == "azure_sql" or source == "sql_server":
                validate_azure_sql(conn)
            elif source == "postgresql":
                validate_postgres(conn)
            elif source == "s3":
                validate_s3(conn)
            elif source == "rest_api":
                validate_api(conn)
            else:
                raise Exception(f"Unknown source type: {source}")

            print("✅ Connection SUCCESS")
            results.append({
                "pipeline_id": pipeline["pipeline_id"],
                "status":      "success",
            })

        except Exception as e:
            print(f"❌ Connection FAILED: {e}")
            results.append({
                "pipeline_id": pipeline["pipeline_id"],
                "status":      "failed",
                "error":       str(e),
            })

    return results


# ============================================================
# PROGRAMMATIC ENTRY  (used by orchestrator)
# ============================================================

def run_validation(credentials_path: str, output_dir: str = ".") -> tuple:
    """
    Validate all connections and save results.

    Args:
        credentials_path: Path to credentials.json from cred.py.
        output_dir:       Directory to save validation_results.json.

    Returns:
        Tuple of (results_list, credentials_path).
        credentials_path is passed through so the next agent can use it.

    Raises:
        RuntimeError: If ALL pipelines failed.
        SystemExit:   If user chose to terminate after partial failure.
    """
    results = validate_connections(credentials_path)

    os.makedirs(output_dir, exist_ok=True)
    results_path = os.path.join(output_dir, "validation_results.json")
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"\nValidation results saved: {results_path}")

    passed = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]

    # ── All passed — happy path ─────────────────────────────────────────
    if not failed:
        print(f"\n✅ All {len(passed)} pipeline(s) validated successfully.")
        return results, credentials_path

    # ── Any failure — ALWAYS show summary and prompt ────────────────────
    print("\n" + "=" * 60)
    print("  ⚠️  VALIDATION FAILURE DETECTED")
    print("=" * 60)

    if passed:
        print(f"\n  ✅ Passed ({len(passed)}):")
        for r in passed:
            print(f"     • {r['pipeline_id']}")
    else:
        print(f"\n  ✅ Passed (0): none")

    print(f"\n  ❌ Failed ({len(failed)}):")
    for r in failed:
        print(f"     • {r['pipeline_id']}  →  {r.get('error', 'unknown error')}")

    print("\n" + "─" * 60)

    # ── Nothing passed — only option is terminate ───────────────────────
    if not passed:
        print("  ⛔ No pipelines passed — nothing to proceed with.")
        print("  Please fix your credentials and restart from Step 1.\n")
        raise RuntimeError("All pipelines failed validation.")

    # ── Some passed — give user the choice ──────────────────────────────
    print("  What would you like to do?")
    print("  [1] Proceed with only the passed pipelines")
    print("  [2] Terminate and restart from the beginning")
    print("─" * 60)

    while True:
        choice = input("\n  Enter your choice (1 or 2): ").strip()

        if choice == "1":
            print(f"\n  ▶  Proceeding with {len(passed)} pipeline(s)...")

            passed_ids = {r["pipeline_id"] for r in passed}

            with open(credentials_path, "r", encoding="utf-8") as f:
                full_plan = json.load(f)

            full_plan["pipelines"] = [
                p for p in full_plan["pipelines"]
                if p["pipeline_id"] in passed_ids
            ]
            full_plan["total_pipelines"] = len(full_plan["pipelines"])
            full_plan["partial_validation"] = {
                "skipped_pipelines": [r["pipeline_id"] for r in failed],
                "reason":            "validation_failed",
            }

            with open(credentials_path, "w", encoding="utf-8") as f:
                json.dump(full_plan, f, indent=2, ensure_ascii=False)

            print(f"  ✅ credentials.json updated — failed pipelines removed.")
            print(f"  ℹ️  Skipped: {[r['pipeline_id'] for r in failed]}\n")

            return results, credentials_path

        elif choice == "2":
            print(
                "\n  🔄 Terminating. Please fix your credentials and run again from Step 1.\n"
            )
            raise SystemExit(0)

        else:
            print("  ⚠️  Invalid choice — please enter 1 or 2.")


# ============================================================
# AZURE SQL VALIDATION
# ============================================================

def validate_azure_sql(conn: dict) -> None:
    driver = get_sql_driver()
    LEGACY_DRIVERS = {"SQL Server"}

    if driver in LEGACY_DRIVERS:
        connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={conn['server']};"
            f"DATABASE={conn['database']};"
            f"UID={conn['username']};"
            f"PWD={conn['password']};"
            "Connection Timeout=30;"
        )
    else:
        connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={conn['server']};"
            f"DATABASE={conn['database']};"
            f"UID={conn['username']};"
            f"PWD={conn['password']};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

    connection = pyodbc.connect(connection_string)
    connection.close()


# ============================================================
# POSTGRES VALIDATION
# ============================================================

def validate_postgres(conn: dict) -> None:
    connection = psycopg2.connect(
        host=conn["host"],
        port=conn["port"],
        database=conn["database"],
        user=conn["username"],
        password=conn["password"],
    )
    connection.close()


# ============================================================
# S3 VALIDATION
# ============================================================

def validate_s3(conn: dict) -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=conn["aws_access_key_id"],
        aws_secret_access_key=conn["aws_secret_access_key"],
        region_name=conn["region"],
    )
    s3.head_bucket(Bucket=conn["bucket"])


# ============================================================
# REST API VALIDATION
# ============================================================

def validate_api(conn: dict) -> None:
    endpoint    = conn.get("endpoint", "")
    auth_type   = conn.get("auth_type", "none")
    api_key     = conn.get("api_key", "")
    header_name = conn.get("header_name", "")

    headers = {}
    params  = {}

    if auth_type == "api_key":
        key_name = header_name or "api_key"
        params[key_name] = api_key
        if header_name:
            headers[header_name] = api_key

    elif auth_type == "bearer":
        headers["Authorization"] = f"Bearer {api_key}"

    elif auth_type == "basic":
        headers["Authorization"] = f"Basic {api_key}"

    print(f"  -> Endpoint  : {endpoint}")
    print(f"  -> Auth type : {auth_type}")
    print(f"  -> Param key : {header_name or 'api_key'} (query param + header)")

    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=15)
    except requests.exceptions.ConnectionError as e:
        raise Exception(f"Cannot reach endpoint: {e}")
    except requests.exceptions.Timeout:
        raise Exception("Request timed out after 15s")

    if response.status_code >= 400:
        raise Exception(
            f"API returned status {response.status_code}. "
            f"Response: {response.text[:200]}"
        )

    content_type = response.headers.get("Content-Type", "")
    if "application/json" not in content_type:
        raise Exception(
            f"API did not return JSON (Content-Type: {content_type}). "
            f"Preview: {response.text[:200]}"
        )

    print(f"  -> Status    : {response.status_code} OK")


# ============================================================
# RUN  (standalone CLI)
# ============================================================

if __name__ == "__main__":
    file_path = input("Enter credential JSON file: ").strip()
    try:
        run_validation(file_path)
    except RuntimeError as e:
        print(f"\n⛔ {e}")
    except SystemExit:
        pass