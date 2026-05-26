"""
SILVER/validation/validate.py — Silver Validation Agent
=========================================================
Validates Databricks connections for all silver pipelines.

Checks:
  1. Workspace reachable  (GET /api/2.0/sql/warehouses)
  2. Warehouse exists     (warehouse_id found in list)
  3. Bronze catalog/schema/table accessible
     (runs a COUNT(*) query against the bronze table)

Called by orchestrator.py after silver credential collection.

Input : silver_credentials.json
Output: silver_validation_results.json
"""

import json
import os
import logging
import requests

logger = logging.getLogger("Silver.Validation")


# =============================================================================
# LOW-LEVEL HELPERS
# =============================================================================

def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _check_workspace(workspace_url: str, token: str) -> None:
    """Confirm the workspace URL is reachable and token is valid."""
    url  = f"{workspace_url}/api/2.0/sql/warehouses"
    resp = requests.get(url, headers=_auth_headers(token), timeout=15)
    if resp.status_code != 200:
        raise Exception(
            f"Workspace unreachable or token invalid. "
            f"Status {resp.status_code}: {resp.text[:200]}"
        )
    print(f"  → Workspace  : {workspace_url}  ✓")


def _check_warehouse(workspace_url: str, token: str, warehouse_id: str) -> None:
    """Confirm the warehouse_id exists in this workspace."""
    url       = f"{workspace_url}/api/2.0/sql/warehouses"
    resp      = requests.get(url, headers=_auth_headers(token), timeout=15)
    warehouses = resp.json().get("warehouses", [])
    ids        = [w["id"] for w in warehouses]

    if warehouse_id not in ids:
        raise Exception(
            f"Warehouse ID '{warehouse_id}' not found in workspace.\n"
            f"Available: {ids}"
        )
    print(f"  → Warehouse  : {warehouse_id}  ✓")


def _run_sql(workspace_url: str, token: str, warehouse_id: str, sql: str) -> dict:
    """
    Submit a SQL statement and poll until complete.
    Returns the full response JSON.
    Raises Exception on failure.
    """
    import time

    submit_url = f"{workspace_url}/api/2.0/sql/statements"
    payload    = {
        "statement":   sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": "30s",
    }
    resp = requests.post(
        submit_url,
        headers={**_auth_headers(token), "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    if resp.status_code != 200:
        raise Exception(
            f"SQL submission failed. Status {resp.status_code}: {resp.text[:300]}"
        )

    result = resp.json()
    state  = result.get("status", {}).get("state", "")

    # Poll if still running
    if state not in ("SUCCEEDED", "FAILED", "CANCELED"):
        stmt_id    = result["statement_id"]
        status_url = f"{workspace_url}/api/2.0/sql/statements/{stmt_id}"
        start      = time.time()
        while True:
            sr    = requests.get(status_url, headers=_auth_headers(token), timeout=15)
            state = sr.json().get("status", {}).get("state", "")
            if state == "SUCCEEDED":
                return sr.json()
            if state in ("FAILED", "CANCELED"):
                raise Exception(
                    f"SQL failed: {sr.json().get('status', {}).get('error', {})}"
                )
            if time.time() - start > 60:
                raise Exception("SQL timed out after 60s")
            time.sleep(2)

    if state != "SUCCEEDED":
        raise Exception(
            f"SQL failed: {result.get('status', {}).get('error', {})}"
        )
    return result


def _check_bronze_table(
    workspace_url: str,
    token:         str,
    warehouse_id:  str,
    catalog:       str,
    schema:        str,
    table:         str,
) -> None:
    """
    Validate the bronze source is accessible.

    If table is given   → COUNT(*) on that specific table.
    If table is blank   → list tables in the schema instead.
    """
    if table:
        sql = f"SELECT COUNT(*) AS row_count FROM `{catalog}`.`{schema}`.`{table}`"
        result = _run_sql(workspace_url, token, warehouse_id, sql)
        # Extract row count from result
        try:
            row_count = result["result"]["data_array"][0][0]
            print(f"  → Bronze     : {catalog}.{schema}.{table}  ({row_count} rows)  ✓")
        except Exception:
            print(f"  → Bronze     : {catalog}.{schema}.{table}  ✓")
    else:
        # No specific table — just verify the schema exists
        sql    = f"SHOW TABLES IN `{catalog}`.`{schema}`"
        result = _run_sql(workspace_url, token, warehouse_id, sql)
        try:
            tables = result.get("result", {}).get("data_array", [])
            print(
                f"  → Bronze     : {catalog}.{schema}  "
                f"({len(tables)} table(s) found)  ✓"
            )
        except Exception:
            print(f"  → Bronze     : {catalog}.{schema}  ✓")


# =============================================================================
# VALIDATE ONE PIPELINE
# =============================================================================

def _validate_pipeline(pipeline: dict) -> dict:
    """
    Run all checks for one silver pipeline.
    Returns {"pipeline_id": ..., "status": "success"|"failed", "error": ...}
    """
    pipeline_id = pipeline["pipeline_id"]
    conn        = pipeline.get("connection_template", {})

    workspace_url = conn.get("workspace_url", "").rstrip("/")
    warehouse_id  = conn.get("warehouse_id", "")
    token         = conn.get("token", "")
    catalog       = conn.get("catalog", "")
    schema        = conn.get("schema", "")
    table         = conn.get("table", "")

    print(f"\n{'=' * 60}")
    print(f"Validating silver pipeline : {pipeline_id}")
    print(f"{'=' * 60}")

    try:
        _check_workspace(workspace_url, token)
        _check_warehouse(workspace_url, token, warehouse_id)
        _check_bronze_table(workspace_url, token, warehouse_id, catalog, schema, table)

        print(f"✅ Silver connection SUCCESS")
        return {"pipeline_id": pipeline_id, "status": "success"}

    except Exception as exc:
        print(f"❌ Silver connection FAILED: {exc}")
        return {"pipeline_id": pipeline_id, "status": "failed", "error": str(exc)}


# =============================================================================
# SILVER VALIDATION AGENT
# =============================================================================

def run_silver_validation(
    credentials_path: str,
    output_dir:       str = ".",
) -> tuple:
    """
    Validate all silver pipeline connections.

    Args:
        credentials_path : Path to silver_credentials.json.
        output_dir       : Where to save silver_validation_results.json.

    Returns:
        (results_list, credentials_path)

    Raises:
        RuntimeError  — if ALL pipelines failed.
        SystemExit    — if user chose to terminate after partial failure.
    """
    with open(credentials_path, "r", encoding="utf-8") as f:
        plan = json.load(f)

    results = [
        _validate_pipeline(p)
        for p in plan.get("pipelines", [])
    ]

    # ── Save results ──────────────────────────────────────────────────────────
    os.makedirs(output_dir, exist_ok=True)
    results_path = os.path.join(output_dir, "silver_validation_results.json")
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nSilver validation results saved: {results_path}")

    passed = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]

    # ── All passed ────────────────────────────────────────────────────────────
    if not failed:
        print(f"\n✅ All {len(passed)} silver pipeline(s) validated successfully.")
        return results, credentials_path

    # ── Some/all failed ───────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  ⚠️  SILVER VALIDATION FAILURE")
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

    if not passed:
        print("  ⛔ No silver pipelines passed — nothing to proceed with.")
        print("  Please fix your Databricks credentials and restart.\n")
        raise RuntimeError("All silver pipelines failed validation.")

    # Partial failure — give user the choice
    print("  What would you like to do?")
    print("  [1] Proceed with only the passed silver pipelines")
    print("  [2] Terminate and restart from the beginning")
    print("─" * 60)

    while True:
        choice = input("\n  Enter your choice (1 or 2): ").strip()

        if choice == "1":
            passed_ids = {r["pipeline_id"] for r in passed}
            plan["pipelines"] = [
                p for p in plan["pipelines"]
                if p["pipeline_id"] in passed_ids
            ]
            with open(credentials_path, "w", encoding="utf-8") as f:
                json.dump(plan, f, indent=2, ensure_ascii=False)
            print(f"\n  ✅ Proceeding with {len(passed)} silver pipeline(s).\n")
            return results, credentials_path

        elif choice == "2":
            print("\n  🔄 Terminating. Fix credentials and restart.\n")
            raise SystemExit(0)

        else:
            print("  ⚠  Invalid choice — enter 1 or 2.")