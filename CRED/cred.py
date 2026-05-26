"""
CRED/cred.py — Credentials Collection Agent
============================================
Step 2 of the orchestration pipeline. Called by orchestrator.py.

Loads credential handlers from CRED/ sibling files using importlib
so they are never confused with the identically-named files in MCA/.

    CRED/s3.py   — AWS S3 prompts + validation
    CRED/sql.py  — Azure SQL prompts + validation
    CRED/pg.py   — PostgreSQL prompts + validation
    CRED/rest.py — REST API prompts + validation
"""

import sys
import os
import importlib.util
import json
import copy
import logging
from datetime import datetime, timezone

logger = logging.getLogger("CredentialsAgent")

# Directory that contains THIS file (always CRED/)
_CRED_DIR = os.path.dirname(os.path.abspath(__file__))


# =============================================================================
# SAFE LOADER — always loads from CRED/ regardless of sys.path order
# =============================================================================

def _load_from_cred(module_name: str):
    """Import a module by absolute path from CRED/ folder."""
    path = os.path.join(_CRED_DIR, f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(f"cred_{module_name}", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_s3   = _load_from_cred("s3")
_sql  = _load_from_cred("sql")
_pg   = _load_from_cred("pg")
_rest = _load_from_cred("rest")


# =============================================================================
# HANDLER REGISTRY
# =============================================================================

HANDLERS = {
    "s3":         _s3,
    "azure_sql":  _sql,
    "postgresql": _pg,
    "rest_api":   _rest,
}

SENSITIVE_FIELDS = (
    _s3.SENSITIVE_FIELDS
    | _sql.SENSITIVE_FIELDS
    | _pg.SENSITIVE_FIELDS
    | _rest.SENSITIVE_FIELDS
)


# =============================================================================
# EXCEPTIONS
# =============================================================================

class CredentialError(Exception):
    """Raised when required credential fields are missing after collection."""

class InvalidPlanError(Exception):
    """Raised when the MCA plan structure is invalid or missing."""


# =============================================================================
# HELPERS
# =============================================================================

def _mask(conn: dict) -> dict:
    """Return a copy with sensitive values masked for safe logging."""
    return {
        k: ("*" * 8 if k in SENSITIVE_FIELDS and v else v)
        for k, v in conn.items()
    }


def _load_plan(source: dict | str) -> dict:
    if isinstance(source, dict):
        logger.info("MCA plan received in-memory.")
        return source
    if isinstance(source, str):
        logger.info("Loading MCA plan from file: %s", source)
        try:
            with open(source, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            raise InvalidPlanError(f"MCA plan file not found: {source}")
        except json.JSONDecodeError as exc:
            raise InvalidPlanError(f"MCA plan is not valid JSON: {exc}")
    raise InvalidPlanError(f"Expected dict or str, got {type(source).__name__}.")


def _validate_plan(plan: dict) -> None:
    if "pipelines" not in plan or not isinstance(plan["pipelines"], list):
        raise InvalidPlanError("MCA plan is missing a valid 'pipelines' list.")
    if not plan["pipelines"]:
        raise InvalidPlanError("MCA plan contains no pipelines.")
    logger.info(
        "Plan validated — session: %s, pipelines: %d",
        plan.get("session_id", "unknown"),
        len(plan["pipelines"]),
    )


# =============================================================================
# SINGLE PIPELINE CREDENTIAL COLLECTOR
# =============================================================================

def _collect_pipeline(pipeline: dict, credentials_map: dict | None) -> dict:
    pipeline_id = pipeline["pipeline_id"]
    source_type = pipeline["source_type"]
    conn        = pipeline.get("connection_template", {})

    print("\n" + "─" * 55)
    print(f"  Pipeline : {pipeline_id}")
    print(f"  Source   : {source_type}")
    print(f"  Mode     : {pipeline.get('ingestion_mode', 'N/A')}")
    print("─" * 55)

    handler = HANDLERS.get(source_type)
    if handler is None:
        raise CredentialError(
            f"No credential handler for source type '{source_type}'."
        )

    # AUTO MODE — inject from credentials_map, no prompts
    if credentials_map is not None:
        overrides = (
            credentials_map.get(pipeline_id)
            or credentials_map.get(source_type)
            or {}
        )
        for field, value in overrides.items():
            conn[field] = value
        logger.info("Credentials injected from config for %s.", pipeline_id)

    # INTERACTIVE MODE — handler prompts for each empty field
    else:
        conn = handler.collect(pipeline_id, conn)

    # Validate via handler
    missing = handler.validate(conn, pipeline_id)
    if missing:
        raise CredentialError(
            f"Pipeline '{pipeline_id}' missing required fields: {missing}"
        )

    logger.info("Credentials OK for %s: %s", pipeline_id, _mask(conn))

    pipeline["connection_template"]      = conn
    pipeline["status"]                   = "credentials_collected"
    pipeline["credentials_collected_at"] = datetime.now(timezone.utc).isoformat()

    return pipeline


# =============================================================================
# CREDENTIALS COLLECTION AGENT
# =============================================================================

class CredentialsCollectionAgent:
    """
    Step 2 — collects credentials for all pipelines in an MCA plan.

    Called by orchestrator.py:
        agent = CredentialsCollectionAgent()
        updated_plan, path = agent.execute(source=mca_plan, output_dir=output_dir)
    """

    def execute(
        self,
        source: dict | str,
        output_file: str | None = None,
        output_dir: str = ".",
        credentials_map: dict | None = None,
    ) -> tuple:
        """
        Args:
            source:          MCA plan dict or JSON file path.
            output_file:     Optional explicit save path (overrides output_dir).
            output_dir:      Directory where credentials.json will be saved.
            credentials_map: Optional pre-filled credentials dict (skips prompts).

        Returns:
            (updated_plan_dict, credentials_filepath)
        """
        logger.info("Credentials Collection Agent started.")

        plan         = _load_plan(source)
        _validate_plan(plan)
        updated_plan = copy.deepcopy(plan)

        total = len(updated_plan["pipelines"])
        print("\n" + "=" * 55)
        print("  CREDENTIAL COLLECTION")
        print(f"  Session   : {updated_plan.get('session_id', 'unknown')}")
        print(f"  Pipelines : {total}")
        print("=" * 55)

        failed = []

        for pipeline in updated_plan["pipelines"]:
            try:
                _collect_pipeline(pipeline, credentials_map)
            except CredentialError as exc:
                logger.error("Credential error — %s: %s", pipeline["pipeline_id"], exc)
                pipeline["status"] = "credentials_failed"
                pipeline["error"]  = str(exc)
                failed.append(pipeline["pipeline_id"])

        updated_plan["stage"]                    = "credentials_collected"
        updated_plan["credentials_collected_at"] = datetime.now(timezone.utc).isoformat()

        if failed:
            updated_plan["credentials_warnings"] = failed
            logger.warning("Failed pipelines: %s", failed)

        out_path = output_file or os.path.join(output_dir, "credentials.json")
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(updated_plan, f, indent=2, ensure_ascii=False)
        logger.info("Credentials saved → %s", out_path)

        self._summary(total, failed)
        return updated_plan, out_path

    @staticmethod
    def _summary(total: int, failed: list) -> None:
        print("\n" + "=" * 55)
        print("  CREDENTIAL COLLECTION COMPLETE")
        print(f"  Total   : {total}")
        print(f"  Success : {total - len(failed)}")
        print(f"  Failed  : {len(failed)}")
        if failed:
            print(f"  Failed IDs : {', '.join(failed)}")
        print("=" * 55 + "\n")


# =============================================================================
# STANDALONE CLI ENTRY POINT
# =============================================================================

def main() -> None:
    import glob

    print("\n=== Bronze Ingestion Platform — Credentials Collection Agent ===\n")

    plan_files = sorted(
        glob.glob("mca_plan_session_*.json"),
        key=os.path.getmtime,
        reverse=True,
    )

    if plan_files:
        file_path = plan_files[0]
        print(f"  Auto-detected MCA plan: {file_path}")
    else:
        file_path = input("No MCA plan found. Enter file path: ").strip()

    try:
        agent = CredentialsCollectionAgent()
        _, path = agent.execute(source=file_path)
        print(f"\nCredentials saved to: {path}")
    except InvalidPlanError as exc:
        logger.error("Invalid plan: %s", exc)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)


if __name__ == "__main__":
    main()