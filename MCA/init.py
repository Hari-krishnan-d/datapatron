"""
sources/__init__.py — Source Registry
======================================
Central dispatcher: maps source_type strings to their handler modules.
Add a new source type here when introducing a new handler.
"""

import sources.aws.s3 as _aws
import sources.azure.sql as _azure
import sources.api.rest_handler as _api
import sources.postgresql.pg as _pg


# Registry: source_type → handler module
# Each module must expose: build_pipeline(), build_connection_template(), split_entries()
REGISTRY: dict = {
    "s3":         _aws,
    "azure_sql":  _azure,
    "rest_api":   _api,
    "postgresql": _pg,
}

SUPPORTED_SOURCES = set(REGISTRY.keys())


def get_handler(source_type: str):
    """
    Return the handler module for the given source_type.
    Raises KeyError with a friendly message if unknown.
    """
    handler = REGISTRY.get(source_type.lower())
    if handler is None:
        raise KeyError(
            f"No handler registered for source_type '{source_type}'. "
            f"Supported: {sorted(SUPPORTED_SOURCES)}"
        )
    return handler