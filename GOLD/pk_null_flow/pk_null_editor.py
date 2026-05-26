"""
pk_null_editor.py — NLP Chat Editor for PKNullSuggestion list
=============================================================

Applies plain-English edits to a list of PKNullSuggestion objects.

Supported intents
-----------------
  anchor    : "change customer_id anchor to first_name and last_name"
              "set sale_id anchor column as car_id and dealer_id"
  prefix    : "set customer_id prefix to cus"
  role      : "mark dealer_id as primary_key"
  remove    : "remove car_id suggestion" / "skip car_id"

BUG FIXED (v2)
--------------
The previous implementation rebuilt PKNullSuggestion from scratch when
applying edits, silently dropping all unspecified fields (table_name,
null_count, total_rows, etc.) and replacing them with dataclass defaults
(empty strings, 0, "primary_key").

Fix: edits are now applied IN-PLACE on the existing suggestion object.
Only the fields explicitly mentioned in the edit are mutated; everything
else is preserved untouched.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

# ---------------------------------------------------------------------------
# Lazy import — PKNullSuggestion lives in the analyzer module.
# We import it here only for type hints; the caller passes real instances.
# ---------------------------------------------------------------------------
try:
    from GOLD.pk_null_flow.pk_null_analyzer import PKNullSuggestion
except ImportError:  # graceful fallback for standalone testing
    PKNullSuggestion = object  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# Intent parsing helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalise(text: str) -> str:
    """Lower-case, collapse whitespace, strip punctuation noise."""
    text = text.lower().strip()
    text = re.sub(r"['\"]", "", text)          # strip quotes
    text = re.sub(r"\s+", " ", text)           # collapse spaces
    return text


def _find_suggestion(
    suggestions : list,
    pk_col      : str,
) -> Optional[object]:
    """
    Return the first suggestion whose pk_column matches *pk_col*.
    Matching is case-insensitive and tolerates partial suffix matches
    (e.g. "customer_id" matches "customer_id").
    """
    pk_col_lower = pk_col.lower().strip()
    for s in suggestions:
        if s.pk_column.lower() == pk_col_lower:
            return s
    # Partial match fallback
    for s in suggestions:
        if pk_col_lower in s.pk_column.lower() or s.pk_column.lower() in pk_col_lower:
            return s
    return None


_COL_SPLIT_RE = re.compile(r"\s+and\s+|\s*,\s*|\s+&\s+")

def _parse_columns(raw: str) -> list[str]:
    """
    Split a string like "first_name and last_name" or "car_id, dealer_id"
    into a list of column name tokens.
    """
    parts = _COL_SPLIT_RE.split(raw.strip())
    return [p.strip() for p in parts if p.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# Intent detectors
# ─────────────────────────────────────────────────────────────────────────────

# Patterns that signal an ANCHOR change.
# Group 1 → pk_column token(s),  Group 2 → new anchor column(s)
_ANCHOR_PATTERNS = [
    # "change customer_id anchor (column) (to|as) first_name and last_name"
    re.compile(
        r"change\s+(\S+)\s+anchor(?:\s+col(?:umn)?)?"
        r"(?:\s+(?:to|as|into))?\s+(.+)",
        re.IGNORECASE,
    ),
    # "set customer_id anchor (column) (to|as) first_name and last_name"
    re.compile(
        r"set\s+(\S+)\s+anchor(?:\s+col(?:umn)?)?"
        r"(?:\s+(?:to|as|into))?\s+(.+)",
        re.IGNORECASE,
    ),
    # "update customer_id anchor to first_name and last_name"
    re.compile(
        r"update\s+(\S+)\s+anchor(?:\s+col(?:umn)?)?"
        r"(?:\s+(?:to|as|into))?\s+(.+)",
        re.IGNORECASE,
    ),
    # "customer_id anchor (column) (should be|is) first_name and last_name"
    re.compile(
        r"(\S+)\s+anchor(?:\s+col(?:umn)?)?(?:\s+(?:should\s+be|is|=))?\s+(.+)",
        re.IGNORECASE,
    ),
]

_PREFIX_PATTERNS = [
    re.compile(
        r"(?:change|set|update)\s+(\S+)\s+(?:bk_?)?prefix(?:\s+(?:to|as))?\s+(\S+)",
        re.IGNORECASE,
    ),
]

_ROLE_PATTERNS = [
    re.compile(
        r"(?:mark|set|change)\s+(\S+)\s+(?:as|to)\s+(primary_key|foreign_key|pk|fk)",
        re.IGNORECASE,
    ),
]

_REMOVE_PATTERNS = [
    re.compile(
        r"(?:remove|delete|drop|skip)\s+(?:the\s+)?(\S+)\s+suggestion",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:remove|delete|drop|skip)\s+(\S+)",
        re.IGNORECASE,
    ),
]


def _normalise_role(raw: str) -> str:
    raw = raw.lower().strip()
    if raw in ("pk", "primary_key", "primary"):
        return "primary_key"
    if raw in ("fk", "foreign_key", "foreign"):
        return "foreign_key"
    return raw


# ─────────────────────────────────────────────────────────────────────────────
# Core apply function
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EditResult:
    success      : bool
    message      : str
    removed_cols : list[str]   # pk_columns that were removed from the list


def apply_nlp_edit(
    edit_text   : str,
    suggestions : list,        # list[PKNullSuggestion] — mutated IN-PLACE
) -> EditResult:
    """
    Parse *edit_text* and apply the described mutation to *suggestions*.

    Mutations are applied IN-PLACE on the matching PKNullSuggestion object
    so that every field NOT mentioned in the edit is preserved exactly as-is.

    Returns an EditResult describing what happened.
    """
    norm = _normalise(edit_text)
    removed: list[str] = []

    # ── REMOVE / SKIP ─────────────────────────────────────────────────────
    for pat in _REMOVE_PATTERNS:
        m = pat.search(norm)
        if m:
            pk_token = m.group(1)
            suggestion = _find_suggestion(suggestions, pk_token)
            if suggestion is None:
                return EditResult(
                    success      = False,
                    message      = f"Could not find a suggestion matching '{pk_token}'.",
                    removed_cols = [],
                )
            suggestions.remove(suggestion)
            removed.append(suggestion.pk_column)
            return EditResult(
                success      = True,
                message      = f"Removed suggestion for '{suggestion.pk_column}'.",
                removed_cols = removed,
            )

    # ── ANCHOR CHANGE ─────────────────────────────────────────────────────
    for pat in _ANCHOR_PATTERNS:
        m = pat.search(norm)
        if m:
            pk_token   = m.group(1)
            cols_raw   = m.group(2)
            new_anchors = _parse_columns(cols_raw)

            if not new_anchors:
                return EditResult(
                    success      = False,
                    message      = "Could not parse any column name(s) from the edit.",
                    removed_cols = [],
                )

            suggestion = _find_suggestion(suggestions, pk_token)
            if suggestion is None:
                return EditResult(
                    success      = False,
                    message      = f"Could not find a suggestion matching '{pk_token}'.",
                    removed_cols = [],
                )

            # ── IN-PLACE update — only anchor fields change ────────────────
            suggestion.anchor_cols = new_anchors
            suggestion.anchor_col  = new_anchors[0]
            # anchor_display is a @property — no assignment needed

            anchor_label = (
                new_anchors[0]
                if len(new_anchors) == 1
                else "CONCAT(" + ", ".join(new_anchors) + ")"
            )
            composite_note = (
                f" (composite anchor — CONCAT_WS will be used)"
                if len(new_anchors) > 1 else ""
            )
            return EditResult(
                success      = True,
                message      = (
                    f"Updated '{suggestion.pk_column}' anchor to "
                    f"{anchor_label}{composite_note}."
                ),
                removed_cols = [],
            )

    # ── PREFIX CHANGE ─────────────────────────────────────────────────────
    for pat in _PREFIX_PATTERNS:
        m = pat.search(norm)
        if m:
            pk_token   = m.group(1)
            new_prefix = m.group(2).strip()

            suggestion = _find_suggestion(suggestions, pk_token)
            if suggestion is None:
                return EditResult(
                    success      = False,
                    message      = f"Could not find a suggestion matching '{pk_token}'.",
                    removed_cols = [],
                )

            old_prefix         = suggestion.bk_prefix
            suggestion.bk_prefix = new_prefix   # IN-PLACE

            return EditResult(
                success      = True,
                message      = (
                    f"Updated '{suggestion.pk_column}' prefix: "
                    f"'{old_prefix}' → '{new_prefix}'."
                ),
                removed_cols = [],
            )

    # ── ROLE CHANGE ───────────────────────────────────────────────────────
    for pat in _ROLE_PATTERNS:
        m = pat.search(norm)
        if m:
            pk_token = m.group(1)
            new_role = _normalise_role(m.group(2))

            suggestion = _find_suggestion(suggestions, pk_token)
            if suggestion is None:
                return EditResult(
                    success      = False,
                    message      = f"Could not find a suggestion matching '{pk_token}'.",
                    removed_cols = [],
                )

            old_role           = suggestion.column_role
            suggestion.column_role = new_role   # IN-PLACE

            return EditResult(
                success      = True,
                message      = (
                    f"Updated '{suggestion.pk_column}' role: "
                    f"'{old_role}' → '{new_role}'."
                ),
                removed_cols = [],
            )

    # ── No intent matched ─────────────────────────────────────────────────
    return EditResult(
        success      = False,
        message      = (
            "Could not understand the edit. Supported patterns:\n"
            "  • change <column> anchor to <col1> and <col2>\n"
            "  • set <column> prefix to <prefix>\n"
            "  • mark <column> as primary_key / foreign_key\n"
            "  • remove <column> suggestion"
        ),
        removed_cols = [],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Interactive editor loop  (drop-in replacement for the broken one)
# ─────────────────────────────────────────────────────────────────────────────

def run_nlp_editor(suggestions: list, print_table_fn) -> list:
    """
    Interactive NLP editing loop.

    Parameters
    ----------
    suggestions   : list[PKNullSuggestion]  — mutated in-place
    print_table_fn: callable that prints the current suggestions table

    Returns the (possibly shortened) suggestions list after editing.
    """
    print()
    print("=" * 62)
    print("  PK NULL EDITOR  —  NLP Chat Mode")
    print("=" * 62)
    print("  Describe your changes in plain English.")
    print("  Examples:")
    print("    \"change customer_id anchor to first_name and last_name\"")
    print("    \"set dealer_id anchor as dealer_name and dealer_city\"")
    print("    \"set customer_id prefix to cus\"")
    print("    \"mark sale_id as primary_key\"")
    print("    \"remove the car_id suggestion\"")
    print("  Type  done / continue  (or press Enter)  to finish editing.")
    print("-" * 62)

    edit_num = 0
    while True:
        edit_num += 1
        try:
            user_input = input(f"  Edit #{edit_num} → ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  [Editor] Interrupted — keeping current suggestions.")
            break

        if not user_input or user_input.lower() in ("done", "continue", "exit", "quit"):
            break

        print("  ⏳ Applying edit...")
        result = apply_nlp_edit(user_input, suggestions)

        if result.success:
            print(f"  ✅  {result.message}")
            print("  ✅  Updated suggestions:")
            print_table_fn(suggestions)
        else:
            print(f"  ❌  {result.message}")
            edit_num -= 1   # don't count failed attempts

        if not suggestions:
            print("  ℹ  All suggestions have been removed.")
            break

        try:
            cont = input("  Make another change? (yes / no  or press Enter to finish): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break
        if cont not in ("yes", "y", ""):
            break

    return suggestions