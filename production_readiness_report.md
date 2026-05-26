# 🔍 Production Readiness Report — Medallion Architecture Pipeline

> **Scope**: Full codebase review of `medallian-architecture/`  
> **Date**: 2026-05-11  
> **Verdict**: ⚠️ **Not production-ready** — 10 critical areas need attention

---

## Severity Legend

| Icon | Level | Meaning |
|------|-------|---------|
| 🔴 | **Critical** | Must fix before any production deployment |
| 🟠 | **High** | Serious risk — fix before going live |
| 🟡 | **Medium** | Should fix — creates operational pain |
| 🟢 | **Low** | Best practice — improve when possible |

---

## 1. 🔴 Security & Secrets Management

### 1a. Hardcoded Credentials in `.env` (committed to repo)

**Files**: [.env](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/.env)

Your `.env` file contains **real** Databricks tokens and host URLs:

```
DATABRICKS_HOST="https://dbc-14f40aec-e3df.cloud.databricks.com"
DATABRICKS_TOKEN="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
```

Even though `.env` is in `.gitignore`, if this repo was **ever** pushed with the `.env` present, the token is already in git history.

**Fix:**
1. **Rotate the token immediately** — the one above is now visible in this review
2. Use a secrets manager (Azure Key Vault, AWS Secrets Manager, or Databricks secrets)
3. Add `.env.example` with placeholder values for onboarding
4. Run `git filter-branch` or BFG to scrub history if `.env` was ever committed

```python
# Instead of loading from .env for production:
import os
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]  # Injected by CI/CD or secrets manager
```

### 1b. Token Leaked in Error Messages

**File**: [llm.py#L270-L273](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/MCA/llm.py#L270-L273)

```python
# CURRENT — token fragment leaked in error output
f"      headers={{'Authorization': 'Bearer {self.token[:10]}...'}})\\n"
```

**Fix:** Never include any part of the token in user-facing messages:
```python
f"      headers={{'Authorization': 'Bearer <YOUR_TOKEN>'}})\\n"
```

### 1c. Session Config Stores Credentials in Plaintext JSON

**File**: [orchestrator.py#L67-L88](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/orchestrator.py#L67-L88)

`session_config.json` stores workspace URL, warehouse ID, and **access token** in plaintext on disk.

**Fix:** Encrypt at rest or use a credential store. At minimum, set restrictive file permissions:
```python
import stat
os.chmod(config_path, stat.S_IRUSR | stat.S_IWUSR)  # 600 — owner only
```

---

## 2. 🔴 SQL Injection Vulnerabilities

### 2a. String-Interpolated SQL in `execute_live.py`

**File**: [execute_live.py#L81-L102](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/execute_live.py#L81-L102)

Every SQL query is built with raw f-strings:

```python
# CURRENT — injectable
spark.sql(f"""
    INSERT INTO {watermark_control_table}
        ...
    VALUES ('{source_name}', '{table_name}', ...)
""")
```

If any value like `source_name` contains a single quote or is user-influenced, this breaks or becomes exploitable.

**Fix:** Use parameterized queries or sanitize all inputs:
```python
# Option 1: Validate identifiers against a whitelist pattern
import re
def _safe_identifier(name: str) -> str:
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', name):
        raise ValueError(f"Invalid identifier: {name}")
    return name

# Option 2: Use Spark's DataFrame API instead of raw SQL
df.write.format("delta").mode("append").saveAsTable(target)
```

### 2b. Same Issue in `execute.py`

**File**: [execute.py#L81](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/execute.py#L81)

```python
existing = spark.sql(f"select max({wm}) m from {tgt}").collect()[0]["m"]
```

Both `wm` and `tgt` come from the JSON config file with zero validation.

---

## 3. 🔴 Logging Is Completely Disabled

### 3a. All Logging Silenced in Production

**File**: [orchestrator.py#L32](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/orchestrator.py#L32)

```python
logging.disable(logging.CRITICAL)  # ← THIS SILENCES EVERYTHING
```

This single line means **zero log output** from any module — LLM calls, credential errors, pipeline failures — all invisible in production.

**Fix:** Remove `logging.disable()` and configure structured logging:
```python
import logging
import json

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        })

handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())

logging.basicConfig(level=logging.INFO, handlers=[handler])
```

### 3b. Excessive `print()` Instead of `logging`

**Files**: Nearly every file (`orchestrator.py`, `execute.py`, `execute_live.py`, `cred.py`, `new_mca.py`)

There are **200+ raw `print()` calls** scattered across the codebase. In production:
- `print()` goes to stdout and is easily lost
- No log levels, no timestamps, no structured output
- Cannot be filtered, aggregated, or sent to monitoring tools

**Fix:** Replace all `print()` with proper `logger.info()` / `logger.warning()` calls:
```python
# BEFORE
print(f"  ✓ Watermark row ready: {source_name}")

# AFTER
logger.info("Watermark row ready", extra={"source_name": source_name, "table": table_name})
```

---

## 4. 🟠 Error Handling & Resilience

### 4a. Bare `except:` Clauses Swallow All Errors

**File**: [execute.py#L85](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/execute.py#L85), [execute.py#L104](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/execute.py#L104)

```python
except:                    # ← catches SystemExit, KeyboardInterrupt, MemoryError...
    print("First load / target empty")
```

```python
except:                    # ← silently ignores schema alignment failures
    return df
```

**Fix:** Always catch specific exceptions:
```python
except (AnalysisException, ValueError) as e:
    logger.warning("Schema alignment failed for %s: %s", target, e)
    return df
```

### 4b. No Retry Logic for LLM Calls

**File**: [llm.py](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/MCA/llm.py)

LLM endpoints fail frequently (rate limits, transient 5xx). The fallback tries different endpoints, but there's **no retry with backoff** for transient failures on the same endpoint.

**Fix:** Add exponential backoff:
```python
import time

def _do_post_with_retry(self, url: str, payload: dict, max_retries: int = 3) -> str:
    for attempt in range(max_retries):
        try:
            return self._do_post(url, payload)
        except LLMCallError as e:
            if attempt < max_retries - 1 and "429" in str(e) or "5" in str(e)[:1]:
                wait = 2 ** attempt
                logger.warning("Retry %d/%d after %ds: %s", attempt+1, max_retries, wait, e)
                time.sleep(wait)
            else:
                raise
```

### 4c. `_active_url` Cache Never Invalidated

**File**: [llm.py#L247-L248](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/MCA/llm.py#L247-L248)

Once a working endpoint is found, it's cached forever. If that endpoint goes down later, **all subsequent calls fail** without trying alternatives.

```python
if self._active_url:
    return self._do_post(self._active_url, payload)  # Never falls back
```

**Fix:** Invalidate cache on failure:
```python
if self._active_url:
    try:
        return self._do_post(self._active_url, payload)
    except LLMCallError:
        logger.warning("Cached endpoint failed, re-discovering...")
        self._active_url = None
        # Fall through to full discovery
```

### 4d. Pipeline Continues After Table Failure

**File**: [execute.py#L148-L150](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/execute.py#L148-L150)

```python
except Exception as e:
    print("FAILED:", table["source_name"])
    print("ERROR:", str(e))
    # ← no tracking, no alerting, pipeline continues silently
```

**Fix:** Track failures, set exit codes, send alerts:
```python
failed_tables = []
# ... in the loop:
except Exception as e:
    logger.error("Table %s failed: %s", table["source_name"], e, exc_info=True)
    failed_tables.append({"table": table["source_name"], "error": str(e)})

if failed_tables:
    raise RuntimeError(f"{len(failed_tables)} table(s) failed: {failed_tables}")
```

---

## 5. 🟠 Configuration Management

### 5a. Hardcoded Paths Everywhere

**Files**: Multiple

```python
# execute.py line 7
CONFIG="/Volumes/test_ui/bronze/new/migration_plan.json"

# execute_live.py line 736
config_path: str = "/Volumes/2data/bronze/new/migration_plan.json"

# .env line 15
SILVER_MIGRATION_PLAN_CONFIG_PATH="/Volumes/test/shit/ll/plan.json"  # ← profanity in path
```

**Fix:**
- Use environment variables for all paths
- Create a central `config.py` module
- Remove test/profanity paths from `.env`

```python
# config.py
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Config:
    databricks_host: str = os.getenv("DATABRICKS_HOST", "")
    databricks_token: str = os.getenv("DATABRICKS_TOKEN", "")
    bronze_config_path: str = os.getenv("BRONZE_MIGRATION_PLAN_CONFIG_PATH", "")
    silver_config_path: str = os.getenv("SILVER_MIGRATION_PLAN_CONFIG_PATH", "")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    llm_timeout: int = int(os.getenv("LLM_TIMEOUT", "60"))
    llm_max_retries: int = int(os.getenv("LLM_MAX_RETRIES", "3"))
```

### 5b. `model_endpoint` in `.env` Has No Quotes

**File**: [.env#L19](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/.env#L19)

```
model_endpoint=databricks-llama-4-maverick   # ← not used by llm.py at all!
```

Meanwhile [llm.py#L43](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/MCA/llm.py#L43) hardcodes:
```python
MODEL_ENDPOINT = "databricks-meta-llama-3.1-405b-instruct"
```

The `.env` value and the code are **completely disconnected**.

**Fix:** Read from environment:
```python
MODEL_ENDPOINT = os.getenv("MODEL_ENDPOINT", "databricks-meta-llama-3.1-405b-instruct")
```

---

## 6. 🟠 Architecture & Maintainability

### 6a. Monolithic 1530-Line Orchestrator

**File**: [orchestrator.py](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/orchestrator.py) — **1,530 lines, 66KB**

This single file contains:
- Module loading logic
- Session config management
- Bronze step runners (7 functions)
- Silver step runners
- Gold step runners (credentials, metadata, data modeling, BK audit, KPI pipeline)
- Flow runners A through K (11 functions)
- Pause/checkpoint helpers
- Main entry point

**Fix:** Split into focused modules:
```
orchestrator/
├── __init__.py          # run_pipeline() entry point
├── loader.py            # _load() and _agent_paths()
├── config.py            # load_session_config()
├── bronze_runner.py     # _run_bronze_steps() and bronze step functions
├── silver_runner.py     # _run_silver_steps() and silver step functions
├── gold_runner.py       # Gold credential, modeling, KPI functions
└── flows.py             # FLOW_RUNNERS dispatch and individual flow functions
```

### 6b. Dynamic Imports via `importlib` Are Fragile

**File**: [orchestrator.py#L46-L60](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/orchestrator.py#L46-L60)

Every agent is loaded at runtime via `importlib.util.spec_from_file_location()`. This:
- Bypasses Python's import system (no IDE support, no type checking)
- Pollutes `sys.modules` with module names that may conflict
- Makes debugging import errors extremely hard

**Fix:** Use standard Python packages with `__init__.py` files:
```python
# Instead of _load("agent_cred", agents["cred"])
from CRED.cred import CredentialsCollectionAgent
```

### 6c. Massive Code Duplication in Flow Runners

**File**: [orchestrator.py#L1132-L1421](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/orchestrator.py#L1132-L1421)

Flows D, E, G are **nearly identical** (~30 lines each), differing only in the header string. Flows H and I duplicate ~60 lines each.

**Fix:** Use a table-driven approach:
```python
FLOW_CONFIG = {
    "D": {"label": "S3 → Bronze → Silver", "has_bronze": True, "has_silver": True},
    "E": {"label": "Azure SQL → Bronze → Silver", "has_bronze": True, "has_silver": True},
    # ...
}

def _run_flow(flow_key, agents, mca_plan, output_dir, config_path, artefacts):
    cfg = FLOW_CONFIG[flow_key]
    print(f"\n  FLOW {flow_key}  —  {cfg['label']}")
    if cfg.get("has_bronze"):
        _run_bronze_steps(...)
    if cfg.get("has_silver"):
        _run_silver_steps(...)
    if cfg.get("has_gold"):
        _run_gold_steps(...)
```

### 6d. Inconsistent File Naming

| File | Convention |
|------|-----------|
| `Table.py` | PascalCase |
| `Vali.py` | PascalCase (abbreviated) |
| `metadata.py` | snake_case |
| `final_gen.py` | snake_case |
| `new_mca.py` | snake_case with prefix |

**Fix:** Standardize to `snake_case.py` for all modules. `Table.py` → `table_creation.py`, `Vali.py` → `validation.py`.

---

## 7. 🟡 Missing Tests

**Current state:** **Zero test files** in the entire repository.

For production, you need at minimum:

| Test Type | What to Test |
|-----------|-------------|
| **Unit** | `extract_json()`, `PlanBuilder.build()`, `_safe_identifier()`, connection templates |
| **Integration** | LLM client against a mock endpoint, credential collection flow |
| **Contract** | Schema validation of `mca_plan.json`, `migration_plan.json`, `credentials.json` |
| **E2E** | At least one happy-path flow (e.g., Flow A) with mocked Databricks/S3 |

**Fix:** Add a `tests/` directory with pytest:
```
tests/
├── conftest.py
├── test_llm.py            # extract_json, endpoint fallback
├── test_plan_builder.py   # All flow types produce valid plans
├── test_credentials.py    # Handler validation
└── test_orchestrator.py   # Flow routing logic
```

---

## 8. 🟡 Dependency Management

**Current state:** No `requirements.txt` at the project root (only inside `GOLD/.venv`). No `pyproject.toml` or `setup.py`.

**Fix:** Create a proper `requirements.txt` or `pyproject.toml`:
```
# requirements.txt
python-dotenv>=1.0.0
requests>=2.31.0
pyspark>=3.5.0       # for Databricks notebooks
```

Also pin versions and use a lockfile:
```bash
pip freeze > requirements-lock.txt
```

---

## 9. 🟡 Interactive `input()` Blocks Automation

**Files**: [orchestrator.py](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/orchestrator.py) (15+ `input()` calls), [new_mca.py](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/MCA/new_mca.py)

The pipeline **cannot run unattended**. Every flow requires multiple `input()` prompts for:
- Databricks workspace config
- Credential reuse decisions
- Gold catalog/schema names
- Business key selection
- Model approval
- Pause checkpoints ("Press Enter when done...")

**Fix:** Support a headless/config-driven mode:
```python
def _ask_or_config(prompt: str, config_key: str, default: str = "") -> str:
    """Use config value if available, otherwise prompt interactively."""
    value = os.getenv(config_key) or config.get(config_key)
    if value:
        logger.info("Using configured value for %s: %s", config_key, value)
        return value
    return input(f"  {prompt} [{default}]: ").strip() or default
```

---

## 10. 🟢 Performance Considerations

### 10a. `df.rdd.isEmpty()` Is Expensive

**File**: [execute_live.py#L702](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/execute_live.py#L702)

```python
if df.rdd.isEmpty():  # ← triggers a full Spark job
```

**Fix:**
```python
if df.head(1) is None:  # ← much cheaper, reads only 1 row
```

### 10b. `df.count()` Called Before Write

**File**: [execute_live.py#L706](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/execute_live.py#L706)

```python
print(f"  → {df.count()} rows fetched")  # ← triggers full scan
df = align_schema(df, target)             # ← scans again
write_append(df, target)                  # ← scans AGAIN
```

Three full data scans for one table. **Fix:** Cache or remove the count:
```python
df = df.cache()
count = df.count()
logger.info("Rows fetched: %d", count)
```

### 10c. `inferSchema` on Every S3 Read

**Files**: [execute.py#L33](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/execute.py#L33), [execute_live.py#L227](file:///c:/Users/Shankari%20G/Downloads/medallian-architecture/medallian-architecture/execute_live.py#L227)

```python
.option("inferSchema", True)  # ← reads entire file to guess types
```

**Fix:** Use explicit schemas from the schema mapping step, or at minimum set `samplingRatio`:
```python
.option("inferSchema", True)
.option("samplingRatio", 0.1)  # Sample only 10% for type inference
```

---

## Summary — Priority Action Items

| # | Severity | Issue | Effort |
|---|----------|-------|--------|
| 1 | 🔴 | **Rotate leaked Databricks token** | 5 min |
| 2 | 🔴 | **Remove `logging.disable(CRITICAL)`** | 1 min |
| 3 | 🔴 | **Fix SQL injection in execute_live.py** | 2 hrs |
| 4 | 🔴 | **Remove token fragment from error messages** | 5 min |
| 5 | 🟠 | **Add retry + cache invalidation to LLM client** | 2 hrs |
| 6 | 🟠 | **Connect `.env` MODEL_ENDPOINT to `llm.py`** | 15 min |
| 7 | 🟠 | **Track and alert on table failures in execute.py** | 1 hr |
| 8 | 🟠 | **Replace bare `except:` with specific exceptions** | 30 min |
| 9 | 🟡 | **Add unit tests for core logic** | 1-2 days |
| 10 | 🟡 | **Add headless/config-driven mode** | 1 day |
| 11 | 🟡 | **Create root `requirements.txt`** | 15 min |
| 12 | 🟡 | **Replace `print()` with structured logging** | 1 day |
| 13 | 🟢 | **Split orchestrator.py into modules** | 1 day |
| 14 | 🟢 | **Fix Spark perf: isEmpty, count, inferSchema** | 2 hrs |

> [!CAUTION]
> **Items 1-4 are blockers.** Do not deploy to any shared or production environment until these are resolved. The leaked token (#1) should be rotated **immediately** regardless of deployment plans.
