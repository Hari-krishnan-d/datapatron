
"""
MCA/llm.py — Databricks LLM Client
=====================================
Handles all LLM communication. Imported by new_mca.py.

ENDPOINT RESOLUTION ORDER:
  1. Hardcoded FALLBACK_ENDPOINTS list (tried in order)
  2. Auto-discovery via /api/2.0/serving-endpoints (if all hardcoded fail)
     Queries the workspace, finds all READY LLM endpoints, tries them too.
  3. Clear error message telling you exactly how to find your endpoint name.

WHY ENDPOINTS FAIL WITH 404:
  Databricks endpoint names are workspace-specific. The names in
  FALLBACK_ENDPOINTS are guesses — your workspace may have a different
  name like "llama-3-3-70b" or "my-llm-endpoint".

HOW TO FIND YOUR ENDPOINT NAME:
  Run in a Databricks notebook:
    import requests
    r = requests.get(
        "https://YOUR_WORKSPACE.azuredatabricks.net/api/2.0/serving-endpoints",
        headers={"Authorization": "Bearer YOUR_TOKEN"}
    )
    for ep in r.json().get("endpoints", []):
        print(ep["name"], "—", ep["state"]["ready"])

  Copy the READY endpoint name and set it as MODEL_ENDPOINT below.
"""

import json
import re
import logging
from dotenv import load_dotenv
import requests
from pathlib import Path
import os
 
# Resolves .env relative to this file's location — works on any machine
_HERE = Path(__file__).resolve().parent
load_dotenv(dotenv_path=_HERE.parent / ".env")
 
DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")

# ── Actual endpoints discovered in your workspace ─────────────────────────────
# These are the real endpoint names from your Databricks workspace.
# Order = preference: largest/most capable first.
MODEL_ENDPOINT   = "databricks-llama-4-maverick"  # best available

FALLBACK_ENDPOINTS = [
    "databricks-meta-llama-3.1-405b-instruct",  # Llama 3.1 405B — most capable
    "databricks-gpt-oss-120b",                   # GPT OSS 120B
    "databricks-qwen3-next-80b-a3b-instruct",    # Qwen3 80B
    "databricks-gpt-oss-20b",                    # GPT OSS 20B — fastest
    "databricks-gemma-3-12b",                    # Gemma 3 12B — lightest
]

logger = logging.getLogger("MCA.llm")


class LLMParseError(Exception):
    """Raised when LLM response cannot be parsed as valid JSON."""


class LLMCallError(Exception):
    """Raised when ALL LLM endpoint attempts fail."""


def extract_json(text: str) -> dict:
    """Safely pull a JSON object or array out of raw LLM output."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    fenced = re.sub(r"```(?:json)?", "", text).strip().rstrip("`").strip()
    try:
        return json.loads(fenced)
    except json.JSONDecodeError:
        pass
    # Try to find JSON object
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    # Try to find JSON array
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    raise LLMParseError(f"Could not extract JSON from LLM output:\n{text}")


def discover_llm_endpoints(host: str, token: str) -> list:
    """
    Query /api/2.0/serving-endpoints and return names of all READY
    endpoints that look like LLM chat/completion endpoints.

    Called automatically when all hardcoded endpoint names return 404.
    """
    try:
        resp = requests.get(
            f"{host}/api/2.0/serving-endpoints",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(
                "Could not list serving endpoints: HTTP %s %s",
                resp.status_code, resp.text[:200]
            )
            return []

        endpoints = resp.json().get("endpoints", [])
        ready_names = []

        for ep in endpoints:
            name  = ep.get("name", "")
            state = ep.get("state", {}).get("ready", "")
            if state != "READY":
                continue
            # Heuristic: LLM endpoints contain these keywords
            name_lower = name.lower()
            if any(kw in name_lower for kw in (
                "llama", "llm", "instruct", "gpt", "claude",
                "mixtral", "dbrx", "mpt", "falcon", "mistral",
                "gemma", "phi", "chat", "completion"
            )):
                ready_names.append(name)

        if ready_names:
            print(f"\n  Auto-discovered {len(ready_names)} LLM endpoint(s): {ready_names}")
            print(f"  Using: {ready_names[0]}\n")
        else:
            # Print ALL endpoints so user can see what's available
            all_names = [ep.get("name") for ep in endpoints]
            print(f"\n  No READY LLM endpoints found.")
            print(f"  All endpoints in workspace: {all_names}")
            print(f"  Go to Databricks → Serving to create or enable an LLM endpoint.\n")

        return ready_names

    except Exception as exc:
        logger.warning("Endpoint auto-discovery failed: %s", exc)
        return []


class DatabricksLLMClient:

    def __init__(
        self,
        host: str          = DATABRICKS_HOST,
        token: str         = DATABRICKS_TOKEN,
        model: str         = MODEL_ENDPOINT,
        temperature: float = 0.1,
        max_tokens: int    = 1500,
        timeout: int       = 60,
    ):
        if not host:
            raise LLMCallError(
                "DATABRICKS_HOST is empty or missing from environment. "
                "1. Check that your .env file is located in the project root folder.\n"
                "2. Ensure it contains: DATABRICKS_HOST='https://<your-workspace-url>'\n"
                "3. Verify that main.py is loading the .env file correctly."
            )

        self.host        = host
        self.token       = token
        self.headers     = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }
        self.temperature = temperature
        self.max_tokens  = max_tokens
        self.timeout     = timeout

        # Build ordered URL list from hardcoded names
        seen, ordered = set(), []
        for ep in [model] + FALLBACK_ENDPOINTS:
            if ep not in seen:
                seen.add(ep)
                ordered.append(f"{host}/serving-endpoints/{ep}/invocations")
        self._endpoints       = ordered
        self._discovered_urls = None       # populated lazily on first failure
        self.url              = self._endpoints[0]
        self._active_url: str | None = None

    # ── Internal ──────────────────────────────────────────────────────────────

    def _do_post(self, url: str, payload: dict) -> str:
        """Single HTTP POST. Raises LLMCallError on any failure."""
        try:
            response = requests.post(
                url, headers=self.headers, json=payload, timeout=self.timeout
            )
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise LLMCallError("LLM request timed out.")
        except requests.exceptions.ConnectionError as exc:
            raise LLMCallError(f"Network error: {exc}")
        except requests.exceptions.HTTPError:
            raise LLMCallError(f"HTTP {response.status_code}: {response.text}")

        result = response.json()
        if "choices" not in result or not result["choices"]:
            raise LLMCallError(f"Unexpected LLM response: {result}")

        content = result["choices"][0]["message"]["content"]

        # Some models return content as a list of blocks:
        # [{"type": "text", "text": "..."}, ...]
        # Others return a plain string. Normalise to string.
        if isinstance(content, list):
            raw = " ".join(
                block.get("text", "") for block in content
                if isinstance(block, dict) and block.get("type") == "text"
            ).strip()
        elif isinstance(content, str):
            raw = content
        else:
            raw = str(content)

        if not raw or not raw.strip():
            raise LLMCallError("LLM returned empty response.")
        return raw

    def _get_all_urls(self) -> list:
        """
        Return full list of URLs to try.
        Discovered endpoints come FIRST because we know the hardcoded
        fallback names may not exist in this workspace.
        Discovery runs once and is cached.
        """
        if self._discovered_urls is None:
            discovered_names = discover_llm_endpoints(self.host, self.token)
            self._discovered_urls = [
                f"{self.host}/serving-endpoints/{name}/invocations"
                for name in discovered_names
            ]

        # Discovered (real) URLs first, then hardcoded fallbacks
        known  = set(self._discovered_urls)
        extras = [u for u in self._endpoints if u not in known]
        return self._discovered_urls + extras

    def _post(self, payload: dict) -> str:
        """Try each endpoint in order. Cache the first that succeeds."""
        # Fast path: already found a working endpoint
        if self._active_url:
            return self._do_post(self._active_url, payload)

        all_urls   = self._get_all_urls()
        last_error = "No endpoints configured."

        for url in all_urls:
            logger.info("Calling LLM: %s", url)
            try:
                result = self._do_post(url, payload)
                self._active_url = url
                logger.info("LLM endpoint resolved: %s", url)
                return result
            except LLMCallError as exc:
                last_error = str(exc)
                logger.warning("Endpoint failed — trying next. Reason: %s", exc)

        raise LLMCallError(
            f"All {len(all_urls)} LLM endpoint(s) failed.\n"
            f"Last error: {last_error}\n\n"
            f"To find your endpoint name, run in Databricks:\n"
            f"  import requests\n"
            f"  r = requests.get('{self.host}/api/2.0/serving-endpoints',\n"
            f"      headers={{'Authorization': 'Bearer {self.token[:10]}...'}})\n"
            f"  for ep in r.json().get('endpoints', []):\n"
            f"      print(ep['name'], ep['state']['ready'])\n\n"
            f"Then set MODEL_ENDPOINT = '<your endpoint name>' in llm.py"
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def call(self, system_prompt: str, user_prompt: str) -> str:
        payload = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            "temperature": self.temperature,
            "max_tokens":  self.max_tokens,
        }
        return self._post(payload)

    def parse_user_intent(self, user_input: str) -> dict:
        system = (
            "You are a data ingestion pipeline planner. "
            "Decompose the user request into INDIVIDUAL pipeline entries. "
            "Rules: "
            "1. Each S3 bucket = one separate source entry. "
            "2. Each Azure SQL database = one separate source entry. "
            "3. Never group multiple buckets or schemas into one entry. "
            "4. Supported source_type values: azure_sql, s3, rest_api, postgresql. "
            "5. Return VALID JSON ONLY. No markdown, no comments, no explanation."
        )
        user = f"""
User ingestion request:
{user_input}

RULES:
- Do NOT fill in bucket names, schema names, or connection details.
- Only identify: source_type, ingestion_mode, schedule, and COUNT of pipelines.
- 2 S3 buckets → 2 separate s3 entries.
- 2 Azure SQL databases → 2 separate azure_sql entries.

EXAMPLE — "2 S3 buckets and 2 Azure SQL databases":
{{
  "sources": [
    {{"source_type": "s3",        "ingestion_mode": "full", "schedule": "one_time"}},
    {{"source_type": "s3",        "ingestion_mode": "full", "schedule": "one_time"}},
    {{"source_type": "azure_sql", "ingestion_mode": "full", "schedule": "one_time"}},
    {{"source_type": "azure_sql", "ingestion_mode": "full", "schedule": "one_time"}}
  ],
  "destination": {{"catalog": "bronze_catalog", "schema": "raw_ingestion"}}
}}

Return JSON for the actual request only. No bucket names. No connection details.
"""
        logger.info("Parsing user intent via LLM.")
        raw = self.call(system, user)
        logger.debug("Raw LLM output:\n%s", raw)
        return extract_json(raw)