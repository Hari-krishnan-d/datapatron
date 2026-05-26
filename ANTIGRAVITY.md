# Antigravity Agent - Pipeline Orchestrator

Antigravity is the command-line interface for the Unified Platform Orchestrator. It uses an LLM-driven Master Control Agent (MCA) to classify data pipeline intent and generate medallion architecture plans.

## 🚀 Workflow
The agent operates in sequential phases:

### Phase 1: Intent Classification
1. **Initiation**: Run `python main.py`.
2. **Request**: Provide a natural language description (e.g., "transforming data").
3. **Classification**: The MCA analyzes intent and suggests a flow (A-K).
4. **flow selection**: based on the user request MCA suggest the flow type
   for example if user want to transform existing bronze table into silver
   then MCA suggest flow C
   if user want to move data from S3 to Bronze and then Bronze to Silver
   then MCA suggest flow D
   if user want to move data from Azure SQL to Bronze and then Bronze to Silver
   then MCA suggest flow E
   if user want to move data from Hybrid (S3 + SQL) to Bronze and then Bronze to Silver
   then MCA suggest flow G
   if user want to move data from Hybrid (S3 + SQL) to Bronze and then Bronze to Silver
   then MCA suggest flow G
   
   like thisMCA should suggest the flow type

### Phase 2: Credential Collection
After selecting a flow, the agent enters the `silver_agent` phase:
- It collects workspace-specific connection details.
- It maps Bronze sources to Silver destinations.

## 📝 Automation Pattern (API / Scripting)
When automating this via your API, you must pipe the inputs using newline characters (`\n`). 

**Example input sequence for Flow C:**
```text
transformingdata\n
c\n