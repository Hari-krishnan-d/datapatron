<<<<<<< HEAD
=======
# DataPatron_Agent
>>>>>>> b0b07043e341492f3ed1b9e0f15db69eaa5804e3
# Unified Data Pipeline Platform: Bronze & Silver Orchestration

This platform automates the end-to-end journey from raw data sources to a cleaned, standardized Silver layer in Databricks.

## Prerequisites

1.  **Environment Setup**: Ensure your `.env` file is configured with your Databricks Workspace URL and Token.
2.  **Package Installation**:
    ```bash
    pip install -r "setup/requirements.txt"
    ```

## Execution Workflow

### Step 1: Run the Local Orchestrator
Launch the platform via the main entry point. This initializes the environment and starts the Master Control Agent (MCA).
```bash
python main.py
```
Follow the CLI prompts. This will generate artifacts in `session_output/run_<timestamp>/`.

### Step 2: Upload the Migration Plan to Databricks
Use the upload utility to move your local JSON plan to a Unity Catalog Volume.
```bash
python "upload/upload.py"
```
*   Provide the **Volume Name** when prompted.
*   The script will output a **Volume Path** (e.g., `/Volumes/catalog/schema/volume/migration_plan.json`). **Copy this path.**

### Step 3: Run Bronze Ingestion in Databricks
To move data from your source (S3/SQL) into the Bronze layer:
1.  Open a Notebook in Databricks.
2.  Copy the entire content of your **`execution.py`** (Bronze agent) into a cell.
3.  Locate the `CONFIG` variable at the top of the code and paste your **Volume Path**:
    ```python
    CONFIG = "/Volumes/your_catalog/your_schema/your_volume/migration_plan.json"
    ```
4.  Run the cell.

### Step 4: Run Silver Transformation in Databricks
Once the Bronze tables are populated, perform the Silver transformation:
1.  Open a new Notebook or cell in Databricks.
2.  Copy the entire content of **`silver_agent/silver_execution.py`** into the cell.
3.  Update the `CONFIG` variable with the path to your **Silver Migration Plan** (uploaded similarly to Step 2 if generated separately, or located in the same volume):
    ```python
    # Example path for Silver execution
    CONFIG = "/Volumes/your_catalog/your_schema/your_volume/silver_plan.json"
    ```
<<<<<<< HEAD
4.  Run the cell to apply renames, type casting, null handling, and value standardization.
=======
4.  Run the cell to apply renames, type casting, null handling, and value standardization.
>>>>>>> b0b07043e341492f3ed1b9e0f15db69eaa5804e3
