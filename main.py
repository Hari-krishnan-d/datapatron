"""
main.py — Bronze Ingestion Platform Entry Point
================================================
Run this file to start everything:

    python main.py

PROJECT STRUCTURE
medallian-architecture/
├── main.py
├── orchestrator.py
├── execute.py
├── execute_live.py
├── .env 
├── .gitignore
├── BRONZE/
│   ├── final_gen.py
│   ├── metadata.py
│   ├── pipeline.py
│   ├── schema.py
│   ├── Table.py
│   └── Vali.py
├── MCA/
│   ├── __init__.py
│   ├── new_mca.py
│   ├── llm.py
│   ├── s3.py
│   ├── sql.py
│   ├── pg.py
│   └── rest_handler.py
├── SILVER/
│   └── silver_agent/
│       ├── silver_credentials.py
│       ├── silver_validation.py
│       ├── silver_metadata.py
│       ├── silver_classifier.py
│       ├── silver_mapping.py
│       ├── silver_transform.py
│       ├── silver_schema.py
│       ├── silver_table.py
│       └── silver_final_gen.py
├── GOLD/
│   ├── gold_layer/
│   │   ├── gold_credentials.py
│   │   ├── metadata.py
│   │   ├── gold_metadata.py
│   │   ├── data_model.py
│   │   ├── kpi_agent.py
│   │   └── agg_agent.py
│   └── business_key_flow/
│       └── bk_agent.py
|       └── bk_analyzer.py
|       └── bk_generator.py
│   └── pk_null_flow/          
|        ├── pk_null_analyzer.py
|        ├── pk_null_filler.py
|        └── pk_null_agent.py
├── session_output/
├── upload/
└── CRED/
    └── cred.py
"""


import sys
import os
from dotenv import load_dotenv

# Project root = folder that contains main.py
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Try to find the .env file in multiple logical locations
possible_env_paths = [
    os.path.join(ROOT_DIR, ".env"),            # Inside 'streaming data'
    os.path.join(os.getcwd(), ".env"),         # Where you ran the command (silver_try)
    os.path.join(os.path.dirname(ROOT_DIR), ".env") # Parent of 'streaming data'
]

env_loaded = False
for path in possible_env_paths:
    if os.path.exists(path):
        load_dotenv(path, override=True)
        print(f"[DEBUG] .env successfully loaded from: {path}")
        env_loaded = True
        break

if not env_loaded:
    print(f"\n[DEBUG] .env file NOT FOUND. Searched in:")
    for p in possible_env_paths: print(f"  - {p}")
    print(f"[DEBUG] Current Working Directory: {os.getcwd()}")

# Expose root to orchestrator via environment variable
os.environ["BRONZE_ROOT_DIR"] = ROOT_DIR

# Add MCA/ to sys.path so orchestrator is importable
sys.path.insert(0, os.path.join(ROOT_DIR, "MCA"))

# Basic validation for critical environment variables
if not os.getenv("DATABRICKS_HOST"):
    raise ValueError("DATABRICKS_HOST is not set. Please check your .env file and ensure it's in the project root.")
if not os.getenv("DATABRICKS_TOKEN"):
    raise ValueError("DATABRICKS_TOKEN is not set. Please check your .env file.")

from orchestrator import run_pipeline

if __name__ == "__main__":
    try:
        run_pipeline()
    except KeyboardInterrupt:
        print("\n\nCancelled by user.")
    except Exception as exc:
        print(f"\n[ERROR] {exc}")
        sys.exit(1) 


    