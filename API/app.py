import sys
import os
from pathlib import Path
import logging
from contextlib import asynccontextmanager
 
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
  # --- DYNAMIC PATH SETUP (Cross-Platform) ---
# 1. Get the directory of this current script

_API_DIR = Path(__file__).resolve().parent  # API/

# 2. Go up ONE level to get to the project root
_ROOT = _API_DIR.parent
 
# 3. Add _API_DIR to sys.path so Python can find your 'routes' folder
sys.path.insert(0, str(_API_DIR))
 
# 4. Correctly load the .env file using the / operator
load_dotenv(dotenv_path=_ROOT / ".env")
 
 
# --- ROUTE IMPORTS ---
# (Import these AFTER sys.path is modified so python knows where 'routes' is)
from routes.mca_routes    import router as mca_router
from routes.flow_a_routes import router as flow_a_router
from routes.flow_b_routes import router as flow_b_router
from routes.flow_c_routes import router as flow_c_router   # ← Flow C
 
 
# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("API")
 
 
# --- FASTAPI LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Medallion API starting up...")
    logger.info("DATABRICKS_HOST = %s", os.getenv("DATABRICKS_HOST", "NOT SET"))
    yield
    logger.info("Medallion API shutting down.")
 
 
# --- APP INITIALIZATION ---
app = FastAPI(
    title="Medallion Architecture API",
    description="REST API for the Medallion data pipeline orchestrator.",
    version="1.0.0",
    lifespan=lifespan,
)
 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)
 
# --- INCLUDE ROUTERS ---
app.include_router(mca_router)
app.include_router(flow_a_router)
app.include_router(flow_b_router)
app.include_router(flow_c_router)   # ← Flow C: /flow-c/start, /taxonomy, /transform, /execute