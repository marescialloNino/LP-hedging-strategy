from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Load environment variables
ROOT_DIR = Path(os.getenv('ROOT_DIR', os.getcwd())).resolve()
LOG_DIR = Path(os.getenv('LP_HEDGE_LOG_DIR', ROOT_DIR / 'logs')).resolve()
DATA_DIR = Path(os.getenv('LP_HEDGE_DATA_DIR', ROOT_DIR / 'lp-data')).resolve()

PYTHON_YAML_CONFIG_PATH = Path(os.getenv('PYTHON_YAML_CONFIG_PATH', ROOT_DIR / 'python/config/config.yaml')).resolve()

# ==================== last calculated positions files ====================
METEORA_LATEST_CSV = DATA_DIR / "LP_meteora_positions_latest.csv"
KRYSTAL_LATEST_CSV = DATA_DIR / "LP_krystal_positions_latest.csv"
HEDGING_LATEST_CSV = DATA_DIR / "hedging_positions_latest.csv"
REBALANCING_LATEST_CSV = DATA_DIR / "rebalancing_results.csv"

# ==================== history positions files ====================
METEORA_HISTORY_CSV = DATA_DIR /"LP_meteora_positions_history.csv"
KRYSTAL_HISTORY_CSV = DATA_DIR /"LP_krystal_positions_history.csv"
HEDGING_HISTORY_CSV = DATA_DIR / "hedging_positions_history.csv"
REBALANCING_HISTORY_DIR = DATA_DIR / "rebalancing_history"

# ==================== error flags files ====================
HEDGE_ERROR_FLAGS_PATH = LOG_DIR / 'hedge_fetching_errors.json'
LP_ERROR_FLAGS_PATH = LOG_DIR / 'lp_fetching_errors.json'

# ==================== pnl files ====================
METEORA_PNL_CSV = DATA_DIR / "position_pnl_results.csv"
KRYSTAL_POOL_PNL_CSV = DATA_DIR /"krystal_pnl_by_pool.csv"

# ==================== hedge orders files ====================
AUTOMATIC_ORDER_MONITOR_CSV = DATA_DIR / "automatic_order_monitor.csv"
MANUAL_ORDER_MONITOR_CSV = DATA_DIR / "manual_order_monitor.csv"
ORDER_HISTORY_CSV = DATA_DIR / "order_history.csv"

# ==================== shell script files ====================
WORKFLOW_SHELL_SCRIPT = ROOT_DIR / "run_workflow.sh"
PNL_SHELL_SCRIPT = ROOT_DIR / "run_pnl_calculations.sh"
HEDGE_SHELL_SCRIPT = ROOT_DIR / "run_hedge_calculations.sh"

# ================ python package internal paths ================
CONFIG_DIR = ROOT_DIR / "python/config"
HEDGEABLE_TOKENS_JSON =  CONFIG_DIR / "hedgeable_tokens.json"
ENCOUNTERED_TOKENS_JSON =  CONFIG_DIR / "encountered_tokens.json"
TICKER_MAPPINGS_PATH = CONFIG_DIR / "ticker_mappings.json"


# Ensure directories exist
LOG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
REBALANCING_HISTORY_DIR.mkdir(parents=True, exist_ok=True)