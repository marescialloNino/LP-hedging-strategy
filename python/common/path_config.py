from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Load environment variables
ROOT_DIR = Path(os.getenv('ROOT_DIR', os.getcwd())).resolve()
LOG_DIR = Path(os.getenv('LP_HEDGE_LOG_DIR', ROOT_DIR / 'logs')).resolve()
DATA_DIR = Path(os.getenv('LP_HEDGE_DATA_DIR', ROOT_DIR / 'lp-data')).resolve()

# Ensure directories exist
LOG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Define specific file paths
HEDGING_HISTORY_CSV = DATA_DIR / "hedging_positions_history.csv"
HEDGING_LATEST_CSV = DATA_DIR / "hedging_positions_latest.csv"
REBALANCING_HISTORY_DIR = DATA_DIR / "rebalancing_history"
REBALANCING_LATEST_CSV = DATA_DIR / "rebalancing_results.csv"
METEORA_LATEST_CSV = DATA_DIR / "LP_meteora_positions_latest.csv"
KRYSTAL_LATEST_CSV = DATA_DIR / "LP_krystal_positions_latest.csv"
METEORA_PNL_CSV = DATA_DIR / "position_pnl_results.csv"
KRYSTAL_POOL_PNL_CSV = DATA_DIR /"krystal_pnl_by_pool.csv"
AUTOMATIC_ORDER_MONITOR_CSV = DATA_DIR / "automatic_order_monitor.csv"
MANUAL_ORDER_MONITOR_CSV = DATA_DIR / "manual_order_monitor.csv"
ORDER_HISTORY_CSV = DATA_DIR / "order_history.csv"

# shell script files
WORKFLOW_SHELL_SCRIPT = ROOT_DIR / "run_workflow.sh"
PNL_SHELL_SCRIPT = ROOT_DIR / "run_pnl_calculations.sh"
HEDGE_SHELL_SCRIPT = ROOT_DIR / "run_hedge_calculations.sh"

# Ensure subdirectories exist
REBALANCING_HISTORY_DIR.mkdir(parents=True, exist_ok=True)

# python package internal paths
CONFIG_DIR = ROOT_DIR / "python/config"