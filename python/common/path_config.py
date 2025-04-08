from pathlib import Path
import os

# Load environment variables
LOG_DIR = Path(os.getenv('LP_HEDGE_LOG_DIR', '../logs')).resolve()
DATA_DIR = Path(os.getenv('LP_HEDGE_DATA_DIR', '../lp-data')).resolve()

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

# Ensure subdirectories exist
REBALANCING_HISTORY_DIR.mkdir(parents=True, exist_ok=True)