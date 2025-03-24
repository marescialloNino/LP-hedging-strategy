# hedge_rebalancer.py
import pandas as pd
import os
from constants import HEDGABLE_TOKENS, METEORA_LATEST_CSV, KRYSTAL_LATEST_CSV, HEDGE_LATEST_CSV
from datetime import datetime
import csv
import logging
import sys

# Get log directory from environment or use default
log_dir = os.getenv('LP_HEDGE_LOG_DIR', './logs')
data_dir = os.getenv('LP_HEDGE_DATA_DIR', './lp-data')

# Create directories if they don't exist
os.makedirs(log_dir, exist_ok=True)
os.makedirs(data_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'hedge_rebalancer.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def calculate_hedge_quantities():
    """Calculate total hedged quantities from Bitget positions by symbol (always negative)."""
    hedge_quantities = {symbol: 0.0 for symbol in HEDGABLE_TOKENS}
    if os.path.exists(HEDGE_LATEST_CSV):
        try:
            hedge_df = pd.read_csv(HEDGE_LATEST_CSV)
            for _, row in hedge_df.iterrows():
                symbol = row["symbol"]
                qty = float(row["quantity"] or 0)  # Negative for short positions
                if symbol in HEDGABLE_TOKENS:
                    hedge_quantities[symbol] += qty  # Accumulate negative quantities
        except Exception as e:
            logger.error(f"Error reading {HEDGE_LATEST_CSV}: {e}")
    else:
        logger.warning(f"{HEDGE_LATEST_CSV} not found.")
    return hedge_quantities

def calculate_lp_quantities():
    """Calculate total LP quantities by Bitget symbol, searching by token address."""
    lp_quantities = {symbol: 0.0 for symbol in HEDGABLE_TOKENS}
    
    # Read Meteora LP positions
    if os.path.exists(METEORA_LATEST_CSV):
        try:
            meteora_df = pd.read_csv(METEORA_LATEST_CSV)
            for _, row in meteora_df.iterrows():
                token_x = row["Token X Address"]
                token_y = row["Token Y Address"]
                qty_x = float(row["Token X Qty"] or 0)
                qty_y = float(row["Token Y Qty"] or 0)

                for symbol, info in HEDGABLE_TOKENS.items():
                    if token_x in info["addresses"]:
                        lp_quantities[symbol] += qty_x
                    if token_y in info["addresses"]:
                        lp_quantities[symbol] += qty_y
        except Exception as e:
            logger.error(f"Error reading {METEORA_LATEST_CSV}: {e}")
    else:
        logger.warning(f"{METEORA_LATEST_CSV} not found.")

    # Read Krystal LP positions
    if os.path.exists(KRYSTAL_LATEST_CSV):
        try:
            krystal_df = pd.read_csv(KRYSTAL_LATEST_CSV)
            for _, row in krystal_df.iterrows():
                token_x = row["Token X Address"]
                token_y = row["Token Y Address"]
                qty_x = float(row["Token X Qty"] or 0)
                qty_y = float(row["Token Y Qty"] or 0)

                for symbol, info in HEDGABLE_TOKENS.items():
                    if token_x in info["addresses"]:
                        lp_quantities[symbol] += qty_x
                    if token_y in info["addresses"]:
                        lp_quantities[symbol] += qty_y
        except Exception as e:
            logger.error(f"Error reading {KRYSTAL_LATEST_CSV}: {e}")
    else:
        logger.warning(f"{KRYSTAL_LATEST_CSV} not found.")
    
    return lp_quantities

def check_hedge_rebalance():
    """Compare LP quantities with absolute hedge quantities and output results to console and CSV."""
    logger.info("Starting hedge-rebalancer...")

    # Calculate quantities
    hedge_quantities = calculate_hedge_quantities()  # Negative values
    lp_quantities = calculate_lp_quantities()        # Positive values

    # Prepare rebalancing results
    rebalance_results = []
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')  # Fixed format with : instead of -

    # Compare and signal rebalance
    for symbol in HEDGABLE_TOKENS:
        hedge_qty = hedge_quantities[symbol]  # Negative (short)
        lp_qty = lp_quantities[symbol]        # Positive (long)
        abs_hedge_qty = abs(hedge_qty)       # Absolute value of short position

        # Calculate directional difference: LP vs absolute hedge qty
        difference = lp_qty - abs_hedge_qty  # Positive: under-hedged, Negative: over-hedged
        abs_difference = abs(difference)
        percentage_diff = (abs_difference / lp_qty) * 100 if lp_qty > 0 else 0

        # Skip if no LP exposure and no hedge
        if lp_qty == 0 and hedge_qty == 0:
            continue

        # Log output
        logger.info(f"Token: {symbol}")
        logger.info(f"  LP Qty: {lp_qty}, Hedged Qty: {hedge_qty} (Short: {abs_hedge_qty})")
        logger.info(f"  Difference: {difference} ({percentage_diff:.2f}% of LP)")

        # Determine rebalance action and value
        rebalance_action = "nothing"
        rebalance_value = 0.0
        
        if lp_qty > 0 and abs_difference > 0.1 * lp_qty:
            if difference > 0:
                rebalance_action = "sell"
                rebalance_value = abs_difference
                logger.warning(f"  *** REBALANCE SIGNAL: {rebalance_action} {rebalance_value:.5f} for {symbol} ***")
            else:
                rebalance_action = "buy"
                rebalance_value = abs_difference
                logger.warning(f"  *** REBALANCE SIGNAL: {rebalance_action} {rebalance_value:.5f} for {symbol} ***")
        elif lp_qty == 0 and hedge_qty != 0:
            rebalance_action = "close"
            rebalance_value = abs_hedge_qty
            logger.warning(f"  *** REBALANCE SIGNAL: {rebalance_action} for {symbol} (no LP exposure) ***")
        else:
            rebalance_action = "nothing"
            rebalance_value = 0.0
            logger.info(f"  No rebalance needed for {symbol}")

        # Add to results
        rebalance_results.append({
            "Timestamp": timestamp,
            "Token": symbol,
            "LP Qty": lp_qty,
            "Hedged Qty": hedge_qty,
            "Difference": difference,
            "Percentage Diff": round(percentage_diff, 2),
            "Rebalance Action": rebalance_action,
            "Rebalance Value": round(rebalance_value, 5)
        })

    # Write results to CSV
    if rebalance_results:
        output_dir = data_dir  # Use centralized data directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Timestamped file for history
        history_filename = f"{output_dir}/rebalancing_results_{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')}.csv"
        # Latest file (without timestamp)
        latest_filename = f"{output_dir}/rebalancing_results.csv"
        
        headers = ["Timestamp", "Token", "LP Qty", "Hedged Qty", "Difference", "Percentage Diff", "Rebalance Action", "Rebalance Value"]
        
        # Write to history file
        with open(history_filename, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rebalance_results)
        logger.info(f"Rebalancing results written to history: {history_filename}")
        
        # Write to latest file (overwrites)
        with open(latest_filename, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rebalance_results)
        logger.info(f"Latest rebalancing results written to: {latest_filename}")

    logger.info("Hedge rebalance check completed.")

if __name__ == "__main__":
    check_hedge_rebalance()