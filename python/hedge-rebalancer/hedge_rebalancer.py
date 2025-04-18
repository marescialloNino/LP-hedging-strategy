# hedge_rebalancer.py
import pandas as pd
import logging
import sys
import csv
from datetime import datetime
from common.constants import HEDGABLE_TOKENS
from common.path_config import (
    LOG_DIR, METEORA_LATEST_CSV, KRYSTAL_LATEST_CSV, HEDGING_LATEST_CSV,
    REBALANCING_HISTORY_DIR, REBALANCING_LATEST_CSV
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'hedge_rebalancer.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def calculate_hedge_quantities():
    """Calculate total hedged quantities from Bitget positions by symbol (always negative)."""
    hedge_quantities = {symbol: 0.0 for symbol in HEDGABLE_TOKENS}
    if HEDGING_LATEST_CSV.exists():
        try:
            hedge_df = pd.read_csv(HEDGING_LATEST_CSV)
            for _, row in hedge_df.iterrows():
                symbol = row["symbol"]
                qty = float(row["quantity"] or 0)  # Negative for short positions
                if symbol in HEDGABLE_TOKENS:
                    hedge_quantities[symbol] += qty
        except Exception as e:
            logger.error(f"Error reading {HEDGING_LATEST_CSV}: {e}")
    else:
        logger.warning(f"{HEDGING_LATEST_CSV} not found.")
    return hedge_quantities

def calculate_lp_quantities():
    """Calculate total LP quantities by Bitget symbol, matching addresses by chain."""
    lp_quantities = {symbol: 0.0 for symbol in HEDGABLE_TOKENS}
    
    # Read Meteora LP positions (Solana-specific, no chain column yet)
    if METEORA_LATEST_CSV.exists():
        try:
            meteora_df = pd.read_csv(METEORA_LATEST_CSV)
            for _, row in meteora_df.iterrows():
                token_x = row["Token X Address"]
                token_y = row["Token Y Address"]
                qty_x = float(row["Token X Qty"] or 0)
                qty_y = float(row["Token Y Qty"] or 0)
                chain = "solana"  # Hardcode Solana for Meteora

                for symbol, chains in HEDGABLE_TOKENS.items():
                    if chain in chains:
                        addresses = chains[chain]
                        if token_x in addresses:
                            lp_quantities[symbol] += qty_x
                        if token_y in addresses:
                            lp_quantities[symbol] += qty_y
        except Exception as e:
            logger.error(f"Error reading {METEORA_LATEST_CSV}: {e}")
    else:
        logger.warning(f"{METEORA_LATEST_CSV} not found.")

    # Read Krystal LP positions (uses Chain column)
    if KRYSTAL_LATEST_CSV.exists():
        try:
            krystal_df = pd.read_csv(KRYSTAL_LATEST_CSV)
            for _, row in krystal_df.iterrows():
                token_x = row["Token X Address"]
                token_y = row["Token Y Address"]
                qty_x = float(row["Token X Qty"] or 0)
                qty_y = float(row["Token Y Qty"] or 0)
                chain = row["Chain"].lower()  

                for symbol, chains in HEDGABLE_TOKENS.items():
                    if chain in chains:
                        addresses = chains[chain]
                        if token_x in addresses:
                            lp_quantities[symbol] += qty_x
                        if token_y in addresses:
                            lp_quantities[symbol] += qty_y
        except Exception as e:
            logger.error(f"Error reading {KRYSTAL_LATEST_CSV}: {e}")
    else:
        logger.warning(f"{KRYSTAL_LATEST_CSV} not found.")
    
    return lp_quantities

def check_hedge_rebalance():
    """Compare LP quantities with absolute hedge quantities and output results."""
    logger.info("Starting hedge-rebalancer...")
    
    hedge_quantities = calculate_hedge_quantities()
    lp_quantities = calculate_lp_quantities()

    rebalance_results = []
    timestamp_for_csv = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    timestamp_for_filename = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

    for symbol in HEDGABLE_TOKENS:
        hedge_qty = hedge_quantities[symbol]
        lp_qty = lp_quantities[symbol]
        abs_hedge_qty = abs(hedge_qty)

        difference = lp_qty - abs_hedge_qty
        abs_difference = abs(difference)
        percentage_diff = (abs_difference / lp_qty) * 100 if lp_qty > 0 else 0

        if lp_qty == 0 and hedge_qty == 0:
            continue

        logger.info(f"Token: {symbol}")
        logger.info(f"  LP Qty: {lp_qty}, Hedged Qty: {hedge_qty} (Short: {abs_hedge_qty})")
        logger.info(f"  Difference: {difference} ({percentage_diff:.2f}% of LP)")

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

        rebalance_results.append({
            "Timestamp": timestamp_for_csv,
            "Token": symbol,
            "LP Qty": lp_qty,
            "Hedged Qty": hedge_qty,
            "Difference": difference,
            "Percentage Diff": round(percentage_diff, 2),
            "Rebalance Action": rebalance_action,
            "Rebalance Value": round(rebalance_value, 5)
        })

    if rebalance_results:
        output_dir = REBALANCING_HISTORY_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        
        history_filename = output_dir / f"rebalancing_results_{timestamp_for_filename}.csv"
        latest_filename = REBALANCING_LATEST_CSV
        
        headers = ["Timestamp", "Token", "LP Qty", "Hedged Qty", "Difference", 
                  "Percentage Diff", "Rebalance Action", "Rebalance Value"]
        
        with open(history_filename, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rebalance_results)
        logger.info(f"Rebalancing results written to history: {history_filename}")
        
        with open(latest_filename, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rebalance_results)
        logger.info(f"Latest rebalancing results written to: {latest_filename}")

    logger.info("Hedge rebalance check completed.")

if __name__ == "__main__":
    check_hedge_rebalance()