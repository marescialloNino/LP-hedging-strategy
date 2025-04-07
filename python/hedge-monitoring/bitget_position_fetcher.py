import asyncio
from .datafeed import bitgetfeed as bg
import sys
import os
from dotenv import load_dotenv
import csv
from datetime import datetime
import logging

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
        logging.FileHandler(os.path.join(log_dir, 'bitget_position_fetcher.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Fix for Windows: Set SelectorEventLoop policy, needed for ccxt and asyncio event loop behaviour
# issue on github: https://github.com/aio-libs/aiodns/issues/86
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def fetch_and_print_positions():
    logger.info("Starting Bitget position fetcher...")
    
    load_dotenv()
    # these are the readonly api keys
    api_key = os.getenv("BITGET_HEDGE1_API_KEY")
    api_secret = os.getenv("BITGET_HEDGE1_API_SECRET")
    api_password = os.getenv("BITGET_API_PASSWORD")
    if not all([api_key, api_secret, api_password]):
        logger.error("One or more required environment variables are missing.")
        raise ValueError("One or more required environment variables are missing.")

    # H1 is the hedging account
    market = bg.BitgetMarket(account='H1')
    
    try:
        logger.info("Fetching positions from Bitget...")
        positions = await market.get_positions_async()
        
        current_time = datetime.utcnow().isoformat()
        
        position_data = [
            {
                "timestamp": current_time,
                "symbol": symbol,
                "quantity": qty,
                "amount": amount,
                "entry_price": entry_price
            }
            for symbol, (qty, amount, entry_price, entry_ts) in positions.items()
        ]

        # Define CSV headers
        headers = ["timestamp", "symbol", "quantity", "amount", "entry_price"]

        # Write to historical CSV (append mode)
        history_file = os.path.join(data_dir, "hedging_positions_history.csv")
        file_exists = os.path.isfile(history_file)
        with open(history_file, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader() 
            writer.writerows(position_data)
        logger.info(f"Appended {len(position_data)} positions to historical CSV")

        # Write to latest CSV (overwrite mode)
        latest_file = os.path.join(data_dir, "hedging_positions_latest.csv")
        with open(latest_file, mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(position_data)
        logger.info("Updated latest positions CSV")

        if positions:
            logger.info("Current positions:")
            for symbol, (qty, amount, entry_price, entry_ts) in positions.items():
                logger.info(f"Symbol: {symbol}, Quantity: {qty}, Amount: {amount}, Entry Price: {entry_price}, Entry Time: {entry_ts}")
        else:
            logger.info("No positions found.")
    
    except Exception as e:
        logger.error(f"Error fetching positions: {e}")
        raise
    finally:
        # Ensure cleanup even if an error occurs
        await market._exchange_async.close()
        logger.info("Exchange connection closed.")

if __name__ == "__main__":
    asyncio.run(fetch_and_print_positions())