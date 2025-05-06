import asyncio
from .datafeed import bitgetfeed as bg
import sys
import os
from dotenv import load_dotenv
import csv
from datetime import datetime
import logging
from common.path_config import LOG_DIR, HEDGING_HISTORY_CSV, HEDGING_LATEST_CSV

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'bitget_position_fetcher.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Fix for Windows: opportunistic SelectorEventLoop policy
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def fetch_4hr_funding_rate(market, symbol):
    """Fetch the current funding rate for a symbol."""
    try:
        # Manually convert raw symbol (e.g., 'POLUSDT') to unified CCXT format (e.g., 'POL/USDT:USDT')
        if symbol.endswith('USDT'):
            base = symbol[:-4]  # Remove 'USDT' suffix
            unified_symbol = f"{base}/USDT:USDT"
        else:
            logger.warning(f"{symbol} does not end with 'USDT'; cannot convert to unified format")
            return 0.0

        logger.info(f"Converted {symbol} to unified symbol: {unified_symbol}")

        # Fetch funding rate using unified symbol
        funding_data = await market.read_funding(symbol=unified_symbol)
        if not funding_data:
            logger.warning(f"No funding rate data found for {unified_symbol}")
            return 0.0

        # Extract the funding rate
        funding_rate = funding_data.get('fundingRate', 0.0)
        logger.info(f"Funding rate for {unified_symbol}: {funding_rate}")
        return float(funding_rate)
    
    except Exception as e:
        logger.error(f"Error fetching funding rate for {symbol}: {e}")
        return 0.0

async def fetch_and_print_positions():
    logger.info("Starting Bitget position fetcher...")
    
    load_dotenv()
    api_key = os.getenv("BITGET_HEDGE1_API_KEY")
    api_secret = os.getenv("BITGET_HEDGE1_API_SECRET")
    api_password = os.getenv("BITGET_API_PASSWORD")
    if not all([api_key, api_secret, api_password]):
        logger.error("One or more required environment variables are missing.")
        raise ValueError("One or more required environment variables are missing.")

    market = bg.BitgetMarket(account='H1')
    
    try:
        logger.info("Fetching positions from Bitget...")
        # Process positions
        positions = await market.get_positions_async()
        
        current_time = datetime.utcnow().isoformat()
        position_data = []
        for symbol, (qty, amount, entry_price, entry_ts) in positions.items():
            # Fetch current funding rate
            funding_rate = await fetch_4hr_funding_rate(market, symbol)
            
            position_data.append({
                "timestamp": current_time,
                "symbol": symbol,
                "quantity": qty,
                "amount": amount,
                "entry_price": entry_price,
                "funding_rate": funding_rate
            })

        # Write to historical CSV (append mode)
        file_exists = HEDGING_HISTORY_CSV.is_file()
        with HEDGING_HISTORY_CSV.open(mode='a', newline='') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["timestamp", "symbol", "quantity", "amount", "entry_price", "funding_rate"]
            )
            if not file_exists:
                writer.writeheader()
            writer.writerows(position_data)
        logger.info(f"Appended {len(position_data)} positions to historical CSV")

        # Write to latest CSV (overwrite mode)
        with HEDGING_LATEST_CSV.open(mode='w', newline='') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["timestamp", "symbol", "quantity", "amount", "entry_price", "funding_rate"]
            )
            writer.writeheader()
            writer.writerows(position_data)
        logger.info("Updated latest positions CSV")

    except Exception as e:
        logger.error(f"Error fetching positions: {e}")
        raise
    finally:
        await market._exchange_async.close()
        logger.info("Exchange connection closed.")

if __name__ == "__main__":
    asyncio.run(fetch_and_print_positions())