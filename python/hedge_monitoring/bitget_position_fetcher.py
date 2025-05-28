import asyncio
from .datafeed import bitgetfeed as bg
import sys
import os
from dotenv import load_dotenv
import csv
from datetime import datetime
import logging
import json
from pathlib import Path
from common.path_config import LOG_DIR, HEDGING_HISTORY_CSV, HEDGING_LATEST_CSV, HEDGE_ERROR_FLAGS_PATH
from common.bot_reporting import TGMessenger

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

# Ensure lp-data directory exists
def ensure_data_directory():
    data_dir = HEDGE_ERROR_FLAGS_PATH.parent
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating data directory {data_dir}: {str(e)}")

# Load existing error flags
def load_error_flags():
    """Load existing error flags or return defaults."""
    try:
        if HEDGE_ERROR_FLAGS_PATH.exists():
            with HEDGE_ERROR_FLAGS_PATH.open('r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error reading error flags from {HEDGE_ERROR_FLAGS_PATH}: {str(e)}")
    return {
        "HEDGING_FETCHING_BITGET_ERROR": False,
        "last_updated_hedge": "",
        "bitget_error_message": ""
    }

# Update error flags JSON
def update_error_flags(flags: dict):
    """Update hedging error flags JSON file."""
    try:
        # Overwrite with new flags
        with HEDGE_ERROR_FLAGS_PATH.open('w') as f:
            json.dump(flags, f, indent=4)
        logger.info(f"Updated hedging error flags: {json.dumps(flags)}")
    except Exception as e:
        logger.error(f"Error writing hedging error flags to {HEDGE_ERROR_FLAGS_PATH}: {str(e)}")

# Fix for Windows event loop
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
        error_msg = f"Error fetching funding rate for {symbol}: {str(e)}"
        logger.error(error_msg)
        error_flags = load_error_flags()
        error_flags.update({
            "HEDGING_FETCHING_BITGET_ERROR": True,
            "bitget_error_message": error_msg
        })
        update_error_flags(error_flags)
        return 0.0

async def fetch_and_print_positions():
    logger.info("Starting Bitget position fetcher...")
    
    # Ensure data directory exists
    ensure_data_directory()
    
    # Initialize error flags
    error_flags = load_error_flags()
    error_flags.update({
        "HEDGING_FETCHING_BITGET_ERROR": False,
        "bitget_error_message": ""
    })
    update_error_flags(error_flags)
    
    load_dotenv()
    api_key = os.getenv("BITGET_HEDGE1_API_KEY")
    api_secret = os.getenv("BITGET_HEDGE1_API_SECRET")
    api_password = os.getenv("BITGET_API_PASSWORD")
    if not all([api_key, api_secret, api_password]):
        error_msg = "One or more required environment variables are missing."
        logger.error(error_msg)
        error_flags = load_error_flags()
        error_flags.update({
            "HEDGING_FETCHING_BITGET_ERROR": True,
            "bitget_error_message": error_msg
        })
        update_error_flags(error_flags)
        raise ValueError(error_msg)

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
            
            # Send alert if funding rate is less than -10 bips
            if funding_rate * 10000 <= -9: 
                try:
                    alert_msg = (
                        f"⚠️ Bitget Funding Rate Alert ⚠️\n"
                        f"Symbol: {symbol}\n"
                        f"Funding Rate: {funding_rate* 10000:.1f} bips\n"
                        f"Hedge position USD amount: {(amount):.2f}\n"
                    )
                    TGMessenger.send(alert_msg, 'LP eagle') 
                except Exception as e:
                    logger.error(f"Failed to send Telegram alert for {symbol}: {str(e)}")

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

        # Update last_updated_hedge on successful completion
        error_flags = load_error_flags()
        error_flags.update({
            "HEDGING_FETCHING_BITGET_ERROR": False,
            "last_updated_hedge": current_time,
            "bitget_error_message": ""
        })
        update_error_flags(error_flags)

    except Exception as e:
        error_msg = f"Error fetching positions: {str(e)}"
        logger.error(error_msg)
        error_flags = load_error_flags()
        error_flags.update({
            "HEDGING_FETCHING_BITGET_ERROR": True,
            "bitget_error_message": error_msg
        })
        update_error_flags(error_flags)
        raise
    finally:
        await market._exchange_async.close()
        logger.info("Exchange connection closed.")

if __name__ == "__main__":
    asyncio.run(fetch_and_print_positions())