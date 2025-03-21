import asyncio
import datafeed.bitgetfeed as bg
import sys
import os
from dotenv import load_dotenv
import csv
from datetime import datetime

# Fix for Windows: Set SelectorEventLoop policy, needed for ccxt and asyncio event loop behaviour
# issue on github: https://github.com/aio-libs/aiodns/issues/86
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def fetch_and_print_positions():

    load_dotenv()
    # these are the readonly api keys
    api_key = os.getenv("BITGET_HEDGE1_API_KEY")
    api_secret = os.getenv("BITGET_HEDGE1_API_SECRET")
    api_password = os.getenv("BITGET_API_PASSWORD")
    if not all([api_key, api_secret, api_password]):
        raise ValueError("One or more required environment variables are missing.")

    # H1 is the hedging account
    market = bg.BitgetMarket(account='H1')
    
    try:
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
        history_file = "./lp-data/hedging_positions_history.csv"
        file_exists = os.path.isfile(history_file)
        with open(history_file, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader() 
            writer.writerows(position_data)

        # Write to latest CSV (overwrite mode)
        with open("./lp-data/hedging_positions_latest.csv", mode='w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(position_data)

        print("Positions:")
        if positions:
            for symbol, (qty, amount, entry_price, entry_ts) in positions.items():
                print(f"Symbol: {symbol}, Quantity: {qty}, Amount: {amount}, Entry Price: {entry_price}, Entry Time: {entry_ts}")
        else:
            print("No positions found.")
    
    finally:
        # Ensure cleanup even if an error occurs
        await market._exchange_async.close()
        print("Exchange connection closed.")

if __name__ == "__main__":
    asyncio.run(fetch_and_print_positions())