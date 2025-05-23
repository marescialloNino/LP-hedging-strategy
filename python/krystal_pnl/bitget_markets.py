"""
build_valid_bitget_tickers.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Query Bitget once via ccxt, extract every market whose quote is USDT, convert
'ABC/USDT', 'ABC/USDT:USDT' → 'ABCUSDT', and save the unique list to
valid_bitget_tickers.csv
"""

import csv
import os
import ccxt
from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent           # …/python/krystal_pnl                    # LP‑hedging‑strategy
INT_DATA_DIR = MODULE_DIR / "pnl_data"                 # …/krystal_pnl/pnl_data
INT_DATA_DIR.mkdir(exist_ok=True)
CSV_FILE = INT_DATA_DIR / "valid_bitget_tickers.csv"


def build_csv(outfile: str = CSV_FILE) -> None:
    exchange = ccxt.bitget({"enableRateLimit": True})
    markets = exchange.load_markets()

    tickers = set()
    for sym in markets:
        # sym examples: 'AAVE/USDT', 'AAVE/USDT:USDT', 'BTC/USDT:USDT'
        base, _, rest = sym.partition("/")
        if not rest:
            continue
        quote = rest.split(":")[0]  # drop ':USDT'
        if quote.upper() == "USDT":
            tickers.add(f"{base.upper()}USDT")

    tickers = sorted(tickers)
    with open(outfile, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker"])
        writer.writerows([[t] for t in tickers])

    print(f"✅ Saved {len(tickers)} USDT markets to {os.path.abspath(outfile)}")


if __name__ == "__main__":
    build_csv()
