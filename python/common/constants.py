# constants.py
from typing import Dict
import json
import logging
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# Path to ticker mappings
CONFIG_DIR = Path("config")
TICKER_MAPPINGS_PATH = CONFIG_DIR / "ticker_mappings.json"

# map between onchain and bitget tickers 
# put in here notable exception between onchain symbols and how to map them to bitget symbols
# wrapped version of tokens on other chains, strange symbols, etc.
SYMBOL_MAP: Dict[str, str] = {
    "WPOL": "POL",
    "WETH": "ETH", 
    "WBNB": "BNB",
    "WS": "SONIC", 
    "BROCCOLI": "BROCCOLIF3B", 
    "USDC.E": "USDC",
}

BITGET_TOKENS_WITH_FACTOR_1000: Dict[str, str] = {
    "XEC": "1000XEC",
    "BONK": "1000BONK", 
    "SATS": "1000SATS",
    "RATS": "1000RATS", 
    "CAT": "1000CAT"
}

BITGET_TOKENS_WITH_FACTOR_10000: Dict[str, str] = {}

