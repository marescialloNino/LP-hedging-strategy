# constants.py
from typing import Dict

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