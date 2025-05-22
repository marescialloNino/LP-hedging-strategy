# constants.py
from typing import Dict

HEDGABLE_TOKENS = {
    # EVM

    "ETHUSDT": {
        "polygon": ["0x7ceb23fd6bc0add59e62ac25578270cff1b9f619"],
        "arbitrum": ["0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"],
        "base": ["0x4200000000000000000000000000000000000006"]
    },
    "LINKUSDT": {
        "polygon": ["0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"]
    },
    "POLUSDT": {
        "polygon": ["0xcccccccccccccccccccccccccccccccccccccccc","0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"]
    },
    "AAVEUSDT": {
        "polygon": ["0xd6df932a45c0f255f85145f286ea0b292b21c90b"]
    },
    
    "BNBUSDT": {
        "bsc": ["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"]
    },
    "CAKEUSDT": {
        "bsc": ["0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"]
    },
    
    "ZROUSDT": {
        "arbitrum": ["0x6985884c4392d348587b19cb9eaaf157f13271cd"]
    },
    "GMXUSDT": {
        "arbitrum": ["0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a"]
    },
    "SONICUSDT": {
        "sonic" : ["0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38"]
    },
    "KILOUSDT": {
        "bsc" : ["0x503fa24b7972677f00c4618e5fbe237780c1df53"]
    },
    "AIXBTUSDT": {
        "base": ["0x4f9fd6be4a90f2620860d680c0d4d5fb53d1a825"]
    },
    "KAITOUSDT": {
        "base": ["0x98d0baa52b2d063e780de12f615f963fe8537553"]
    },
    "VIRTUALUSDT": {
        "base": ["0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b"],
        "solana" : ["3iQL8BFS2vE7mww4ehAqQHAsbmRNCrPxizWAT2Zfyr9y"]
    },
    "BROCCOLIF3BUSDT": {
        "bsc": ["0x12b4356c65340fb02cdff01293f95febb1512f3b"]
    },
    "PEPEUSDT": {
        "bsc": ["0x25d887ce7a35172c62febfd67a1856f20faebb00"]
    },
    "VVVUSDT": {
        "base": ["0xacfe6019ed1a7dc6f7b508c02d1b04ec88cc21bf"]
    },
    "TOSHIUSDT": {
        "base": ["0xac1bd2486aaf3b5c0fc3fd868558b082a531b2b4"]
    },
    "ARBUSDT": {
        "arbitrum": ["0x912ce59144191c1204e64559fe8253a0e49e6548"]
    },
    "GAMEUSDT": {
        "base": ["0x1c4cca7c5db003824208adda61bd749e55f463a3"]
    },
    # Solana
    "SOLUSDT": {
        "solana": ["So11111111111111111111111111111111111111112"]
    },
    "GRIFFAINUSDT": {
        "solana": ["KENJSUYLASHUMfHyy5o4Hp2FdNqZg1AsUPhfH2kYvEP"]
    },
    "ARCUSDT": {
        "solana": ["61V8vBaqAGMpgDQi4JcAwo1dmBGHsyhzodcPqnEVpump"]
    },
    "FARTCOINUSDT": {
        "solana": ["9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"]
    },
    "AI16ZUSDT": {
        "solana": ["HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC"]
    },
    "HOUSEUSDT": {
        "solana": ["DitHyRMQiSDhn5cnKMJV2CDDt6sVct96YrECiM49pump"]
    },
    "LAUNCHCOINUSDT": {
        "solana": ["Ey59PH7Z4BFU4HjyKnyMdWt5GGN76KazTAwQihoUXRnk"]
    }
}

# map between onchain and bitget tickers 
SYMBOL_MAP: Dict[str, str] = {
    "WPOL": "POL",
    "WETH": "ETH", 
    "WBNB": "BNB",
    "WS": "SONIC", 
    "BROCCOLI": "BROCCOLIF3B", 
    "USDC.E": "USDC",
}






# File paths (relative to lp-monitor output)
METEORA_LATEST_CSV = "./lp-data/LP_meteora_positions_latest.csv"
KRYSTAL_LATEST_CSV = "./lp-data/LP_krystal_positions_latest.csv"
HEDGE_LATEST_CSV = "./lp-data/hedging_positions_latest.csv"

METEORA_HISTORY_CSV = "./lp-data/LP_meteora_positions_history.csv"
KRYSTAL_HISTORY_CSV = "./lp-data/LP_krystal_positions_history.csv"
HEDGE_HISTORY_CSV = "./lp-data/hedging_positions_history.csv"