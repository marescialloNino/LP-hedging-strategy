# constants.py

# Hedgable tokens with addresses, symbols, and decimals
HEDGABLE_TOKENS = {

    "LINKUSDT": {"addresses" : ["0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"]},
    "ETHUSDT": {"addresses" : ["0x7ceb23fd6bc0add59e62ac25578270cff1b9f619"]},
    "POLUSDT": {"addresses" : ["0xcccccccccccccccccccccccccccccccccccccccc"]},
    "AAVEUSDT": {"addresses" : ["0xd6df932a45c0f255f85145f286ea0b292b21c90b"]},
    "SOLUSDT": {"addresses" : ["So11111111111111111111111111111111111111112"]},
    # "JUPUSDT": {"addresses" : ["JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"]},
    "GRIFFAINUSDT": {"addresses" : ["KENJSUYLASHUMfHyy5o4Hp2FdNqZg1AsUPhfH2kYvEP"]},
    "ARCUSDT": {"addresses" : ["61V8vBaqAGMpgDQi4JcAwo1dmBGHsyhzodcPqnEVpump"]},
    "AI16ZUSDT": {"addresses" : ["HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC"]},

}

# File paths (relative to lp-monitor output)
METEORA_LATEST_CSV = "./lp-data/LP_meteora_positions_latest.csv"
KRYSTAL_LATEST_CSV = "./lp-data/LP_krystal_positions_latest.csv"
HEDGE_LATEST_CSV = "./lp-data/hedging_positions_latest.csv"

METEORA_HISTORY_CSV = "./lp-data/LP_meteora_positions_history.csv"
KRYSTAL_HISTORY_CSV = "./lp-data/LP_krystal_positions_history.csv"
HEDGE_HISTORY_CSV = "./lp-data/hedging_positions_history.csv"