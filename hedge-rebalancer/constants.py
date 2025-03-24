# constants.py

# Hedgable tokens with addresses, symbols, and decimals
HEDGABLE_TOKENS = {

    # EVM
    "LINKUSDT": {"addresses" : ["0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39"]},
    "ETHUSDT": {"addresses" : ["0x7ceb23fd6bc0add59e62ac25578270cff1b9f619","0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"]},
    "POLUSDT": {"addresses" : ["0xcccccccccccccccccccccccccccccccccccccccc"]},
    "AAVEUSDT": {"addresses" : ["0xd6df932a45c0f255f85145f286ea0b292b21c90b"]},
     # BSC
    "BNBUSDT" : {"addresses" : ["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]},
    "CAKEUSDT" : {"addresses" : ["0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82"]},
    # Arbitrum
    "ZROUSDT" : {"addresses" : ["0x6985884c4392d348587b19cb9eaaf157f13271cd"]},
    "GMXUSDT" : {"addresses" : ["0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a"]},
    # Sonic
    # "SONICUSDT": {"addresses" : ["0x039e2fB66102314Ce7b64Ce5Ce3E5183bc94aD38"]},


    # Solana
    "SOLUSDT": {"addresses" : ["So11111111111111111111111111111111111111112"]},
    # "JUPUSDT": {"addresses" : ["JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"]},
    "GRIFFAINUSDT": {"addresses" : ["KENJSUYLASHUMfHyy5o4Hp2FdNqZg1AsUPhfH2kYvEP"]},
    "ARCUSDT": {"addresses" : ["61V8vBaqAGMpgDQi4JcAwo1dmBGHsyhzodcPqnEVpump"]},
    "FARTCOINUSDT": {"addresses" : ["9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"]},
    "AI16ZUSDT": {"addresses" : ["HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC"]},


    
    


}

# File paths (relative to lp-monitor output)
METEORA_LATEST_CSV = "./lp-data/LP_meteora_positions_latest.csv"
KRYSTAL_LATEST_CSV = "./lp-data/LP_krystal_positions_latest.csv"
HEDGE_LATEST_CSV = "./lp-data/hedging_positions_latest.csv"

METEORA_HISTORY_CSV = "./lp-data/LP_meteora_positions_history.csv"
KRYSTAL_HISTORY_CSV = "./lp-data/LP_krystal_positions_history.csv"
HEDGE_HISTORY_CSV = "./lp-data/hedging_positions_history.csv"