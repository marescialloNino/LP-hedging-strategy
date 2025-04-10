# LP Hedging Strategy

LP Hedging Strategy is a multi-part project designed to monitor liquidity pool (LP) positions and implement automated hedging on BitGet and rebalancing strategies across various blockchain networks. The project is divided into two main components:

- **lp-monitor (TypeScript/Node.js):**  
  This module retrieves and processes LP positions from decentralized exchanges on Solana and Ethereum. It gathers data, calculates profit & loss (PnL), and generates CSV reports for tracking the latest positions and overall liquidity profiles.

- **Python Modules:**  
  Packages for hedge monitoring, hedge rebalancing, and hedge automation. 

## Features

- **Multi-Chain Support:**  
  Track LP positions on Solana (only Meteora via meteora API) and EVM chains (via Krystal API).

- **Real-Time Position Tracking:**  
  Retrieve LP positions from multiple wallet addresses, calculate PnL (only for meteora positions), and generate CSV reports.

- **Automated Hedging & Rebalancing:**  
  Python scripts to monitor, hedge, and rebalance positions automatically based on market changes.



## Installation & Setup

0. **Clone repository:**
   ```bash
   cd <your repo path>
   git clone <repo github url>

### For lp-monitor (Node.js/TypeScript)

1. **Install Dependencies:**
   ```bash
   cd lp-monitor
   npm install

2. **Build project:**
   ```bash
   npm run build

3. **Run project:**
   ```bash
   npm start

### For python/various-hedging-tools (Python)

1. **install the tools as modules from the root directory (where setup.py is located):**
   ```bash
   pip install -e .

2. **run hedging tools, from the python/ folder which is the base dir for the python packages**
   ```bash
   cd python

    2a. **run hedge-monitoring for fetching active hedge positions on Bitget:**
        ```bash
        python -m hedge_monitoring.bitget_position_fetcher

    2b. **run hedge-rebalancer to compute which positions needs rebalancing**
        ```bash
        python -m hedge_rebalancer.hedge_rebalancer

3. **run simple visualization webapp:**
    ```bash
    python -m display_results


### Configuration

1. **Configure Environment:**
   Update the configuration in config.ts or set the environment variables (wallet addresses, API keys, output folders paths etc.).

   ENV vars for lp-monitor: 
    RPC_ENDPOINT=https://rpc-proxy.segfaultx0.workers.dev
    SOLANA_WALLET_ADDRESSES=wallet1,wallet2,...
    EVM_WALLET_ADDRESSES=wallet1,wallet2,...
    KRYSTAL_CHAIN_IDS=1,137,56,...

   ENV vars for lp-monitor:
    BITGET_HEDGE1_API_KEY=...
    BITGET_HEDGE1_API_SECRET=...
    BITGET_API_PASSWORD=...

   other ENV vars:
    EXECUTION_IP=54.249.138.8
    LP_HEDGE_LOG_DIR=absolute_path\LP-hedging-strategy\logs
    LP_HEDGE_DATA_DIR=absolute_path\LP-hedging-strategy\lp-data

2. **Configure hedgeable tokens**
    To track new hedge tokens on bitget, a mapping between the bitget ticker and the various onchain addresses of the tken is needed.
    Add desired tokens to the HEDGEABLE_TOKENS dictionary in ./python/common/constants.py  with the following structure:

    "BITGET_TICKER" : {
        "chain_1" : ["address1_chain1", "address2_chain1"],
        "chain_2" : ["address2_chain1", "address2_chain2"],
    }




