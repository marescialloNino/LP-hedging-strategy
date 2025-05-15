
#!/usr/bin/env python3
# display_results.py

import os
import pandas as pd
import asyncio
import numpy as np
import json
import sys
import logging
import uuid
import subprocess
import atexit
from pathlib import Path
from common.path_config import (
    REBALANCING_LATEST_CSV, KRYSTAL_LATEST_CSV, METEORA_LATEST_CSV, HEDGING_LATEST_CSV, METEORA_PNL_CSV, KRYSTAL_POOL_PNL_CSV, LOG_DIR, ROOT_DIR
)

# Fix for Windows event loop issue
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

main_loop = asyncio.get_event_loop()

from pywebio import start_server
from pywebio.output import *
from pywebio.session import run_async  # Import run_async for session context

# Import from hedge-automation folder
from hedge_automation.data_handler import BrokerHandler
from hedge_automation.hedge_orders_sender import BitgetOrderSender 

from common.constants import HEDGABLE_TOKENS

# Error JSON paths
HEDGE_ERROR_FLAGS_PATH = Path(LOG_DIR) / 'hedge_fetching_errors.json'
LP_ERROR_FLAGS_PATH = Path(LOG_DIR) / 'lp_fetching_errors.json'

# Set up logging to output to terminal and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'display_results.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


async def run_shell_script(script_path):
    try:
        os.chmod(script_path, 0o755)
        process = await asyncio.create_subprocess_exec(
            script_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=os.path.dirname(script_path) or '.'
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            logger.info(f"Successfully executed {script_path}: {stdout.decode()}")
            return True, stdout.decode()
        else:
            logger.error(f"Failed to execute {script_path}: {stderr.decode()}")
            return False, stderr.decode()
    except Exception as e:
        logger.error(f"Exception running {script_path}: {str(e)}")
        return False, str(e)


# Set up BrokerHandler and BitgetOrderSender globally in real mode
params = {
    'exchange_trade': 'bitget',
    'account_trade': 'H1',
    'send_orders': 'bitget'  
}
end_point = BrokerHandler.build_end_point('bitget', account='H1')
bh = BrokerHandler(
    market_watch='bitget',
    strategy_param=params,
    end_point_trade=end_point,
    logger_name='bitget_order_sender'
)
order_sender = BitgetOrderSender(bh)

async def execute_hedge_trade(token, rebalance_value):
    logger = logging.getLogger('hedge_execution')
    logger.info(f"Hedge button pressed for token: {token}, rebalance_value: {rebalance_value}")
    print(f"DEBUG: Executing hedge for {token}", flush=True)
    
    order_size = abs(rebalance_value)
    direction = 1 if rebalance_value > 0 else -1
    ticker = f"{token}USDT"
    logger.info(f"Sending order for ticker: {ticker} with order_size: {order_size} and direction: {direction}")
    
    try:
        result = await order_sender.send_order(ticker, direction, order_size)
        if isinstance(result, tuple) and len(result) == 2:
            success, request = result
            logger.info(f"Result from send_order: success={success}, request={request}")
            
            if success:
                logger.info("Hedge order request built and sent successfully")
                return {
                    'success': True,
                    'token': token,
                    'request': request
                }
            else:
                logger.error(f"Failed to send hedge order for {token}")
                return {'success': False, 'token': token}
        else:
            logger.error(f"Unexpected return value from send_order for {token}: {result}")
            return {'success': False, 'token': token}
    except Exception as e:
        logger.error(f"Exception in execute_hedge_trade for {token}: {str(e)}")
        return {'success': False, 'token': token}

def truncate_wallet(wallet):
    return f"{wallet[:5]}..." if isinstance(wallet, str) and len(wallet) > 5 else wallet

def calculate_token_usd_value(token, krystal_df=None, meteora_df=None, use_krystal=True, use_meteora=True):
    """
    Calculate total USD value and quantity for a token based on its addresses from HEDGABLE_TOKENS,
    matching chain-specific addresses for Krystal and Solana addresses for Meteora. Returns 0 if
    the token has positions in an affected protocol.

    Args:
        token (str): Bitget ticker without USDT (e.g., "ETH", "BNB")
        krystal_df (pd.DataFrame): Krystal LP positions
        meteora_df (pd.DataFrame): Meteora LP positions
        use_krystal (bool): Include Krystal data in quantity computation if available
        use_meteora (bool): Include Meteora data in quantity computation if available

    Returns:
        tuple: (total_usd, total_qty, has_krystal, has_meteora)
            - total_usd (float): Total USD value of the token (0 if affected protocol has positions)
            - total_qty (float): Total quantity of the token (0 if affected protocol has positions)
            - has_krystal (bool): True if token has Krystal positions
            - has_meteora (bool): True if token has Meteora positions
    """
    total_usd = 0.0
    total_qty = 0.0
    has_krystal = False
    has_meteora = False
    ticker = f"{token}USDT"
    if ticker not in HEDGABLE_TOKENS:
        logger.warning(f"Token {ticker} not found in HEDGABLE_TOKENS. Returning 0 USD.")
        return total_usd, total_qty, has_krystal, has_meteora

    token_info = HEDGABLE_TOKENS[ticker]

    # Check for Krystal positions (multi-chain)
    if krystal_df is not None and not krystal_df.empty:
        for chain, addresses in token_info.items():
            if chain == "solana":
                continue  # Skip Solana for Krystal
            addresses = [addr.lower() for addr in addresses]
            chain_matches = krystal_df[krystal_df["Chain"].str.lower() == chain.lower()]
            for _, row in chain_matches.iterrows():
                token_x_addr = row["Token X Address"].lower() if pd.notna(row["Token X Address"]) else ""
                token_y_addr = row["Token Y Address"].lower() if pd.notna(row["Token Y Address"]) else ""
                if token_x_addr in addresses or token_y_addr in addresses:
                    has_krystal = True
                    break
            if has_krystal:
                break

    # Check for Meteora positions (Solana-specific)
    if meteora_df is not None and not meteora_df.empty:
        solana_addresses = token_info.get("solana", [])
        solana_addresses = [addr.lower() for addr in solana_addresses]
        for _, row in meteora_df.iterrows():
            token_x_addr = row["Token X Address"].lower() if pd.notna(row["Token X Address"]) else ""
            token_y_addr = row["Token Y Address"].lower() if pd.notna(row["Token Y Address"]) else ""
            if token_x_addr in solana_addresses or token_y_addr in solana_addresses:
                has_meteora = True
                break
    elif "solana" in token_info:
        # If meteora_df is unavailable but token has Solana addresses, assume it may have Meteora positions
        has_meteora = True

    # Return 0 if token has positions in an affected protocol
    if (not use_krystal and has_krystal) or (not use_meteora and has_meteora):
        return 0.0, 0.0, has_krystal, has_meteora

    # Compute LP data if no errors or token is unaffected
    if use_krystal and has_krystal and krystal_df is not None and not krystal_df.empty:
        for chain, addresses in token_info.items():
            if chain == "solana":
                continue
            addresses = [addr.lower() for addr in addresses]
            chain_matches = krystal_df[krystal_df["Chain"].str.lower() == chain.lower()]
            token_x_matches = chain_matches[chain_matches["Token X Address"].str.lower().isin(addresses)]
            for _, row in token_x_matches.iterrows():
                total_usd += float(row["Token X USD Amount"]) if pd.notna(row["Token X USD Amount"]) else 0
                total_qty += float(row["Token X Qty"]) if pd.notna(row["Token X Qty"]) else 0
            token_y_matches = chain_matches[chain_matches["Token Y Address"].str.lower().isin(addresses)]
            for _, row in token_y_matches.iterrows():
                total_usd += float(row["Token Y USD Amount"]) if pd.notna(row["Token Y USD Amount"]) else 0
                total_qty += float(row["Token Y Qty"]) if pd.notna(row["Token Y Qty"]) else 0

    if use_meteora and has_meteora and meteora_df is not None and not meteora_df.empty:
        solana_addresses = token_info.get("solana", [])
        solana_addresses = [addr.lower() for addr in solana_addresses]
        token_x_matches = meteora_df[meteora_df["Token X Address"].str.lower().isin(solana_addresses)]
        for _, row in token_x_matches.iterrows():
            total_usd += float(row["Token X USD Amount"]) if pd.notna(row["Token X USD Amount"]) else 0
            total_qty += float(row["Token X Qty"]) if pd.notna(row["Token X Qty"]) else 0
        token_y_matches = meteora_df[meteora_df["Token Y Address"].str.lower().isin(solana_addresses)]
        for _, row in token_y_matches.iterrows():
            total_usd += float(row["Token Y USD Amount"]) if pd.notna(row["Token Y USD Amount"]) else 0
            total_qty += float(row["Token Y Qty"]) if pd.notna(row["Token Y Qty"]) else 0

    return total_usd, total_qty, has_krystal, has_meteora

async def main():
    put_markdown("# Hedging Dashboard 💸 🧪 👽")
    
    async def handle_run_workflow():
        script_path = os.path.join(ROOT_DIR, 'run_workflow.sh')
        logger.info(f"Run Workflow button clicked, executing {script_path}")
        toast("Running workflow...could take a while, you can check BTC dominance in the meantime 🟠", duration=10, color="warning")
        success, output = await run_shell_script(script_path)
        if success:
            toast("Workflow executed successfully", duration=5, color="success")
        else:
            toast(f"Failed to execute workflow: {output}", duration=5, color="error")
    put_buttons(
        [{'label': 'Run Workflow 🚀', 'value': 'run_workflow', 'color': 'primary'}],
        onclick=lambda _: run_async(handle_run_workflow()))

    hedge_error_flags = {}
    lp_error_flags = {}
    has_error = False
    error_messages = []
    krystal_error = False
    meteora_error = False
    hedging_error = False

    try:
        if HEDGE_ERROR_FLAGS_PATH.exists():
            with HEDGE_ERROR_FLAGS_PATH.open('r') as f:
                hedge_error_flags = json.load(f)
                if hedge_error_flags.get("HEDGING_FETCHING_BITGET_ERROR", False):
                    has_error = True
                    hedging_error = True
                    error_messages.append("Failed to fetch Bitget hedging data")
                if "last_updated_hedge" not in hedge_error_flags:
                    logger.warning("last_updated_hedge missing in hedge_fetching_errors.json")
        else:
            logger.warning(f"Hedging error flags file not found: {HEDGE_ERROR_FLAGS_PATH}")
            has_error = True
            hedging_error = True
            error_messages.append("Hedging error flags file missing")
    except Exception as e:
        logger.error(f"Error reading hedging error flags: {str(e)}")
        has_error = True
        hedging_error = True
        error_messages.append("Error reading hedging error flags")

    try:
        if LP_ERROR_FLAGS_PATH.exists():
            with LP_ERROR_FLAGS_PATH.open('r') as f:
                lp_error_flags = json.load(f)
                if lp_error_flags.get("LP_FETCHING_KRYSTAL_ERROR", False):
                    has_error = True
                    krystal_error = True
                    error_messages.append("Failed to fetch Krystal LP data")
                if lp_error_flags.get("LP_FETCHING_METEORA_ERROR", False):
                    has_error = True
                    meteora_error = True
                    error_messages.append("Failed to fetch Meteora LP data")
                if "last_meteora_lp_update" not in lp_error_flags:
                    logger.warning("last_meteora_lp_update missing in lp_fetching_errors.json")
                if "last_krystal_lp_update" not in lp_error_flags:
                    logger.warning("last_krystal_lp_update missing in lp_fetching_errors.json")
        else:
            logger.warning(f"LP error flags file not found: {LP_ERROR_FLAGS_PATH}")
            has_error = True
            krystal_error = True
            meteora_error = True
            error_messages.append("LP error flags file missing")
    except Exception as e:
        logger.error(f"Error reading LP error flags: {str(e)}")
        has_error = True
        krystal_error = True
        meteora_error = True
        error_messages.append("Error reading LP error flags")

    if has_error:
        error_text = "\n".join([f"- {msg}" for msg in error_messages])
        put_error(f"Data Fetching Errors:\n{error_text}", scope=None)
        logger.info(f"Displayed error messages: {error_text}")
    else:
        logger.info("No data fetching errors detected")

    csv_files = {
        "Rebalancing": REBALANCING_LATEST_CSV,
        "Krystal": KRYSTAL_LATEST_CSV,
        "Meteora": METEORA_LATEST_CSV,
        "Hedging": HEDGING_LATEST_CSV,
        "Meteora PnL": METEORA_PNL_CSV,
        "Krystal PnL": KRYSTAL_POOL_PNL_CSV,          
    }
    dataframes = {}
    for name, path in csv_files.items():
        if not os.path.exists(path):
            put_error(f"Error: {path} not found!")
            logger.warning(f"CSV file not found: {path}")
            continue
        try:
            dataframes[name] = pd.read_csv(path)
            logger.info(f"Loaded CSV: {path}")
            if name == "Krystal" and krystal_error:
                logger.warning(f"Krystal CSV {path} may be stale due to LP fetching error")
            if name == "Meteora" and meteora_error:
                logger.warning(f"Meteora CSV {path} may be stale due to LP fetching error")
            if name == "Hedging" and hedging_error:
                logger.warning(f"Hedging CSV {path} may be stale due to hedging fetching error")
        except Exception as e:
            logger.error(f"Error reading CSV {path}: {str(e)}")
            put_error(f"Error reading {name} CSV: {str(e)}")

    put_markdown("## Wallet Positions")
    meteora_updated = lp_error_flags.get("last_meteora_lp_update", "Not available")
    krystal_updated = lp_error_flags.get("last_krystal_lp_update", "Not available")
    put_markdown(f"**Last Meteora LP Update:** {meteora_updated}  \n**Last Krystal LP Update:** {krystal_updated}")

    wallet_headers = [
        "Source", "Wallet", "Chain", "Protocol", "Pair", "In Range", "Fee APR", "Initial USD", "Present USD",
        "Token X Qty", "Token Y Qty", "Current Price", "Min Price", "Max Price", "Pool Address"
    ]
    wallet_data = []

    if "Krystal" in dataframes:
        krystal_df = dataframes["Krystal"]
        ticker_source = "symbol" if "Token X Symbol" in krystal_df.columns and "Token X Symbol" in krystal_df.columns else "address"
        for _, row in krystal_df.iterrows():
            pair_ticker = (f"{row['Token X Symbol']}-{row['Token Y Symbol']}" if ticker_source == "symbol" 
                          else f"{row['Token X Address'][:5]}...-{row['Token Y Address'][:5]}...")
            wallet_data.append([
                "Krystal",
                truncate_wallet(row["Wallet Address"]),
                row["Chain"],
                row["Protocol"],
                pair_ticker,
                "Yes" if row["Is In Range"] else "No",
                f"{row['Fee APR']:.2%}" if pd.notna(row["Fee APR"]) else "N/A",
                f"{row['Initial Value USD']:.2f}" if pd.notna(row["Initial Value USD"]) else "N/A",
                f"{row['Actual Value USD']:.2f}" if pd.notna(row["Actual Value USD"]) else "N/A",
                row["Token X Qty"],
                row["Token Y Qty"],
                f"{row['Current Price']:.6f}" if pd.notna(row["Current Price"]) else "N/A",
                f"{row['Min Price']:.6f}" if pd.notna(row["Min Price"]) else "N/A",
                f"{row['Max Price']:.6f}" if pd.notna(row["Max Price"]) else "N/A",
                row["Pool Address"]
            ])

    if "Meteora" in dataframes:
        meteora_df = dataframes["Meteora"]
        for _, row in meteora_df.iterrows():
            pair_ticker = f"{row['Token X Symbol']}-{row['Token Y Symbol']}"
            qty_x = float(row["Token X Qty"]) if pd.notna(row["Token X Qty"]) else 0
            qty_y = float(row["Token Y Qty"]) if pd.notna(row["Token Y Qty"]) else 0
            price_x = float(row["Token X Price USD"]) if pd.notna(row["Token X Price USD"]) else 0
            price_y = float(row["Token Y Price USD"]) if pd.notna(row["Token Y Price USD"]) else 0
            present_usd = (qty_x * price_x) + (qty_y * price_y)
            wallet_data.append([
                "Meteora",
                truncate_wallet(row["Wallet Address"]),
                "Solana",
                "Meteora",
                pair_ticker,
                "Yes" if row["Is In Range"] else "No",
                "N/A",
                "N/A",
                f"{present_usd:.2f}",
                row["Token X Qty"],
                row["Token Y Qty"],
                "N/A",
                f"{row['Lower Boundary']:.6f}" if pd.notna(row["Lower Boundary"]) else "N/A",
                f"{row['Upper Boundary']:.6f}" if pd.notna(row["Upper Boundary"]) else "N/A",
                row["Pool Address"]
            ])

    if wallet_data:
        put_table(wallet_data, header=wallet_headers)
    else:
        put_text("No wallet positions found in Krystal or Meteora CSVs.")

    put_markdown("# LP positions PnL")

    async def handle_calculate_pnl():
        script_path = os.path.join(ROOT_DIR, 'run_pnl_calculations.sh')
        logger.info(f"Calculate PnL button clicked, executing {script_path}")
        toast("Running pnl calculations...could take a (long) while, you can find some new shitcoins in the meanitme 📈", duration=10, color="warning")
        success, output = await run_shell_script(script_path)
        if success: 
            toast("PnL calculations executed successfully, how did it go? can you buy a Lambo 🚗 or a scooter 🛵?", duration=10, color="success")
        else:
            toast(f"Failed to execute PnL calculations: {output}", duration=5, color="error")
    put_buttons(
        [{'label': 'Calculate PnL 💰', 'value': 'calculate_pnl', 'color': 'primary'}],
        onclick=lambda _: run_async(handle_calculate_pnl())
    )
    if "Meteora PnL" in dataframes:   

        put_markdown("## Meteora Positions PnL")
        pnl_headers = [
            "Timestamp", "Owner", "Pair", "Realized PNL (USD)", "Unrealized PNL (USD)", "Net PNL (USD)",
            "Realized PNL (Token B)", "Unrealized PNL (Token B)", "Net PNL (Token B)", "Position ID", "Pool Address"
        ]
        pnl_data = []
        meteora_pnl_df = dataframes["Meteora PnL"]
        for _, row in meteora_pnl_df.iterrows():
            pair = f"{row['Token X Symbol']}-{row['Token Y Symbol']}"
            pnl_data.append([
                row["Timestamp"],
                truncate_wallet(row["Owner"]),
                pair,
                f"{row['Realized PNL (USD)']:.2f}",
                f"{row['Unrealized PNL (USD)']:.2f}",
                f"{row['Net PNL (USD)']:.2f}",
                f"{row['Realized PNL (Token B)']:.2f}",
                f"{row['Unrealized PNL (Token B)']:.2f}",
                f"{row['Net PNL (Token B)']:.2f}",
                row["Position ID"],
                row["Pool Address"]
            ])
        if pnl_data:
            put_table(pnl_data, header=pnl_headers)
        else:
            put_text("No PnL data found in Meteora PnL CSV.")

    if "Krystal PnL" in dataframes:
        put_markdown("## Krystal Positions PnL by Pool (Open Pools)")
        k_pnl_df = dataframes["Krystal PnL"].copy()
        for col in ["earliest_createdTime", "hold_pnl_usd", "lp_minus_hold_usd", "lp_pnl_usd"]:
            if col not in k_pnl_df.columns:
                k_pnl_df[col] = np.nan
        pnl_headers = [
            "Chain", "Owner","Pair",
            "First Deposit",
            "LP PnL (USD)","LP TokenB PnL", "50-50 Hold PnL (USD)", "Compare With Hold",
            "Pool Address"
        ]
        pnl_rows = []
        for _, r in k_pnl_df.iterrows():
            pair = f"{r['tokenA_symbol']}-{r['tokenB_symbol']}"
            pnl_rows.append([
                r["chainName"],
                truncate_wallet(r["userAddress"]),
                pair,
                r["earliest_createdTime"],
                f"{r['lp_pnl_usd']:.2f}" if pd.notna(r['lp_pnl_usd']) else "N/A",
                f"{r['lp_pnl_tokenB']:.5f}" if pd.notna(r['lp_pnl_tokenB']) else "N/A",
                f"{r['hold_pnl_usd']:.2f}" if pd.notna(r['hold_pnl_usd']) else "N/A",
                f"{r['lp_minus_hold_usd']:.2f}" if pd.notna(r['lp_minus_hold_usd']) else "N/A",
                r["poolAddress"],
            ])
        put_table(pnl_rows, header=pnl_headers)

    if "Rebalancing" in dataframes or "Hedging" in dataframes:
        put_markdown("# Hedging Dashboard")
        meteora_updated = lp_error_flags.get("last_meteora_lp_update", "Not available")
        krystal_updated = lp_error_flags.get("last_krystal_lp_update", "Not available")
        hedge_updated = hedge_error_flags.get("last_updated_hedge", "Not available")
        put_markdown(f"**Last Meteora LP Update:** {meteora_updated}  \n**Last Krystal LP Update:** {krystal_updated}  \n**Last Hedge Data Update:** {hedge_updated}")
        logger.info(f"Displayed timestamps: Meteora={meteora_updated}, Krystal={krystal_updated}, Hedge={hedge_updated}")

        put_buttons(
            [{'label': 'Close All Hedges 🦍', 'value': 'all', 'color': 'danger'}],
            onclick=lambda _: run_async(handle_close_all_hedges())
        )

        token_data = []
        # Always pass DataFrames for position checking, even if there's an error
        krystal_df = dataframes.get("Krystal")
        meteora_df = dataframes.get("Meteora")
        def strip_usdt(token):
            return token.replace("USDT", "").strip() if isinstance(token, str) else token

        hedge_processing = {}

        async def handle_hedge_click(token, rebalance_value, action):
            logger = logging.getLogger('hedge_execution')
            logger.info(f"Handling hedge click for {token} with rebalance_value {rebalance_value}, action {action}")
            
            if hedge_processing.get(token, False):
                toast(f"Hedge already in progress for {token}", duration=5, color="warning")
                return

            hedge_processing[token] = True
            try:
                # Adjust sign based on action
                signed_rebalance_value = rebalance_value if action == "buy" else -rebalance_value if action == "sell" else 0.0
                task = asyncio.create_task(execute_hedge_trade(token, signed_rebalance_value))
                result = await task
                
                if result['success']:
                    put_markdown(f"### Hedge Order Request for {result['token']}")
                    put_code(json.dumps(result['request'], indent=2), language='json')
                    toast(f"Hedge trade triggered for {result['token']}", duration=5, color="success")
                else:
                    toast(f"Failed to generate hedge order for {result['token']}", duration=5, color="error")
            except Exception as e:
                logger.error(f"Exception in handle_hedge_click for {token}: {str(e)}")
                toast(f"Error processing hedge for {token}", duration=5, color="error")
            finally:
                hedge_processing[token] = False

        async def handle_close_hedge(token, hedged_qty):
            logger = logging.getLogger('hedge_execution')
            logger.info(f"Handling close hedge for {token} with hedged_qty {hedged_qty}")
            
            if hedge_processing.get(token, False):
                toast(f"Close hedge already in progress for {token}", duration=5, color="warning")
                return

            hedge_processing[token] = True
            try:
                close_qty = -hedged_qty
                task = asyncio.create_task(execute_hedge_trade(token, close_qty))
                result = await task
                
                if result['success']:
                    put_markdown(f"### Close Hedge Order Request for {result['token']}")
                    put_code(json.dumps(result['request'], indent=2), language='json')
                    toast(f"Close hedge triggered for {result['token']}", duration=5, color="success")
                    if "Hedging" in dataframes:
                        hedging_df = dataframes["Hedging"]
                        ticker = f"{token}USDT"
                        hedging_df.loc[hedging_df["symbol"] == ticker, "quantity"] = 0
                        hedging_df.loc[hedging_df["symbol"] == ticker, "amount"] = 0
                        hedging_df.loc[hedging_df["symbol"] == ticker, "funding_rate"] = 0
                        hedging_df.to_csv(HEDGING_LATEST_CSV, index=False)
                        logger.info(f"Updated {HEDGING_LATEST_CSV} for {ticker}")
                else:
                    toast(f"Failed to close hedge for {result['token']}", duration=5, color="error")
            except Exception as e:
                logger.error(f"Exception in handle_close_hedge for {token}: {str(e)}")
                toast(f"Error processing close hedge for {token}", duration=5, color="error")
            finally:
                hedge_processing[token] = False

        async def handle_close_all_hedges():
            logger = logging.getLogger('hedge_execution')
            logger.info("Handling close all hedges")
            
            if any(hedge_processing.values()):
                toast("Hedge or close operation in progress, please wait", duration=5, color="warning")
                return

            if hedging_error:
                toast("Cannot close hedges due to hedging data fetch error", duration=5, color="error")
                logger.warning("Skipped close all hedges due to hedging_error")
                return

            if "Rebalancing" in dataframes:
                rebalancing_df = dataframes["Rebalancing"]
                token_agg = rebalancing_df.groupby("Token").agg({
                    "LP Qty": "sum",
                    "Hedged Qty": "sum",
                    "Rebalance Value": "sum",
                    "Rebalance Action": "first"
                }).reset_index()
                
                if "Hedging" in dataframes:
                    hedging_df = dataframes["Hedging"]
                    hedging_agg = hedging_df.groupby("symbol").agg({
                        "quantity": "sum",
                        "amount": "sum",
                        "funding_rate": "mean"
                    }).reset_index().rename(columns={"symbol": "Token"})
                    token_summary = pd.merge(token_agg, hedging_agg, on="Token", how="left")
                    token_summary["quantity"] = token_summary["quantity"].fillna(0)
                    token_summary["amount"] = token_summary["amount"].fillna(0)
                    token_summary["funding_rate"] = token_summary["funding_rate"].fillna(0)
                else:
                    token_summary = token_agg
                    token_summary["quantity"] = 0
                    token_summary["amount"] = 0
                    token_summary["funding_rate"] = 0
            elif "Hedging" in dataframes:
                hedging_df = dataframes["Hedging"]
                token_summary = hedging_df.groupby("symbol").agg({
                    "quantity": "sum",
                    "amount": "sum",
                    "funding_rate": "mean"
                }).reset_index().rename(columns={"symbol": "Token"})
                token_summary["LP Qty"] = 0
                token_summary["Hedged Qty"] = token_summary["quantity"]
                token_summary["Rebalance Value"] = 0
                token_summary["Rebalance Action"] = ""
            else:
                token_summary = pd.DataFrame(columns=["Token", "LP Qty", "Hedged Qty", "Rebalance Value", "Rebalance Action", "quantity", "amount", "funding_rate"])

            if token_summary.empty or token_summary["quantity"].eq(0).all():
                toast("No hedge positions to close", duration=5, color="info")
                return

            results = []
            for _, row in token_summary.iterrows():
                token = strip_usdt(row["Token"])
                hedged_qty = row["quantity"]
                if hedged_qty != 0:
                    hedge_processing[token] = True
                    try:
                        close_qty = -hedged_qty
                        task = asyncio.create_task(execute_hedge_trade(token, close_qty))
                        result = await task
                        if result['success']:
                            if "Hedging" in dataframes:
                                hedging_df = dataframes["Hedging"]
                                ticker = f"{token}USDT"
                                hedging_df.loc[hedging_df["symbol"] == ticker, "quantity"] = 0
                                hedging_df.loc[hedging_df["symbol"] == ticker, "amount"] = 0
                                hedging_df.loc[hedging_df["symbol"] == ticker, "funding_rate"] = 0
                                hedging_df.to_csv(HEDGING_LATEST_CSV, index=False)
                                logger.info(f"Updated {HEDGING_LATEST_CSV} for {ticker}")
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Exception closing hedge for {token}: {str(e)}")
                        results.append({'success': False, 'token': token})
                    finally:
                        hedge_processing[token] = False

            success_count = sum(1 for r in results if r['success'])
            if success_count == len(results) and results:
                toast("All hedge positions closed successfully", duration=5, color="success")
            elif results:
                toast(f"Closed {success_count}/{len(results)} hedge positions", duration=5, color="error")
            else:
                toast("No hedge positions were processed", duration=5, color="info")
                
            for result in results:
                if result['success']:
                    put_markdown(f"### Close Hedge Order Request for {result['token']}")
                    put_code(json.dumps(result['request'], indent=2), language='json')

        if "Rebalancing" in dataframes:
            rebalancing_df = dataframes["Rebalancing"]
            token_agg = rebalancing_df.groupby("Token").agg({
                "LP Qty": "sum",
                "Hedged Qty": "sum",
                "Rebalance Value": "sum",
                "Rebalance Action": "first"
            }).reset_index()
            
            if "Hedging" in dataframes:
                hedging_df = dataframes["Hedging"]
                hedging_agg = hedging_df.groupby("symbol").agg({
                    "quantity": "sum",
                    "amount": "sum",
                    "funding_rate": "mean"
                }).reset_index().rename(columns={"symbol": "Token"})
                token_summary = pd.merge(token_agg, hedging_agg, on="Token", how="left")
                token_summary["quantity"] = token_summary["quantity"].fillna(0)
                token_summary["amount"] = token_summary["amount"].fillna(0)
                token_summary["funding_rate"] = token_summary["funding_rate"].fillna(0)
            else:
                token_summary = token_agg
                token_summary["quantity"] = 0
                token_summary["amount"] = 0
                token_summary["funding_rate"] = 0

            for _, row in token_summary.iterrows():
                token = strip_usdt(row["Token"])
                # Determine which data sources to use for quantity computation
                use_krystal = not krystal_error
                use_meteora = not meteora_error
                lp_amount_usd, lp_qty, has_krystal, has_meteora = calculate_token_usd_value(
                    token, krystal_df, meteora_df, use_krystal, use_meteora
                )

                # Initialize fields
                hedged_qty = row["quantity"]
                hedge_amount = row["amount"]
                funding_rate = row["funding_rate"] * 10000
                action = row["Rebalance Action"].strip().lower() if pd.notna(row["Rebalance Action"]) else ""
                rebalance_value = row["Rebalance Value"] if pd.notna(row["Rebalance Value"]) else np.nan
                if action == "buy":
                    rebalance_value = abs(rebalance_value) if pd.notna(rebalance_value) else np.nan
                elif action == "sell":
                    rebalance_value = -abs(rebalance_value) if pd.notna(rebalance_value) else np.nan

                # Adjust LP and rebalance fields based on errors and token positions
                if (meteora_error and has_meteora) or (krystal_error and has_krystal):
                    # Token has positions in an affected protocol: show N/A
                    lp_qty = np.nan
                    lp_amount_usd = np.nan
                    rebalance_value = np.nan
                    action = ""
                else:
                    # Use computed LP values
                    lp_qty = lp_qty if pd.notna(lp_qty) else np.nan
                    lp_amount_usd = lp_amount_usd if pd.notna(lp_amount_usd) else np.nan

                # Handle hedging error
                if hedging_error:
                    hedged_qty = np.nan
                    hedge_amount = np.nan
                    funding_rate = np.nan
                    rebalance_value = np.nan
                    action = ""

                # Button logic
                hedge_button = None
                close_button = None
                if action in ["buy", "sell"] and pd.notna(rebalance_value) and not hedging_error:
                    hedge_button = put_buttons(
                        [{'label': 'Hedge', 'value': f"hedge_{token}", 'color': 'primary'}],
                        onclick=lambda v, t=token, rv=rebalance_value, a=action: run_async(handle_hedge_click(t, rv, a))
                    )
                if abs(hedged_qty) > 0 and not pd.isna(hedged_qty) and not hedging_error:
                    close_button = put_buttons(
                        [{'label': 'Close', 'value': f"close_{token}", 'color': 'danger'}],
                        onclick=lambda v, t=token, hq=hedged_qty: run_async(handle_close_hedge(t, hq))
                    )

                if hedge_button or close_button:
                    button = put_row([
                        hedge_button if hedge_button else put_text(""),
                        put_text(" "),
                        close_button if close_button else put_text("")
                    ], size='auto 5px auto')
                else:
                    button = put_text("No action needed")

                token_data.append([
                    token,
                    f"{lp_amount_usd:.2f}" if pd.notna(lp_amount_usd) else "N/A",
                    f"{hedge_amount:.4f}" if pd.notna(hedge_amount) else "N/A",
                    f"{lp_qty:.4f}" if pd.notna(lp_qty) else "N/A",
                    f"{hedged_qty:.4f}" if pd.notna(hedged_qty) else "N/A",
                    f"{rebalance_value:.6f}" if pd.notna(rebalance_value) else "N/A",
                    button,
                    f"{funding_rate:.0f}" if pd.notna(funding_rate) else "N/A"
                ])
                
            token_headers = [
                "Token", "LP Amount USD", "Hedge Amount USD", "LP Qty", "Hedge Qty", "Suggested Hedge Qty", "Action", "Funding Rate (BIPS)"
            ]
            put_table(token_data, header=token_headers)
        elif "Hedging" in dataframes and not hedging_error:
            hedging_df = dataframes["Hedging"]
            token_data = []
            hedging_agg = hedging_df.groupby("symbol").agg({
                "quantity": "sum",
                "amount": "sum",
                "funding_rate": "mean"
            }).reset_index()

            for _, row in hedging_agg.iterrows():
                token = strip_usdt(row["symbol"])
                hedged_qty = row["quantity"]
                hedge_amount = row["amount"]
                funding_rate = row["funding_rate"] * 10000
                
                # Check for LP data
                use_krystal = not krystal_error
                use_meteora = not meteora_error
                lp_amount_usd, lp_qty, has_krystal, has_meteora = calculate_token_usd_value(
                    token, krystal_df, meteora_df, use_krystal, use_meteora
                )
                
                # Adjust LP fields based on errors and token positions
                if (meteora_error and has_meteora) or (krystal_error and has_krystal):
                    lp_qty = np.nan
                    lp_amount_usd = np.nan
                    rebalance_value = np.nan
                    action = ""
                else:
                    lp_qty = lp_qty if pd.notna(lp_qty) else np.nan
                    lp_amount_usd = lp_amount_usd if pd.notna(lp_amount_usd) else np.nan
                    action = row["Rebalance Action"].strip().lower() if pd.notna(row["Rebalance Action"]) else ""
                    rebalance_value = row["Rebalance Value"] if pd.notna(row["Rebalance Value"]) else np.nan
                    if action == "buy":
                        rebalance_value = abs(rebalance_value) if pd.notna(rebalance_value) else np.nan
                    elif action == "sell":
                        rebalance_value = -abs(rebalance_value) if pd.notna(rebalance_value) else np.nan

                action_buttons = []
                if abs(hedged_qty) > 0:
                    action_buttons.append({'label': 'Close', 'value': f"close_{token}", 'color': 'danger'})
                if action in ["buy", "sell"] and not pd.isna(rebalance_value):
                    action_buttons.append({'label': 'Hedge', 'value': f"hedge_{token}", 'color': 'primary'})

                if action_buttons:
                    button = put_buttons(
                        action_buttons,
                        onclick=lambda v, t=token, hq=hedged_qty, rv=rebalance_value, a=action: run_async(
                            handle_hedge_click(t, rv, a) if 'hedge' in v else handle_close_hedge(t, hq)
                        )
                    )
                else:
                    button = put_text("No action needed")

                token_data.append([
                    token,
                    f"{lp_amount_usd:.2f}" if pd.notna(lp_amount_usd) else "N/A",
                    f"{hedge_amount:.4f}" if pd.notna(hedge_amount) else "N/A",
                    f"{lp_qty:.4f}" if pd.notna(lp_qty) else "N/A",
                    f"{hedged_qty:.4f}" if pd.notna(hedged_qty) else "N/A",
                    f"{rebalance_value:.6f}" if pd.notna(rebalance_value) else "N/A",
                    button,
                    f"{funding_rate:.0f}" if pd.notna(funding_rate) else "N/A"
                ])

            token_headers = [
                "Token", "LP Amount USD", "Hedge Amount USD", "LP Qty", "Hedge Qty", "Suggested Hedge Qty", "Action", "Funding Rate (BIPS)"
            ]
            put_table(token_data, header=token_headers)
        else:
            put_text("No rebalancing or hedging data available.")

def cleanup():
    asyncio.run(order_sender.close())

atexit.register(cleanup)

if __name__ == "__main__":
    start_server(main, port=8080, host="0.0.0.0", debug=True)
