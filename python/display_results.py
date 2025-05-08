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
import atexit
from pathlib import Path
from common.path_config import (
    REBALANCING_LATEST_CSV, KRYSTAL_LATEST_CSV, METEORA_LATEST_CSV, HEDGING_LATEST_CSV, METEORA_PNL_CSV, KRYSTAL_POOL_PNL_CSV, LOG_DIR
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

# Set up BrokerHandler and BitgetOrderSender globally in dummy mode
""" params = {
    'exchange_trade': 'bitget',
    'account_trade': 'H1',
    'send_orders': 'dummy'
}
end_point = BrokerHandler.build_end_point('bitget', account='H1')
bh = BrokerHandler(
    market_watch='bitget',
    strategy_param=params,
    end_point_trade=end_point,
    logger_name='bitget_order_sender'
)
order_sender = BitgetOrderSender(bh) """

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
        # Check if result is a tuple (expected case)
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
            # Handle case where send_order returns False or other non-tuple
            logger.error(f"Unexpected return value from send_order for {token}: {result}")
            return {'success': False, 'token': token}
    except Exception as e:
        logger.error(f"Exception in execute_hedge_trade for {token}: {str(e)}")
        return {'success': False, 'token': token}

# Function to truncate wallet address
def truncate_wallet(wallet):
    return f"{wallet[:5]}..." if isinstance(wallet, str) and len(wallet) > 5 else wallet

def calculate_token_usd_value(token, krystal_df=None, meteora_df=None):
    """
    Calculate total USD value for a token based on its addresses from HEDGABLE_TOKENS.
    
    Args:
        token (str): Bitget ticker (e.g., "SONICUSDT", "BNBUSDT")
        krystal_df (pd.DataFrame): Krystal LP positions
        meteora_df (pd.DataFrame): Meteora LP positions
    
    Returns:
        float: Total USD value of the token across LP positions
    """
    total_usd = 0.0
    token = f"{token}USDT"
    # Check if token is in HEDGABLE_TOKENS
    if token not in HEDGABLE_TOKENS:
        print(f"Warning: Token {token} not found in HEDGABLE_TOKENS. Returning 0 USD.")
        return total_usd

    # Get all possible addresses for the token across chains
    token_info = HEDGABLE_TOKENS[token]
    all_addresses = []
    for chain, addresses in token_info.items():
        all_addresses.extend(addresses)

    # Remove duplicates (e.g., wrapped and native might overlap)
    unique_addresses = set(all_addresses)

    # Process Krystal LP data (multi-chain)
    if krystal_df is not None and not krystal_df.empty:
        for address in unique_addresses:
            # Match by Token X Address
            token_x_matches = krystal_df[krystal_df["Token X Address"] == address]
            for _, row in token_x_matches.iterrows():
                total_usd += float(row["Token X USD Amount"]) if pd.notna(row["Token X USD Amount"]) else 0
            
            # Match by Token Y Address
            token_y_matches = krystal_df[krystal_df["Token Y Address"] == address]
            for _, row in token_y_matches.iterrows():
                total_usd += float(row["Token Y USD Amount"]) if pd.notna(row["Token Y USD Amount"]) else 0

    # Process Meteora LP data (Solana-specific)
    if meteora_df is not None and not meteora_df.empty:
        # Meteora is Solana-only, so filter addresses for Solana if available
        solana_addresses = token_info.get("solana", all_addresses)
        for address in set(solana_addresses):
            # Match by Token X Address
            token_x_matches = meteora_df[meteora_df["Token X Address"] == address]
            for _, row in token_x_matches.iterrows():
                total_usd += float(row["Token X USD Amount"]) if pd.notna(row["Token X USD Amount"]) else 0
            
            # Match by Token Y Address
            token_y_matches = meteora_df[meteora_df["Token Y Address"] == address]
            for _, row in token_y_matches.iterrows():
                total_usd += float(row["Token Y USD Amount"]) if pd.notna(row["Token Y USD Amount"]) else 0

    return total_usd

async def main():
    put_markdown("# Hedging Dashboard")
    
    # Check error flags
    hedge_error_flags = {}
    lp_error_flags = {}
    has_error = False
    error_messages = []

    # Read hedging error flags
    # Note: json.load() converts JSON 'true'/'false' (lowercase, from TypeScript) to Python True/False
    try:
        if HEDGE_ERROR_FLAGS_PATH.exists():
            with HEDGE_ERROR_FLAGS_PATH.open('r') as f:
                hedge_error_flags = json.load(f)
                if hedge_error_flags.get("HEDGING_FETCHING_BITGET_ERROR", False):
                    has_error = True
                    error_messages.append("Failed to fetch Bitget hedging data")
        else:
            logger.warning(f"Hedging error flags file not found: {HEDGE_ERROR_FLAGS_PATH}")
            has_error = True
            error_messages.append("Hedging error flags file missing")
    except Exception as e:
        logger.error(f"Error reading hedging error flags: {str(e)}")
        has_error = True
        error_messages.append("Error reading hedging error flags")

    # Read LP error flags
    try:
        if LP_ERROR_FLAGS_PATH.exists():
            with LP_ERROR_FLAGS_PATH.open('r') as f:
                lp_error_flags = json.load(f)
                if lp_error_flags.get("LP_FETCHING_KRYSTAL_ERROR", False):
                    has_error = True
                    error_messages.append("Failed to fetch Krystal LP data")
                if lp_error_flags.get("LP_FETCHING_METEORA_ERROR", False):
                    has_error = True
                    error_messages.append("Failed to fetch Meteora LP data")
        else:
            logger.warning(f"LP error flags file not found: {LP_ERROR_FLAGS_PATH}")
            has_error = True
            error_messages.append("LP error flags file missing")
    except Exception as e:
        logger.error(f"Error reading LP error flags: {str(e)}")
        has_error = True
        error_messages.append("Error reading LP error flags")

    # Display error messages if any (website remains fully functional)
    if has_error:
        error_text = "\n".join([f"- {msg}" for msg in error_messages])
        put_error(f"Data Fetching Errors:\n{error_text}", scope=None)
        logger.info(f"Displayed error messages: {error_text}")
    else:
        logger.info("No data fetching errors detected")

    # Load CSVs with error handling
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
        except Exception as e:
            logger.error(f"Error reading CSV {path}: {str(e)}")
            put_error(f"Error reading {name} CSV: {str(e)}")

    # Table 1: Wallet Positions (Krystal and Meteora)
    put_markdown("## Wallet Positions")

    lp_updated = lp_error_flags.get("last_updated", "Unknown")
    put_markdown(f"**Last LP Data Update:** {lp_updated}")


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

    # Table 2: Meteora Positions PnL
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

    # Table: Krystal PnL by Pool (only pools with open positions)
    if "Krystal PnL" in dataframes:
        put_markdown("## Krystal Positions PnL by Pool (Open Pools)")

        k_pnl_df = dataframes["Krystal PnL"].copy()

        # graceful fallback if CSV lacks the new columns
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
                f"{r['lp_pnl_usd']:.2f}"          if pd.notna(r["lp_pnl_usd"])       else "N/A",
                f"{r['lp_pnl_tokenB']:.5f}"      if pd.notna(r["lp_pnl_tokenB"])   else "N/A",
                f"{r['hold_pnl_usd']:.2f}"        if pd.notna(r["hold_pnl_usd"])     else "N/A",
                f"{r['lp_minus_hold_usd']:.2f}"   if pd.notna(r["lp_minus_hold_usd"])else "N/A",
                r["poolAddress"],
            ])

        put_table(pnl_rows, header=pnl_headers)

    # Table 3: Token Hedge Summary (Rebalancing + Hedging)
    if "Rebalancing" in dataframes or "Hedging" in dataframes:
        put_markdown("## Token Hedge Summary")


        hedge_updated = hedge_error_flags.get("last_updated_hedge", "Unknown")
        put_markdown(f"**Last Hedge Data Update:** {hedge_updated}")


        # Add "Close All Hedges" button
        put_buttons(
            [{'label': 'Close All Hedges', 'value': 'all', 'color': 'danger'}],
            onclick=lambda _: run_async(handle_close_all_hedges())
        )

        token_data = []
        krystal_df = dataframes.get("Krystal")
        meteora_df = dataframes.get("Meteora")
        def strip_usdt(token):
            return token.replace("USDT", "").strip() if isinstance(token, str) else token

        hedge_processing = {}

        async def handle_hedge_click(token, rebalance_value):
            logger = logging.getLogger('hedge_execution')
            logger.info(f"Handling hedge click for {token} with rebalance_value {rebalance_value}")
            
            if hedge_processing.get(token, False):
                toast(f"Hedge already in progress for {token}", duration=5, color="warning")
                return

            hedge_processing[token] = True
            try:
                task = asyncio.create_task(execute_hedge_trade(token, rebalance_value))
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
                # To close the hedge, send an order with the opposite quantity
                close_qty = -hedged_qty  # Opposite of current hedge to bring to zero
                task = asyncio.create_task(execute_hedge_trade(token, close_qty))
                result = await task
                
                if result['success']:
                    put_markdown(f"### Close Hedge Order Request for {result['token']}")
                    put_code(json.dumps(result['request'], indent=2), language='json')
                    toast(f"Close hedge triggered for {result['token']}", duration=5, color="success")
                    # Update Hedging CSV
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
                "Rebalance Action": "first"   # Use the first encountered action for each token
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
                lp_qty = row["LP Qty"]
                hedged_qty = row["quantity"]  # Use quantity from Hedging CSV
                rebalance_value = row["Rebalance Value"]
                hedge_amount = row["amount"]
                funding_rate = row["funding_rate"] * 10000  # Convert to BIPS
                action = row["Rebalance Action"].strip().lower()  
                
                lp_amount_usd = calculate_token_usd_value(token, krystal_df, meteora_df)
                rebalance_value_with_sign = float(f"{'+' if action == 'buy' else '-'}{abs(rebalance_value):.6f}")
                
                # Create action buttons
                hedge_button = None
                close_button = None
                if abs(rebalance_value) != 0.0:
                    hedge_button = put_buttons(
                        [{'label': 'Hedge', 'value': f"hedge_{token}", 'color': 'primary'}],
                        onclick=lambda v, t=token, rv=rebalance_value_with_sign: run_async(handle_hedge_click(t, rv))
                    )
                if abs(hedged_qty) > 0:
                    close_button = put_buttons(
                        [{'label': 'Close', 'value': f"close_{token}", 'color': 'danger'}],
                        onclick=lambda v, t=token, hq=hedged_qty: run_async(handle_close_hedge(t, hq))
                    )

                if hedge_button or close_button:
                    button = put_row([
                        hedge_button if hedge_button else put_text(""),  # Empty placeholder if no Hedge button
                        put_text(" "),  # Small spacer between buttons
                        close_button if close_button else put_text("")   # Empty placeholder if no Close button
                    ], size='auto 5px auto')  # Adjust spacing as needed
                else:
                    button = put_text("No action needed")

                token_data.append([
                    token,
                    f"{lp_amount_usd:.2f}",
                    f"{hedge_amount:.4f}",
                    f"{lp_qty:.4f}",
                    f"{hedged_qty:.4f}",
                    rebalance_value_with_sign,
                    button,
                    f"{funding_rate}" if pd.notna(funding_rate) else "N/A"
                ])
                
            token_headers = [
                "Token", "LP Amount USD", "Hedge Amount USD", "LP Qty", "Hedge Qty", "Suggested Hedge Qty", "Action", "Funding Rate (BIPS)"
            ]
            put_table(token_data, header=token_headers)
        elif "Hedging" in dataframes:
            hedging_df = dataframes["Hedging"]
            token_data = []
            hedging_agg = hedging_df.groupby("symbol").agg({
                "quantity": "sum",
                "amount": "sum",
                "funding_rate": "mean"
            }).reset_index()

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
                        # Update Hedging CSV
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

            for _, row in hedging_agg.iterrows():
                token = strip_usdt(row["symbol"])
                hedged_qty = row["quantity"]
                hedge_amount = row["amount"]
                funding_rate = row["funding_rate"]* 10000  # Convert to BIPS
                
                action_buttons = []
                if abs(hedged_qty) > 0:
                    action_buttons.append({'label': 'Close', 'value': f"close_{token}", 'color': 'danger'})

                if action_buttons:
                    button = put_buttons(
                        action_buttons,
                        onclick=lambda v, t=token, hq=hedged_qty: run_async(handle_close_hedge(t, hq))
                    )
                else:
                    button = put_text("No action needed")

                token_data.append([
                    token,
                    f"{hedge_amount:.4f}",
                    f"{hedged_qty:.4f}",
                    button,
                    f"{funding_rate:.6f}" if pd.notna(funding_rate) else "N/A"
                ])

            token_headers = ["Token", "Hedge Amount USD", "Hedge Qty", "Action", "Funding Rate (BIPS)"]
            put_table(token_data, header=token_headers)

# Ensure cleanup on exit
def cleanup():
    asyncio.run(order_sender.close())

atexit.register(cleanup)

if __name__ == "__main__":
    start_server(main, port=8080, host="0.0.0.0", debug=True)