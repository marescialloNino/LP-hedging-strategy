import pandas as pd
import numpy as np
import logging
import asyncio
import logging
from common.data_loader import load_hedgeable_tokens

HEDGABLE_TOKENS = load_hedgeable_tokens()

async def run_shell_script(script_path):
    logger = logging.getLogger('sheel_script_execution')
    try:
        import os
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



async def execute_hedge_trade(token, rebalance_value, order_sender):
    logger = logging.getLogger('hedge_execution')
    logger.info(f"Executing hedge trade for token: {token}, rebalance_value: {rebalance_value}")
    
    order_size = abs(rebalance_value)
    direction = 1 if rebalance_value > 0 else -1
    ticker = token if token.upper().endswith("USDT") else f"{token}USDT"
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
                return {
                    'success': False,
                    'token': token,
                    'request': request
                }
        else:
            logger.error(f"Unexpected return value from send_order for {token}: {result}")
            return {'success': False, 'token': token}
    except Exception as e:
        logger.error(f"Exception in execute_hedge_trade for {token}: {str(e)}")
        return {'success': False, 'token': token}

def strip_usdt(token):
    return token.replace("USDT", "").strip() if isinstance(token, str) else token

def calculate_token_usd_value(token, krystal_df=None, meteora_df=None, use_krystal=True, use_meteora=True):
    logger = logging.getLogger('usd_value_calculation')
    total_usd = 0.0
    total_qty = 0.0
    has_krystal = False
    has_meteora = False
    ticker = f"{token}USDT"
    if ticker not in HEDGABLE_TOKENS:
        logger.warning(f"Token {ticker} not found in HEDGABLE_TOKENS. Returning 0 USD.")
        return total_usd, total_qty, has_krystal, has_meteora

    token_info = HEDGABLE_TOKENS[ticker]

    if krystal_df is not None and not krystal_df.empty:
        for chain, addresses in token_info.items():
            if chain == "solana":
                continue
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
        has_meteora = True

    if (not use_krystal and has_krystal) or (not use_meteora and has_meteora):
        return 0.0, 0.0, has_krystal, has_meteora

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