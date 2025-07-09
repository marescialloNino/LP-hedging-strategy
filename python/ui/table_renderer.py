import pandas as pd
import numpy as np
import json
from pathlib import Path
from pywebio.output import put_table, put_text, put_row, put_markdown, put_html, toast, put_buttons
from common.utils import calculate_token_usd_value
from common.data_loader import load_hedgeable_tokens, load_ticker_mappings
from common.path_config import CONFIG_DIR, ACTIVE_POOLS_TVL

AUTO_HEDGE_TOKENS_PATH = CONFIG_DIR / "auto_hedge_tokens.json"
HEDGABLE_TOKENS = load_hedgeable_tokens()
mappings = load_ticker_mappings()
SYMBOL_MAP=  mappings["SYMBOL_MAP"]
BITGET_TOKENS_WITH_FACTOR_1000 =  mappings["BITGET_TOKENS_WITH_FACTOR_1000"]
BITGET_TOKENS_WITH_FACTOR_10000 =  mappings["BITGET_TOKENS_WITH_FACTOR_10000"]

def truncate_wallet(wallet):
    return f"{wallet[:5]}..." if isinstance(wallet, str) and len(wallet) > 5 else wallet

def load_auto_hedge_tokens():
    """
    Load tokens' automation status from auto_hedge_tokens.json.
    If the file doesn't exist, initialize it with all hedgeable tokens set to false.
    Returns: dict of token symbols to automation status (e.g., {"ETH": false, "BTC": false}).
    """
    try:
        if AUTO_HEDGE_TOKENS_PATH.exists():
            with AUTO_HEDGE_TOKENS_PATH.open('r') as f:
                content = f.read().strip()
                if not content:
                    raise ValueError("File is empty")
                data = json.loads(content)
                # Ensure all hedgeable tokens are included
                hedgable_tokens = [ticker.replace("USDT", "") for ticker in HEDGABLE_TOKENS.keys()]
                default = {token: False for token in hedgable_tokens}
                default.update(data)
                return default
        else:
            # Initialize with all hedgeable tokens set to false
            hedgable_tokens = [ticker.replace("USDT", "") for ticker in HEDGABLE_TOKENS.keys()]
            data = {token: False for token in hedgable_tokens}
            save_auto_hedge_tokens(data)
            return data
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error loading auto_hedge_tokens.json: {str(e)}")
        # Initialize with defaults on error
        hedgable_tokens = [ticker.replace("USDT", "") for ticker in HEDGABLE_TOKENS.keys()]
        data = {token: False for token in hedgable_tokens}
        save_auto_hedge_tokens(data)
        return data

def save_auto_hedge_tokens(tokens):
    """
    Save token automation status to auto_hedge_tokens.json.
    Args:
        tokens: dict of token symbols to automation status (e.g., {"ETH": false, "BTC": true}).
    """
    try:
        CONFIG_DIR.mkdir(exist_ok=True)
        with AUTO_HEDGE_TOKENS_PATH.open('w') as f:
            json.dump(tokens, f, indent=2)

    except Exception as e:
        print(f"Error saving auto_hedge_tokens.json: {str(e)}")


# Chain name mapping to GeckoTerminal's conventions
CHAIN_MAPPING = {
    "solana": "solana",
    "ethereum": "eth",
    "bsc": "bsc",
    "polygon": "polygon_pos",
    "arbitrum": "arbitrum",
    "sui": "sui-network",
    "base": "base"
}

def render_wallet_positions(dataframes, error_flags):
    """
    Render wallet positions table for Krystal and Meteora with TVL and 24h Volume/TVL columns.
    Includes debugging logs to diagnose matching issues.
    """
    krystal_error = error_flags.get('krystal_error', False)
    meteora_error = error_flags.get('meteora_error', False)
    wallet_headers = [
        "Source", "Wallet", "Chain", "Protocol", "Pair", "In Range", "Fee APR", "Initial USD", "Present USD",
        "Price Position %", "Width %", "TVL (USD)", "My TVL/TVL %", "24h Volume/TVL", "Pool Address"
    ]
    wallet_data = []

    # Load active_pools.csv for TVL and volume data
    pool_metrics_df = None
    try:
        pool_metrics_df = dataframes["Active Pools TVL"]
        print(f"Loaded pool_metrics_df from dataframes['Active Pools TVL'] with {len(pool_metrics_df)} rows")
        print(f"Columns in pool_metrics_df: {pool_metrics_df.columns.tolist()}")
        print(f"Sample pool_metrics_df:\n{pool_metrics_df.head().to_string()}")
        pool_metrics_df['chain'] = pool_metrics_df['chain'].str.lower()
        pool_metrics_df['pool_address'] = pool_metrics_df['pool_address'].str.lower()
    except KeyError:
        print("Key 'Active Pools TVL' not found in dataframes. Falling back to loading from ACTIVE_POOLS_TVL")
        try:
            pool_metrics_df = pd.read_csv(ACTIVE_POOLS_TVL)
            print(f"Loaded pool_metrics_df from {ACTIVE_POOLS_TVL} with {len(pool_metrics_df)} rows")
            print(f"Columns in pool_metrics_df: {pool_metrics_df.columns.tolist()}")
            print(f"Sample pool_metrics_df:\n{pool_metrics_df.head().to_string()}")
            pool_metrics_df['chain'] = pool_metrics_df['chain'].str.lower()
            pool_metrics_df['pool_address'] = pool_metrics_df['pool_address'].str.lower()
        except Exception as e:
            print(f"Error loading active_pools.csv from {ACTIVE_POOLS_TVL}: {e}")
            pool_metrics_df = pd.DataFrame(columns=['chain', 'pool_address', 'tvl_usd', 'volume_24h_usd'])

    # Log unique chains in pool_metrics_df
    if not pool_metrics_df.empty:
        print(f"Unique chains in pool_metrics_df: {pool_metrics_df['chain'].unique().tolist()}")
    else:
        print("pool_metrics_df is empty")

    if "Krystal" in dataframes and not krystal_error:
        krystal_df = dataframes["Krystal"]
        ticker_source = "symbol" if "Token X Symbol" in krystal_df.columns and "Token Y Symbol" in krystal_df.columns else "address"
        for _, row in krystal_df.iterrows():
            pair_ticker = (f"{row['Token X Symbol']}-{row['Token Y Symbol']}" if ticker_source == "symbol"
                          else f"{row['Token X Address'][:5]}...-{row['Token Y Address'][:5]}...")
            # Calculate Price Position % and Width %
            current_price = float(row["Current Price"]) if pd.notna(row["Current Price"]) else np.nan
            min_price = float(row["Min Price"]) if pd.notna(row["Min Price"]) else np.nan
            max_price = float(row["Max Price"]) if pd.notna(row["Max Price"]) else np.nan
            price_position = ((current_price - min_price) / (max_price - min_price) * 100) if pd.notna(current_price) and pd.notna(min_price) and pd.notna(max_price) and max_price != min_price else np.nan
            width = ((max_price / min_price - 1) * 100) if pd.notna(max_price) and pd.notna(min_price) and min_price != 0 else np.nan

            # Get TVL and volume from active_pools.csv
            pool_address = row["Pool Address"].lower() if isinstance(row["Pool Address"], str) else ""
            chain = CHAIN_MAPPING.get(row["Chain"].lower(), row["Chain"].lower()) if isinstance(row["Chain"], str) else ""
            print(f"Krystal: Matching pool_address={pool_address}, chain={chain}")
            pool_match = pool_metrics_df[(pool_metrics_df['pool_address'] == pool_address) & (pool_metrics_df['chain'] == chain)]
            if pool_match.empty:
                print(f"Krystal: No match found for pool_address={pool_address}, chain={chain}")
                tvl = np.nan
                volume_24h = np.nan
            else:
                try:
                    tvl = float(pool_match['tvl_usd'].iloc[0])
                    volume_24h = float(pool_match['volume_24h_usd'].iloc[0])
                    print(f"Krystal: Match found for pool_address={pool_address}, chain={chain}, tvl={tvl}, volume_24h={volume_24h}")
                except (ValueError, TypeError) as e:
                    print(f"Krystal: Error converting tvl_usd or volume_24h_usd for pool_address={pool_address}, chain={chain}: {e}")
                    tvl = np.nan
                    volume_24h = np.nan

            volume_tvl_ratio = (volume_24h / tvl) if pd.notna(tvl) and pd.notna(volume_24h) and tvl != 0 else np.nan

            # Calculate My TVL/TVL %
            actual_value_usd = float(row["Actual Value USD"]) if pd.notna(row["Actual Value USD"]) else np.nan
            my_tvl_ratio = (actual_value_usd / tvl * 100) if pd.notna(actual_value_usd) and pd.notna(tvl) and tvl != 0 else np.nan

            wallet_data.append([
                "Krystal",
                truncate_wallet(row["Wallet Address"]),
                row["Chain"],
                row["Protocol"],
                pair_ticker,
                "Yes" if row["Is In Range"] else "No",
                f"{row['Fee APR']:.0%}" if pd.notna(row["Fee APR"]) else "N/A",
                f"{row['Initial Value USD']:.0f}" if pd.notna(row["Initial Value USD"]) else "N/A",
                f"{actual_value_usd:.0f}" if pd.notna(actual_value_usd) else "N/A",
                f"{price_position:.0f}%" if pd.notna(price_position) else "N/A",
                f"{width:.0f}%" if pd.notna(width) else "N/A",
                f"{tvl:.0f}" if pd.notna(tvl) else "N/A",
                f"{my_tvl_ratio:.3f}%" if pd.notna(my_tvl_ratio) else "N/A",
                f"{volume_tvl_ratio:.1f}" if pd.notna(volume_tvl_ratio) else "N/A",
                row["Pool Address"]
            ])

    if "Meteora" in dataframes and not meteora_error:
        meteora_df = dataframes["Meteora"]
        for _, row in meteora_df.iterrows():
            pair_ticker = f"{row['Token X Symbol']}-{row['Token Y Symbol']}"
            qty_x = float(row["Token X Qty"]) if pd.notna(row["Token X Qty"]) else 0
            qty_y = float(row["Token Y Qty"]) if pd.notna(row["Token Y Qty"]) else 0
            price_x = float(row["Token X Price USD"]) if pd.notna(row["Token X Price USD"]) else 0
            price_y = float(row["Token Y Price USD"]) if pd.notna(row["Token Y Price USD"]) else 0
            present_usd = (qty_x * price_x) + (qty_y * price_y)
            current_price = float(price_x/price_y) if pd.notna(price_x) and pd.notna(price_y) and price_y != 0 else np.nan
            min_price = float(row["Lower Boundary"]) if pd.notna(row["Lower Boundary"]) else np.nan
            max_price = float(row["Upper Boundary"]) if pd.notna(row["Upper Boundary"]) else np.nan
            price_position = ((current_price - min_price) / (max_price - min_price) * 100) if pd.notna(current_price) and pd.notna(min_price) and pd.notna(max_price) and max_price != min_price else np.nan
            width = ((max_price / min_price - 1) * 100) if pd.notna(max_price) and pd.notna(min_price) and min_price != 0 else np.nan

            # Get TVL and volume from active_pools.csv
            pool_address = row["Pool Address"].lower() if isinstance(row["Pool Address"], str) else ""
            chain = CHAIN_MAPPING.get("solana", "solana")  # Meteora is always on Solana
            print(f"Meteora: Matching pool_address={pool_address}, chain={chain}")
            pool_match = pool_metrics_df[(pool_metrics_df['pool_address'] == pool_address) & (pool_metrics_df['chain'] == chain)]
            if pool_match.empty:
                print(f"Meteora: No match found for pool_address={pool_address}, chain={chain}")
                tvl = np.nan
                volume_24h = np.nan
            else:
                try:
                    tvl = float(pool_match['tvl_usd'].iloc[0])
                    volume_24h = float(pool_match['volume_24h_usd'].iloc[0])
                    print(f"Meteora: Match found for pool_address={pool_address}, chain={chain}, tvl={tvl}, volume_24h={volume_24h}")
                except (ValueError, TypeError) as e:
                    print(f"Meteora: Error converting tvl_usd or volume_24h_usd for pool_address={pool_address}, chain={chain}: {e}")
                    tvl = np.nan
                    volume_24h = np.nan

            volume_tvl_ratio = (volume_24h / tvl) if pd.notna(tvl) and pd.notna(volume_24h) and tvl != 0 else np.nan

            # Calculate My TVL/TVL %
            my_tvl_ratio = (present_usd / tvl * 100) if pd.notna(present_usd) and pd.notna(tvl) and tvl != 0 else np.nan

            wallet_data.append([
                "Meteora",
                truncate_wallet(row["Wallet Address"]),
                "Solana",
                "Meteora",
                pair_ticker,
                "Yes" if row["Is In Range"] else "No",
                "N/A",
                "N/A",
                f"{present_usd:.0f}",
                f"{price_position:.0f}%" if pd.notna(price_position) else "N/A",
                f"{width:.0f}%" if pd.notna(width) else "N/A",
                f"{tvl:.0f}" if pd.notna(tvl) else "N/A",
                f"{my_tvl_ratio:.3f}%" if pd.notna(my_tvl_ratio) else "N/A",
                f"{volume_tvl_ratio:.1f}" if pd.notna(volume_tvl_ratio) else "N/A",
                row["Pool Address"]
            ])

    if wallet_data:
        put_table(wallet_data, header=wallet_headers)
    else:
        put_text("No wallet positions found in Krystal or Meteora CSVs.")

def render_pnl_tables(dataframes, error_flags):
    """
    Render PnL tables for Meteora and Krystal.
    """
    meteora_error = error_flags.get('meteora_error', False)
    krystal_error = error_flags.get('krystal_error', False) or error_flags.get('vault_error', False)

    if "Meteora PnL" in dataframes and not meteora_error:
        put_markdown("## Meteora Positions PnL")
        pnl_headers = [
            "Chain", "Owner", "Pair", "Realized PNL (USD)", "Unrealized PNL (USD)", "Net PNL (USD)",
            "Realized PNL (Token B)", "Unrealized PNL (Token B)", "Net PNL (Token B)", "Position ID", "Pool Address"
        ]
        pnl_data = []
        meteora_pnl_df = dataframes["Meteora PnL"]
        for _, row in meteora_pnl_df.iterrows():
            pair = f"{row['Token X Symbol']}-{row['Token Y Symbol']}"
            pnl_data.append([
                "solana",
                truncate_wallet(row["Owner"]),
                pair,
                f"{row['Realized PNL (USD)']:.0f}",
                f"{row['Unrealized PNL (USD)']:.0f}",
                f"{row['Net PNL (USD)']:.0f}",
                f"{row['Realized PNL (Token B)']:.3f}",
                f"{row['Unrealized PNL (Token B)']:.3f}",
                f"{row['Net PNL (Token B)']:.3f}",
                row["Position ID"],
                row["Pool Address"]
            ])
        if pnl_data:
            put_table(pnl_data, header=pnl_headers)
        else:
            put_text("No PnL data found in Meteora PnL CSV.")

    if "Krystal PnL" in dataframes and not krystal_error:
        put_markdown("## Krystal Positions PnL by Pool (Open Positions Only)")
        k_pnl_df = dataframes["Krystal PnL"].copy()
        for col in ["earliest_createdTime", "hold_pnl_usd", "lp_minus_hold_usd", "lp_pnl_usd"]:
            if col not in k_pnl_df.columns:
                k_pnl_df[col] = np.nan
        pnl_headers = [
            "Chain", "Owner", "Pair", "First Deposit", "LP PnL (USD)", "LP TokenB PnL",
            "50-50 Hold PnL (USD)", "Compare With Hold", "Pool Address"
        ]
        pnl_rows = []
        for _, r in k_pnl_df.iterrows():
            pair = f"{r['tokenA_symbol']}-{r['tokenB_symbol']}"
            pnl_rows.append([
                r["chainName"],
                truncate_wallet(r["userAddress"]),
                pair,
                r["earliest_createdTime"],
                f"{r['lp_pnl_usd']:.0f}" if pd.notna(r['lp_pnl_usd']) else "N/A",
                f"{r['lp_pnl_tokenB']:.5f}" if pd.notna(r['lp_pnl_tokenB']) else "N/A",
                f"{r['hold_pnl_usd']:.0f}" if pd.notna(r['hold_pnl_usd']) else "N/A",
                f"{r['lp_minus_hold_usd']:.0f}" if pd.notna(r['lp_minus_hold_usd']) else "N/A",
                r["poolAddress"]
            ])
        put_table(pnl_rows, header=pnl_headers)


def render_hedging_table(dataframes, error_flags, hedge_actions):
    """
    Render the hedging table with updated columns for Net/Gross Ratio (%) and Suggested Hedge Qty/LP Qty (%).
    
    Args:
        dataframes (dict): Loaded CSVs from data_loader
        error_flags (dict): Error flags from data_loader
        hedge_actions (HedgeActions): Instance for handling hedge/close actions
    """
    krystal_error = error_flags.get('krystal_error', False) or error_flags.get('vault_error', False)
    meteora_error = error_flags.get('meteora_error', False)
    hedging_error = error_flags.get('hedging_error', False)
    krystal_df = dataframes.get("Krystal")
    meteora_df = dataframes.get("Meteora")
    auto_hedge_tokens = load_auto_hedge_tokens()

    if "Rebalancing" in dataframes or "Hedging" in dataframes:
        token_headers = [
            "Token", "LP Amount USD", "Hedge Amount USD", "LP Qty", 
            "Net/Gross Ratio (%)", "Suggested Hedge Qty/LP Qty (%)", 
            "Action", "Funding Rate (BIPS)"
        ]
        token_data = []

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
                token_summary = rebalancing_df
                token_summary["quantity"] = 0
                token_summary["amount"] = 0
                token_summary["funding_rate"] = 0

            for _, row in token_summary.iterrows():
                token = row["Token"].replace("USDT", "").strip()
                use_krystal = not krystal_error
                use_meteora = not meteora_error
                lp_amount_usd, lp_qty, has_krystal, has_meteora = calculate_token_usd_value(
                    token, krystal_df, meteora_df, use_krystal, use_meteora
                )

                # Adjust lp_qty for factored tokens
                factor = (
                    1000 if any(token.startswith(factor_symbol) for factor_symbol in BITGET_TOKENS_WITH_FACTOR_1000.values())
                    else 10000 if any(token.startswith(factor_symbol) for factor_symbol in BITGET_TOKENS_WITH_FACTOR_10000.values())
                    else 1
                )
                if pd.notna(lp_qty):
                    lp_qty = lp_qty / factor

                hedged_qty = row["quantity"]
                hedge_amount = row["amount"]
                funding_rate = row["funding_rate"] * 10000
                is_auto = auto_hedge_tokens.get(token, False)

                # Calculate Net/Gross Ratio (%) : ((lp_qty + hedge_qty) / (lp_qty - hedge_qty)) * 100
                net_gross_ratio = (
                    ((lp_qty + hedged_qty) / (lp_qty - hedged_qty)) * 100
                    if pd.notna(hedged_qty) and pd.notna(lp_qty) and (lp_qty - hedged_qty) != 0
                    else np.nan
                )

                # Calculate Suggested Hedge Qty/LP Qty (%)
                if is_auto:
                    rebalance_value = np.nan
                    action = ""
                    suggested_hedge_ratio = np.nan
                else:
                    action = row["Rebalance Action"].strip().lower() if pd.notna(row["Rebalance Action"]) else ""
                    rebalance_value = row["Rebalance Value"] if pd.notna(row["Rebalance Value"]) else np.nan
                    if action == "buy":
                        rebalance_value = abs(rebalance_value) if pd.notna(rebalance_value) else np.nan
                    elif action == "sell":
                        rebalance_value = -abs(rebalance_value) if pd.notna(rebalance_value) else np.nan
                    suggested_hedge_ratio = (
                        (rebalance_value / lp_qty * 100)
                        if pd.notna(rebalance_value) and pd.notna(lp_qty) and lp_qty != 0
                        else np.nan
                    )

                # Error handling for hedging data
                if hedging_error:
                    hedged_qty = np.nan
                    hedge_amount = np.nan
                    funding_rate = np.nan
                    rebalance_value = np.nan
                    action = ""
                    net_gross_ratio = np.nan
                    suggested_hedge_ratio = np.nan

                hedge_button = None
                close_button = None
                if not is_auto and action in ["buy", "sell"] and pd.notna(rebalance_value) and not hedging_error:
                    hedge_button = put_buttons(
                        [{'label': 'Hedge', 'value': f"hedge_{token}", 'color': 'primary'}],
                        onclick=lambda v, t=token, rv=abs(rebalance_value), a=action: run_async(
                            hedge_actions.handle_hedge_click(t, rv, a)
                        )
                    )
                if not is_auto and abs(hedged_qty) != 0 and not pd.isna(hedged_qty) and not hedging_error:
                    close_button = put_buttons(
                        [{'label': 'Close', 'value': f"close_{token}", 'color': 'danger'}],
                        onclick=lambda v, t=token, hq=hedged_qty: run_async(
                            hedge_actions.handle_close_hedge(t, hq, dataframes.get("Hedging"))
                        )
                    )

                if hedge_button or close_button:
                    button = put_row([
                        hedge_button if hedge_button else put_text(""),
                        put_text(" "),
                        close_button if close_button else put_text("")
                    ], size='auto 5px auto')
                else:
                    button = put_text("Auto" if is_auto else "No action needed")

                token_data.append([
                    token,
                    lp_amount_usd,  # Store raw value for sorting
                    f"{lp_amount_usd:.0f}" if pd.notna(lp_amount_usd) else "N/A",
                    f"{hedge_amount:.0f}" if pd.notna(hedge_amount) else "N/A",
                    f"{lp_qty:.4f}" if pd.notna(lp_qty) else "N/A",
                    f"{net_gross_ratio:.0f}%" if pd.notna(net_gross_ratio) else "N/A",
                    f"{suggested_hedge_ratio:.0f}%" if pd.notna(suggested_hedge_ratio) else "N/A",
                    button,
                    f"{funding_rate:.0f}" if pd.notna(funding_rate) else "N/A"
                ])

        elif "Hedging" in dataframes and not hedging_error:
            hedging_df = dataframes["Hedging"]
            hedging_agg = hedging_df.groupby("symbol").agg({
                "quantity": "sum",
                "amount": "sum",
                "funding_rate": "mean"
            }).reset_index()

            for _, row in hedging_agg.iterrows():
                token = row["symbol"].replace("USDT", "").strip()
                use_krystal = not krystal_error
                use_meteora = not meteora_error
                lp_amount_usd, lp_qty, has_krystal, has_meteora = calculate_token_usd_value(
                    token, krystal_df, meteora_df, use_krystal, use_meteora
                )

                # Adjust lp_qty for factored tokens
                factor = (
                    1000 if any(token.startswith(factor_symbol) for factor_symbol in BITGET_TOKENS_WITH_FACTOR_1000.values())
                    else 10000 if any(token.startswith(factor_symbol) for factor_symbol in BITGET_TOKENS_WITH_FACTOR_10000.values())
                    else 1
                )
                if pd.notna(lp_qty):
                    lp_qty = lp_qty / factor

                hedged_qty = row["quantity"]
                hedge_amount = row["amount"]
                funding_rate = row["funding_rate"] * 10000
                is_auto = auto_hedge_tokens.get(token, False)

                # Calculate Net/Gross Ratio (%) : ((lp_qty + hedge_qty) / (lp_qty - hedge_qty)) * 100
                net_gross_ratio = (
                    ((lp_qty + hedged_qty) / (lp_qty - hedged_qty)) * 100
                    if pd.notna(hedged_qty) and pd.notna(lp_qty) and (lp_qty - hedged_qty) != 0
                    else np.nan
                )
                action = ""
                rebalance_value = np.nan
                suggested_hedge_ratio = np.nan

                action_buttons = []
                if not is_auto and abs(hedged_qty) > 0:
                    action_buttons.append({'label': 'Close', 'value': f"close_{token}", 'color': 'danger'})
                if not is_auto and action in ["buy", "sell"] and not pd.isna(rebalance_value):
                    action_buttons.append({'label': 'Hedge', 'value': f"hedge_{token}", 'color': "primary"})

                if action_buttons:
                    button = put_buttons(
                        action_buttons,
                        onclick=lambda v, t=token, hq=hedged_qty, rv=abs(rebalance_value) if pd.notna(rebalance_value) else 0, a=action: run_async(
                            hedge_actions.handle_hedge_click(t, rv, a) if 'hedge' in v else hedge_actions.handle_close_hedge(t, hq, dataframes.get("Hedging"))
                        )
                    )
                else:
                    button = put_text("Auto" if is_auto else "No action needed")

                token_data.append([
                    token,
                    lp_amount_usd,  # Store raw value for sorting
                    f"{lp_amount_usd:.0f}" if pd.notna(lp_amount_usd) else "N/A",
                    f"{hedge_amount:.0f}" if pd.notna(hedge_amount) else "N/A",
                    f"{lp_qty:.4f}" if pd.notna(lp_qty) else "N/A",
                    f"{net_gross_ratio:.0f}%" if pd.notna(net_gross_ratio) else "N/A",
                    f"{suggested_hedge_ratio:.0f}%" if pd.notna(suggested_hedge_ratio) else "N/A",
                    button,
                    f"{funding_rate:.0f}" if pd.notna(funding_rate) else "N/A"
                ])

        if token_data:
            # Sort token_data by lp_amount_usd (index 1) in descending order, handling "N/A" values
            token_data_sorted = sorted(
                token_data,
                key=lambda x: x[1] if pd.notna(x[1]) else float('-inf'),
                reverse=True
            )
            # Remove the raw lp_amount_usd value (index 1) from each row before rendering
            token_data_sorted = [row[0:1] + row[2:] for row in token_data_sorted]
            put_table(token_data_sorted, header=token_headers)
        else:
            put_text("No rebalancing or hedging data available.")
    else:
        put_text("No rebalancing or hedging data available.")

def render_hedge_automation():
    """
    Prepare checkbox options for hedgeable tokens and ensure auto_hedge_tokens.json is synced.
    Returns: tuple of (options, auto_hedge_tokens)
        - options: List of checkbox options for PyWebIO.
        - auto_hedge_tokens: Current auto-hedge tokens dictionary.
    """
    auto_hedge_tokens = load_auto_hedge_tokens()
    hedgable_tokens = sorted([ticker.replace("USDT", "") for ticker in HEDGABLE_TOKENS.keys()])
    
    # Sync auto_hedge_tokens with hedgable_tokens
    updated = False
    # Add new tokens
    for token in hedgable_tokens:
        if token not in auto_hedge_tokens:
            auto_hedge_tokens[token] = False
            updated = True
    # Remove obsolete tokens
    for token in list(auto_hedge_tokens.keys()):
        if token not in hedgable_tokens:
            del auto_hedge_tokens[token]
            updated = True
    
    # Save if updated
    if updated:
        save_auto_hedge_tokens(auto_hedge_tokens)
    
    # Create checkbox options
    options = [
        {'label': token, 'value': token, 'selected': auto_hedge_tokens[token]}
        for token in hedgable_tokens
    ]
    
    return options, auto_hedge_tokens


from pywebio.output import put_markdown, toast, popup
from pywebio.input import input_group, input, select, actions
import pandas as pd
from pywebio.session import run_async
import logging

logger = logging.getLogger('render_ui_tables')

async def render_custom_hedge_section(hedge_actions):
    put_markdown("## Custom Hedge Order")
    put_text("Select token to hedge, enter UNSIGNED quantity, choose action (buy/sell), and submit to create a custom hedge order.")
    try:
        # Load hedgeable tokens from HEDGABLE_TOKENS
        logger.debug("Loading HEDGABLE_TOKENS for custom hedge")
        tokens = sorted([ticker.replace("USDT", "") for ticker in HEDGABLE_TOKENS.keys()])
        if not tokens:
            put_markdown("No hedgeable tokens available.")
            logger.warning("No tokens found in HEDGABLE_TOKENS")
            return

        logger.debug(f"Tokens available: {tokens}")
        # Render button to trigger custom hedge form
        put_buttons(
            [{'label': 'Build Custom Hedge Order', 'value': 'build_hedge', 'color': 'primary'}],
            onclick=lambda _: run_async(show_custom_hedge_form(hedge_actions, tokens))
        )
    except Exception as e:
        logger.error(f"Error rendering custom hedge section: {str(e)}")
        toast(f"Error rendering custom hedge section: {str(e)}", duration=5, color="error")

async def show_custom_hedge_form(hedge_actions, tokens):
    logger.debug("Showing custom hedge form")
    inputs = await input_group(
        "Enter custom hedge details",
        [
            select(
                "Token",
                name="token",
                options=[{"label": token, "value": token} for token in tokens]
            ),
            input(
                "Quantity",
                name="quantity",
                type="number",
                validate=lambda x: None if isinstance(x, (int, float)) and x > 0 else "Quantity must be a positive number",
                placeholder="Enter quantity (e.g., 100.5)"
            ),
            select(
                "Action",
                name="action",
                options=[
                    {"label": "Buy", "value": "buy"},
                    {"label": "Sell", "value": "sell"}
                ]
            )
        ],
        cancelable=True
    )
    if inputs is None:
        logger.debug("Custom hedge input cancelled")
        toast("Custom hedge cancelled", duration=5, color="info")
        return

    logger.debug(f"Custom hedge inputs: {inputs}")
    await hedge_actions.handle_custom_hedge(inputs)