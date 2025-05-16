import pandas as pd
import numpy as np
import json
from pathlib import Path
from pywebio.output import put_table, put_text, put_buttons, put_row, put_widget, put_markdown
from pywebio.session import run_async
from common.utils import calculate_token_usd_value
from common.constants import HEDGABLE_TOKENS
from common.path_config import CONFIG_DIR

AUTO_HEDGE_TOKENS_PATH = CONFIG_DIR / "auto_hedge_tokens.json"

def truncate_wallet(wallet):
    return f"{wallet[:5]}..." if isinstance(wallet, str) and len(wallet) > 5 else wallet

def load_auto_hedge_tokens():
    """
    Load tokens selected for auto-hedging from auto_hedge_tokens.json.
    Returns: set of token symbols (e.g., {"ETH", "BTC"}).
    """
    try:
        if AUTO_HEDGE_TOKENS_PATH.exists():
            with AUTO_HEDGE_TOKENS_PATH.open('r') as f:
                data = json.load(f)
                return set(data.get("tokens", []))
        return set()
    except Exception as e:
        print(f"Error loading auto_hedge_tokens.json: {str(e)}")
        return set()

def save_auto_hedge_tokens(tokens):
    """
    Save selected tokens to auto_hedge_tokens.json.
    Args:
        tokens: set or list of token symbols.
    """
    try:
        CONFIG_DIR.mkdir(exist_ok=True)
        with AUTO_HEDGE_TOKENS_PATH.open('w') as f:
            json.dump({"tokens": list(tokens)}, f, indent=2)
    except Exception as e:
        print(f"Error saving auto_hedge_tokens.json: {str(e)}")

def render_wallet_positions(dataframes, error_flags):
    """
    Render wallet positions table for Krystal and Meteora.
    """
    krystal_error = error_flags['errors']['krystal_error']
    meteora_error = error_flags['errors']['meteora_error']
    wallet_headers = [
        "Source", "Wallet", "Chain", "Protocol", "Pair", "In Range", "Fee APR", "Initial USD", "Present USD",
        "Token X Qty", "Token Y Qty", "Current Price", "Min Price", "Max Price", "Pool Address"
    ]
    wallet_data = []

    if "Krystal" in dataframes and not krystal_error:
        krystal_df = dataframes["Krystal"]
        ticker_source = "symbol" if "Token X Symbol" in krystal_df.columns and "Token Y Symbol" in krystal_df.columns else "address"
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

    if "Meteora" in dataframes and not meteora_error:
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

def render_pnl_tables(dataframes, error_flags):
    """
    Render PnL tables for Meteora and Krystal.
    """
    if "Meteora PnL" in dataframes and not error_flags['errors']['meteora_error']:
        put_text("## Meteora Positions PnL")
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

    if "Krystal PnL" in dataframes and not error_flags['errors']['krystal_error']:
        put_text("## Krystal Positions PnL by Pool (Open Pools)")
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
                f"{r['lp_pnl_usd']:.2f}" if pd.notna(r['lp_pnl_usd']) else "N/A",
                f"{r['lp_pnl_tokenB']:.5f}" if pd.notna(r['lp_pnl_tokenB']) else "N/A",
                f"{r['hold_pnl_usd']:.2f}" if pd.notna(r['hold_pnl_usd']) else "N/A",
                f"{r['lp_minus_hold_usd']:.2f}" if pd.notna(r['lp_minus_hold_usd']) else "N/A",
                r["poolAddress"]
            ])
        put_table(pnl_rows, header=pnl_headers)


def render_hedging_dashboard(dataframes, error_flags, hedge_actions):
    """
    Render the hedging dashboard table with a Hedge Automation section above it.
    
    Args:
        dataframes (dict): Loaded CSVs from data_loader
        error_flags (dict): Error flags from data_loader
        hedge_actions (HedgeActions): Instance for handling hedge/close actions
    """
    from pywebio.output import put_markdown, put_html, put_table, put_text, put_buttons, put_row, toast
    from pywebio.session import run_async
    import pandas as pd
    import numpy as np

    errors = error_flags['errors']
    krystal_df = dataframes.get("Krystal")
    meteora_df = dataframes.get("Meteora")
    auto_hedge_tokens = load_auto_hedge_tokens()

    # Hedge Automation Section
    put_markdown("## Hedge Automation")
    hedgable_tokens = [ticker.replace("USDT", "") for ticker in HEDGABLE_TOKENS.keys()]
    async def update_auto_hedge(token, checked):
        auto_hedge_tokens = load_auto_hedge_tokens()
        if checked:
            auto_hedge_tokens.add(token)
            toast(f"Enabled auto-hedge for {token}", duration=3, color="success")
        else:
            auto_hedge_tokens.discard(token)
            toast(f"Disabled auto-hedge for {token}", duration=3, color="success")
        save_auto_hedge_tokens(auto_hedge_tokens)

    checkboxes = []
    for token in sorted(hedgable_tokens):
        checked = "checked" if token in auto_hedge_tokens else ""
        button_id = f"toggle_{token}"
        checkboxes.append(f"""
            <label style='margin-right: 15px;'>
                <input type='checkbox' {checked} onchange='
                    var checked = this.checked;
                    document.getElementById("{button_id}").click();
                '>
                {token}
            </label>
            <button id="{button_id}" style="display: none;"></button>
        """)
        # Add hidden button to trigger Python callback
        put_buttons(
            [{'label': '', 'value': token, 'color': 'primary'}],
            onclick=lambda v, t=token: run_async(update_auto_hedge(t, v in auto_hedge_tokens ^ True)),
            scope=None,
            group=button_id
        )
    checkbox_html = f"<div style='display: flex; flex-wrap: wrap; gap: 10px;'>{''.join(checkboxes)}</div>"
    put_html(checkbox_html)

    if "Rebalancing" in dataframes or "Hedging" in dataframes:
        token_headers = [
            "Token", "LP Amount USD", "Hedge Amount USD", "LP Qty", "Hedge Qty",
            "Suggested Hedge Qty", "Action", "Funding Rate (BIPS)"
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
                token_summary = token_agg
                token_summary["quantity"] = 0
                token_summary["amount"] = 0
                token_summary["funding_rate"] = 0

            for _, row in token_summary.iterrows():
                token = row["Token"].replace("USDT", "").strip()
                use_krystal = not errors['krystal_error']
                use_meteora = not errors['meteora_error']
                lp_amount_usd, lp_qty, has_krystal, has_meteora = calculate_token_usd_value(
                    token, krystal_df, meteora_df, use_krystal, use_meteora
                )
                hedged_qty = row["quantity"]
                hedge_amount = row["amount"]
                funding_rate = row["funding_rate"] * 10000
                is_auto = token in auto_hedge_tokens

                # For auto-hedged tokens, suppress Suggested Hedge Qty and Action
                if is_auto:
                    rebalance_value = np.nan
                    action = ""
                else:
                    action = row["Rebalance Action"].strip().lower() if pd.notna(row["Rebalance Action"]) else ""
                    rebalance_value = row["Rebalance Value"] if pd.notna(row["Rebalance Value"]) else np.nan
                    if action == "buy":
                        rebalance_value = abs(rebalance_value) if pd.notna(rebalance_value) else np.nan
                    elif action == "sell":
                        rebalance_value = -abs(rebalance_value) if pd.notna(rebalance_value) else np.nan

                if (errors['meteora_error'] and has_meteora) or (errors['krystal_error'] and has_krystal):
                    lp_qty = np.nan
                    lp_amount_usd = np.nan
                    rebalance_value = np.nan
                    action = ""
                else:
                    lp_qty = lp_qty if pd.notna(lp_qty) else np.nan
                    lp_amount_usd = lp_amount_usd if pd.notna(lp_amount_usd) else np.nan

                if errors['hedging_error']:
                    hedged_qty = np.nan
                    hedge_amount = np.nan
                    funding_rate = np.nan
                    rebalance_value = np.nan
                    action = ""

                hedge_button = None
                close_button = None
                if not is_auto and action in ["buy", "sell"] and pd.notna(rebalance_value) and not errors['hedging_error']:
                    hedge_button = put_buttons(
                        [{'label': 'Hedge', 'value': f"hedge_{token}", 'color': 'primary'}],
                        onclick=lambda v, t=token, rv=abs(rebalance_value), a=action: run_async(
                            hedge_actions.handle_hedge_click(t, rv, a)
                        )
                    )
                if abs(hedged_qty) != 0 and not pd.isna(hedged_qty) and not errors['hedging_error']:
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
                    f"{lp_amount_usd:.2f}" if pd.notna(lp_amount_usd) else "N/A",
                    f"{hedge_amount:.4f}" if pd.notna(hedge_amount) else "N/A",
                    f"{lp_qty:.4f}" if pd.notna(lp_qty) else "N/A",
                    f"{hedged_qty:.4f}" if pd.notna(hedged_qty) else "N/A",
                    f"{rebalance_value:.6f}" if pd.notna(rebalance_value) else "N/A",
                    button,
                    f"{funding_rate:.0f}" if pd.notna(funding_rate) else "N/A"
                ])

        elif "Hedging" in dataframes and not errors['hedging_error']:
            hedging_df = dataframes["Hedging"]
            hedging_agg = hedging_df.groupby("symbol").agg({
                "quantity": "sum",
                "amount": "sum",
                "funding_rate": "mean"
            }).reset_index()

            for _, row in hedging_agg.iterrows():
                token = row["symbol"].replace("USDT", "").strip()
                hedged_qty = row["quantity"]
                hedge_amount = row["amount"]
                funding_rate = row["funding_rate"] * 10000
                use_krystal = not errors['krystal_error']
                use_meteora = not errors['meteora_error']
                lp_amount_usd, lp_qty, has_krystal, has_meteora = calculate_token_usd_value(
                    token, krystal_df, meteora_df, use_krystal, use_meteora
                )
                is_auto = token in auto_hedge_tokens

                # For auto-hedged tokens, suppress Suggested Hedge Qty and Action
                if is_auto:
                    rebalance_value = np.nan
                    action = ""
                else:
                    action = row.get("Rebalance Action", "").strip().lower() if pd.notna(row.get("Rebalance Action")) else ""
                    rebalance_value = row.get("Rebalance Value") if pd.notna(row.get("Rebalance Value")) else np.nan
                    if action == "buy":
                        rebalance_value = abs(rebalance_value) if pd.notna(rebalance_value) else np.nan
                    elif action == "sell":
                        rebalance_value = -abs(rebalance_value) if pd.notna(rebalance_value) else np.nan

                if (errors['meteora_error'] and has_meteora) or (errors['krystal_error'] and has_krystal):
                    lp_qty = np.nan
                    lp_amount_usd = np.nan
                    rebalance_value = np.nan
                    action = ""
                else:
                    lp_qty = lp_qty if pd.notna(lp_qty) else np.nan
                    lp_amount_usd = lp_amount_usd if pd.notna(lp_amount_usd) else np.nan

                if errors['hedging_error']:
                    hedged_qty = np.nan
                    hedge_amount = np.nan
                    funding_rate = np.nan
                    rebalance_value = np.nan
                    action = ""

                action_buttons = []
                if not is_auto and abs(hedged_qty) > 0:
                    action_buttons.append({'label': 'Close', 'value': f"close_{token}", 'color': 'danger'})
                if not is_auto and action in ["buy", "sell"] and not pd.isna(rebalance_value):
                    action_buttons.append({'label': 'Hedge', 'value': f"hedge_{token}", 'color': 'primary'})

                if action_buttons:
                    button = put_buttons(
                        action_buttons,
                        onclick=lambda v, t=token, hq=hedged_qty, rv=abs(rebalance_value), a=action: run_async(
                            hedge_actions.handle_hedge_click(t, rv, a) if 'hedge' in v else hedge_actions.handle_close_hedge(t, hq, dataframes.get("Hedging"))
                        )
                    )
                else:
                    button = put_text("Auto" if is_auto else "No action needed")

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

        if token_data:
            put_table(token_data, header=token_headers)
        else:
            put_text("No rebalancing or hedging data available.")
    else:
        put_text("No rebalancing or hedging data available.")