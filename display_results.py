#!/usr/bin/env python3
# display_results.py

import os
import pandas as pd
from pywebio import start_server
from pywebio.output import *

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "lp-data")

# CSV file paths (relative to DATA_DIR)
REBALANCING_CSV = os.path.join(DATA_DIR, "rebalancing_results.csv")
KRYSTAL_CSV = os.path.join(DATA_DIR, "LP_krystal_positions_latest.csv")
METEORA_CSV = os.path.join(DATA_DIR, "LP_meteora_positions_latest.csv")
HEDGING_CSV = os.path.join(DATA_DIR, "hedging_positions_latest.csv")
METEORA_PNL_CSV = os.path.join(DATA_DIR, "position_pnl_results.csv")  # New PnL CSV

# Placeholder function for hedge trade execution with Bitget message
def execute_hedge_trade(token, rebalance_value):
    order_size = abs(rebalance_value)
    order_type = "Buy" if rebalance_value > 0 else "Sell"
    order_message = f"Placing {order_type} order on Bitget: {order_size:.4f} {token} @ market price"
    put_text(order_message)
    toast(f"Hedge trade triggered for {token}: {order_message}", duration=5, color="success")

# Function to truncate wallet address
def truncate_wallet(wallet):
    return f"{wallet[:5]}..." if isinstance(wallet, str) and len(wallet) > 5 else wallet

# Function to calculate total USD value for a token from Krystal and Meteora CSVs
def calculate_token_usd_value(token, krystal_df=None, meteora_df=None):
    total_usd = 0.0

    # Process Krystal positions
    if krystal_df is not None and not krystal_df.empty:
        # Check Token X
        token_x_matches = krystal_df[krystal_df["Token X Symbol"] == token]
        for _, row in token_x_matches.iterrows():
            total_usd += float(row["Token X USD Amount"]) if pd.notna(row["Token X USD Amount"]) else 0

        # Check Token Y
        token_y_matches = krystal_df[krystal_df["Token Y Symbol"] == token]
        for _, row in token_y_matches.iterrows():
            total_usd += float(row["Token Y USD Amount"]) if pd.notna(row["Token Y USD Amount"]) else 0

    # Process Meteora positions
    if meteora_df is not None and not meteora_df.empty:
        # Check Token X
        token_x_matches = meteora_df[meteora_df["Token X Symbol"] == token]
        for _, row in token_x_matches.iterrows():
            total_usd += float(row["Token X USD Amount"]) if pd.notna(row["Token X USD Amount"]) else 0

        # Check Token Y
        token_y_matches = meteora_df[meteora_df["Token Y Symbol"] == token]
        for _, row in token_y_matches.iterrows():
            total_usd += float(row["Token Y USD Amount"]) if pd.notna(row["Token Y USD Amount"]) else 0

    return total_usd

# Main web application function
def main():
    # Load CSVs with error handling
    csv_files = {
        "Rebalancing": REBALANCING_CSV,
        "Krystal": KRYSTAL_CSV,
        "Meteora": METEORA_CSV,
        "Hedging": HEDGING_CSV,
        "Meteora PnL": METEORA_PNL_CSV
    }
    dataframes = {}
    for name, path in csv_files.items():
        if not os.path.exists(path):
            put_error(f"Error: {path} not found!")
            continue
        dataframes[name] = pd.read_csv(path)

    # Table 1: Wallet Positions (Krystal and Meteora)
    put_markdown("## Wallet Positions")

    wallet_headers = [
        "Source", "Wallet", "Chain", "Protocol", "Pair", "Token X Qty", "Token Y Qty", "Current Price", "Min Price", "Max Price",
        "In Range", "Fee APR", "Initial USD", "Present USD", "Pool Address"
    ]
    wallet_data = []

    if "Krystal" in dataframes:
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
                row["Token X Qty"],
                row["Token Y Qty"],
                f"{row['Current Price']:.6f}" if pd.notna(row["Current Price"]) else "N/A",
                f"{row['Min Price']:.6f}" if pd.notna(row["Min Price"]) else "N/A",
                f"{row['Max Price']:.6f}" if pd.notna(row["Max Price"]) else "N/A",
                "Yes" if row["Is In Range"] else "No",
                f"{row['Fee APR']:.2%}" if pd.notna(row["Fee APR"]) else "N/A",
                f"{row['Initial Value USD']:.2f}" if pd.notna(row["Initial Value USD"]) else "N/A",
                f"{row['Actual Value USD']:.2f}" if pd.notna(row["Actual Value USD"]) else "N/A",
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
                row["Token X Qty"],
                row["Token Y Qty"],
                "N/A",
                f"{row['Lower Boundary']:.6f}" if pd.notna(row["Lower Boundary"]) else "N/A",
                f"{row['Upper Boundary']:.6f}" if pd.notna(row["Upper Boundary"]) else "N/A",
                "Yes" if row["Is In Range"] else "No",
                "N/A",
                "N/A",
                f"{present_usd:.2f}",
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
            "Timestamp", "Position ID", "Owner", "Pool Address", "Pair",
            "Realized PNL (USD)", "Unrealized PNL (USD)", "Net PNL (USD)",
            "Realized PNL (Token B)", "Unrealized PNL (Token B)", "Net PNL (Token B)"
        ]
        pnl_data = []

        meteora_pnl_df = dataframes["Meteora PnL"]
        for _, row in meteora_pnl_df.iterrows():
            pair = f"{row['Token X Symbol']}-{row['Token Y Symbol']}"
            pnl_data.append([
                row["Timestamp"],
                row["Position ID"],
                truncate_wallet(row["Owner"]),
                row["Pool Address"],
                pair,
                f"{row['Realized PNL (USD)']:.2f}",
                f"{row['Unrealized PNL (USD)']:.2f}",
                f"{row['Net PNL (USD)']:.2f}",
                f"{row['Realized PNL (Token B)']:.2f}",
                f"{row['Unrealized PNL (Token B)']:.2f}",
                f"{row['Net PNL (Token B)']:.2f}"
            ])

        if pnl_data:
            put_table(pnl_data, header=pnl_headers)
        else:
            put_text("No PnL data found in Meteora PnL CSV.")

    # Table 3: Token Hedge Summary (Rebalancing + Hedging)
    if "Rebalancing" in dataframes:
        put_markdown("## Token Hedge Summary")
        rebalancing_df = dataframes["Rebalancing"]
        
        token_agg = rebalancing_df.groupby("Token").agg({
            "LP Qty": "sum",
            "Hedged Qty": "sum",
            "Rebalance Value": "sum"
        }).reset_index()

        if "Hedging" in dataframes:
            hedging_df = dataframes["Hedging"]
            hedging_agg = hedging_df.groupby("symbol").agg({
                "quantity": "sum",
                "amount": "sum"
            }).reset_index().rename(columns={"symbol": "Token"})
            token_summary = pd.merge(token_agg, hedging_agg, on="Token", how="left")
            token_summary["quantity"] = token_summary["quantity"].fillna(0)
            token_summary["amount"] = token_summary["amount"].fillna(0)
        else:
            token_summary = token_agg
            token_summary["quantity"] = 0
            token_summary["amount"] = 0

        token_data = []
        krystal_df = dataframes.get("Krystal")
        meteora_df = dataframes.get("Meteora")
        def strip_usdt(token):
            return token.replace("USDT", "").strip() if isinstance(token, str) else token

        for _, row in token_summary.iterrows():
            token = strip_usdt(row["Token"])
            lp_qty = row["LP Qty"]
            hedged_qty = row["Hedged Qty"]
            rebalance_value = row["Rebalance Value"]
            hedge_amount = row["amount"]
            
            lp_amount_usd = calculate_token_usd_value(token, krystal_df, meteora_df)
            rebalance_value_with_sign = f"{'+' if rebalance_value > 0 else ''}{rebalance_value:.4f}"
            
            token_data.append([
                token,
                f"{lp_qty:.4f}",
                f"{hedged_qty:.4f}",
                f"{hedge_amount:.4f}",
                f"{lp_amount_usd:.2f}",
                rebalance_value_with_sign,
                put_buttons([{'label': 'Hedge', 'value': token, 'color': 'primary'}], 
                           onclick=lambda t, rv=rebalance_value: execute_hedge_trade(t, rv))
            ])
        
        token_headers = [
            "Token", "LP Qty", "Hedged Qty", "Hedged Amount (USDT)", 
            "LP Amount USD", "Suggested Hedge Qty", "Action"
        ]
        put_table(token_data, header=token_headers)
    elif "Hedging" in dataframes:
        put_markdown("## Token Hedge Summary (Hedging Only)")
        hedging_df = dataframes["Hedging"]
        token_data = hedging_df.groupby("symbol").agg({
            "quantity": "sum",
            "amount": "sum"
        }).reset_index().to_dict('records')
        put_table(token_data, header=["Token", "Hedged Qty", "Hedged Amount (USDT)"])

# Start the PyWebIO server
if __name__ == "__main__":
    start_server(main, port=8080, host="0.0.0.0", debug=True)