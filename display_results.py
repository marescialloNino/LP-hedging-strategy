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

# Placeholder function for hedge trade execution
def execute_hedge_trade(token, rebalance_value):
    put_text(f"Executing hedge trade for {token}: {rebalance_value} USDT")
    toast(f"Hedge trade triggered for {token}", duration=3, color="success")

# Function to truncate wallet address
def truncate_wallet(wallet):
    return f"{wallet[:5]}..." if isinstance(wallet, str) and len(wallet) > 5 else wallet

# Main web application function
def main():
    # Load CSVs with error handling
    csv_files = {
        "Rebalancing": REBALANCING_CSV,
        "Krystal": KRYSTAL_CSV,
        "Meteora": METEORA_CSV,
        "Hedging": HEDGING_CSV
    }
    dataframes = {}
    for name, path in csv_files.items():
        if not os.path.exists(path):
            put_error(f"Error: {path} not found!")
            continue
        dataframes[name] = pd.read_csv(path)

    # Table 1: Wallet Positions (Krystal and Meteora)
    put_markdown("## Wallet Positions")

    # Define headers for the table
    wallet_headers = [
        "Source", "Wallet", "Chain", "Protocol", "Pair", "Token X Qty", "Token X Qty","Current Price", "Min Price", "Max Price",
        "In Range", "Fee APR", "Initial USD", "Present USD", "Pool Address"
    ]
    wallet_data = []

    # Process Krystal positions
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

    # Process Meteora positions
    if "Meteora" in dataframes:
        meteora_df = dataframes["Meteora"]
        for _, row in meteora_df.iterrows():
            # Use token symbols instead of addresses
            pair_ticker = f"{row['Token X Symbol']}-{row['Token Y Symbol']}"
            
            # Calculate Present USD value
            qty_x = float(row["Token X Qty"])
            qty_y = float(row["Token Y Qty"])
            price_x = float(row["Token X Price USD"])
            price_y = float(row["Token Y Price USD"])
            present_usd = (qty_x * price_x) + (qty_y * price_y)
            
            wallet_data.append([
                "Meteora",
                truncate_wallet(row["Wallet Address"]),
                "Solana",
                "Meteora",
                pair_ticker,
                row["Token X Qty"],
                row["Token Y Qty"],
                "N/A",  # Current price not available
                f"{row['Lower Boundary']:.6f}" if pd.notna(row["Lower Boundary"]) else "N/A",
                f"{row['Upper Boundary']:.6f}" if pd.notna(row["Upper Boundary"]) else "N/A",
                "Yes" if row["Is In Range"] else "No",
                "N/A",  # Fee APR not available
                "N/A",  # Initial USD not available
                f"{present_usd:.2f}",  # Present USD value
                row["Pool Address"]
            ])

    # Display the table if there's data
    if wallet_data:
        put_table(wallet_data, header=wallet_headers)
    else:
        put_text("No wallet positions found in Krystal or Meteora CSVs.")

    # Table 2: Token Hedge Summary (Rebalancing + Hedging)
    if "Rebalancing" in dataframes:
        put_markdown("## Token Hedge Summary")
        rebalancing_df = dataframes["Rebalancing"]
        
        # Calculate LP Amount USD for each token
        if "Meteora" in dataframes:
            meteora_df = dataframes["Meteora"]
            # Create a mapping of token symbols to their USD values
            token_usd_values = {}
            for _, row in meteora_df.iterrows():
                token_x = row["Token X Symbol"]
                token_y = row["Token Y Symbol"]
                qty_x = float(row["Token X Qty"])
                qty_y = float(row["Token Y Qty"])
                price_x = float(row["Token X Price USD"])
                price_y = float(row["Token Y Price USD"])
                
                # Add USD values for both tokens
                if token_x not in token_usd_values:
                    token_usd_values[token_x] = 0
                if token_y not in token_usd_values:
                    token_usd_values[token_y] = 0
                    
                token_usd_values[token_x] += qty_x * price_x
                token_usd_values[token_y] += qty_y * price_y

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
        for _, row in token_summary.iterrows():
            token = row["Token"]
            lp_qty = row["LP Qty"]
            hedged_qty = row["Hedged Qty"]
            rebalance_value = row["Rebalance Value"]
            hedge_amount = row["amount"]
            
            # Get USD value for the token
            lp_amount_usd = token_usd_values.get(token, 0)
            
            # Add sign to rebalance value
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