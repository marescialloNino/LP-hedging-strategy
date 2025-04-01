#!/usr/bin/env python3
# display_results.py

import os
import pandas as pd
from pywebio import start_server
from pywebio.output import *
from decimal import Decimal, InvalidOperation

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
    put_text(f"Executing hedge trade for {token}: {rebalance_value}") # Value might need sign adjustment based on final implementation
    toast(f"Hedge trade triggered for {token}", duration=3, color="success")

# Function to truncate wallet address
def truncate_wallet(wallet):
    return f"{wallet[:5]}..." if isinstance(wallet, str) and len(wallet) > 5 else wallet

# Function to safely convert to Decimal
def safe_decimal(value, default=Decimal(0)):
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return default

# Function to get token prices from Meteora and Krystal data
def get_token_prices(meteora_df, krystal_df):
    prices = {}
    # Prioritize Meteora prices
    if meteora_df is not None:
        for _, row in meteora_df.iterrows():
            if pd.notna(row['Token X Symbol']) and pd.notna(row['Token X Price USD']) and row['Token X Price USD'] > 0:
                 # Use uppercase token symbol consistent with hedging/rebalancing
                prices[row['Token X Symbol'].upper() + 'USDT'] = safe_decimal(row['Token X Price USD'])
            if pd.notna(row['Token Y Symbol']) and pd.notna(row['Token Y Price USD']) and row['Token Y Price USD'] > 0:
                 # Use uppercase token symbol consistent with hedging/rebalancing
                prices[row['Token Y Symbol'].upper() + 'USDT'] = safe_decimal(row['Token Y Price USD'])

    # Add Krystal prices if not already present (Needs logic refinement based on Krystal 'Current Price' definition)
    # Example: if krystal_df is not None:
    #     for _, row in krystal_df.iterrows():
    #         # Logic to determine which token price 'Current Price' represents and convert if needed
    #         token_symbol_x = row['Token X Symbol'].upper() + 'USDT'
    #         token_symbol_y = row['Token Y Symbol'].upper() + 'USDT'
    #         if token_symbol_x not in prices and pd.notna(row['Current Price']) and row['Current Price'] > 0:
    #              # This assumes 'Current Price' is price of X in terms of Y - needs verification
    #              # prices[token_symbol_x] = safe_decimal(row['Current Price']) * prices.get(token_symbol_y, Decimal(0)) # Example complex logic
    #              pass # Add specific logic based on how Krystal 'Current Price' works

    return prices


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
            put_warning(f"Warning: {path} not found!") # Changed to warning
            dataframes[name] = None # Set to None if not found
            continue
        try:
            dataframes[name] = pd.read_csv(path)
        except Exception as e:
            put_error(f"Error loading {path}: {e}")
            dataframes[name] = None


    # --- Table 1: Wallet Positions (Krystal and Meteora) ---
    put_markdown("## Wallet Positions")
    # (Code for Wallet Positions Table remains largely the same, ensure it handles None dataframes)
    # ... [Existing Wallet Positions Table Code - check for dataframes[name] is not None before processing] ...
    # Example check:
    wallet_data = []
    if dataframes["Krystal"] is not None:
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

    if dataframes["Meteora"] is not None:
         meteora_df = dataframes["Meteora"]
         for _, row in meteora_df.iterrows():
             # Placeholder pair ticker (update later with symbols if added)
             pair_ticker = f"{row['Token X Address'][:5]}...-{row['Token Y Address'][:5]}..."
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
                 "N/A",  # Present USD not available
                 row["Pool Address"]
             ])

    if wallet_data:
         # Headers need to be adjusted based on Krystal CSV changes
         wallet_headers = [
             "Source", "Wallet", "Chain", "Protocol", "Pair", "Token X Qty", "Token Y Qty",
             "Current Price", "Min Price", "Max Price", "In Range", "Fee APR",
             "Initial USD", "Current Underlying USD", "Current Position USD", # Updated Krystal value headers
             "Pool Address"
         ]
         put_table(wallet_data, header=wallet_headers) # Adjust headers based on actual columns loaded
    else:
         put_text("No wallet positions found or CSVs missing.")


    # --- Table 2: Token Hedge Summary (Rebalancing + Hedging) ---
    put_markdown("## Token Hedge Summary")

    if dataframes["Rebalancing"] is not None:
        rebalancing_df = dataframes["Rebalancing"]
        token_prices = get_token_prices(dataframes["Meteora"], dataframes["Krystal"])

        # Aggregate rebalancing data first
        token_agg = rebalancing_df.groupby("Token").agg(
            lp_qty_total=('LP Qty', 'sum'),
            hedged_qty_total=('Hedged Qty', 'sum')
            # We recalculate difference and action based on totals
        ).reset_index()

        # Merge hedging data if available
        if dataframes["Hedging"] is not None:
            hedging_df = dataframes["Hedging"]
            hedging_agg = hedging_df.groupby("symbol").agg(
                hedge_qty_actual=('quantity', 'sum'),
                hedge_amount_usd=('amount', 'sum')
            ).reset_index().rename(columns={"symbol": "Token"})
            token_summary = pd.merge(token_agg, hedging_agg, on="Token", how="left")
            token_summary["hedge_qty_actual"] = token_summary["hedge_qty_actual"].fillna(0)
            token_summary["hedge_amount_usd"] = token_summary["hedge_amount_usd"].fillna(0)
        else:
            token_summary = token_agg
            token_summary["hedge_qty_actual"] = 0.0
            token_summary["hedge_amount_usd"] = 0.0

        token_data = []
        for _, row in token_summary.iterrows():
            token = row["Token"]
            lp_qty = safe_decimal(row["lp_qty_total"])
            # Use actual hedge quantity from hedging file if available, else from rebalancing calc
            hedge_qty = safe_decimal(row.get("hedge_qty_actual", row.get("hedged_qty_total", 0)))
            hedge_amount_usd = safe_decimal(row["hedge_amount_usd"])

            # Calculate LP Amount USD
            price = token_prices.get(token, Decimal(0))
            lp_amount_usd = lp_qty * price if price > 0 else Decimal(0)

            # Recalculate difference based on aggregated values
            abs_hedge_qty = abs(hedge_qty)
            difference = lp_qty - abs_hedge_qty
            abs_difference = abs(difference)

            # Determine signed rebalance quantity based on aggregated values
            signed_rebalance_qty = Decimal(0.0)
            # Re-fetch the action from the original df based on token (simplistic, assumes one action per token)
            # A more robust approach might involve flags or recalculating the dominant action needed.
            original_actions = rebalancing_df[rebalancing_df['Token'] == token]
            action = "nothing"
            rebalance_value = Decimal(0.0)
            if not original_actions.empty:
                 # Prioritize 'close', then 'buy'/'sell' if multiple rows exist per token
                 if 'close' in original_actions['Rebalance Action'].values:
                      action = 'close'
                      rebalance_value = abs_hedge_qty # Value needed to close
                 else:
                      # Sum values for buy/sell if needed, or take first action
                      action = original_actions['Rebalance Action'].iloc[0]
                      rebalance_value = safe_decimal(original_actions['Rebalance Value'].iloc[0])


            # Determine signed quantity for display
            if action == "sell":  # Increase short -> Negative value
                signed_rebalance_qty = -rebalance_value
            elif action == "buy": # Decrease short -> Positive value
                signed_rebalance_qty = rebalance_value
            elif action == "close": # Close short -> Positive value (buying back)
                signed_rebalance_qty = rebalance_value # Value is the amount to buy back
            else: # nothing
                signed_rebalance_qty = Decimal(0.0)


            token_data.append([
                token,
                f"{lp_qty:.4f}",
                f"{lp_amount_usd:.2f}",          # Added LP Amount USD
                f"{hedge_qty:.4f}",             # Renamed Header: Hedge Qty
                f"{hedge_amount_usd:.2f}",      # Renamed Header: Hedge Amount USD
                # Difference column removed
                f"{signed_rebalance_qty:+.4f}",   # Modified Suggested Hedge Qty with sign
                # Keep the button, but the value passed might need context (token + signed_qty)
                put_buttons([{'label': 'Hedge', 'value': {'token': token, 'qty': float(signed_rebalance_qty)}, 'color': 'primary'}],
                           onclick=lambda val: execute_hedge_trade(val['token'], val['qty']))
            ])

        token_headers = [
            "Token", "LP Qty", "LP Amount USD", "Hedge Qty", "Hedge Amount USD",
            "Suggested Hedge Qty", "Action"
        ]
        put_table(token_data, header=token_headers)

    # Handle case where only hedging data exists (optional)
    elif dataframes["Hedging"] is not None:
        put_markdown("## Token Hedge Summary (Hedging Only)")
        hedging_df = dataframes["Hedging"]
        token_data = hedging_df.groupby("symbol").agg(
             hedge_qty_actual=('quantity', 'sum'),
             hedge_amount_usd=('amount', 'sum')
        ).reset_index().to_dict('records')
        # Format numbers
        formatted_data = [[
            rec['symbol'],
            f"{rec['hedge_qty_actual']:.4f}",
            f"{rec['hedge_amount_usd']:.2f}"
            ] for rec in token_data]
        put_table(formatted_data, header=["Token", "Hedge Qty", "Hedge Amount USD"])
    else:
        put_text("Rebalancing results CSV not found. Cannot display hedge summary.")


# Start the PyWebIO server
if __name__ == "__main__":
    # Ensure DATA_DIR exists before starting
    if not os.path.isdir(DATA_DIR):
         put_error(f"Data directory not found: {DATA_DIR}")
         put_text("Please ensure the 'lp-data' directory exists relative to the script or is correctly specified.")
    else:
         start_server(main, port=8080, host="0.0.0.0", debug=True)