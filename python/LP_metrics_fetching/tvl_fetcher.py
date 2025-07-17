import pandas as pd
import os
from LP_metrics_fetching.geckoTerminalClient import GeckoTerminalClient
from common.path_config import METEORA_LATEST_CSV, KRYSTAL_LATEST_CSV, ACTIVE_POOLS_TVL
import time
import logging

logger = logging.getLogger(__name__)

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

def process_lp_positions(csv_files: list, output_csv: str = "active_pools.csv", batch_size: int = 50):
    """Process LP positions from multiple CSV files, fetch pool metrics in batches, and save to output CSV incrementally."""
    # Initialize output CSV with headers, overwriting if it exists
    headers = ['chain', 'pool_address', 'tvl_usd', 'volume_24h_usd']
    pd.DataFrame(columns=headers).to_csv(output_csv, index=False)

    # Read and combine CSV files
    dfs = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error reading CSV file {csv_file}: {e}")
            continue

    if not dfs:
        logger.warning("No valid CSV files provided")
        return

    df = pd.concat(dfs, ignore_index=True)

    # Ensure required columns exist
    required_columns = {'Pool Address', 'Chain'}
    if not required_columns.issubset(df.columns):
        logger.warning(f"CSV files must contain {required_columns} columns")
        return

    # Extract unique pool addresses and their chains
    df = df[['Pool Address', 'Chain']].drop_duplicates()
    df['Chain'] = df['Chain'].str.lower().map(CHAIN_MAPPING)
    df = df.dropna(subset=['Chain'])  # Drop rows with unmapped chains

    if df.empty:
        logger.warning("No valid pool addresses with mapped chains found")
        return

    # Initialize GeckoTerminal client
    client = GeckoTerminalClient()

    # Group pool addresses by network
    grouped = df.groupby('Chain')['Pool Address'].apply(list).to_dict()

    # Fetch metrics for each network in batches
    for network, pool_addresses in grouped.items():
        # Process pool addresses in batches of batch_size
        for i in range(0, len(pool_addresses), batch_size):
            batch = pool_addresses[i:i + batch_size]
            metrics_list = client.fetch_multi_pool_metrics(network, batch)
            if metrics_list:
                # Write each batch to CSV incrementally
                batch_df = pd.DataFrame(metrics_list)
                batch_df.to_csv(output_csv, mode='a', header=False, index=False)
            time.sleep(0.2)  # Small delay to avoid hitting rate limits

    logger.info(f"Output saved to {output_csv}")

if __name__ == "__main__":
    # Example usage with METEORA and KRYSTAL CSVs
    csv_files = [METEORA_LATEST_CSV, KRYSTAL_LATEST_CSV]
    process_lp_positions(csv_files=csv_files, output_csv=ACTIVE_POOLS_TVL)