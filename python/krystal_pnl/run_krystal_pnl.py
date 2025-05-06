#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_krystal_pnl.py
~~~~~~~~~~~~~~~~~~
Master script to run the Krystal PnL calculation pipeline.
1. Fetches latest open/closed positions.
2. Ensures valid Bitget tickers list exists.
3. Scans positions for required tickers and time windows.
4. Downloads missing price data.
5. Calculates PnL including start date and 50/50 hold comparison.
"""

import asyncio
import sys
import logging
from pathlib import Path
import pandas as pd # Added for loading CSVs in step 3

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path if needed
MODULE_DIR = Path(__file__).resolve().parent
ROOT_DIR = MODULE_DIR.parents[1]
if str(ROOT_DIR) not in sys.path: sys.path.append(str(ROOT_DIR))
if str(MODULE_DIR.parent) not in sys.path: sys.path.append(str(MODULE_DIR.parent)) # For 'common' etc.


# Import necessary functions/modules from the pipeline scripts
try:
    from krystal_pnl import balance_tracker
    from krystal_pnl import bitget_markets
    from krystal_pnl import scan_tickers
    from krystal_pnl import price_downloader
    # We will run the calculator script at the end using runpy
except ImportError as e:
    logger.error(f"ImportError: Failed to import necessary modules. Check imports and paths. Error: {e}", exc_info=True)
    sys.exit(1)
except Exception as e:
     logger.error(f"Unexpected error during imports: {e}", exc_info=True)
     sys.exit(1)


# Fix for Windows asyncio if needed
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def run_pipeline():
    """Executes the Krystal PnL calculation steps."""
    logger.info("--- Starting Krystal PnL Pipeline ---")
    windows_to_download = [] # Initialize

    # 1. Fetch latest positions
    logger.info("Step 1: Fetching Krystal positions...")
    try:
        await balance_tracker.main()
        logger.info("Step 1: Successfully fetched positions.")
    except Exception as e:
        logger.error(f"Step 1: Failed to fetch positions: {e}", exc_info=True)
        return # Stop if cannot fetch positions

    # Check if position files were actually created
    if not balance_tracker.CLOSED_CSV.exists() or not balance_tracker.OPEN_CSV.exists():
         logger.error(f"Step 1: Position files ({balance_tracker.CLOSED_CSV.name}, {balance_tracker.OPEN_CSV.name}) not found after fetch attempt.")
         return

    # 2. Ensure valid Bitget tickers list exists
    logger.info("Step 2: Ensuring valid Bitget tickers list...")
    try:
        if not bitget_markets.CSV_FILE.exists():
             logger.info(f"{bitget_markets.CSV_FILE.name} not found, creating...")
             bitget_markets.build_csv()
        else:
             logger.info(f"{bitget_markets.CSV_FILE.name} found.")
        logger.info("Step 2: Valid tickers list check complete.")
    except Exception as e:
        logger.error(f"Step 2: Failed to ensure valid tickers list: {e}", exc_info=True)
        return # Stop if we can't get valid tickers

    # 3. Scan positions for required tickers and time windows
    logger.info("Step 3: Scanning positions for required tickers and time windows...")
    try:
        # Make sure files exist before reading
        if not balance_tracker.CLOSED_CSV.exists() or not balance_tracker.OPEN_CSV.exists():
             raise FileNotFoundError("Position CSV files for scanning not found.")

        # Call the function with file paths
        required_windows = scan_tickers.build_ticker_timewindows(
            closed_csv=balance_tracker.CLOSED_CSV, # Pass Path object or string
            open_csv=balance_tracker.OPEN_CSV
        )

         # Filter by valid tickers (load them again here for safety)
        valid_tickers_set = price_downloader.load_valid_tickers()
        windows_to_download = [w for w in required_windows if w[0].upper() in valid_tickers_set]

        if not windows_to_download:
            logger.warning("Step 3: No relevant tickers found in positions requiring price downloads.")
        else:
            logger.info(f"Step 3: Found {len(windows_to_download)} ticker/window pairs requiring prices.")

    except FileNotFoundError as e:
         logger.error(f"Step 3: Position files not found for scanning: {e}", exc_info=True)
         return
    except Exception as e:
        logger.error(f"Step 3: Failed to scan tickers/windows: {e}", exc_info=True)
        return

    # 4. Download missing price data
    logger.info("Step 4: Downloading/updating price data...")
    try:
        if windows_to_download: # Only download if needed
             price_downloader.fetch_bitget_open_prices(windows_to_download, fill_gaps=None) # Changed fill_gaps to None
             logger.info("Step 4: Price download/update process complete.")
        else:
            logger.info("Step 4: Skipping price download as no new windows were identified.")
    except Exception as e:
        logger.error(f"Step 4: Failed during price download: {e}", exc_info=True)
        return # Stop if price download fails

    # 5. Calculate PnL (Run the calculator script)
    logger.info("Step 5: Calculating PnL...")
    try:
        # Execute the v3_pnl_calculator script in its own context
        import runpy
        # Construct the full path to the script
        calculator_script_path = MODULE_DIR / "v3_pnl_calculator.py"
        if calculator_script_path.exists():
             runpy.run_path(str(calculator_script_path), run_name="__main__") # run_name ensures it behaves like main script
             logger.info("Step 5: PnL calculation script executed.")
        else:
             logger.error(f"Step 5: PnL calculator script not found at {calculator_script_path}")
             return

    except Exception as e:
        logger.error(f"Step 5: Failed during PnL calculation execution: {e}", exc_info=True)
        return

    logger.info("--- Krystal PnL Pipeline Finished Successfully ---")


if __name__ == "__main__":
    # Ensure the event loop is properly managed
    try:
        asyncio.run(run_pipeline())
    except RuntimeError as e:
        if "Cannot run the event loop while another loop is running" in str(e):
             logger.warning("Event loop already running. Attempting to schedule pipeline.")
             loop = asyncio.get_event_loop()
             loop.create_task(run_pipeline())
             # Note: If run this way, the script might exit before the task completes
             # Depending on the environment, might need `loop.run_forever()` or similar
        else:
             logger.error(f"RuntimeError during pipeline execution: {e}", exc_info=True)
    except Exception as e:
         logger.error(f"Unexpected error running pipeline: {e}", exc_info=True)