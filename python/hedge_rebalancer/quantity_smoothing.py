import pandas as pd
import logging
import numpy as np
from datetime import datetime, timezone
from common.path_config import LOG_DIR, LP_SMOOTHED_CSV

# Configure logging to match hedge_rebalancer
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'quantity_smoothing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def compute_ma(quantities, lookback=20, persist=True):
    """
    Compute the EWMA of a position dict.
    :param quantities: dict with token symbols as keys and quantities as values
    :param lookback: Window size for moving average (in hours)
    :param persist: If True, save the smoothed positions to a CSV file
    :return: dict with token symbols as keys and smoothed quantities as values
    """
    if not quantities:
        logger.warning("Received empty quantities input. Returning empty dict.")
        return {}

    if not isinstance(quantities, pd.Series):
        quantities = pd.Series(quantities)
    now = datetime.now(tz=timezone.utc)

    logger.debug(f"Input quantities: {quantities.to_dict()}")

    # Check if all quantities are zero
    if all(qty == 0.0 for qty in quantities.values):
        logger.warning("All input quantities are zero. Returning input quantities as dict.")
        return quantities.to_dict()

    if LP_SMOOTHED_CSV.exists():
        try:
            lp_smoothed_prev = pd.read_csv(LP_SMOOTHED_CSV, index_col=0, parse_dates=True)
            logger.info(f"Loaded previous smoothed quantities")
            # Initialize new tokens with their snapshot value
            for token in quantities.index:
                if token not in lp_smoothed_prev.columns:
                    lp_smoothed_prev[token] = quantities[token]
                    logger.info(f"Initialized MA for new token {token} with snapshot value {quantities[token]}")
            # Remove tokens no longer in quantities and set their MA to 0.0
            tokens_to_remove = [col for col in lp_smoothed_prev.columns if col not in quantities.index]
            for token in tokens_to_remove:
                lp_smoothed_prev[token] = 0.0
                logger.info(f"Set MA to 0.0 for token {token} no longer in LP positions")

            prev_date = lp_smoothed_prev.index[-1] if not lp_smoothed_prev.empty else None
            if prev_date is not None:
                # Calculate the time difference in seconds
                time_diff = (now - prev_date).total_seconds() / 3600
                if time_diff <= 0:
                    logger.warning("Current time is earlier than the last recorded time. Using current raw quantities.")
                    return quantities.to_dict()  # Return raw quantities as dict

                alpha = 1 - np.exp(-time_diff / lookback)
                # Compute EWMA for each token
                lp_smoothed = (1 - alpha) * lp_smoothed_prev.iloc[-1].fillna(0.0) + alpha * quantities
                logger.info(f"Computed EWMA with alpha={alpha:.4f}")
            else:
                # If CSV is empty or invalid, use current quantities
                lp_smoothed = quantities
                logger.info("Previous LP_SMOOTHED_CSV is empty. Using current quantities.")
        except Exception as e:
            logger.error(f"Error reading previous smoothed positions: {e}")
            return quantities.to_dict()  # Return raw quantities as dict
    else:
        # If CSV doesn't exist, initialize with current quantities
        lp_smoothed = quantities
        logger.info(f"No previous LP_SMOOTHED_CSV found. Initializing with current quantities")

    # Ensure lp_smoothed is a Series
    if not isinstance(lp_smoothed, pd.Series):
        lp_smoothed = pd.Series(lp_smoothed)

    # Set MA to 0.0 for tokens not in quantities
    for token in lp_smoothed.index:
        if token not in quantities.index:
            lp_smoothed[token] = 0.0

    # Create DataFrame with tokens as columns
    lp_smoothed_df = pd.DataFrame({token: [lp_smoothed[token]] for token in lp_smoothed.index}, index=[now]).fillna(0.0)
    logger.debug(f"lp_smoothed_df before saving")

    if persist and not lp_smoothed_df.empty:
        try:
            # Save all columns in lp_smoothed_df (all tokens from quantities or previous CSV)
            lp_smoothed_df.to_csv(LP_SMOOTHED_CSV, mode='w', header=True)
            logger.info(f"Saved smoothed quantities to {LP_SMOOTHED_CSV}")
        except Exception as e:
            logger.error(f"Error writing smoothed positions to CSV: {e}")

    return lp_smoothed.to_dict()  # Return as dict