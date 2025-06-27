import pandas as pd
import logging
import sys
import csv
from datetime import datetime, timezone
import json
from common.path_config import (
    LOG_DIR, LP_SMOOTHED_CSV
)


def compute_ma(quantities, lookback=20, persist=True):
    """
    Compute the ewma of a position dict.
    :param quantities: dict with price
    :param lookback: Window size for moving average
    :param persist: If True, save the smoothed positions to a CSV file
    :return: dict with price
    """

    if LP_SMOOTHED_CSV.exists():
        try:
            lp_smoothed_prev = pd.read_csv(LP_SMOOTHED_CSV, index_col=0, parse_dates=True)
        except Exception as e:
            logging.error(f"Error reading previous smoothed positions: {e}")
            return quantities
    else:
        lp_smoothed_prev = pd.DataFrame.from_dict(quantities, index=datetime.now(tz=timezone.utc))

    dt = 1  # Assuming dt is 1 for simplicity, can be adjusted based on actual time step if file dates are not regular
    alpha = 1 - exp(-dt / lookback)

    lp_smoothed = (1 - alpha) * lp_smoothed_prev + alpha * quantities

    if persist:
        try:
            lp_smoothed.to_csv(LP_SMOOTHED_CSV, mode='w', header=True)
        except Exception as e:
            logging.error(f"Error writing smoothed positions to CSV: {e}")

    return lp_smoothed