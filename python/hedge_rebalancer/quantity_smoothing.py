import pandas as pd
import logging
import numpy as np
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
    if not isinstance(quantities, pd.Series):
        quantities = pd.Series(quantities)
    now = datetime.now(tz=timezone.utc)

    quantities_df = pd.DataFrame(quantities, index=[now])

    def save(position):
        position.to_csv(LP_SMOOTHED_CSV, mode='w', header=True)

    if LP_SMOOTHED_CSV.exists():
        try:
            lp_smoothed_prev = pd.read_csv(LP_SMOOTHED_CSV, index_col=0, parse_dates=True)
        except Exception as e:
            logging.error(f"Error reading previous smoothed positions: {e}")
            return quantities
    else:
        lp_smoothed_prev = quantities_df

    prev_date = lp_smoothed_prev.index[-1] if not lp_smoothed_prev.empty else None

    if prev_date is not None:
        # Calculate the time difference in seconds
        time_diff = (now - prev_date).total_seconds() / 3600

        if time_diff <= 0:
            logging.warning("Current time is earlier than the last recorded time. Using previous data.")
            return lp_smoothed_prev

        alpha = 1 - np.exp(-time_diff / lookback)

        lp_smoothed = (1 - alpha) * lp_smoothed_prev.iloc[-1] + alpha * quantities_df.iloc[-1]
        lp_smoothed_df = pd.DataFrame(lp_smoothed, index=[now])

        if persist:
            try:
                lp_smoothed_df.to_csv(LP_SMOOTHED_CSV, mode='w', header=True)
            except Exception as e:
                logging.error(f"Error writing smoothed positions to CSV: {e}")

        return lp_smoothed_df
    else:
        return quantities_df