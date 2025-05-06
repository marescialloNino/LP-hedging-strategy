"""
positions_scan.py
~~~~~~~~~~~~~~~~~
Build a list of (ticker, (start_time, end_time)) tuples from your open / closed
position files – ready for fetch_bitget_open_prices() – but with **naive**
datetimes (no tzinfo attached).
"""

import os
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple, Union

from common.constants import SYMBOL_MAP

# --------------------------------------------------------------------------- #
# 1.  Symbol mapper with user‑defined overrides
# --------------------------------------------------------------------------- #

CUSTOM_OVERRIDES = SYMBOL_MAP


def default_symbol_mapper(token_symbol: str, base: str = "USDT") -> str:
    sym = token_symbol.upper()
    return CUSTOM_OVERRIDES.get(sym, f"{sym}{base}")


# --------------------------------------------------------------------------- #
# 2.  Main helper
# --------------------------------------------------------------------------- #

def build_ticker_timewindows(
    closed_csv: Union[str, os.PathLike],
    open_csv: Union[str, os.PathLike],
    *,
    symbol_mapper: Callable[[str], str] = default_symbol_mapper,
    now: Optional[datetime] = None,
) -> List[Tuple[str, Tuple[datetime, datetime]]]:
    """
    Returns
    -------
    list[(ticker, (start_dt, end_dt))]   with naïve UTC datetimes
    """
    # 'now' is naive UTC
    now = now or datetime.utcnow()

    usecols = ["tokenA_symbol", "tokenB_symbol", "createdTime", "closedTime"]
    dfs = [
        pd.read_csv(
            path,
            usecols=usecols,
            dtype={"createdTime": "int64", "closedTime": "Int64"},
        )
        for path in (closed_csv, open_csv)
        if os.path.isfile(path)
    ]
    if not dfs:
        raise FileNotFoundError("Neither open nor closed CSV file was found.")

    df = pd.concat(dfs, ignore_index=True)

    # closedTime == 0 / NA → still open → use 'now'   (naive UTC timestamp)
    df["closedTime"] = df["closedTime"].where(
        df["closedTime"].notna() & (df["closedTime"] != 0),
        int(now.timestamp()),
    )

    # Stack tokenA_symbol / tokenB_symbol into one column
    melted = (
        df.melt(
            value_vars=["tokenA_symbol", "tokenB_symbol"],
            id_vars=["createdTime", "closedTime"],
            value_name="symbol",
        )
        .dropna(subset=["symbol"])
    )

    grouped = (
        melted.groupby("symbol", as_index=False)
        .agg(start_ts=("createdTime", "min"), end_ts=("closedTime", "max"))
    )

    result: List[Tuple[str, Tuple[datetime, datetime]]] = []
    for _, row in grouped.iterrows():
        ticker = symbol_mapper(row["symbol"])
        start_dt = datetime.utcfromtimestamp(int(row["start_ts"]))  # naïve
        end_dt = datetime.utcfromtimestamp(int(row["end_ts"]))      # naïve
        result.append((ticker, (start_dt, end_dt)))

    return result

