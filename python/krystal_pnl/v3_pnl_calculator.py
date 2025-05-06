#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
krystal_compute_pnl_open_only.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Makes a per‑user, per‑pool PnL table for Krystal LPs that are **still open**,
plus a 50‑50 hold benchmark.  Final CSV → lp-data/krystal_pnl_by_pool.csv
"""

from __future__ import annotations
import math
from pathlib import Path
from typing import Dict, List

import dotenv
import pandas as pd

# ── PATHS & ENV ─────────────────────────────────────────────────────────────
MODULE_DIR   = Path(__file__).resolve().parent
ROOT_DIR     = MODULE_DIR.parents[1]
INT_DATA_DIR = MODULE_DIR / "pnl_data";  INT_DATA_DIR.mkdir(exist_ok=True)
dotenv.load_dotenv(ROOT_DIR / ".env")
from common.path_config import DATA_DIR          # lp-data/

# ── CONSTANTS ───────────────────────────────────────────────────────────────

from common.constants import SYMBOL_MAP

SKIP_SYMBOLS: set[str] = set()

PRICES_CSV          = INT_DATA_DIR / "bitget_open_15m.csv"
CLOSED_POS_CSV      = INT_DATA_DIR / "krystal_closed_positions.csv"
OPEN_POS_CSV        = INT_DATA_DIR / "krystal_open_positions.csv"
DETAIL_CSV          = INT_DATA_DIR / "closed_positions_pnl.csv"
CLOSED_AGG_CSV      = INT_DATA_DIR / "closed_positions_pnl_by_chain_pool.csv"
FINAL_PNL_CSV       = DATA_DIR / "krystal_pnl_by_pool.csv"

# ── HELPERS ─────────────────────────────────────────────────────────────────
def map_symbol(sym: str) -> str:
    return SYMBOL_MAP.get(sym, sym)

def get_open_price(sym: str, ts: pd.Timestamp, prices: pd.DataFrame):
    col = f"{map_symbol(sym)}USDT"
    if col not in prices.columns: return None
    if ts.tz is None: ts = ts.tz_localize("UTC")
    idx = prices.index.get_indexer([ts], method="nearest")[0]
    return prices.iloc[idx][col]

def compute_L(x0: float, y0: float, p_min: float, p_max: float) -> float:
    alpha, beta = math.sqrt(p_min), 1.0 / math.sqrt(p_max)
    A, B, C = alpha*beta - 1, x0*alpha + y0*beta, x0*y0
    disc = B*B - 4*A*C
    if disc < 0: raise ValueError("No real L")
    L = max(( -B + math.sqrt(disc))/(2*A), ( -B - math.sqrt(disc))/(2*A))
    if L <= 0: raise ValueError("No positive L"); return L
    return L

def solve_v3_withdrawals(W, Pa, Pb, p, L, p_min, p_max):
    s_eta, inv_s_beta = math.sqrt(p_min), 1/math.sqrt(p_max)
    num = W - Pb * (p*L*inv_s_beta - L*s_eta)
    den = Pa + Pb*p
    x = num/den
    y = p * (x + L*inv_s_beta) - L*s_eta
    return x, y

# ╭──────────────────── CLOSED POSITIONS ────────────────────╮
closed_raw = pd.read_csv(CLOSED_POS_CSV)
closed_raw["opened_dt"] = pd.to_datetime(closed_raw["createdTime"], unit="s", utc=True)

prices = pd.read_csv(PRICES_CSV, parse_dates=["timestamp"]).set_index("timestamp")
prices.index = prices.index.tz_localize("UTC")

detail_rows: List[dict] = []

for _, r in closed_raw.iterrows():
    if r["tokenA_symbol"] in SKIP_SYMBOLS or r["tokenB_symbol"] in SKIP_SYMBOLS:
        continue
    pA0 = get_open_price(r["tokenA_symbol"], r["opened_dt"], prices)
    pB0 = get_open_price(r["tokenB_symbol"], r["opened_dt"], prices)
    pA1, pB1 = r["tokenA_price"], r["tokenB_price"]
    if None in (pA0, pB0, pA1, pB1): continue
    if any(x <= 0 for x in [pA0, pB0, pA1, pB1,
                            r["totalDepositValue"], r["totalWithdrawValue"]]): continue
    if r["minPrice"] >= r["maxPrice"]: continue

    dep = r["totalDepositValue"]
    QA0, QB0 = (dep/2)/pA0, (dep/2)/pB0
    L = compute_L(QA0, QB0, r["minPrice"], r["maxPrice"])
    W = r["totalWithdrawValue"]

    P_open, P_close = pA0/pB0, pA1/pB1
    if P_close <= r["minPrice"]:      QA1, QB1 = W/pA1, 0
    elif P_close >= r["maxPrice"]:    QA1, QB1 = 0, W/pB1
    else:                             QA1, QB1 = solve_v3_withdrawals(W, pA1, pB1,
                                                                      P_close, L,
                                                                      r["minPrice"], r["maxPrice"])
    pnl_usd = W - dep
    val0, val1 = QA0*P_open + QB0, QA1*P_close + QB1
    pnl_tokenB = val1 - val0

    detail_rows.append({
        "chainName":   r["chainName"],
        "poolAddress": r["poolAddress"],
        "userAddress": r["userAddress"],          # ← NEW
        "tokenA_symbol": r["tokenA_symbol"],
        "tokenB_symbol": r["tokenB_symbol"],
        "createdTime":  r["createdTime"],
        "quantityA0":   QA0,
        "quantityB0":   QB0,
        "initialDepositValue": dep,
        "pnl_usd":      pnl_usd,
        "pnl_tokenB":   pnl_tokenB,
    })

closed_detail = pd.DataFrame(detail_rows)
closed_detail.to_csv(DETAIL_CSV, index=False)

closed_agg = (
    closed_detail.sort_values("createdTime")
    .groupby(["chainName", "poolAddress", "userAddress"])
    .agg(
        tokenA_symbol        = ("tokenA_symbol", "first"),
        tokenB_symbol        = ("tokenB_symbol", "first"),
        earliest_createdTime = ("createdTime",   "first"),
        quantityA0           = ("quantityA0",    "first"),
        quantityB0           = ("quantityB0",    "first"),
        initialDepositValue  = ("initialDepositValue", "first"),
        pnl_usd              = ("pnl_usd", "sum"),
        pnl_tokenB           = ("pnl_tokenB", "sum"),
    )
    .reset_index()
)
closed_agg.to_csv(CLOSED_AGG_CSV, index=False)

# ╭──────────────────── OPEN POSITIONS ──────────────────────╮
open_raw  = pd.read_csv(OPEN_POS_CSV)
open_rows: List[dict] = []

for _, r in open_raw.iterrows():
    pA1, pB1 = r["tokenA_price"], r["tokenB_price"]
    if None in (pA1, pB1) or any(x <= 0 for x in [pA1, pB1]): continue
    QA0, QB0, QA1, QB1 = (r["tokenA_provided"], r["tokenB_provided"],
                          r["tokenA_current"],  r["tokenB_current"])
    if any(x < 0 for x in [QA0, QB0, QA1, QB1]): continue

    feeA = r["tokenA_feePending"] + r["tokenA_feesClaimed"]
    feeB = r["tokenB_feePending"] + r["tokenB_feesClaimed"]
    initial_value, current_value = r["initialUnderlyingValue"], r["currentUnderlyingValue"]

    pA0, pB0 = (initial_value/2)/QA0, (initial_value/2)/QB0
    if pA0 <= 0 or pB0 <= 0: continue

    pnl_usd = current_value + feeA*pA1 + feeB*pB1 - initial_value
    val0 = QA0*(pA0/pB0) + QB0
    val1 = QA1*(pA1/pB1) + QB1 + feeA*(pA1/pB1) + feeB
    pnl_tokenB = val1 - val0

    open_rows.append({
        "chainName":   r["chainName"],
        "poolAddress": r["poolAddress"],
        "userAddress": r["userAddress"],          # ← NEW
        "tokenA_symbol": r["tokenA_symbol"],
        "tokenB_symbol": r["tokenB_symbol"],
        "pnl_usd":     pnl_usd,
        "pnl_tokenB":  pnl_tokenB,
        "pA_now":      pA1,
        "pB_now":      pB1,
        "lp_current_value": current_value,
    })

open_agg = (
    pd.DataFrame(open_rows)
    .groupby(["chainName", "poolAddress", "userAddress"])
    .agg(
        tokenA_symbol=("tokenA_symbol", "first"),
        tokenB_symbol=("tokenB_symbol", "first"),
        pnl_usd=("pnl_usd", "sum"),
        pnl_tokenB=("pnl_tokenB", "sum"),
        pA_now=("pA_now", "first"),
        pB_now=("pB_now", "first"),
        lp_current_value=("lp_current_value", "sum"),
    )
    .reset_index()
)

# ╭──────────────────── MERGE & BENCHMARK ───────────────────╮
final_df = open_agg.merge(
    closed_agg[
        ["chainName","poolAddress","userAddress",
         "earliest_createdTime","quantityA0","quantityB0","initialDepositValue",
         "pnl_usd","pnl_tokenB"]
    ],
    on=["chainName","poolAddress","userAddress"],
    how="left",
    suffixes=("_open","_closed"),
)

final_df[["pnl_usd_closed","pnl_tokenB_closed"]] = \
    final_df[["pnl_usd_closed","pnl_tokenB_closed"]].fillna(0)

final_df["lp_pnl_usd"]     = final_df["pnl_usd_open"]    + final_df["pnl_usd_closed"]
final_df["lp_pnl_tokenB"]  = final_df["pnl_tokenB_open"] + final_df["pnl_tokenB_closed"]

# 50‑50 hold benchmark
final_df["hold_value_usd"] = (
      final_df["quantityA0"].fillna(0)*final_df["pA_now"]
    + final_df["quantityB0"].fillna(0)*final_df["pB_now"]
)
final_df["hold_pnl_usd"]     = final_df["hold_value_usd"] - final_df["initialDepositValue"].fillna(0)
final_df["lp_minus_hold_usd"] = final_df["lp_pnl_usd"] - final_df["hold_pnl_usd"]


final_df["earliest_createdTime"] = (
    pd.to_datetime(final_df["earliest_createdTime"], unit="s")
)

final_df = final_df[[
    "chainName","poolAddress","userAddress",
    "tokenA_symbol","tokenB_symbol",
    "earliest_createdTime","quantityA0","quantityB0","initialDepositValue",
    "lp_current_value","hold_value_usd","hold_pnl_usd",
    "lp_pnl_usd","lp_minus_hold_usd","lp_pnl_tokenB"
]]

final_df.to_csv(FINAL_PNL_CSV, index=False)

print(f"Final PnL (open pools) → {FINAL_PNL_CSV.relative_to(ROOT_DIR)}")
print(f"Closed NFTs processed: {len(closed_detail)} | Open NFTs processed: {len(open_rows)}")
