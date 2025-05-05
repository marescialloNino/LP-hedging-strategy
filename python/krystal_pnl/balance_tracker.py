#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
krystal_fetch_positions.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Fetch open/closed LP positions from Krystal, save intermediate CSVs inside
python/krystal_pnl/pnl_data/, and rely on the shared lp-data/ folder (declared
in python/common/path_config.py) for any final artefacts you may add later.
"""

from __future__ import annotations

# ── 0) UNIVERSAL PATH & ENV SET‑UP ──────────────────────────────────────────
import os
import sys
import asyncio
import csv
from pathlib import Path
from typing import Any, Dict, List

import aiohttp
import dotenv
import pandas as pd

# Where am I?
MODULE_DIR = Path(__file__).resolve().parent           # …/python/krystal_pnl
ROOT_DIR   = MODULE_DIR.parents[1]                     # LP‑hedging‑strategy
INT_DATA_DIR = MODULE_DIR / "pnl_data"                 # …/krystal_pnl/pnl_data
INT_DATA_DIR.mkdir(exist_ok=True)

# Load environment variables from ONE .env at project root
dotenv.load_dotenv(ROOT_DIR / ".env")

# Import shared “final output” directory from common.path_config
from common.path_config import DATA_DIR  #  …/lp-data (already exists)

# Intermediate file paths (inside krystal_pnl/pnl_data)
CLOSED_CSV = INT_DATA_DIR / "krystal_closed_positions.csv"
OPEN_CSV   = INT_DATA_DIR / "krystal_open_positions.csv"

# ── 1) CONFIG CONSTANTS ─────────────────────────────────────────────────────
API_V1    = "https://api.krystal.app/all/v1/"
EP_POS    = "lp/userPositions"
PAGE_SIZE = 100

ADDRESSES = os.getenv("EVM_WALLET_ADDRESSES", "")   # comma‑separated
print(f"🪙  Addresses: {ADDRESSES}")
CHAIN_IDS = os.getenv("KRYSTAL_CHAIN_IDS", "")    
print(f"{CHAIN_IDS}")  # comma‑separated

# ── 2) API HELPERS ──────────────────────────────────────────────────────────
async def position_fetcher(
    session: aiohttp.ClientSession,
    addresses: str,
    chains: str,
    status: str,
    offset: int = 0,
    limit: int = PAGE_SIZE,
) -> Dict[str, Any]:
    url = (
        f"{API_V1}{EP_POS}"
        f"?addresses={addresses}"
        f"&chainIds={chains}"
        f"&positionStatus={status}"
        f"&limit={limit}"
        f"&offset={offset}"
    )
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json()


async def fetch_all_positions(
    session: aiohttp.ClientSession, addresses: str, chains: str, status: str
) -> List[Dict[str, Any]]:
    all_positions: List[Dict[str, Any]] = []
    offset = 0

    while True:
        resp = await position_fetcher(session, addresses, chains, status, offset)
        batch = resp.get("positions", [])
        if not batch:
            break
        all_positions.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return all_positions


# ── 3) CSV EXPORT ───────────────────────────────────────────────────────────
FIELDNAMES = [
    "chainName",
    "userAddress",
    "tokenAddress",
    "tokenId",
    "minPrice",
    "maxPrice",
    "tokenA_address",
    "tokenA_symbol",
    "tokenB_address",
    "tokenB_symbol",
    "tokenA_current",
    "tokenB_current",
    "tokenA_provided",
    "tokenB_provided",
    "tokenA_feePending",
    "tokenB_feePending",
    "tokenA_feesClaimed",
    "tokenB_feesClaimed",
    "tokenA_price",
    "tokenB_price",
    "status",
    "gasUsed",
    "initialUnderlyingValue",
    "currentUnderlyingValue",
    "createdTime",
    "closedTime",
    "closedPrice",
    "totalDepositValue",
    "totalWithdrawValue",
    "poolAddress",
    "pool_price",
]


def _extract_two_tokens(arr: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Guarantee exactly two slots with address/symbol/balance/price keys."""
    out: List[Dict[str, Any]] = []
    for i in range(2):
        if i < len(arr):
            tok = arr[i]["token"]
            bal = int(arr[i].get("balance", "0"))
            adj = bal / (10 ** tok.get("decimals", 18))
            out.append(
                {
                    "address": tok.get("address"),
                    "symbol": tok.get("symbol"),
                    "balance": adj,
                    "price": tok.get("price"),
                }
            )
        else:
            out.append(
                {"address": None, "symbol": None, "balance": 0.0, "price": None}
            )
    return out


def export_positions_to_csv(positions: List[Dict[str, Any]], csv_path: Path) -> None:
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        for pos in positions:
            row: Dict[str, Any] = {
                "chainName": pos.get("chainName"),
                "userAddress": pos.get("userAddress"),
                "tokenAddress": pos.get("tokenAddress"),
                "tokenId": pos.get("tokenId"),
                "minPrice": pos.get("minPrice"),
                "maxPrice": pos.get("maxPrice"),
                "status": pos.get("status"),
                "gasUsed": pos.get("gasUsed"),
                "initialUnderlyingValue": pos.get("initialUnderlyingValue"),
                "currentUnderlyingValue": pos.get("currentUnderlyingValue"),
                "createdTime": pos.get("createdTime"),
                "closedTime": pos.get("closedTime"),
                "closedPrice": pos.get("closedPrice"),
                "totalDepositValue": pos.get("totalDepositValue"),
                "totalWithdrawValue": pos.get("totalWithdrawValue"),
                "poolAddress": pos.get("pool", {}).get("poolAddress"),
                "pool_price": pos.get("pool", {}).get("price"),
            }

            # current amounts
            curA, curB = _extract_two_tokens(pos.get("currentAmounts", []))
            row.update(
                {
                    "tokenA_address": curA["address"],
                    "tokenA_symbol": curA["symbol"],
                    "tokenB_address": curB["address"],
                    "tokenB_symbol": curB["symbol"],
                    "tokenA_current": curA["balance"],
                    "tokenB_current": curB["balance"],
                    "tokenA_price": curA["price"],
                    "tokenB_price": curB["price"],
                }
            )

            # provided
            provA, provB = _extract_two_tokens(pos.get("providedAmounts", []))
            row["tokenA_provided"] = provA["balance"]
            row["tokenB_provided"] = provB["balance"]

            # fees
            feeP_A, feeP_B = _extract_two_tokens(pos.get("feePending", []))
            feeC_A, feeC_B = _extract_two_tokens(pos.get("feesClaimed", []))
            row.update(
                {
                    "tokenA_feePending": feeP_A["balance"],
                    "tokenB_feePending": feeP_B["balance"],
                    "tokenA_feesClaimed": feeC_A["balance"],
                    "tokenB_feesClaimed": feeC_B["balance"],
                }
            )

            writer.writerow(row)


# ── 4) ASYNC MAIN ───────────────────────────────────────────────────────────
async def main() -> None:
    async with aiohttp.ClientSession() as session:
        # CLOSED
        closed = await fetch_all_positions(session, ADDRESSES, CHAIN_IDS, "closed")
        export_positions_to_csv(closed, CLOSED_CSV)
        print(f"✅  Exported {len(closed):,} closed positions → {CLOSED_CSV.relative_to(ROOT_DIR)}")

        # OPEN
        opened = await fetch_all_positions(session, ADDRESSES, CHAIN_IDS, "open")
        export_positions_to_csv(opened, OPEN_CSV)
        print(f"✅  Exported {len(opened):,} open positions  → {OPEN_CSV.relative_to(ROOT_DIR)}")


# ── 5) ENTRY POINT ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
