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
from common.path_config import  ROOT_DIR 

import aiohttp
import yaml
import pandas as pd

# Where am I?
MODULE_DIR = ROOT_DIR / "python/krystal_pnl"          # …/python/krystal_pnl                     # LP‑hedging‑strategy
INT_DATA_DIR = MODULE_DIR / "pnl_data"                # …/krystal_pnl/pnl_data
INT_DATA_DIR.mkdir(exist_ok=True)

# Load YAML configuration from project root
CONFIG_PATH = ROOT_DIR / "lp-monitor/lpMonitorConfig.yaml"  # …/python/config.yaml 

try:
    with CONFIG_PATH.open('r') as f:
        config = yaml.safe_load(f) or {}
except Exception as e:
    print(f"Error loading {CONFIG_PATH}: {e}")
    config = {}

# Extract configuration
EVM_WALLET_ADDRESSES = config.get('evm_wallet_addresses', [])
KRYSTAL_CHAIN_IDS = config.get('krystal_chain_ids', ['137', '56', '42161'])
KRYSTAL_VAULT_WALLET_CHAIN_MAP = {
    entry['wallet']: {'chains': entry['chains'], 'vault_share': entry['vault_share']}
    for entry in config.get('krystal_vault_wallet_chain_ids', [])
    if entry.get('wallet') and entry.get('chains') and 'vault_share' in entry
}
print(f"🪙 EVM Addresses: {EVM_WALLET_ADDRESSES}")
print(f"🔗 Chain IDs: {KRYSTAL_CHAIN_IDS}")
print(f"🏦 Vault Wallet Map: {KRYSTAL_VAULT_WALLET_CHAIN_MAP}")

# Import shared “final output” directory from common.path_config
from common.path_config import DATA_DIR  # …/lp-data (already exists)

# Intermediate file paths (inside krystal_pnl/pnl_data)
CLOSED_CSV = INT_DATA_DIR / "krystal_closed_positions.csv"
OPEN_CSV = INT_DATA_DIR / "krystal_open_positions.csv"

# ── 1) CONFIG CONSTANTS ─────────────────────────────────────────────────────
API_V1 = "https://api.krystal.app/all/v1/"
EP_POS = "lp/userPositions"
PAGE_SIZE = 100

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

# ── 3) POSITION PROCESSING ──────────────────────────────────────────────────
def scale_vault_position(position: Dict[str, Any], vault_share: float) -> Dict[str, Any]:
    """Scale vault position quantities and values by vault_share."""
    scaled = position.copy()
    
    # Scale currentAmounts
    if "currentAmounts" in scaled:
        scaled["currentAmounts"] = [
            {
                **amt,
                "balance": str(int(int(amt.get("balance", "0")) * vault_share))
            }
            for amt in scaled["currentAmounts"]
        ]
    
    # Scale providedAmounts
    if "providedAmounts" in scaled:
        scaled["providedAmounts"] = [
            {
                **amt,
                "balance": str(int(int(amt.get("balance", "0")) * vault_share))
            }
            for amt in scaled["providedAmounts"]
        ]
    
    # Scale feePending
    if "feePending" in scaled:
        scaled["feePending"] = [
            {
                **fee,
                "balance": str(int(int(fee.get("balance", "0")) * vault_share))
            }
            for fee in scaled["feePending"]
        ]
    
    # Scale feesClaimed
    if "feesClaimed" in scaled:
        scaled["feesClaimed"] = [
            {
                **fee,
                "balance": str(int(int(fee.get("balance", "0")) * vault_share))
            }
            for fee in scaled["feesClaimed"]
        ]
    
    # Scale USD values
    if "initialUnderlyingValue" in scaled:
        scaled["initialUnderlyingValue"] = scaled["initialUnderlyingValue"] * vault_share
    if "currentUnderlyingValue" in scaled:
        scaled["currentUnderlyingValue"] = scaled["currentUnderlyingValue"] * vault_share
    if "totalDepositValue" in scaled:
        scaled["totalDepositValue"] = scaled["totalDepositValue"] * vault_share
    if "totalWithdrawValue" in scaled:
        scaled["totalWithdrawValue"] = scaled["totalWithdrawValue"] * vault_share

    return scaled

# ── 4) CSV EXPORT ───────────────────────────────────────────────────────────
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

# ── 5) ASYNC MAIN ───────────────────────────────────────────────────────────
async def main() -> None:
    async with aiohttp.ClientSession() as session:
        all_closed_positions = []
        all_open_positions = []

        # Separate vault and non-vault addresses
        vault_addresses = set(KRYSTAL_VAULT_WALLET_CHAIN_MAP.keys())
        non_vault_addresses = [addr for addr in EVM_WALLET_ADDRESSES if addr not in vault_addresses]

        # Fetch positions for non-vault addresses (all chain IDs)
        if non_vault_addresses:
            addresses_str = ','.join(non_vault_addresses)
            chains_str = ','.join(KRYSTAL_CHAIN_IDS)
            print(f"Fetching non-vault positions for {addresses_str} on chains {chains_str}")
            closed = await fetch_all_positions(session, addresses_str, chains_str, "closed")
            open_pos = await fetch_all_positions(session, addresses_str, chains_str, "open")
            all_closed_positions.extend(closed)
            all_open_positions.extend(open_pos)

        # Fetch positions for vault addresses (specific chain IDs)
        for vault_addr, vault_info in KRYSTAL_VAULT_WALLET_CHAIN_MAP.items():
            chains = vault_info['chains']
            vault_share = vault_info['vault_share']
            chains_str = ','.join(chains)
            print(f"Fetching vault positions for {vault_addr} on chains {chains_str} with vault_share {vault_share}")
            closed = await fetch_all_positions(session, vault_addr, chains_str, "closed")
            open_pos = await fetch_all_positions(session, vault_addr, chains_str, "open")
            # Scale vault positions
            scaled_closed = [scale_vault_position(pos, vault_share) for pos in closed]
            scaled_open = [scale_vault_position(pos, vault_share) for pos in open_pos]
            all_closed_positions.extend(scaled_closed)
            all_open_positions.extend(scaled_open)

        # Export to CSV
        export_positions_to_csv(all_closed_positions, CLOSED_CSV)
        print(f"✅ Exported {len(all_closed_positions):,} closed positions → {CLOSED_CSV.relative_to(ROOT_DIR)}")
        export_positions_to_csv(all_open_positions, OPEN_CSV)
        print(f"✅ Exported {len(all_open_positions):,} open positions → {OPEN_CSV.relative_to(ROOT_DIR)}")

# ── 6) ENTRY POINT ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())