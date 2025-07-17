Krystal LP PnL Calculation Steps for Open Positions
This document outlines the process for calculating Profit and Loss (PnL) for Krystal liquidity provider (LP) positions that remain open, including a 50-50 hold benchmark for comparison. The calculations, implemented in krystal_compute_pnl_open_only.py, handle Uniswap V3-like liquidity positions and produce a per-user, per-pool PnL table saved to lp-data/krystal_pnl_by_pool.csv.

Step-by-Step Process
1. Paths and Environment Setup

2. Constants and Helper Functions

Constants:

SYMBOL_MAP: Maps token symbols (from common.constants).
SKIP_SYMBOLS: Set of symbols to exclude (empty by default).
File paths:
PRICES_CSV: Historical price data (bitget_open_15m.csv).
CLOSED_POS_CSV: Closed positions data (krystal_closed_positions.csv).
OPEN_POS_CSV: Open positions data (krystal_open_positions.csv).
DETAIL_CSV: Detailed PnL for closed positions (closed_positions_pnl.csv).
CLOSED_AGG_CSV: Aggregated closed position PnL (closed_positions_pnl_by_chain_pool.csv).
FINAL_PNL_CSV: Final output (krystal_pnl_by_pool.csv).

Helper Functions:

map_symbol(sym: str) -> str: Returns mapped symbol from SYMBOL_MAP or the original symbol.
get_open_price(sym: str, ts: pd.Timestamp, prices: pd.DataFrame) -> float: Fetches the nearest price for a symbol at a given timestamp from the prices DataFrame.
compute_L(x0: float, y0: float, p_min: float, p_max: float) -> float:
Calculates Uniswap V3 liquidity parameter L.
Formula: Solves the quadratic equation A * L^2 + B * L + C = 0, where:
alpha = sqrt(p_min), beta = 1 / sqrt(p_max)
A = alpha * beta - 1
B = x0 * alpha + y0 * beta
C = x0 * y0


Returns the positive L solution.


solve_v3_withdrawals(W, Pa, Pb, p, L, p_min, p_max) -> (float, float):
Computes withdrawal quantities (x, y) for Token A and Token B based on withdrawal value W.



3. Closed Positions Processing

Actions:

Load closed positions from CLOSED_POS_CSV into closed_raw.
Convert createdTime (Unix seconds) to UTC timestamps (opened_dt).
Load prices from PRICES_CSV, set timestamp index (UTC-localized).
For each position:
Skip if tokenA_symbol or tokenB_symbol is in SKIP_SYMBOLS.
Get opening prices (pA0, pB0) using get_open_price at opened_dt.
Use closing prices (pA1, pB1) from the row.
Skip if prices or deposit/withdrawal values are missing, zero, negative, or if minPrice >= maxPrice.
Calculate initial quantities:
QA0 = (totalDepositValue / 2) / pA0
QB0 = (totalDepositValue / 2) / pB0


Compute liquidity L using compute_L.
Determine withdrawal quantities (QA1, QB1) based on P_close = pA1 / pB1:
If P_close <= minPrice: QA1 = W / pA1, QB1 = 0
If P_close >= maxPrice: QA1 = 0, QB1 = W / pB1
Otherwise, use solve_v3_withdrawals.


Calculate PnL:
USD: pnl_usd = totalWithdrawValue - totalDepositValue
Token B:
val0 = QA0 * (pA0 / pB0) + QB0
val1 = QA1 * (pA1 / pB1) + QB1
pnl_tokenB = val1 - val0




Store results in detail_rows.


Save detailed PnL to DETAIL_CSV as closed_detail.
Aggregate by chainName, poolAddress, userAddress:
Take first values for symbols, createdTime, initial quantities, and deposit value.
Sum pnl_usd and pnl_tokenB.


Save aggregated data to CLOSED_AGG_CSV.


Purpose: Computes and aggregates PnL for closed positions to include in the final table.



4. Open Positions Processing

Actions:

Load open positions from OPEN_POS_CSV into open_raw.
For each position:
Use current prices (pA1, pB1) from the row.
Skip if prices are missing or non-positive.
Get quantities:
Initial: QA0 = tokenA_provided, QB0 = tokenB_provided
Current: QA1 = tokenA_current, QB1 = tokenB_current niejsza - Skip if any quantity is negative.


Calculate fees: feeA = tokenA_feePending + tokenA_feesClaimed, similarly for Token B.
Use initialUnderlyingValue and currentUnderlyingValue.
Estimate initial prices:
pA0 = (initialUnderlyingValue / 2) / QA0
pB0 = (initialUnderlyingValue / 2) / QB0


Skip if pA0 or pB0 is non-positive.
Calculate PnL:
USD: pnl_usd = currentUnderlyingValue + feeA * pA1 + feeB * pB1 - initialUnderlyingValue
Token B:
val0 = QA0 * (pA0 / pB0) + QB0
val1 = QA1 * (pA1 / pB1) + QB1 + feeA * (pA1 / pB1) + feeB
pnl_tokenB = val1 - val0




Store results in open_rows.


Aggregate by chainName, poolAddress, userAddress:
Take first values for symbols and current prices.
Sum pnl_usd, pnl_tokenB, and lp_current_value.




Purpose: Computes PnL for open positions, including fees, for the final table.



5. Merging and Benchmark Calculation

Actions:

Merge open_agg and selected closed_agg columns on chainName, poolAddress, userAddress (left join).
Fill missing closed PnL values with 0.
Calculate total LP PnL:
lp_pnl_usd = pnl_usd_open + pnl_usd_closed
lp_pnl_tokenB = pnl_tokenB_open + pnl_tokenB_closed


Compute 50-50 hold benchmark:
hold_value_usd = (QA0 * pA_now) + (QB0 * pB_now)
hold_pnl_usd = hold_value_usd - initialDepositValue
lp_minus_hold_usd = lp_pnl_usd - hold_pnl_usd


Convert earliest_createdTime to datetime format.
Select final columns and save to FINAL_PNL_CSV.


Purpose: Combines open and closed position data, calculates total PnL, and provides a benchmark for comparison.



Notes

Assumptions:
Initial deposits are split 50-50 in USD value.
Closed position withdrawal quantities depend on the closing price relative to the price range.
Open position PnL includes current position value and both pending and claimed fees.


Benchmark: The 50-50 hold benchmark simulates holding initial token quantities without providing liquidity.
Output: The final table includes per-user, per-pool PnL in USD and Token B, alongside benchmark metrics.

