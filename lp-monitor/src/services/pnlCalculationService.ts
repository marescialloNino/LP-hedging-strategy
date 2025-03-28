// src/services/pnlCalculationService.ts
import axios from 'axios';
import { getTokenMapping } from './tokenMappingService';

// Simple sleep function for rate limiting
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const MIN_API_DELAY = 1000; // milliseconds
async function apiRateLimit(): Promise<void> {
  await sleep(MIN_API_DELAY);
}

export interface PnlEvent {
  tx_id: string;
  position_address: string;
  pair_address: string;
  active_bin_id: number;
  token_x_amount: number;
  token_y_amount: number;
  token_x_usd_amount: number;
  token_y_usd_amount: number;
  onchain_timestamp: number;
}

/**
 * Fetch withdrawal events for a given position.
 */
export async function fetchWithdrawals(positionId: string): Promise<PnlEvent[]> {
  await apiRateLimit();
  const url = `https://dlmm-api.meteora.ag/position/${positionId}/withdraws`;
  const response = await axios.get(url);
  return response.data as PnlEvent[];
}

/**
 * Fetch deposit events for a given position.
 */
export async function fetchDeposits(positionId: string): Promise<PnlEvent[]> {
  await apiRateLimit();
  const url = `https://dlmm-api.meteora.ag/position/${positionId}/deposits`;
  const response = await axios.get(url);
  return response.data as PnlEvent[];
}

/**
 * Fetch fee claim events for a given position.
 */
export async function fetchFeeClaims(positionId: string): Promise<PnlEvent[]> {
  await apiRateLimit();
  const url = `https://dlmm-api.meteora.ag/position/${positionId}/claim_fees`;
  const response = await axios.get(url);
  return response.data as PnlEvent[];
}

/**
 * Helper to compute the total USD value of an event.
 */
function eventUsdValue(evt: PnlEvent): number {
  return evt.token_x_usd_amount + evt.token_y_usd_amount;
}

/**
 * -----------------------------------------------------------------------------
 * Standard USD-Based PnL Calculation
 * -----------------------------------------------------------------------------
 *
 * For a given Meteora position, this function computes:
 *  - Realized PnL (from withdrawals)
 *  - Unrealized PnL (open position value minus remaining capital)
 *  - Net PnL (current position value plus withdrawals, minus capital deposits)
 *
 * It reconstructs the cost basis by subtracting fee rewards (which have a zero cost)
 * from total deposits.
 *
 * @param positionId - The position’s address.
 * @param currentPositionValueUsd - The current USD value of the open position.
 * @returns An object with realized, unrealized, and net PnL in USD.
 */
export async function calculateMeteoraPositionPNLUsd(
  positionId: string,
  currentPositionValueUsd: number
): Promise<{
  realizedPNLUsd: number;
  unrealizedPNLUsd: number;
  netPNLUsd: number;
}> {
  // Fetch events.
  const deposits = await fetchDeposits(positionId);
  const withdrawals = await fetchWithdrawals(positionId);
  const feeClaims = await fetchFeeClaims(positionId);

  const totalDepositUsd = deposits.reduce((sum, evt) => sum + eventUsdValue(evt), 0);
  const totalFeeRewardUsd = feeClaims.reduce((sum, evt) => sum + eventUsdValue(evt), 0);
  const totalWithdrawalUsd = withdrawals.reduce((sum, evt) => sum + eventUsdValue(evt), 0);

  // Capital deposits exclude fee rewards.
  const capitalDepositsUsd = totalDepositUsd - totalFeeRewardUsd;
  if (totalDepositUsd === 0) {
    throw new Error(`No deposits found for position ${positionId}`);
  }

  // Assume withdrawals remove deposits proportionally.
  const withdrawalRatio = totalWithdrawalUsd / totalDepositUsd;
  const withdrawnCapitalUsd = capitalDepositsUsd * withdrawalRatio;

  const realizedPNLUsd = totalWithdrawalUsd - withdrawnCapitalUsd;
  const remainingCapitalUsd = capitalDepositsUsd - withdrawnCapitalUsd;
  const unrealizedPNLUsd = currentPositionValueUsd - remainingCapitalUsd;
  const netPNLUsd = (currentPositionValueUsd + totalWithdrawalUsd) - capitalDepositsUsd;

  return {
    realizedPNLUsd,
    unrealizedPNLUsd,
    netPNLUsd,
  };
}

/**
 * -----------------------------------------------------------------------------
 * Token B-Based (Quote Token) PnL Calculation
 * -----------------------------------------------------------------------------
 *
 * In this function we rebase all cash-flow events (deposits, withdrawals, fee claims)
 * directly into token B units. For each event we compute a token B price from the event’s
 * token_y data as follows:
 *
 *    tokenBPrice_at_event = (token_y_usd_amount) ÷ ( (token_y_amount ÷ 10^(decimals)) )
 *
 * Then, the event’s token B equivalent is:
 *
 *    eventTokenBValue = eventUsdValue(evt) ÷ tokenBPrice_at_event
 *
 * The function then reconstructs:
 *  - Total deposits in token B,
 *  - Fee rewards in token B,
 *  - Capital deposits in token B (total deposits minus fee rewards),
 *  - Total withdrawals in token B.
 *
 * The same proportional withdrawal method is applied to separate realized from unrealized PnL.
 *
 * Additionally, the current position’s USD value is converted into token B units using the
 * current token B price (currentTokenBPriceUsd).
 *
 * This function now fetches the token decimals from token mapping (via getTokenMapping)
 * using the provided tokenBAddress.
 *
 * @param positionId - The position’s address.
 * @param currentPositionValueUsd - The current USD value of the position.
 * @param currentTokenBPriceUsd - The current USD price for token B.
 * @param tokenBAddress - The token B address (used to fetch decimals from token mapping).
 * @returns An object with realized, unrealized, and net PnL in token B.
 */
export async function calculateMeteoraPositionPNLTokenB(
  positionId: string,
  currentPositionValueUsd: number,
  currentTokenBPriceUsd: number,
  tokenBAddress: string
): Promise<{
  realizedPNLTokenB: number;
  unrealizedPNLTokenB: number;
  netPNLTokenB: number;
}> {
  // Fetch the token mapping for token B to get decimals.
  const tokenMapping = await getTokenMapping(tokenBAddress);
  const decimals = tokenMapping.decimals;

  // Fetch events.
  const deposits = await fetchDeposits(positionId);
  const withdrawals = await fetchWithdrawals(positionId);
  const feeClaims = await fetchFeeClaims(positionId);

  // For each event, convert its USD value into token B units.
  // We assume token B is the Y token and use its raw amount and usd amount.
  async function convertEventToTokenB(evt: PnlEvent): Promise<number> {
    const tokenYAmount = evt.token_y_amount;
    if (tokenYAmount === 0) {
      return eventUsdValue(evt) / currentTokenBPriceUsd;
    }
    // Convert raw token_y_amount to actual quantity using decimals.
    const actualTokenYQuantity = tokenYAmount / Math.pow(10, decimals);
    // Compute the token B price at the event.
    const tokenBPriceAtEvent = evt.token_y_usd_amount / actualTokenYQuantity;
    // Convert the total USD value of the event into token B units.
    return eventUsdValue(evt) / tokenBPriceAtEvent;
  }

  let totalDepositTokenB = 0;
  for (const dep of deposits) {
    totalDepositTokenB += await convertEventToTokenB(dep);
  }

  let totalFeeRewardTokenB = 0;
  for (const fc of feeClaims) {
    totalFeeRewardTokenB += await convertEventToTokenB(fc);
  }

  let totalWithdrawalTokenB = 0;
  for (const wd of withdrawals) {
    totalWithdrawalTokenB += await convertEventToTokenB(wd);
  }

  // Capital deposits in token B exclude fee rewards.
  const capitalDepositsTokenB = totalDepositTokenB - totalFeeRewardTokenB;
  if (totalDepositTokenB === 0) {
    throw new Error(`No deposits found for position ${positionId}`);
  }

  // Assume withdrawals remove deposits proportionally.
  const withdrawalRatio = totalWithdrawalTokenB / totalDepositTokenB;
  const withdrawnCapitalTokenB = capitalDepositsTokenB * withdrawalRatio;

  const realizedPNLTokenB = totalWithdrawalTokenB - withdrawnCapitalTokenB;

  // Convert the current position's USD value into token B units using the current token B price.
  const currentPositionValueTokenB = currentPositionValueUsd / currentTokenBPriceUsd;
  const remainingCapitalTokenB = capitalDepositsTokenB - withdrawnCapitalTokenB;
  const unrealizedPNLTokenB = currentPositionValueTokenB - remainingCapitalTokenB;
  const netPNLTokenB = (currentPositionValueTokenB + totalWithdrawalTokenB) - capitalDepositsTokenB;

  return {
    realizedPNLTokenB,
    unrealizedPNLTokenB,
    netPNLTokenB,
  };
}
