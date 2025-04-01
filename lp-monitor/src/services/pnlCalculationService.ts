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
  event_type?: 'Deposit' | 'Withdrawal' | 'Claim Fees';
}

/**
 * Fetch withdrawal events for a given position.
 */
export async function fetchWithdrawals(positionId: string): Promise<PnlEvent[]> {
  await apiRateLimit();
  const url = `https://dlmm-api.meteora.ag/position/${positionId}/withdraws`;
  const response = await axios.get(url);
  return (response.data as PnlEvent[]).map(event => ({ ...event, event_type: 'Withdrawal' }));
}

/**
 * Fetch deposit events for a given position.
 */
export async function fetchDeposits(positionId: string): Promise<PnlEvent[]> {
  await apiRateLimit();
  const url = `https://dlmm-api.meteora.ag/position/${positionId}/deposits`;
  const response = await axios.get(url);
  return (response.data as PnlEvent[]).map(event => ({ ...event, event_type: 'Deposit' }));
}

/**
 * Fetch fee claim events for a given position.
 */
export async function fetchFeeClaims(positionId: string): Promise<PnlEvent[]> {
  await apiRateLimit();
  const url = `https://dlmm-api.meteora.ag/position/${positionId}/claim_fees`;
  const response = await axios.get(url);
  return (response.data as PnlEvent[]).map(event => ({ ...event, event_type: 'Claim Fees' }));
}

/**
 * Helper to compute the total USD value of an event.
 */
function eventUsdValue(evt: PnlEvent): number {
  return evt.token_x_usd_amount + evt.token_y_usd_amount;
}

/**
 * Interface for token metadata (assuming this comes from tokenMappingService).
 */
interface TokenMetadata {
  address: string;
  decimals: number;
}

/**
 * Calculate PnL in USD with reinvested fees tracked in token quantities.
 */
export async function calculateMeteoraPositionPNLUsd(
  positionId: string,
  currentPositionValueUsd: number,
  tokenXAddress: string,
  tokenYAddress: string
): Promise<{
  realizedPNLUsd: number;
  unrealizedPNLUsd: number;
  netPNLUsd: number;
  capitalDepositsUsd: number;
  reinvestedFeesUsd: number;
  totalFeeRewardUsd: number;
}> {
  // Fetch token decimals
  const tokenXMapping = await getTokenMapping(tokenXAddress);
  const tokenYMapping = await getTokenMapping(tokenYAddress);
  const tokenXDecimals = tokenXMapping.decimals;
  const tokenYDecimals = tokenYMapping.decimals;

  // Fetch events
  const deposits = await fetchDeposits(positionId);
  const withdrawals = await fetchWithdrawals(positionId);
  const feeClaims = await fetchFeeClaims(positionId);

  // Combine and sort events by timestamp
  const allEvents: PnlEvent[] = [...deposits, ...withdrawals, ...feeClaims].sort(
    (a, b) => a.onchain_timestamp - b.onchain_timestamp
  );

  // Track cumulative and available claimed tokens
  let cumulativeClaimedX = 0;
  let cumulativeClaimedY = 0;
  let availableClaimedX = 0;
  let availableClaimedY = 0;
  let capitalDepositsUsd = 0;
  let totalDepositUsd = 0;
  let totalFeeRewardUsd = 0;
  let totalWithdrawalUsd = 0;

  for (const evt of allEvents) {
    if (evt.event_type === 'Claim Fees') {
      cumulativeClaimedX += evt.token_x_amount;
      cumulativeClaimedY += evt.token_y_amount;
      availableClaimedX = cumulativeClaimedX;
      availableClaimedY = cumulativeClaimedY;
      totalFeeRewardUsd += eventUsdValue(evt);
    } else if (evt.event_type === 'Deposit') {
      const depositXAdjusted = evt.token_x_amount / Math.pow(10, tokenXDecimals);
      const depositYAdjusted = evt.token_y_amount / Math.pow(10, tokenYDecimals);
      const availableXAdjusted = availableClaimedX / Math.pow(10, tokenXDecimals);
      const availableYAdjusted = availableClaimedY / Math.pow(10, tokenYDecimals);

      // Calculate reinvested ratios
      const reinvestedRatioX = depositXAdjusted > 0 ? Math.min(availableXAdjusted / depositXAdjusted, 1) : 0;
      const reinvestedRatioY = depositYAdjusted > 0 ? Math.min(availableYAdjusted / depositYAdjusted, 1) : 0;

      // Split USD values
      const reinvestedXUsd = evt.token_x_usd_amount * reinvestedRatioX;
      const capitalXUsd = evt.token_x_usd_amount * (1 - reinvestedRatioX);
      const reinvestedYUsd = evt.token_y_usd_amount * reinvestedRatioY;
      const capitalYUsd = evt.token_y_usd_amount * (1 - reinvestedRatioY);

      // Total cost basis for this deposit
      const costBasis = capitalXUsd + capitalYUsd;
      capitalDepositsUsd += costBasis;
      totalDepositUsd += eventUsdValue(evt);

      // Update available claimed tokens
      const reinvestedX = evt.token_x_amount * reinvestedRatioX;
      const reinvestedY = evt.token_y_amount * reinvestedRatioY;
      availableClaimedX = Math.max(0, availableClaimedX - reinvestedX);
      availableClaimedY = Math.max(0, availableClaimedY - reinvestedY);
    } else if (evt.event_type === 'Withdrawal') {
      totalWithdrawalUsd += eventUsdValue(evt);
    }
    // Add event_type for consistency
    evt['event_type'] = evt['event_type'] || (deposits.includes(evt) ? 'Deposit' : withdrawals.includes(evt) ? 'Withdrawal' : 'Claim Fees');
  }

  if (totalDepositUsd === 0) {
    throw new Error(`No deposits found for position ${positionId}`);
  }

  // Reinvested fees
  const reinvestedFeesUsd = totalDepositUsd - capitalDepositsUsd;

  // Total inflows
  const totalInflowsUsd = capitalDepositsUsd + totalFeeRewardUsd + reinvestedFeesUsd;

  // PnL calculations
  const withdrawalRatio = totalInflowsUsd > 0 ? Math.min(totalWithdrawalUsd / totalInflowsUsd, 1) : 0;
  const withdrawnCapitalUsd = capitalDepositsUsd * withdrawalRatio;
  const realizedPNLUsd = totalWithdrawalUsd - withdrawnCapitalUsd;
  const remainingCapitalUsd = capitalDepositsUsd - withdrawnCapitalUsd;
  const unrealizedPNLUsd = currentPositionValueUsd - remainingCapitalUsd;
  const netPNLUsd = (currentPositionValueUsd + totalWithdrawalUsd) - capitalDepositsUsd;

  return {
    realizedPNLUsd,
    unrealizedPNLUsd,
    netPNLUsd,
    capitalDepositsUsd,
    reinvestedFeesUsd,
    totalFeeRewardUsd,
  };
}

/**
 * Calculate PnL in Token Y (Token B) with reinvested fees tracked.
 */
export async function calculateMeteoraPositionPNLTokenB(
  positionId: string,
  currentPositionValueUsd: number,
  currentTokenBPriceUsd: number,
  tokenXAddress: string,
  tokenYAddress: string
): Promise<{
  realizedPNLTokenB: number;
  unrealizedPNLTokenB: number;
  netPNLTokenB: number;
  capitalDepositsTokenB: number;
  reinvestedFeesTokenB: number;
}> {
  // Fetch token decimals
  const tokenXMapping = await getTokenMapping(tokenXAddress);
  const tokenYMapping = await getTokenMapping(tokenYAddress);
  const tokenXDecimals = tokenXMapping.decimals;
  const tokenYDecimals = tokenYMapping.decimals;

  // Fetch events
  const deposits = await fetchDeposits(positionId);
  const withdrawals = await fetchWithdrawals(positionId);
  const feeClaims = await fetchFeeClaims(positionId);

  // Combine and sort events
  const allEvents: PnlEvent[] = [...deposits, ...withdrawals, ...feeClaims].sort(
    (a, b) => a.onchain_timestamp - b.onchain_timestamp
  );

  // Convert event to Token B units
  async function convertEventToTokenB(evt: PnlEvent): Promise<number> {
    const tokenYAmount = evt.token_y_amount;
    if (tokenYAmount === 0) {
      return eventUsdValue(evt) / currentTokenBPriceUsd;
    }
    const actualTokenYQuantity = tokenYAmount / Math.pow(10, tokenYDecimals);
    const tokenBPriceAtEvent = evt.token_y_usd_amount / actualTokenYQuantity;
    return eventUsdValue(evt) / tokenBPriceAtEvent;
  }

  let cumulativeClaimedX = 0;
  let cumulativeClaimedY = 0;
  let availableClaimedX = 0;
  let availableClaimedY = 0;
  let capitalDepositsTokenB = 0;
  let totalDepositTokenB = 0;
  let totalFeeRewardTokenB = 0;
  let totalWithdrawalTokenB = 0;

  for (const evt of allEvents) {
    const eventValueTokenB = await convertEventToTokenB(evt);
    if (evt.event_type === 'Claim Fees') {
      cumulativeClaimedX += evt.token_x_amount;
      cumulativeClaimedY += evt.token_y_amount;
      availableClaimedX = cumulativeClaimedX;
      availableClaimedY = cumulativeClaimedY;
      totalFeeRewardTokenB += eventValueTokenB;
    } else if (evt.event_type === 'Deposit') {
      const depositXAdjusted = evt.token_x_amount / Math.pow(10, tokenXDecimals);
      const depositYAdjusted = evt.token_y_amount / Math.pow(10, tokenYDecimals);
      const availableXAdjusted = availableClaimedX / Math.pow(10, tokenXDecimals);
      const availableYAdjusted = availableClaimedY / Math.pow(10, tokenYDecimals);

      // Calculate reinvested ratios
      const reinvestedRatioX = depositXAdjusted > 0 ? Math.min(availableXAdjusted / depositXAdjusted, 1) : 0;
      const reinvestedRatioY = depositYAdjusted > 0 ? Math.min(availableYAdjusted / depositYAdjusted, 1) : 0;

      // Split USD values and convert to Token B
      const reinvestedXUsd = evt.token_x_usd_amount * reinvestedRatioX;
      const capitalXUsd = evt.token_x_usd_amount * (1 - reinvestedRatioX);
      const reinvestedYUsd = evt.token_y_usd_amount * reinvestedRatioY;
      const capitalYUsd = evt.token_y_usd_amount * (1 - reinvestedRatioY);
      const costBasisTokenB = (capitalXUsd + capitalYUsd) / currentTokenBPriceUsd;

      capitalDepositsTokenB += costBasisTokenB;
      totalDepositTokenB += eventValueTokenB;

      // Update available claimed tokens
      const reinvestedX = evt.token_x_amount * reinvestedRatioX;
      const reinvestedY = evt.token_y_amount * reinvestedRatioY;
      availableClaimedX = Math.max(0, availableClaimedX - reinvestedX);
      availableClaimedY = Math.max(0, availableClaimedY - reinvestedY);
    } else if (evt.event_type === 'Withdrawal') {
      totalWithdrawalTokenB += eventValueTokenB;
    }
    evt['event_type'] = evt['event_type'] || (deposits.includes(evt) ? 'Deposit' : withdrawals.includes(evt) ? 'Withdrawal' : 'Claim Fees');
  }

  if (totalDepositTokenB === 0) {
    throw new Error(`No deposits found for position ${positionId}`);
  }

  // Reinvested fees
  const reinvestedFeesTokenB = totalDepositTokenB - capitalDepositsTokenB;

  // Total inflows
  const totalInflowsTokenB = capitalDepositsTokenB + totalFeeRewardTokenB + reinvestedFeesTokenB;

  // PnL calculations
  const withdrawalRatio = totalInflowsTokenB > 0 ? Math.min(totalWithdrawalTokenB / totalInflowsTokenB, 1) : 0;
  const withdrawnCapitalTokenB = capitalDepositsTokenB * withdrawalRatio;
  const realizedPNLTokenB = totalWithdrawalTokenB - withdrawnCapitalTokenB;
  const currentPositionValueTokenB = currentPositionValueUsd / currentTokenBPriceUsd;
  const remainingCapitalTokenB = capitalDepositsTokenB - withdrawnCapitalTokenB;
  const unrealizedPNLTokenB = currentPositionValueTokenB - remainingCapitalTokenB;
  const netPNLTokenB = (currentPositionValueTokenB + totalWithdrawalTokenB) - capitalDepositsTokenB;

  return {
    realizedPNLTokenB,
    unrealizedPNLTokenB,
    netPNLTokenB,
    capitalDepositsTokenB,
    reinvestedFeesTokenB,
  };
}