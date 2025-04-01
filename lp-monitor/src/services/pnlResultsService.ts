// src/services/pnlResultsService.ts
import { calculateMeteoraPositionPNLUsd, calculateMeteoraPositionPNLTokenB } from './pnlCalculationService';
import { createObjectCsvWriter } from 'csv-writer';
import path from 'path';

export interface PnlResult {
  timestamp: string;
  positionId: string;
  owner: string;
  pool: string;
  tokenXSymbol: string;
  tokenYSymbol: string;
  realizedPNLUsd: number;
  unrealizedPNLUsd: number;
  netPNLUsd: number;
  realizedPNLTokenB: number;
  unrealizedPNLTokenB: number;
  netPNLTokenB: number;
}

/**
 * Process PnL for each position.
 *
 * For each position object (assumed to be of type PositionInfo),
 * we compute the current USD value as:
 *
 *     currentPositionValueUsd = (amountX * tokenXPriceUsd) + (amountY * tokenYPriceUsd)
 *
 * Then we calculate both the USD-based and tokenB-based PnL.
 *
 * We assume that tokenB is the Y token. Its address is taken as position.tokenY,
 * and the current tokenB price is taken from position.tokenYPriceUsd.
 */
export async function processPnlForPositions(positions: any[]): Promise<PnlResult[]> {
  const results: PnlResult[] = [];
  const now = new Date().toISOString();

  for (const pos of positions) {
    // Compute the current USD value of the position.
    const amountX = parseFloat(pos.amountX);
    const amountY = parseFloat(pos.amountY);
    const tokenXPriceUsd = pos.tokenXPriceUsd || 0;
    const tokenYPriceUsd = pos.tokenYPriceUsd || 0;
    const currentPositionValueUsd = (amountX * tokenXPriceUsd) + (amountY * tokenYPriceUsd);

    try {
      // Calculate USD-based PnL
      const pnlUsd = await calculateMeteoraPositionPNLUsd(
        pos.id,
        currentPositionValueUsd,
        pos.tokenX,
        pos.tokenY
      );

      // Calculate tokenB-based PnL
      const pnlTokenB = await calculateMeteoraPositionPNLTokenB(
        pos.id,
        currentPositionValueUsd,
        tokenYPriceUsd,
        pos.tokenX,
        pos.tokenY
      );

      results.push({
        timestamp: now,
        positionId: pos.id,
        owner: pos.owner,
        pool: pos.pool,
        tokenXSymbol: pos.tokenXSymbol || 'Unknown',
        tokenYSymbol: pos.tokenYSymbol || 'Unknown',
        realizedPNLUsd: pnlUsd.realizedPNLUsd,
        unrealizedPNLUsd: pnlUsd.unrealizedPNLUsd,
        netPNLUsd: pnlUsd.netPNLUsd,
        realizedPNLTokenB: pnlTokenB.realizedPNLTokenB,
        unrealizedPNLTokenB: pnlTokenB.unrealizedPNLTokenB,
        netPNLTokenB: pnlTokenB.netPNLTokenB,
      });
    } catch (err) {
      console.error(`Error processing position ${pos.id}: ${err}`);
    }
  }

  return results;
}

/**
 * Save the PnL results to a CSV file.
 */
export async function savePnlResultsCsv(results: PnlResult[]): Promise<void> {
  const filePath = path.join(__dirname, '../../../lp-data/position_pnl_results.csv');
  const csvWriter = createObjectCsvWriter({
    path: filePath,
    header: [
      { id: 'timestamp', title: 'Timestamp' },
      { id: 'positionId', title: 'Position ID' },
      { id: 'owner', title: 'Owner' },
      { id: 'pool', title: 'Pool Address' },
      { id: 'tokenXSymbol', title: 'Token X Symbol' },
      { id: 'tokenYSymbol', title: 'Token Y Symbol' },
      { id: 'realizedPNLUsd', title: 'Realized PNL (USD)' },
      { id: 'unrealizedPNLUsd', title: 'Unrealized PNL (USD)' },
      { id: 'netPNLUsd', title: 'Net PNL (USD)' },
      { id: 'realizedPNLTokenB', title: 'Realized PNL (Token B)' },
      { id: 'unrealizedPNLTokenB', title: 'Unrealized PNL (Token B)' },
      { id: 'netPNLTokenB', title: 'Net PNL (Token B)' },
    ],
    append: false,
  });

  await csvWriter.writeRecords(results);
  console.log(`PnL results saved to ${filePath}`);
}
