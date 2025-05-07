// src/services/meteoraPositionService.ts
import { fetchMeteoraPositions } from '../dexes/meteoraDlmmAdapter';
import { PositionInfo } from './types';
import { logger } from '../utils/logger';

export async function retrieveMeteoraPositions(walletAddress: string): Promise<PositionInfo[]> {
  try {
    const positions = await fetchMeteoraPositions(walletAddress);
    if (positions.length === 0) {
      logger.info(`No Meteora positions found for wallet ${walletAddress}`);
    } else {
      logger.info(`Retrieved ${positions.length} Meteora positions for wallet ${walletAddress}`);
    }
    return positions;
  } catch (error) {
    throw error; // Rethrow to propagate to index.ts
  }
}