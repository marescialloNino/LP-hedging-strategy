// src/services/krystalPositionService.ts
import { fetchKrystalPositions } from '../dexes/krystalAdapter';
import { KrystalPositionInfo } from './types';
import { config } from '../config';
import { logger } from '../utils/logger'; // Import logger from utils/logger

export async function retrieveKrystalPositions(walletAddress: string): Promise<KrystalPositionInfo[]> {
  try {
    // Use chain IDs from config
    const chainIds = config.KRYSTAL_CHAIN_IDS.join(',');
    logger.info(`Fetching Krystal positions for wallet ${walletAddress} on chains: ${chainIds}`);

    const positions = await fetchKrystalPositions(walletAddress, chainIds);
    if (positions.length === 0) {
      logger.info(`No Krystal positions found for wallet ${walletAddress} on chains ${chainIds}`);
    } else {
      logger.info(`Retrieved ${positions.length} Krystal positions for wallet ${walletAddress}`);
    }
    return positions;
  } catch (error) {
    logger.error(`Error fetching Krystal positions for wallet ${walletAddress}: ${error}`);
    return [];
  }
}