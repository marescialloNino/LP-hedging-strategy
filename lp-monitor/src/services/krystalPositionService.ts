import { fetchKrystalPositions, fetchKrystalVaultPositions } from '../dexes/krystalAdapter';
import { KrystalPositionInfo } from './types';
import { logger } from '../utils/logger';

export async function retrieveKrystalPositions(walletAddress: string, chainIds: string): Promise<KrystalPositionInfo[]> {
  try {
    logger.info(`Fetching Krystal positions for wallet ${walletAddress} on chains: ${chainIds}`);
    const positions = await fetchKrystalPositions(walletAddress, chainIds);
    if (positions.length === 0) {
      logger.info(`No Krystal positions found for wallet ${walletAddress} on chains ${chainIds}`);
    } else {
      logger.info(`Retrieved ${positions.length} Krystal positions for wallet ${walletAddress}`);
    }
    return positions;
  } catch (error) {
    throw error; // Rethrow to propagate to index.ts
  }
}

export async function retrieveKrystalVaultPositions(walletAddress: string, chainIds: string, vaultShare: number): Promise<KrystalPositionInfo[]> {
  try {
    logger.info(`Fetching Krystal Vault positions for wallet ${walletAddress} on chains: ${chainIds}`);
    const positions = await fetchKrystalVaultPositions(walletAddress, vaultShare, chainIds);
    if (positions.length === 0) {
      logger.info(`No Krystal Vault positions found for wallet ${walletAddress} on chains ${chainIds}`);
    } else {
      logger.info(`Retrieved ${positions.length} Krystal Vault positions for wallet ${walletAddress}`);
    }
    return positions;
  } catch (error) {
    throw error; // Rethrow to propagate to index.ts
  }
}