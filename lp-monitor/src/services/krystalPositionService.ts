// src/services/krystalPositionService.ts
import { fetchKrystalPositions } from '../dexes/krystalAdapter';
import { KrystalPositionInfo } from './types';
import axios from 'axios';
import { logger } from '../utils/logger';

// Add chain IDs constant
const CHAIN_IDS = ['56', '42161']; // BSC and Arbitrum

export async function retrieveKrystalPositions(walletAddress: string): Promise<any[]> {
  try {
    // Join chain IDs with commas
    const chainIdsParam = CHAIN_IDS.join(',');
    const response = await axios.get(`https://api.krystal.app/v1/lp/userPositions`, {
      params: {
        walletAddress,
        chainIds: chainIdsParam  // Add chainIds parameter
      }
    });
    
    return response.data?.data || [];
  } catch (error) {
    logger.error(`Error fetching Krystal positions: ${error}`);
    return [];
  }
}