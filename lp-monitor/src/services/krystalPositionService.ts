// src/services/krystalPositionService.ts
import { fetchKrystalPositions } from '../dexes/krystalAdapter';
import { KrystalPositionInfo } from './types';

export async function retrieveKrystalPositions(walletAddress: string): Promise<KrystalPositionInfo[]> {
  const positions = await fetchKrystalPositions(walletAddress);
  console.log(`Retrieved ${positions.length} Krystal LP positions for wallet ${walletAddress}`);
  return positions;
}