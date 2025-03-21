// src/services/meteoraPositionService.ts
import { fetchMeteoraPositions } from '../dexes/meteoraDlmmAdapter';
import { PositionInfo } from './types';

export async function retrieveMeteoraPositions(walletAddress: string): Promise<PositionInfo[]> {
  return await fetchMeteoraPositions(walletAddress);
}