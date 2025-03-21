// src/dexes/krystalAdapter.ts
import axios from 'axios';
import { KrystalPositionInfo } from '../services/types';

interface KrystalApiPosition {
  chainName: string;
  chainId: number;
  userAddress: string;
  id: string;
  tokenAddress: string;
  tokenId: string;
  liquidity: string;
  minPrice: number;
  maxPrice: number;
  currentAmounts: { token: { address: string; symbol: string; decimals: number }; balance: string }[];
  providedAmounts: { token: { address: string; symbol: string; decimals: number }; balance: string }[];
  feePending: { token: { address: string }; balance: string }[];
  initialUnderlyingValue: number;
  currentPositionValue: number;
  impermanentLoss: number;
  feeApr: number;
  status: string;
  pool: { poolAddress: string; price: number; project: string };
}

interface KrystalApiResponse {
  statsByChain: { [chainId: string]: { totalFeeEarned: number } };
  positions: KrystalApiPosition[];
}

export async function fetchKrystalPositions(walletAddress: string): Promise<KrystalPositionInfo[]> {
  const baseUrl = 'https://api.krystal.app/all/v1/lp/userPositions';
  const params = {
    addresses: walletAddress,
    chainIds: '137', // Polygon for now
    limit: 100,
    orderBy: 'liquidity',
  };

  try {
    const response = await axios.get<KrystalApiResponse>(baseUrl, { params, headers: { 'accept': 'application/json' } });
    const data = response.data;

    if (!data.positions || data.positions.length === 0) {
      console.log('No LP positions returned from Krystal API.');
      return [];
    }

    const totalFeeEarnedUsd = data.statsByChain['137']?.totalFeeEarned || 0;

    const krystalPositions: KrystalPositionInfo[] = data.positions.map((pos: KrystalApiPosition) => {
      const tokenX = pos.currentAmounts[0] || { token: { address: '', symbol: 'Unknown', decimals: 0 }, balance: '0' };
      const tokenY = pos.currentAmounts[1] || { token: { address: '', symbol: 'Unknown', decimals: 0 }, balance: '0' };
      const providedX = pos.providedAmounts[0] || { token: { address: '', symbol: 'Unknown', decimals: 0 }, balance: '0' };
      const providedY = pos.providedAmounts[1] || { token: { address: '', symbol: 'Unknown', decimals: 0 }, balance: '0' };
      const feeX = pos.feePending.find((f: { token: { address: string }; balance: string }) => f.token.address === tokenX.token.address) || { balance: '0' };
      const feeY = pos.feePending.find((f: { token: { address: string }; balance: string }) => f.token.address === tokenY.token.address) || { balance: '0' };

      return {
        id: pos.id,
        owner: pos.userAddress,
        chain: pos.chainName,
        protocol: pos.pool.project,
        poolAddress: pos.pool.poolAddress,
        tokenXSymbol: tokenX.token.symbol,
        tokenYSymbol: tokenY.token.symbol,
        tokenXAddress: tokenX.token.address,
        tokenYAddress: tokenY.token.address,
        tokenXAmount: tokenX.balance,
        tokenYAmount: tokenY.balance,
        tokenXProvidedAmount: providedX.balance,
        tokenYProvidedAmount: providedY.balance,
        tokenXDecimals: tokenX.token.decimals,
        tokenYDecimals: tokenY.token.decimals,
        minPrice: pos.minPrice,
        maxPrice: pos.maxPrice,
        currentPrice: pos.pool.price,
        isInRange: pos.status === 'IN_RANGE',
        initialValueUsd: pos.initialUnderlyingValue,
        actualValueUsd: pos.currentPositionValue,
        impermanentLoss: pos.impermanentLoss,
        unclaimedFeeX: feeX.balance,
        unclaimedFeeY: feeY.balance,
        feeApr: pos.feeApr,
        totalFeeEarnedUsd,
      };
    });

    console.log(`Fetched ${krystalPositions.length} LP positions from Krystal API for wallet ${walletAddress}`);
    return krystalPositions;
  } catch (error) {
    console.error('Error fetching positions from Krystal API:', error);
    return [];
  }
}