// src/services/types.ts

export interface LiquidityProfileEntry {
  binId: number;             // Bin identifier
  price: string;             // Price at this bin
  positionLiquidity: string; //  liquidity contribution to this bin
  positionXAmount: string;   //  X token amount in this bin (adjusted for decimals)
  positionYAmount: string;   //  Y token amount in this bin (adjusted for decimals)
  liquidityShare: string;    //  percentage share of the bin's total liquidity
}

export interface PositionInfo {
  id: string;
  owner: string;
  chain: string;
  pool: string;
  tokenX: string;
  tokenY: string;
  tokenXSymbol?: string;
  tokenYSymbol?: string;
  tokenXDecimals: number;
  tokenYDecimals: number;
  amountX: string;
  amountY: string;
  lowerBinId: number;
  upperBinId: number;
  activeBinId: number;
  isInRange: boolean;
  unclaimedFeeX: string;
  unclaimedFeeY: string;
  liquidityProfile: LiquidityProfileEntry[];
  tokenXPriceUsd?: number; // Add price in USD for token X
  tokenYPriceUsd?: number; // Add price in USD for token Y
}

export interface KrystalPositionInfo {
  id: string;
  owner: string;
  chain: string;
  protocol: string;
  poolAddress: string;
  tokenXSymbol: string;
  tokenYSymbol: string;
  tokenXAddress: string;
  tokenYAddress: string;
  tokenXAmount: string; // Current amount
  tokenYAmount: string; // Current amount
  tokenXProvidedAmount: string; // Initial provided amount
  tokenYProvidedAmount: string; // Initial provided amount
  tokenXDecimals: number;
  tokenYDecimals: number;
  minPrice: number; // Lower price boundary
  maxPrice: number; // Upper price boundary
  currentPrice: number; // Current pool price
  isInRange: boolean; // Derived from status
  initialValueUsd: number; // initialUnderlyingValue
  actualValueUsd: number; // currentPositionValue
  impermanentLoss: number;
  unclaimedFeeX: string;
  unclaimedFeeY: string;
  feeApr: number;
  totalFeeEarnedUsd: number; // From statsByChain if available
  tokenXPriceUsd: number; // Token X price in USD
  tokenYPriceUsd: number; // Token Y price in USD
}


interface ErrorFlags {
  LP_FETCHING_METEORA_ERROR: boolean;
  meteora_error_message: string;
  last_meteora_lp_update: string;
  LP_FETCHING_KRYSTAL_ERROR: boolean;
  krystal_error_message: string;
  last_krystal_lp_update: string;
  LP_FETCHING_VAULT_ERROR: boolean;
  vault_error_message: string;
  last_vault_lp_update: string;
}