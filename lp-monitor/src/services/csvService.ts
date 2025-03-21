// src/services/csvService.ts
import fs from 'fs';
import path from 'path';
import { createObjectCsvWriter } from 'csv-writer';
import { PositionInfo, LiquidityProfileEntry, KrystalPositionInfo } from './types';

// Fixed file paths for history and latest positions
const METEORA_HISTORY_CSV_PATH = path.join(__dirname, '../../../lp-data/LP_meteora_positions_history.csv');
const METEORA_LATEST_CSV_PATH = path.join(__dirname, '../../../lp-data/LP_meteora_positions_latest.csv');
const KRYSTAL_HISTORY_CSV_PATH = path.join(__dirname, '../../../lp-data/LP_krystal_positions_history.csv');
const KRYSTAL_LATEST_CSV_PATH = path.join(__dirname, '../../../lp-data/LP_krystal_positions_latest.csv');
const LIQUIDITY_PROFILE_CSV_PATH = path.join(__dirname, '../../../lp-data/meteora_liquidity_profiles.csv');

async function writeCSV<T extends Record<string, any>>(filePath: string, records: T[], headers: { id: string; title: string }[], append: boolean = true): Promise<void> {
  const csvWriter = createObjectCsvWriter({
    path: filePath,
    header: headers,
    append: append && fs.existsSync(filePath),
  });

  await csvWriter.writeRecords(records);
  console.log(`CSV ${append ? 'appended' : 'written'} to ${filePath} with ${records.length} rows`);
}

// Helper function to calculate human-readable quantity for Krystal
function calculateQuantity(rawAmount: string, decimals: number): number {
  return parseFloat(rawAmount) / Math.pow(10, decimals);
}

export async function generateMeteoraCSV(walletAddress: string, positions: PositionInfo[]): Promise<any[]> {
  const headers = [
    { id: 'timestamp', title: 'Timestamp' },
    { id: 'walletAddress', title: 'Wallet Address' },
    { id: 'positionKey', title: 'Position Key' },
    { id: 'poolAddress', title: 'Pool Address' },
    { id: 'tokenXAddress', title: 'Token X Address' },
    { id: 'tokenYAddress', title: 'Token Y Address' },
    { id: 'amountX', title: 'Token X Qty' },
    { id: 'amountY', title: 'Token Y Qty' },
    { id: 'lowerBinId', title: 'Lower Boundary' },
    { id: 'upperBinId', title: 'Upper Boundary' },
    { id: 'isInRange', title: 'Is In Range' },
    { id: 'unclaimedFeeX', title: 'Unclaimed Fee X' },
    { id: 'unclaimedFeeY', title: 'Unclaimed Fee Y' },
  ];

  const records = positions
    .filter(pos => !(pos.amountX === '0' && pos.amountY === '0'))
    .map(pos => ({
      timestamp: new Date().toISOString(),
      walletAddress,
      positionKey: pos.id,
      poolAddress: pos.pool,
      tokenXAddress: pos.tokenX,
      tokenYAddress: pos.tokenY,
      amountX: pos.amountX,
      amountY: pos.amountY,
      lowerBinId: pos.lowerBinId,
      upperBinId: pos.upperBinId,
      isInRange: pos.isInRange,
      unclaimedFeeX: pos.unclaimedFeeX,
      unclaimedFeeY: pos.unclaimedFeeY,
    }));

  // Write to history file (append)
  await writeCSV(METEORA_HISTORY_CSV_PATH, records, headers, true);
  // Return records for latest file
  return records;
}

export async function generateAndWriteLiquidityProfileCSV(walletAddress: string, positions: PositionInfo[]): Promise<void> {
  const headers = [
    { id: 'walletAddress', title: 'Wallet Address' },
    { id: 'positionId', title: 'Position ID' },
    { id: 'binId', title: 'Bin ID' },
    { id: 'price', title: 'Price' },
    { id: 'positionLiquidity', title: 'Position Liquidity' },
    { id: 'positionXAmount', title: 'Position X Amount' },
    { id: 'positionYAmount', title: 'Position Y Amount' },
    { id: 'liquidityShare', title: 'Liquidity Share' },
  ];

  const records = positions.flatMap(pos =>
    pos.liquidityProfile.map(entry => ({
      walletAddress: pos.owner,
      positionId: pos.id,
      binId: entry.binId,
      price: entry.price,
      positionLiquidity: entry.positionLiquidity,
      positionXAmount: entry.positionXAmount,
      positionYAmount: entry.positionYAmount,
      liquidityShare: entry.liquidityShare,
    }))
  );

  await writeCSV(LIQUIDITY_PROFILE_CSV_PATH, records, headers, false); // Overwrite mode
}

export async function generateKrystalCSV(walletAddress: string, positions: KrystalPositionInfo[]): Promise<any[]> {
  const headers = [
    { id: 'timestamp', title: 'Timestamp' },
    { id: 'walletAddress', title: 'Wallet Address' },
    { id: 'chain', title: 'Chain' },
    { id: 'protocol', title: 'Protocol' },
    { id: 'poolAddress', title: 'Pool Address' },
    { id: 'tokenXAddress', title: 'Token X Address' },
    { id: 'tokenXQty', title: 'Token X Qty' },
    { id: 'tokenYAddress', title: 'Token Y Address' },
    { id: 'tokenYQty', title: 'Token Y Qty' },
    { id: 'minPrice', title: 'Min Price' },
    { id: 'maxPrice', title: 'Max Price' },
    { id: 'currentPrice', title: 'Current Price' },
    { id: 'isInRange', title: 'Is In Range' },
    { id: 'initialValueUsd', title: 'Initial Value USD' },
    { id: 'actualValueUsd', title: 'Actual Value USD' },
    { id: 'impermanentLoss', title: 'Impermanent Loss' },
    { id: 'unclaimedFeeX', title: 'Unclaimed Fee X' },
    { id: 'unclaimedFeeY', title: 'Unclaimed Fee Y' },
    { id: 'feeApr', title: 'Fee APR' },
  ];

  const records = positions.map(pos => ({
    timestamp: new Date().toISOString(),
    walletAddress,
    chain: pos.chain,
    protocol: pos.protocol,
    poolAddress: pos.poolAddress,
    tokenXAddress: pos.tokenXAddress,
    tokenXQty: calculateQuantity(pos.tokenXAmount, pos.tokenXDecimals),
    tokenYAddress: pos.tokenYAddress,
    tokenYQty: calculateQuantity(pos.tokenYAmount, pos.tokenYDecimals),
    minPrice: pos.minPrice,
    maxPrice: pos.maxPrice,
    currentPrice: pos.currentPrice,
    isInRange: pos.isInRange,
    initialValueUsd: pos.initialValueUsd,
    actualValueUsd: pos.actualValueUsd,
    impermanentLoss: pos.impermanentLoss,
    unclaimedFeeX: calculateQuantity(pos.unclaimedFeeX, pos.tokenXDecimals),
    unclaimedFeeY: calculateQuantity(pos.unclaimedFeeY, pos.tokenYDecimals),
    feeApr: pos.feeApr,
  }));

  // Write to history file (append)
  await writeCSV(KRYSTAL_HISTORY_CSV_PATH, records, headers, true);
  // Return records for latest file
  return records;
}

// Export headers and write functions for latest CSVs
export const METEORA_LATEST_HEADERS = [
  { id: 'timestamp', title: 'Timestamp' },
  { id: 'walletAddress', title: 'Wallet Address' },
  { id: 'positionKey', title: 'Position Key' },
  { id: 'poolAddress', title: 'Pool Address' },
  { id: 'tokenXAddress', title: 'Token X Address' },
  { id: 'tokenYAddress', title: 'Token Y Address' },
  { id: 'amountX', title: 'Token X Qty' },
  { id: 'amountY', title: 'Token Y Qty' },
  { id: 'lowerBinId', title: 'Lower Boundary' },
  { id: 'upperBinId', title: 'Upper Boundary' },
  { id: 'isInRange', title: 'Is In Range' },
  { id: 'unclaimedFeeX', title: 'Unclaimed Fee X' },
  { id: 'unclaimedFeeY', title: 'Unclaimed Fee Y' },
];

export const KRYSTAL_LATEST_HEADERS = [
  { id: 'timestamp', title: 'Timestamp' },
  { id: 'walletAddress', title: 'Wallet Address' },
  { id: 'chain', title: 'Chain' },
  { id: 'protocol', title: 'Protocol' },
  { id: 'poolAddress', title: 'Pool Address' },
  { id: 'tokenXAddress', title: 'Token X Address' },
  { id: 'tokenXQty', title: 'Token X Qty' },
  { id: 'tokenYAddress', title: 'Token Y Address' },
  { id: 'tokenYQty', title: 'Token Y Qty' },
  { id: 'minPrice', title: 'Min Price' },
  { id: 'maxPrice', title: 'Max Price' },
  { id: 'currentPrice', title: 'Current Price' },
  { id: 'isInRange', title: 'Is In Range' },
  { id: 'initialValueUsd', title: 'Initial Value USD' },
  { id: 'actualValueUsd', title: 'Actual Value USD' },
  { id: 'impermanentLoss', title: 'Impermanent Loss' },
  { id: 'unclaimedFeeX', title: 'Unclaimed Fee X' },
  { id: 'unclaimedFeeY', title: 'Unclaimed Fee Y' },
  { id: 'feeApr', title: 'Fee APR' },
];

export async function writeMeteoraLatestCSV(records: any[]): Promise<void> {
  await writeCSV(METEORA_LATEST_CSV_PATH, records, METEORA_LATEST_HEADERS, false);
}

export async function writeKrystalLatestCSV(records: any[]): Promise<void> {
  await writeCSV(KRYSTAL_LATEST_CSV_PATH, records, KRYSTAL_LATEST_HEADERS, false);
}