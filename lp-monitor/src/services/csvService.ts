// src/services/csvService.ts
import { createObjectCsvWriter } from 'csv-writer';
import path from 'path';
import fs from 'fs';
import { PositionInfo, KrystalPositionInfo } from './types';

const PROJECT_ROOT = path.resolve(__dirname, '../../..');
const METEORA_LATEST_CSV_PATH = path.join(PROJECT_ROOT, 'lp-data', 'LP_meteora_positions_latest.csv');
const METEORA_HISTORY_CSV_PATH = path.join(PROJECT_ROOT, 'lp-data', 'LP_meteora_positions_history.csv');
const KRYSTAL_LATEST_CSV_PATH = path.join(PROJECT_ROOT, 'lp-data', 'LP_krystal_positions_latest.csv');
const KRYSTAL_HISTORY_CSV_PATH = path.join(PROJECT_ROOT, 'lp-data', 'LP_krystal_positions_history.csv');
const LIQUIDITY_PROFILE_CSV_PATH = path.join(PROJECT_ROOT, 'lp-data', 'meteora_liquidity_profile.csv');

/**
 * Ensure the directory for `filePath` exists.
 */
function ensureDirExists(filePath: string) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/**
 * If `filePath` does not yet exist, create it with only the header row.
 */
async function ensureCsvFile(
  filePath: string,
  headers: { id: string; title: string }[]
) {
  if (!fs.existsSync(filePath)) {
    const writer = createObjectCsvWriter({
      path: filePath,
      header: headers,
      append: false
    });
    await writer.writeRecords([]);  // writes just the header row
    console.info(`Initialized CSV ${filePath} with headers: ${headers.map(h => h.title).join(', ')}`);
  }
}

/**
 * Generic CSV writer that bootstraps its own directory and header row.
 */
async function writeCSV<T extends Record<string, any>>(
  filePath: string,
  records: T[],
  headers: { id: string; title: string }[],
  append: boolean = true
): Promise<void> {
  // 1) ensure parent folder exists
  ensureDirExists(filePath);

  // 2) if appending, make sure file is bootstrapped
  if (append) {
    await ensureCsvFile(filePath, headers);
  }

  const csvWriter = createObjectCsvWriter({
    path: filePath,
    header: headers,
    append: append && fs.existsSync(filePath),
  });

  await csvWriter.writeRecords(records);
  console.log(`CSV ${append ? 'appended' : 'written'} to ${filePath} with ${records.length} rows`);
}

export async function generateMeteoraCSV(walletAddress: string, positions: PositionInfo[]): Promise<any[]> {
  console.log('[DEBUG] Entering generateMeteoraCSV');
  const records = positions.map(pos => {
    const tokenXQty = parseFloat(pos.amountX);
    const tokenYQty = parseFloat(pos.amountY);
    const tokenXPriceUsd = pos.tokenXPriceUsd || 0;
    const tokenYPriceUsd = pos.tokenYPriceUsd || 0;
    const tokenXUsdAmount = tokenXQty * tokenXPriceUsd;
    const tokenYUsdAmount = tokenYQty * tokenYPriceUsd;

    const record = {
      timestamp: new Date().toISOString(),
      walletAddress,
      positionKey: pos.id,
      poolAddress: pos.pool,
      tokenXSymbol: pos.tokenXSymbol || 'Unknown',
      tokenXAddress: pos.tokenX,
      amountX: pos.amountX,
      tokenYSymbol: pos.tokenYSymbol || 'Unknown',
      tokenYAddress: pos.tokenY,
      amountY: pos.amountY,
      lowerBinId: pos.lowerBinId,
      upperBinId: pos.upperBinId,
      isInRange: pos.isInRange,
      unclaimedFeeX: pos.unclaimedFeeX,
      unclaimedFeeY: pos.unclaimedFeeY,
      tokenXPriceUsd,
      tokenYPriceUsd,
      tokenXUsdAmount,
      tokenYUsdAmount,
    };

    console.log(`[DEBUG] Generated record for position ${pos.id}: ${JSON.stringify(record)}`);
    return record;
  });

  await writeCSV(METEORA_HISTORY_CSV_PATH, records, METEORA_LATEST_HEADERS, true);
  console.log(`[INFO] Appended ${records.length} records to ${METEORA_HISTORY_CSV_PATH}`);

  return records;
}

export async function generateLiquidityProfileCSV(walletAddress: string, positions: PositionInfo[]): Promise<void> {
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

  await writeCSV(LIQUIDITY_PROFILE_CSV_PATH, records, headers, false);
  console.log(`[INFO] Wrote ${records.length} records to ${LIQUIDITY_PROFILE_CSV_PATH}`);
}

export async function generateKrystalCSV(walletAddress: string, positions: KrystalPositionInfo[]): Promise<any[]> {
  const records = positions.map(pos => {
    const tokenXQty = calculateQuantity(pos.tokenXAmount, pos.tokenXDecimals);
    const tokenYQty = calculateQuantity(pos.tokenYAmount, pos.tokenYDecimals);
    const tokenXPriceUsd = pos.tokenXPriceUsd || 0;
    const tokenYPriceUsd = pos.tokenYPriceUsd || 0;
    const tokenXUsdAmount = tokenXQty * tokenXPriceUsd;
    const tokenYUsdAmount = tokenYQty * tokenYPriceUsd;
    const tvl = pos.tvl;

    const record = {
      timestamp: new Date().toISOString(),
      walletAddress,
      chain: pos.chain,
      protocol: pos.protocol,
      poolAddress: pos.poolAddress,
      tokenXSymbol: pos.tokenXSymbol,
      tokenXAddress: pos.tokenXAddress,
      tokenXQty,
      tokenXPriceUsd,
      tokenYSymbol: pos.tokenYSymbol,
      tokenYAddress: pos.tokenYAddress,
      tokenYQty,
      tokenYPriceUsd,
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
      tokenXUsdAmount,
      tokenYUsdAmount,
      tvl
    };

    return record;
  });

  await writeCSV(KRYSTAL_HISTORY_CSV_PATH, records, KRYSTAL_LATEST_HEADERS, true);
  console.log(`[INFO] Appended ${records.length} records to ${KRYSTAL_HISTORY_CSV_PATH}`);

  return records;
}

function calculateQuantity(amount: string, decimals: number): number {
  const parsedAmount = parseFloat(amount);
  return isNaN(parsedAmount) ? 0 : parsedAmount / Math.pow(10, decimals);
}

export const METEORA_LATEST_HEADERS = [
  { id: 'timestamp', title: 'Timestamp' },
  { id: 'walletAddress', title: 'Wallet Address' },
  { id: 'positionKey', title: 'Position Key' },
  { id: 'poolAddress', title: 'Pool Address' },
  { id: 'tokenXSymbol', title: 'Token X Symbol' },
  { id: 'tokenXAddress', title: 'Token X Address' },
  { id: 'amountX', title: 'Token X Qty' },
  { id: 'tokenYSymbol', title: 'Token Y Symbol' },
  { id: 'tokenYAddress', title: 'Token Y Address' },
  { id: 'amountY', title: 'Token Y Qty' },
  { id: 'lowerBinId', title: 'Lower Boundary' },
  { id: 'upperBinId', title: 'Upper Boundary' },
  { id: 'isInRange', title: 'Is In Range' },
  { id: 'unclaimedFeeX', title: 'Unclaimed Fee X' },
  { id: 'unclaimedFeeY', title: 'Unclaimed Fee Y' },
  { id: 'tokenXPriceUsd', title: 'Token X Price USD' },
  { id: 'tokenYPriceUsd', title: 'Token Y Price USD' },
  { id: 'tokenXUsdAmount', title: 'Token X USD Amount' },
  { id: 'tokenYUsdAmount', title: 'Token Y USD Amount' },
];

export const KRYSTAL_LATEST_HEADERS = [
  { id: 'timestamp', title: 'Timestamp' },
  { id: 'walletAddress', title: 'Wallet Address' },
  { id: 'chain', title: 'Chain' },
  { id: 'protocol', title: 'Protocol' },
  { id: 'poolAddress', title: 'Pool Address' },
  { id: 'tokenXSymbol', title: 'Token X Symbol' },
  { id: 'tokenXAddress', title: 'Token X Address' },
  { id: 'tokenXQty', title: 'Token X Qty' },
  { id: 'tokenXPriceUsd', title: 'Token X Price USD' },
  { id: 'tokenYSymbol', title: 'Token Y Symbol' },
  { id: 'tokenYAddress', title: 'Token Y Address' },
  { id: 'tokenYQty', title: 'Token Y Qty' },
  { id: 'tokenYPriceUsd', title: 'Token Y Price USD' },
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
  { id: 'tokenXUsdAmount', title: 'Token X USD Amount' },
  { id: 'tokenYUsdAmount', title: 'Token Y USD Amount' },
  { id: 'tvl', title: 'tvl' },
];

export async function writeMeteoraLatestCSV(records: any[]): Promise<void> {
  console.log(`[INFO] Writing ${records.length} records to ${METEORA_LATEST_CSV_PATH}`);
  if (records.length > 0) {
    console.log(`[DEBUG] Sample record before writing: ${JSON.stringify(records[0])}`);
    console.log(`[DEBUG] Headers for latest CSV: ${METEORA_LATEST_HEADERS.map(h => h.title).join(', ')}`);
  } else {
    console.log('[WARN] No Meteora records to write');
  }
  await writeCSV(METEORA_LATEST_CSV_PATH, records, METEORA_LATEST_HEADERS, false);
  console.log(`[INFO] Completed writing to ${METEORA_LATEST_CSV_PATH}`);
}

export async function writeKrystalLatestCSV(records: any[]): Promise<any> {
  await writeCSV(KRYSTAL_LATEST_CSV_PATH, records, KRYSTAL_LATEST_HEADERS, false);
  console.log(`[INFO] Completed writing to ${KRYSTAL_LATEST_CSV_PATH}`);
}