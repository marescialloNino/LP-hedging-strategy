// src/dexes/meteoraDlmmAdapter.ts
import { PublicKey } from '@solana/web3.js';
import DLMM from '@meteora-ag/dlmm';
import { getSolanaConnection } from '../chains/solana';
import { PositionInfo, LiquidityProfileEntry } from '../services/types';
import BN from 'bn.js';
import fs from 'fs/promises';
import util from 'util';
import axios from 'axios';
import { createObjectCsvWriter } from 'csv-writer';
import { getTokenMapping, getTokenPrices } from '../services/tokenMappingService';
import { loadOpenPositions, loadClosedPositions, saveOpenPosition, closePosition, formatUnixTimestamp } from '../services/positionTrackingService';
import path from 'path';

// Path relative to project root
const PROJECT_ROOT = path.resolve(__dirname, '../../..');
const METEORA_POSITIONS_CSV_PATH = path.join(PROJECT_ROOT, 'lp-data', 'LP_meteora_positions_latest.csv');

async function withRetry<T>(fn: () => Promise<T>, retries = 3): Promise<T> {
  let lastError: Error | undefined = undefined;
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;
      console.warn(`Retry ${i + 1}/${retries} failed: ${error}`);
      await new Promise(resolve => setTimeout(resolve, 1000 * (i + 1)));
    }
  }
  if (lastError) throw lastError;
  throw new Error('No result after retries and no error captured');
}

async function logToFile(filePath: string, message: string): Promise<void> {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] ${message}\n`;
  try {
    await fs.appendFile(filePath, logEntry, 'utf8');
  } catch (error) {
    console.error(`Failed to write to log file ${filePath}:`, error);
  }
}

interface Deposit {
  tx_id: string;
  position_address: string;
  pair_address: string;
  active_bin_id: number;
  token_x_amount: number;
  token_y_amount: number;
  price: number;
  token_x_usd_amount: number;
  token_y_usd_amount: number;
  onchain_timestamp: number;
}

async function fetchDeposits(positionKey: string): Promise<Deposit[]> {
  try {
    const response = await axios.get(`https://dlmm-api.meteora.ag/position/${positionKey}/deposits`);
    return response.data as Deposit[];
  } catch (error) {
    console.error(`Failed to fetch deposits for position ${positionKey}:`, error);
    return [];
  }
}

async function saveMeteoraPositionsToCsv(positions: PositionInfo[]): Promise<void> {
  const csvWriter = createObjectCsvWriter({
    path: METEORA_POSITIONS_CSV_PATH,
    header: [
      { id: 'timestamp', title: 'Timestamp' },
      { id: 'owner', title: 'Wallet Address' },
      { id: 'id', title: 'Position Key' },
      { id: 'pool', title: 'Pool Address' },
      { id: 'tokenXSymbol', title: 'Token X Symbol' },
      { id: 'tokenX', title: 'Token X Address' },
      { id: 'amountX', title: 'Token X Qty' },
      { id: 'tokenYSymbol', title: 'Token Y Symbol' },
      { id: 'tokenY', title: 'Token Y Address' },
      { id: 'amountY', title: 'Token Y Qty' },
      { id: 'lowerBinId', title: 'Lower Boundary' },
      { id: 'upperBinId', title: 'Upper Boundary' },
      { id: 'isInRange', title: 'Is In Range' },
      { id: 'unclaimedFeeX', title: 'Unclaimed Fee X' },
      { id: 'unclaimedFeeY', title: 'Unclaimed Fee Y' },
      { id: 'tokenXPriceUsd', title: 'Token X Price USD' },
      { id: 'tokenYPriceUsd', title: 'Token Y Price USD' },
      { id: 'tokenXPriceUsd', title: 'Token X Price USD' },
      { id: 'tokenYPriceUsd', title: 'Token Y Price USD' },
    ],
    append: false,
  });

  const positionsWithTimestamp = positions.map(pos => ({
    ...pos,
    timestamp: formatUnixTimestamp(Math.floor(Date.now() / 1000)),
  }));

  await csvWriter.writeRecords(positionsWithTimestamp);
}

async function updatePositionTracking(positions: PositionInfo[]): Promise<void> {
  const openPositions = await loadOpenPositions();
  const closedPositions = await loadClosedPositions();

  // Check for new positions
  for (const pos of positions) {
    if (!openPositions.has(pos.id) && !closedPositions.has(pos.id)) {
      const deposits = await fetchDeposits(pos.id);
      if (deposits.length > 0) {
        const firstDeposit = deposits.sort((a, b) => a.onchain_timestamp - b.onchain_timestamp)[0];
        const initialValueUsd = firstDeposit.token_x_usd_amount + firstDeposit.token_y_usd_amount;

        await saveOpenPosition({
          positionKey: pos.id,
          poolAddress: pos.pool,
          entryTime: formatUnixTimestamp(firstDeposit.onchain_timestamp),
          tokenXAmount: (firstDeposit.token_x_amount / Math.pow(10, pos.tokenXDecimals)).toString(),
          tokenYAmount: (firstDeposit.token_y_amount / Math.pow(10, pos.tokenYDecimals)).toString(),
          initialValueUsd,
          tokenXSymbol: pos.tokenXSymbol ?? 'unknown',
          tokenYSymbol: pos.tokenYSymbol ?? 'unknown',
          chain: 'Solana',
          protocol: 'Meteora',
        });
        console.log(`Added new position ${pos.id} to open_positions.csv`);
      }
    }
  }

  // Check for closed positions (only if not present in current positions)
  for (const [positionKey, openPos] of openPositions) {
    if (!positionKey || isNaN(openPos.initialValueUsd)) {
      console.warn(`Skipping invalid open position with key: ${positionKey}`);
      openPositions.delete(positionKey);
      continue;
    }

    const currentPos = positions.find(p => p.id === positionKey);
    if (!currentPos) { // Only close if the position is no longer present
      const exitTime = formatUnixTimestamp(Math.floor(Date.now() / 1000));
      const exitValueUsd = 0; // Placeholder until closing value logic is determined

      await closePosition(positionKey, exitTime, exitValueUsd);
      console.log(`Moved position ${positionKey} to closed_positions.csv with exit value ${exitValueUsd}`);
    }
  }
  // Removed redundant rewrite here
}

export async function fetchMeteoraPositions(walletAddress: string): Promise<PositionInfo[]> {
  const connection = getSolanaConnection();
  const user = new PublicKey(walletAddress);
  const logFilePath = './positionData.log';

  try {
    const positionsData: Map<string, any> = await withRetry(async () => {
      return await DLMM.getAllLbPairPositionsByUser(connection, user);
    });

    await logToFile(logFilePath, 'Raw positions data:\n' + util.inspect(positionsData, { depth: null }));

    if (!positionsData || positionsData.size === 0) {
      await logToFile(logFilePath, 'No positions returned from DLMM.');
      console.log('No positions returned from DLMM.');
      return [];
    }

    const tokenMappings = new Map<string, { symbol: string; coingeckoId: string; decimals: number }>();
    const positionInfos: PositionInfo[] = [];

    for (const [positionKey, pos] of positionsData) {
      console.log(`Processing pool ${positionKey}`);

      for (const [subIndex, positionDataEntry] of pos.lbPairPositionsData.entries()) {
        try {
          const positionData = positionDataEntry.positionData || {};
          const positionPubKey = Array.isArray(positionDataEntry.publicKey)
            ? positionDataEntry.publicKey[0]?.toString()
            : positionDataEntry.publicKey?.toString() || `${positionKey}-${subIndex}`;

          await logToFile(
            logFilePath,
            `Expanded positionData for ${positionPubKey}:\n` +
            util.inspect(positionData, { depth: null })
          );

          const lowerBin = positionData.positionBinData?.find((bin: any) => bin.binId === positionData.lowerBinId);
          const upperBin = positionData.positionBinData?.find((bin: any) => bin.binId === positionData.upperBinId);

          const tokenXAddress = pos.tokenX?.publicKey?.toString() || pos.tokenX?.mint?.toString() || 'unknown';
          const tokenYAddress = pos.tokenY?.publicKey?.toString() || pos.tokenY?.mint?.toString() || 'unknown';

          async function isValidPublicKey(address: string): Promise<boolean> {
            try {
              if (address === 'unknown') {
                await logToFile(logFilePath, `Invalid address: ${address} (unknown)`);
                return false;
              }
              const publicKey = new PublicKey(address);
              const isOnCurve = PublicKey.isOnCurve(publicKey);
              if (!isOnCurve) {
                await logToFile(logFilePath, `Invalid address: ${address} (not on curve)`);
              }
              return isOnCurve;
            } catch (error) {
              const errorMessage = error instanceof Error ? error.message : String(error);
              await logToFile(logFilePath, `Error checking public key ${address}: ${errorMessage}`);
              return false;
            }
          }

          if (!(await isValidPublicKey(tokenXAddress)) || !(await isValidPublicKey(tokenYAddress))) {
            await logToFile(logFilePath, `Skipping position ${positionKey} due to invalid token mints: tokenX=${tokenXAddress}, tokenY=${tokenYAddress}`);
            continue;
          }

          let tokenXMapping = tokenMappings.get(tokenXAddress);
          let tokenYMapping = tokenMappings.get(tokenYAddress);
          if (!tokenXMapping) {
            tokenXMapping = await getTokenMapping(tokenXAddress);
            tokenMappings.set(tokenXAddress, tokenXMapping);
          }
          if (!tokenYMapping) {
            tokenYMapping = await getTokenMapping(tokenYAddress);
            tokenMappings.set(tokenYAddress, tokenYMapping);
          }

          const tokenXDecimals = tokenXMapping.decimals || 0;
          const tokenYDecimals = tokenYMapping.decimals || 0;

          if (tokenXDecimals === 0 || tokenYDecimals === 0) {
            await logToFile(logFilePath, `Warning: Missing decimals for tokens: tokenX=${tokenXAddress} (${tokenXDecimals}), tokenY=${tokenYAddress} (${tokenYDecimals})`);
          }

          const rawFeeX = positionData.feeX;
          const rawFeeY = positionData.feeY;
          const feeX = rawFeeX instanceof BN ? rawFeeX.toNumber() : 0;
          const feeY = rawFeeY instanceof BN ? rawFeeY.toNumber() : 0;
          const scaledFeeX = tokenXDecimals > 0 ? feeX / Math.pow(10, tokenXDecimals) : feeX;
          const scaledFeeY = tokenYDecimals > 0 ? feeY / Math.pow(10, tokenYDecimals) : feeY;

          const rawAmountX = positionData.totalXAmount instanceof BN ? positionData.totalXAmount.toNumber() : parseFloat(positionData.totalXAmount) || 0;
          const rawAmountY = positionData.totalYAmount instanceof BN ? positionData.totalYAmount.toNumber() : parseFloat(positionData.totalYAmount) || 0;
          const scaledAmountX = tokenXDecimals > 0 ? rawAmountX / Math.pow(10, tokenXDecimals) : rawAmountX;
          const scaledAmountY = tokenYDecimals > 0 ? rawAmountY / Math.pow(10, tokenYDecimals) : rawAmountY;

          const liquidityProfile: LiquidityProfileEntry[] = positionData.positionBinData?.map((bin: any) => {
            const binLiq = parseFloat(bin.binLiquidity) || 0;
            const posLiq = parseFloat(bin.positionLiquidity) || 0;
            const share = binLiq > 0 ? (posLiq / binLiq * 100).toFixed(2) + '%' : '0%';
            return {
              binId: bin.binId,
              price: bin.price || '0',
              positionLiquidity: bin.positionLiquidity || '0',
              positionXAmount: bin.positionXAmount
                ? (parseFloat(bin.positionXAmount) / Math.pow(10, tokenXDecimals)).toString()
                : '0',
              positionYAmount: bin.positionYAmount
                ? (parseFloat(bin.positionYAmount) / Math.pow(10, tokenYDecimals)).toString()
                : '0',
              liquidityShare: share,
            };
          }) || [];

          const position: PositionInfo = {
            id: positionPubKey,
            owner: walletAddress,
            pool: positionKey,
            tokenX: tokenXAddress,
            tokenY: tokenYAddress,
            tokenXSymbol: tokenXMapping.symbol,
            tokenYSymbol: tokenYMapping.symbol,
            tokenXDecimals,
            tokenYDecimals,
            amountX: scaledAmountX.toString(),
            amountY: scaledAmountY.toString(),
            lowerBinId: lowerBin?.price ? parseFloat(lowerBin.price) : 0,
            upperBinId: upperBin?.price ? parseFloat(upperBin.price) : 0,
            activeBinId: pos.lbPair?.activeId ?? 0,
            isInRange: pos.lbPair?.activeId && positionData.lowerBinId && positionData.upperBinId
              ? pos.lbPair.activeId >= positionData.lowerBinId && pos.lbPair.activeId <= positionData.upperBinId
              : false,
            unclaimedFeeX: scaledFeeX.toString(),
            unclaimedFeeY: scaledFeeY.toString(),
            tokenXPriceUsd: 0,
            tokenYPriceUsd: 0,
            liquidityProfile,
          };

          positionInfos.push(position);
          await logToFile(logFilePath, `Mapped position ${positionPubKey}:\n` + util.inspect(position, { depth: null }));
        } catch (error) {
          await logToFile(logFilePath, `Error mapping position ${positionDataEntry.publicKey || subIndex} in ${positionKey}: ${error}`);
          console.error(`Error mapping position ${positionDataEntry.publicKey || subIndex} in ${positionKey}:`, error);
        }
      }
    }

    const coingeckoIds = Array.from(tokenMappings.values()).map(m => m.coingeckoId);
    const priceMap = await getTokenPrices(coingeckoIds);
    for (const pos of positionInfos) {
      const tokenXMapping = tokenMappings.get(pos.tokenX)!;
      const tokenYMapping = tokenMappings.get(pos.tokenY)!;
      pos.tokenXPriceUsd = priceMap.get(tokenXMapping.coingeckoId) || 0;
      pos.tokenYPriceUsd = priceMap.get(tokenYMapping.coingeckoId) || 0;
    }

    await updatePositionTracking(positionInfos);

    await logToFile(logFilePath, 'All processed positions with prices:\n' + util.inspect(positionInfos, { depth: null }));
    console.log(`Processed ${positionInfos.length} Meteora positions logged to ${logFilePath}`);
    return positionInfos;
  } catch (error) {
    await logToFile(logFilePath, `Error fetching Meteora positions: ${error}`);
    console.error('Error fetching Meteora positions:', error);
    return [];
  }
}