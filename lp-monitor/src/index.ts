// src/index.ts
import { config } from './config';
import { retrieveMeteoraPositions } from './services/meteoraPositionService';
import { retrieveKrystalPositions } from './services/krystalPositionService';
import { generateMeteoraCSV, generateAndWriteLiquidityProfileCSV, generateKrystalCSV, writeMeteoraLatestCSV, writeKrystalLatestCSV } from './services/csvService';
import { processPnlForPositions, savePnlResultsCsv } from './services/pnlResultsService';
import { logger } from './utils/logger'; // Import logger from utils/logger
import path from 'path';
import fs from 'fs';

// Get data directory from environment or use default with absolute path
const dataDir = process.env.LP_HEDGE_DATA_DIR || path.join(process.cwd(), '../lp-data');

// Create data directory if it doesn't exist (using sync functions for startup)
try {
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
} catch (error) {
  console.error('Error creating data directory:', error);
}

// Add uncaught exception and unhandled rejection handlers
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception:', { error: error.message, stack: error.stack });
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection:', { reason, promise });
  process.exit(1);
});

async function processSolanaWallet(walletAddress: string): Promise<any[]> {
  logger.info(`Processing Solana wallet: ${walletAddress}`);
  const meteoraPositions = await retrieveMeteoraPositions(walletAddress);
  if (meteoraPositions.length > 0) {
    const records = await generateMeteoraCSV(walletAddress, meteoraPositions);
    return records;
  } else {
    logger.info(`No Meteora positions found for ${walletAddress}`);
    return [];
  }
}

async function processEvmWallet(walletAddress: string): Promise<any[]> {
  logger.info(`Processing EVM wallet: ${walletAddress}`);
  const krystalPositions = await retrieveKrystalPositions(walletAddress);
  if (krystalPositions.length > 0) {
    const records = await generateKrystalCSV(walletAddress, krystalPositions);
    return records;
  } else {
    logger.info(`No Krystal positions found for ${walletAddress}`);
    return [];
  }
}

async function main() {
  logger.info('Starting lp-monitor batch process...');

  // Accumulate records for latest CSVs
  let allMeteoraRecords: any[] = [];
  let allKrystalRecords: any[] = [];

  // Process Solana wallets (Meteora positions)
  for (const solWallet of config.SOLANA_WALLET_ADDRESSES) {
    const meteoraRecords = await processSolanaWallet(solWallet);
    allMeteoraRecords = allMeteoraRecords.concat(meteoraRecords);
  }

  // Write all Meteora latest positions
  if (allMeteoraRecords.length > 0) {
    await writeMeteoraLatestCSV(allMeteoraRecords);
    logger.info(`Wrote ${allMeteoraRecords.length} Meteora positions to latest CSV`);
  } else {
    logger.info('No Meteora positions found across all wallets');
  }

  // Process EVM wallets (Krystal positions)
  for (const evmWallet of config.EVM_WALLET_ADDRESSES) {
    const krystalRecords = await processEvmWallet(evmWallet);
    allKrystalRecords = allKrystalRecords.concat(krystalRecords);
  }

  // Write all Krystal latest positions
  if (allKrystalRecords.length > 0) {
    await writeKrystalLatestCSV(allKrystalRecords);
    logger.info(`Wrote ${allKrystalRecords.length} Krystal positions to latest CSV`);
  } else {
    logger.info('No Krystal positions found across all wallets');
  }

  // Write combined liquidity profile for all Meteora positions
  if (allMeteoraRecords.length > 0) {
    const allMeteoraPositions = await retrieveMeteoraPositions(config.SOLANA_WALLET_ADDRESSES.join(',')); // Adjust if needed
    await generateAndWriteLiquidityProfileCSV('all_wallets', allMeteoraPositions);
  } else {
    logger.info('No Meteora positions found across all wallets for liquidity profile');
  }

  // --- NEW: Calculate PnL for all Meteora positions and save to CSV ---
  if (allMeteoraRecords.length > 0) {
    // Retrieve full positions (including amounts, prices, symbols, etc.) for PnL calculation.
    const pnlPositions = await retrieveMeteoraPositions(config.SOLANA_WALLET_ADDRESSES.join(','));
    if (pnlPositions.length > 0) {
      const pnlResults = await processPnlForPositions(pnlPositions);
      await savePnlResultsCsv(pnlResults);
      logger.info(`Calculated and saved PnL results for ${pnlResults.length} Meteora positions.`);
    } else {
      logger.info('No Meteora positions available for PnL calculation.');
    }
  } else {
    logger.info('No Meteora records available for PnL calculation.');
  }
  // ---------------------------------------------------------------------

  logger.info('Batch process completed.');
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    logger.error('Error in batch process:', { error: error.message, stack: error.stack });
    process.exit(1);
  });
