// src/index.ts
import { config } from './config';
import { retrieveMeteoraPositions } from './services/meteoraPositionService';
import { retrieveKrystalPositions } from './services/krystalPositionService';
import { generateMeteoraCSV, generateKrystalCSV, writeMeteoraLatestCSV, writeKrystalLatestCSV } from './services/csvService';
import { logger } from './utils/logger';
import path from 'path';
import fs from 'fs';
import fsPromises from 'fs/promises';

// Get data directory from environment or use default with absolute path
const dataDir = process.env.LP_HEDGE_LOG_DIR || path.join(process.cwd(), '../logs');
const ERROR_FLAGS_PATH = path.join(dataDir, 'lp_fetching_errors.json');

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

// Initialize or update error flags
async function updateErrorFlags(flags: { [key: string]: boolean | string }) {
  try {
    await fsPromises.writeFile(ERROR_FLAGS_PATH, JSON.stringify({ ...flags, LP_positions_last_updated: new Date().toISOString() }, null, 2));
  } catch (error) {
    logger.error(`Error writing error flags: ${ String(error) }`);
  }
}

async function processSolanaWallet(walletAddress: string): Promise<any[]> {
  logger.info(`Processing Solana wallet: ${walletAddress}`);
  try {
    const meteoraPositions = await retrieveMeteoraPositions(walletAddress);
    if (meteoraPositions.length > 0) {
      const records = await generateMeteoraCSV(walletAddress, meteoraPositions);
      return records;
    } else {
      logger.info(`No Meteora positions found for ${walletAddress}`);
      return [];
    }
  } catch (error) {
    throw error; // Rethrow to ensure main() catches it
  }
}

async function processEvmWallet(walletAddress: string): Promise<any[]> {
  logger.info(`Processing EVM wallet: ${walletAddress}`);
  try {
    const krystalPositions = await retrieveKrystalPositions(walletAddress);
    if (krystalPositions.length > 0) {
      const records = await generateKrystalCSV(walletAddress, krystalPositions);
      return records;
    } else {
      logger.info(`No Krystal positions found for ${walletAddress}`);
      return [];
    }
  } catch (error) {
    throw error; // Rethrow to ensure main() catches it
  }
}

async function main() {
  logger.info('Starting lp-monitor batch process...');

  // Initialize error flags at the start of the run
  let errorFlags = {
    LP_FETCHING_KRYSTAL_ERROR: false,
    LP_FETCHING_METEORA_ERROR: false,
    LP_positions_last_updated: new Date().toISOString(),
  };
  await updateErrorFlags(errorFlags);

  // Accumulate records for latest CSVs
  let allMeteoraRecords: any[] = [];
  let allKrystalRecords: any[] = [];

  // Process Solana wallets (Meteora positions)
  for (const solWallet of config.SOLANA_WALLET_ADDRESSES) {
    try {
      const meteoraRecords = await processSolanaWallet(solWallet);
      allMeteoraRecords = allMeteoraRecords.concat(meteoraRecords);
    } catch (error) {
      logger.error(`Failed to process Solana wallet ${solWallet}: ${ String(error) }`);
      errorFlags.LP_FETCHING_METEORA_ERROR = true;
      await updateErrorFlags(errorFlags);
    }
  }

  console.log(allMeteoraRecords);

  // Write all Meteora latest positions
  if (allMeteoraRecords.length > 0) {
    await writeMeteoraLatestCSV(allMeteoraRecords);
    logger.info(`Wrote ${allMeteoraRecords.length} Meteora positions to latest CSV`);
  } else {
    logger.info('No Meteora positions found across all wallets');
  }

  // Process EVM wallets (Krystal positions)
  for (const evmWallet of config.EVM_WALLET_ADDRESSES) {
    try {
      const krystalRecords = await processEvmWallet(evmWallet);
      allKrystalRecords = allKrystalRecords.concat(krystalRecords);
    } catch (error) {
      logger.error(`Failed to process EVM wallet ${evmWallet}:  ${ String(error) }` );
      errorFlags.LP_FETCHING_KRYSTAL_ERROR = true;
      await updateErrorFlags(errorFlags);
    }
  }

  // Write all Krystal latest positions
  if (allKrystalRecords.length > 0) {
    await writeKrystalLatestCSV(allKrystalRecords);
    logger.info(`Wrote ${allKrystalRecords.length} Krystal positions to latest CSV`);
  } else {
    logger.info('No Krystal positions found across all wallets');
  }

  logger.info('Batch process completed.');
}

main()
  .then(() => process.exit(0))
  .catch(async (error) => {
    logger.error('Error in batch process:', { error: error.message, stack: error.stack });
    await updateErrorFlags({
      LP_FETCHING_KRYSTAL_ERROR: true,
      LP_FETCHING_METEORA_ERROR: true,
      LP_positions_last_updated: new Date().toISOString(),
    });
    process.exit(1);
  });