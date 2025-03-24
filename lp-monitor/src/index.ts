// src/index.ts
import { config } from './config';
import { retrieveMeteoraPositions } from './services/meteoraPositionService';
import { retrieveKrystalPositions } from './services/krystalPositionService';
import { generateMeteoraCSV, generateAndWriteLiquidityProfileCSV, generateKrystalCSV, writeMeteoraLatestCSV, writeKrystalLatestCSV } from './services/csvService';
import { PositionInfo } from './services/types';
import winston from 'winston';
import { TransformableInfo } from 'logform'; // Import TransformableInfo from logform
import path from 'path';
import fs from 'fs';

// Get log directory from environment or use default with absolute path
const logDir = process.env.LP_HEDGE_LOG_DIR || path.join(process.cwd(), 'logs');
const dataDir = process.env.LP_HEDGE_DATA_DIR || path.join(process.cwd(), 'lp-data');

// Create directories if they don't exist (using sync functions for startup)
try {
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
} catch (error) {
  console.error('Error creating directories:', error);
}

// Configure Winston logger with absolute paths
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss,SSS'
    }),
    winston.format.printf((info: TransformableInfo) => {
      const message = typeof info.message === 'string' ? info.message : String(info.message);
      return `${info.timestamp} - main - ${info.level.toUpperCase()} - ${message}`;
    })
  ),
  transports: [
    new winston.transports.File({ 
      filename: path.join(logDir, 'lp-monitor.log'),
      maxsize: 5242880, // 5MB
      maxFiles: 5,
    }),
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.timestamp({
          format: 'YYYY-MM-DD HH:mm:ss,SSS'
        }),
        winston.format.printf((info: TransformableInfo) => {
          const message = typeof info.message === 'string' ? info.message : String(info.message);
          return `${info.timestamp} - main - ${info.level.toUpperCase()} - ${message}`;
        })
      )
    })
  ]
});

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

  // Process Solana wallets
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

  // Process EVM wallets
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

  logger.info('Batch process completed.');
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    logger.error('Error in batch process:', { error: error.message, stack: error.stack });
    process.exit(1);
  });