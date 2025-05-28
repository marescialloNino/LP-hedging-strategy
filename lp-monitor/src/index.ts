import { config } from './config';
import { retrieveMeteoraPositions } from './services/meteoraPositionService';
import { retrieveKrystalPositions } from './services/krystalPositionService';
import { generateMeteoraCSV, generateKrystalCSV, writeMeteoraLatestCSV, writeKrystalLatestCSV } from './services/csvService';
import { logger } from './utils/logger';
import path from 'path';
import fs from 'fs';
import fsPromises from 'fs/promises';


// Get data directory from environment or use default with absolute path
const logDir = process.env.LP_HEDGE_LOG_DIR || path.join(process.cwd(), '../logs');
const ERROR_FLAGS_PATH = path.join(logDir, 'lp_fetching_errors.json');

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

// Read existing error flags to preserve timestamps and messages
async function readErrorFlags(): Promise<ErrorFlags> {
  try {
    if (fs.existsSync(ERROR_FLAGS_PATH)) {
      const data = await fsPromises.readFile(ERROR_FLAGS_PATH, 'utf-8');
      const parsed = JSON.parse(data);
      // Migrate from old LP_positions_last_updated if present
      const lastUpdate = parsed.LP_positions_last_updated || '';
      return {
        LP_FETCHING_KRYSTAL_ERROR: parsed.LP_FETCHING_KRYSTAL_ERROR || false,
        LP_FETCHING_METEORA_ERROR: parsed.LP_FETCHING_METEORA_ERROR || false,
        LP_FETCHING_VAULT_ERROR: parsed.LP_FETCHING_KRYSTAL_ERROR || false,
        last_meteora_lp_update: parsed.last_meteora_lp_update || lastUpdate,
        last_krystal_lp_update: parsed.last_krystal_lp_update || lastUpdate,
        last_vault_lp_update: parsed.last_krystal_lp_update || lastUpdate,
        krystal_error_message: parsed.krystal_error_message || '',
        meteora_error_message: parsed.meteora_error_message || '',
        vault_error_message: parsed.meteora_error_message || '',
      };
    }
  } catch (error) {
    logger.error(`Error reading error flags: ${String(error)}`);
  }
  // Return default flags if file doesn't exist or read fails
  return {
    LP_FETCHING_KRYSTAL_ERROR: false,
    LP_FETCHING_METEORA_ERROR: false,
    LP_FETCHING_VAULT_ERROR: false,
    last_meteora_lp_update: '',
    last_krystal_lp_update: '',
    last_vault_lp_update: '',
    krystal_error_message: '',
    meteora_error_message: '',
    vault_error_message: '',
  };
}

// Write error flags
async function updateErrorFlags(flags: ErrorFlags) {
  try {
    await fsPromises.writeFile(ERROR_FLAGS_PATH, JSON.stringify(flags, null, 2));
    logger.debug(`Updated error flags: ${JSON.stringify(flags)}`);
  } catch (error) {
    logger.error(`Error writing error flags: ${String(error)}`);
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
  const chainIds = config.KRYSTAL_CHAIN_IDS.join(',');
  const krystalPositions = await retrieveKrystalPositions(walletAddress, chainIds);
  if (krystalPositions.length > 0) {
    const records = await generateKrystalCSV(walletAddress, krystalPositions);
    return records;
  } else {
    logger.info(`No Krystal positions found for ${walletAddress} on chains ${chainIds}`);
    return [];
  }
}

async function processEvmVaultWallet(walletAddress: string, chainIds: string[]): Promise<any[]> {
  logger.info(`Processing EVM vault wallet: ${walletAddress} on chain: ${chainIds.join(',')}`);
  const chainIdsStr = chainIds.join(',');
  const krystalPositions = await retrieveKrystalPositions(walletAddress, chainIdsStr);
  if (krystalPositions.length > 0) {
    const records = await generateKrystalCSV(walletAddress, krystalPositions);
    return records;
  } else {
    logger.info(`No Krystal positions found for vault wallet ${walletAddress} on chains ${chainIdsStr}`);
    return [];
  }
}

async function main() {
  logger.info('Starting lp-monitor batch process...');

  // Load existing error flags to preserve timestamps and messages
  let errorFlags: ErrorFlags = await readErrorFlags();

  // Accumulate records for latest CSVs
  let allMeteoraRecords: any[] = [];
  let allKrystalRecords: any[] = [];
  let meteoraSuccess = true;
  let krystalSuccess = true;
  let vaultSuccess = true;
  let meteoraErrorMessage = '';
  let krystalErrorMessage = '';
  let vaultErrorMessage = '';

  // Process Solana wallets (Meteora positions)
  for (const solWallet of config.SOLANA_WALLET_ADDRESSES) {
    try {
      const meteoraRecords = await processSolanaWallet(solWallet);
      allMeteoraRecords = allMeteoraRecords.concat(meteoraRecords);
    } catch (error) {
      const errorMsg = `Failed to process Solana wallet ${solWallet}: ${String(error)}`;
      logger.error(errorMsg);
      meteoraSuccess = false;
      meteoraErrorMessage += (meteoraErrorMessage ? '; ' : '') + errorMsg;
    }
  }

  // Write Meteora latest positions
  if (allMeteoraRecords.length > 0) {
    await writeMeteoraLatestCSV(allMeteoraRecords);
    logger.info(`Wrote ${allMeteoraRecords.length} Meteora positions to latest CSV`);
  } else {
    logger.info('No Meteora positions found across all wallets');
  }

  // Update Meteora error flag and timestamp
  errorFlags.LP_FETCHING_METEORA_ERROR = !meteoraSuccess;
  errorFlags.meteora_error_message = meteoraSuccess ? '' : meteoraErrorMessage;
  if (meteoraSuccess && allMeteoraRecords.length > 0) {
    errorFlags.last_meteora_lp_update = new Date().toISOString();
  }

  // Process normal EVM wallets (Krystal positions)
  for (const evmWallet of config.EVM_WALLET_ADDRESSES) {
    // Skip if wallet is a vault wallet to avoid duplicate processing
    if (Object.keys(config.KRYSTAL_VAULT_WALLET_CHAIN_MAP).includes(evmWallet)) {
      logger.warn(`Skipping wallet ${evmWallet} in normal processing as it is a vault wallet`);
      continue;
    }
    try {
      const krystalRecords = await processEvmWallet(evmWallet);
      allKrystalRecords = allKrystalRecords.concat(krystalRecords);
    } catch (error) {
      const errorMsg = `Failed to process EVM wallet ${evmWallet}: ${String(error)}`;
      logger.error(errorMsg);
      krystalSuccess = false;
      krystalErrorMessage += (krystalErrorMessage ? '; ' : '') + errorMsg;
    }
  }

  // Process vault EVM wallets (Krystal positions)
  for (const [vaultWallet, chainIds] of Object.entries(config.KRYSTAL_VAULT_WALLET_CHAIN_MAP)) {
    try {
      const krystalRecords = await processEvmVaultWallet(vaultWallet, chainIds);
      allKrystalRecords = allKrystalRecords.concat(krystalRecords);
    } catch (error) {
      const errorMsg = `Failed to process vault wallet ${vaultWallet}: ${String(error)}`;
      logger.error(errorMsg);
      vaultSuccess = false;
      vaultErrorMessage += (vaultErrorMessage ? '; ' : '') + errorMsg;
    }
  }

  // Write Krystal latest positions
  if (allKrystalRecords.length > 0) {
    await writeKrystalLatestCSV(allKrystalRecords);
    logger.info(`Wrote ${allKrystalRecords.length} Krystal positions to latest CSV`);
  } else {
    logger.info('No Krystal positions found across all wallets');
  }

  // Update Krystal error flag and timestamp
  errorFlags.LP_FETCHING_KRYSTAL_ERROR = !krystalSuccess;
  errorFlags.krystal_error_message = krystalSuccess ? '' : krystalErrorMessage;
  if (krystalSuccess && allKrystalRecords.length > 0) {
    errorFlags.last_krystal_lp_update = new Date().toISOString();
  }

  // Update vault error flag and timestamp
  errorFlags.LP_FETCHING_VAULT_ERROR = !vaultSuccess;
  errorFlags.vault_error_message = vaultSuccess ? '' : vaultErrorMessage;
  if (vaultSuccess && allKrystalRecords.length > 0) {
    errorFlags.last_vault_lp_update = new Date().toISOString();
  }

  // Write updated error flags
  await updateErrorFlags(errorFlags);

  logger.info('Batch process completed.');
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    logger.error('Error in batch process:', { error: error.message, stack: error.stack });
    process.exit(1);
  });