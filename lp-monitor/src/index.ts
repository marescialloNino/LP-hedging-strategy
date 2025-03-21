// src/index.ts
import { config } from './config';
import { retrieveMeteoraPositions } from './services/meteoraPositionService';
import { retrieveKrystalPositions } from './services/krystalPositionService';
import { generateMeteoraCSV, generateAndWriteLiquidityProfileCSV, generateKrystalCSV, writeMeteoraLatestCSV, writeKrystalLatestCSV } from './services/csvService';
import { PositionInfo } from './services/types';

async function processSolanaWallet(walletAddress: string): Promise<any[]> {
  console.log(`Processing Solana wallet: ${walletAddress}`);
  const meteoraPositions = await retrieveMeteoraPositions(walletAddress);
  if (meteoraPositions.length > 0) {
    const records = await generateMeteoraCSV(walletAddress, meteoraPositions);
    return records;
  } else {
    console.log(`No Meteora positions found for ${walletAddress}`);
    return [];
  }
}

async function processEvmWallet(walletAddress: string): Promise<any[]> {
  console.log(`Processing EVM wallet: ${walletAddress}`);
  const krystalPositions = await retrieveKrystalPositions(walletAddress);
  if (krystalPositions.length > 0) {
    const records = await generateKrystalCSV(walletAddress, krystalPositions);
    return records;
  } else {
    console.log(`No Krystal positions found for ${walletAddress}`);
    return [];
  }
}

async function main() {
  console.log('Starting lp-monitor batch process...');

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
    console.log(`Wrote ${allMeteoraRecords.length} Meteora positions to latest CSV`);
  } else {
    console.log('No Meteora positions found across all wallets');
  }

  // Process EVM wallets
  for (const evmWallet of config.EVM_WALLET_ADDRESSES) {
    const krystalRecords = await processEvmWallet(evmWallet);
    allKrystalRecords = allKrystalRecords.concat(krystalRecords);
  }

  // Write all Krystal latest positions
  if (allKrystalRecords.length > 0) {
    await writeKrystalLatestCSV(allKrystalRecords);
    console.log(`Wrote ${allKrystalRecords.length} Krystal positions to latest CSV`);
  } else {
    console.log('No Krystal positions found across all wallets');
  }

  // Write combined liquidity profile for all Meteora positions
  if (allMeteoraRecords.length > 0) {
    const allMeteoraPositions = await retrieveMeteoraPositions(config.SOLANA_WALLET_ADDRESSES.join(',')); // Adjust if needed
    await generateAndWriteLiquidityProfileCSV('all_wallets', allMeteoraPositions);
  } else {
    console.log('No Meteora positions found across all wallets for liquidity profile');
  }

  console.log('Batch process completed.');
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('Error in batch process:', error);
    process.exit(1);
  });