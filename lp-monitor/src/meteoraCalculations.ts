// src/meteoraCalculations.ts
import { config } from './config';
import { retrieveMeteoraPositions } from './services/meteoraPositionService';
import { generateLiquidityProfileCSV } from './services/csvService';
import { processPnlForPositions, savePnlResultsCsv } from './services/pnlResultsService';
import { logger } from './utils/logger';

async function calculateMeteoraMetrics() {
  logger.info('Starting Meteora metrics calculation process...');

  // Accumulate all Meteora positions
  let allMeteoraPositions: any[] = [];

  // Fetch positions for each wallet individually
  for (const solWallet of config.SOLANA_WALLET_ADDRESSES) {
    try {
      const positions = await retrieveMeteoraPositions(solWallet);
      allMeteoraPositions = allMeteoraPositions.concat(positions);
    } catch (error) {
      logger.error(`Error fetching Meteora positions for wallet ${solWallet}: ${error}`);
    }
  }

  // Write combined liquidity profile for all Meteora positions
  if (allMeteoraPositions.length > 0) {
    try {
      await generateLiquidityProfileCSV('all_wallets', allMeteoraPositions);
      logger.info('Generated liquidity profile CSV for all Meteora positions.');
    } catch (error) {
      logger.error(`Error generating liquidity profile: ${error}`);
    }
  } else {
    logger.info('No Meteora positions found across all wallets for liquidity profile');
  }

  // Calculate PnL for all Meteora positions and save to CSV
  if (allMeteoraPositions.length > 0) {
    try {
      if (allMeteoraPositions.length > 0) {
        const pnlResults = await processPnlForPositions(allMeteoraPositions);
        await savePnlResultsCsv(pnlResults);
        logger.info(`Calculated and saved PnL results for ${pnlResults.length} Meteora positions.`);
      } else {
        logger.info('No Meteora positions available for PnL calculation.');
      }
    } catch (error) {
      logger.error(`Error calculating PnL: ${error}`);
    }
  } else {
    logger.info('No Meteora positions available for PnL calculation.');
  }

  logger.info('Meteora metrics calculation completed.');
}

calculateMeteoraMetrics()
  .then(() => process.exit(0))
  .catch((error) => {
    logger.error('Error in Meteora metrics calculation:', { error: error.message, stack: error.stack });
    process.exit(1);
  });