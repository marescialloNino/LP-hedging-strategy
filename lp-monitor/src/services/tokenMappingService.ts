// src/services/tokenMappingService.ts
import fs from 'fs/promises';
import path from 'path';
import { createObjectCsvWriter } from 'csv-writer';
import axios from 'axios';

const TOKEN_MAPPINGS_CSV_PATH = path.join(__dirname, '../../../lp-data/token_mappings.csv');

// Utility function to delay execution
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Simple rate limiter to enforce delay between calls
let lastCallTimestamp = 0;
const MIN_DELAY_MS = 500; 

async function rateLimit(): Promise<void> {
  const now = Date.now();
  const timeSinceLastCall = now - lastCallTimestamp;
  if (timeSinceLastCall < MIN_DELAY_MS) {
    await sleep(MIN_DELAY_MS - timeSinceLastCall);
  }
  lastCallTimestamp = Date.now();
}

interface TokenMapping {
  address: string;
  symbol: string;
  coingeckoId: string;
}

// Load existing token mappings from CSV
async function loadTokenMappings(): Promise<Map<string, { symbol: string; coingeckoId: string }>> {
  const mappings = new Map<string, { symbol: string; coingeckoId: string }>();
  try {
    const data = await fs.readFile(TOKEN_MAPPINGS_CSV_PATH, 'utf8');
    const lines = data.trim().split('\n').slice(1); // Skip header
    for (const line of lines) {
      const [address, symbol, coingeckoId] = line.split(',').map(val => val.trim());
      mappings.set(address, { symbol, coingeckoId });
    }
  } catch (error) {
    // File doesn’t exist yet or is empty; initialize it
    await initializeTokenMappingsCSV();
  }
  return mappings;
}

// Initialize CSV if it doesn’t exist
async function initializeTokenMappingsCSV(): Promise<void> {
  const csvWriter = createObjectCsvWriter({
    path: TOKEN_MAPPINGS_CSV_PATH,
    header: [
      { id: 'address', title: 'Token Address' },
      { id: 'symbol', title: 'Symbol' },
      { id: 'coingeckoId', title: 'CoinGecko ID' },
    ],
    append: false,
  });
  await csvWriter.writeRecords([]);
}

// Retry wrapper for API calls
async function withRetry<T>(fn: () => Promise<T>, retries = 3, baseDelayMs = 2000): Promise<T> {
  for (let i = 0; i < retries; i++) {
    try {
      await rateLimit(); // Enforce rate limit before each attempt
      return await fn();
    } catch (error: any) {
      if (error.response?.status === 429 && i < retries - 1) {
        const waitTime = baseDelayMs * Math.pow(2, i); // Exponential backoff: 2s, 4s, 8s
        console.warn(`Rate limit hit (429), retrying in ${waitTime}ms... (attempt ${i + 1}/${retries})`);
        await sleep(waitTime);
        continue;
      }
      throw error; // Rethrow if not 429 or out of retries
    }
  }
  throw new Error('Max retries reached');
}

// Fetch token info from CoinGecko
async function fetchTokenInfoFromCoinGecko(address: string, chain: string = 'solana'): Promise<{ symbol: string; coingeckoId: string } | null> {
  return await withRetry(async () => {
    const url = `https://api.coingecko.com/api/v3/coins/${chain}/contract/${address}`;
    const response = await axios.get(url);
    return {
      symbol: response.data.symbol.toUpperCase(),
      coingeckoId: response.data.id,
    };
  });
}

// Fetch token prices from CoinGecko
async function fetchTokenPrices(coingeckoIds: string[]): Promise<Map<string, number>> {
  return await withRetry(async () => {
    const url = 'https://api.coingecko.com/api/v3/simple/price';
    const params = {
      ids: coingeckoIds.join(','),
      vs_currencies: 'usd',
    };
    const response = await axios.get(url, { params });
    const prices = new Map<string, number>();
    for (const [id, priceData] of Object.entries(response.data)) {
      prices.set(id, (priceData as any).usd);
    }
    return prices;
  });
}

// Get or fetch token mapping
export async function getTokenMapping(address: string): Promise<{ symbol: string; coingeckoId: string }> {
  const mappings = await loadTokenMappings();
  let mapping = mappings.get(address);

  if (!mapping) {
    const fetchedMapping = await fetchTokenInfoFromCoinGecko(address);
    mapping = fetchedMapping || { symbol: 'Unknown', coingeckoId: '' };
    if (fetchedMapping) {
      mappings.set(address, mapping);
      const csvWriter = createObjectCsvWriter({
        path: TOKEN_MAPPINGS_CSV_PATH,
        header: [
          { id: 'address', title: 'Token Address' },
          { id: 'symbol', title: 'Symbol' },
          { id: 'coingeckoId', title: 'CoinGecko ID' },
        ],
        append: true,
      });
      await csvWriter.writeRecords([{ address, symbol: mapping.symbol, coingeckoId: mapping.coingeckoId }]);
    }
  }

  return mapping;
}

// Fetch prices for multiple tokens
export async function getTokenPrices(coingeckoIds: string[]): Promise<Map<string, number>> {
  const uniqueIds = [...new Set(coingeckoIds.filter(id => id))]; // Remove empty IDs
  if (uniqueIds.length === 0) return new Map();
  return await fetchTokenPrices(uniqueIds);
}