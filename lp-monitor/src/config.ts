// src/config.ts
import dotenv from 'dotenv';
import path from 'path';

// Load .env file from one directory level up (parent of lp-monitor)
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

export const config = {
  SOLANA_WALLET_ADDRESSES: process.env.SOLANA_WALLET_ADDRESSES
    ? process.env.SOLANA_WALLET_ADDRESSES.split(',').map(addr => addr.trim())
    : [],
  EVM_WALLET_ADDRESSES: process.env.EVM_WALLET_ADDRESSES
    ? process.env.EVM_WALLET_ADDRESSES.split(',').map(addr => addr.trim())
    : [],
  RPC_ENDPOINT: process.env.RPC_ENDPOINT || 'https://api.mainnet-beta.solana.com', // Default Solana mainnet
  KRYSTAL_CHAIN_IDS: process.env.KRYSTAL_CHAIN_IDS
  ? process.env.KRYSTAL_CHAIN_IDS.split(',').map(id => id.trim())
  : ['137','56','42161'], // Default to Polygon (chain ID 137), add more as needed
  KRYSTAL_VAULT_WALLET_CHAIN_MAP: process.env.KRYSTAL_VAULT_WALLET_CHAIN_IDS
    ? process.env.KRYSTAL_VAULT_WALLET_CHAIN_IDS.split(';').reduce((map, pair) => {
        const [wallet, chainsStr] = pair.split(':');
        if (wallet && chainsStr) {
          const chains = chainsStr.split(',').map(id => id.trim());
          map[wallet.trim()] = chains;
        }
        return map;
      }, {} as Record<string, string[]>)
    : {},
};