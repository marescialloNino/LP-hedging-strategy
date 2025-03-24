// src/config.ts
import dotenv from 'dotenv';

dotenv.config();

export const config = {
  SOLANA_WALLET_ADDRESSES: process.env.SOLANA_WALLET_ADDRESSES
    ? process.env.SOLANA_WALLET_ADDRESSES.split(',').map(addr => addr.trim())
    : [],
  EVM_WALLET_ADDRESSES: process.env.EVM_WALLET_ADDRESSES
    ? process.env.EVM_WALLET_ADDRESSES.split(',').map(addr => addr.trim())
    : [],
  SCHEDULE_INTERVAL: parseInt(process.env.SCHEDULE_INTERVAL || '30', 10),
  RPC_ENDPOINT: process.env.RPC_ENDPOINT || 'https://api.mainnet-beta.solana.com', // Default Solana mainnet
  KRYSTAL_CHAIN_IDS: process.env.KRYSTAL_CHAIN_IDS
  ? process.env.KRYSTAL_CHAIN_IDS.split(',').map(id => id.trim())
  : ['137','56','42161'], // Default to Polygon (chain ID 137), add more as needed
};