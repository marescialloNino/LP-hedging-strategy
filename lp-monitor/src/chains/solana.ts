import { Connection } from '@solana/web3.js';
import { config } from '../config';

export function getSolanaConnection(): Connection {
  return new Connection(config.RPC_ENDPOINT, 'confirmed');
}