import yaml from 'js-yaml';
import fs from 'fs';
import path from 'path';

// Define interface for vault wallet chain map values
interface VaultWalletConfig {
  chains: string[];
  vaultShare: number;
}

// Define interface for the entire config object
interface Config {
  SOLANA_WALLET_ADDRESSES: string[];
  EVM_WALLET_ADDRESSES: string[];
  RPC_ENDPOINT: string;
  KRYSTAL_CHAIN_IDS: string[];
  KRYSTAL_VAULT_WALLET_CHAIN_MAP: Record<string, VaultWalletConfig>;
}

// Load YAML file from one directory level up (parent of lp-monitor)
const yamlPath = path.resolve(__dirname, '../lpMonitorConfig.yaml');
let yamlData: any;
try {
  yamlData = yaml.load(fs.readFileSync(yamlPath, 'utf8')) || {};
} catch (error) {
  console.error(`Failed to load YAML config file at ${yamlPath}:`, error);
  yamlData = {};
}

export const config: Config = {
  SOLANA_WALLET_ADDRESSES: yamlData.solana_wallet_addresses
    ? Array.isArray(yamlData.solana_wallet_addresses)
      ? yamlData.solana_wallet_addresses.map((addr: string) => addr.trim())
      : []
    : [],
  EVM_WALLET_ADDRESSES: yamlData.evm_wallet_addresses
    ? Array.isArray(yamlData.evm_wallet_addresses)
      ? yamlData.evm_wallet_addresses.map((addr: string) => addr.trim())
      : []
    : [],
  RPC_ENDPOINT: yamlData.rpc_endpoint || 'https://api.mainnet-beta.solana.com', // Default Solana mainnet
  KRYSTAL_CHAIN_IDS: yamlData.krystal_chain_ids
    ? Array.isArray(yamlData.krystal_chain_ids)
      ? yamlData.krystal_chain_ids.map((id: string) => id.trim())
      : ['137', '56', '42161'] // Default to Polygon (chain ID 137), BSC (56), Arbitrum (42161)
    : ['137', '56', '42161'],
  KRYSTAL_VAULT_WALLET_CHAIN_MAP: yamlData.krystal_vault_wallet_chain_ids
    ? Array.isArray(yamlData.krystal_vault_wallet_chain_ids)
      ? yamlData.krystal_vault_wallet_chain_ids.reduce((map: Record<string, VaultWalletConfig>, entry: any) => {
          if (!entry || !entry.wallet || !entry.chains || entry.vault_share === undefined) {
            console.warn(`Malformed vault entry in YAML: ${JSON.stringify(entry)}. Expected: { wallet: string, chains: string[], vault_share: number }`);
            return map;
          }
          const wallet = entry.wallet.trim();
          const chains = Array.isArray(entry.chains) ? entry.chains.map((id: string) => id.trim()) : [];
          const vaultShare = parseFloat(entry.vault_share);
          if (!isNaN(vaultShare) && vaultShare >= 0 && vaultShare <= 1) {
            map[wallet] = { chains, vaultShare };
          } else {
            console.warn(`Invalid vault_share "${entry.vault_share}" for wallet ${wallet}. Must be a number between 0 and 1.`);
          }
          return map;
        }, {})
      : {}
    : {}
};