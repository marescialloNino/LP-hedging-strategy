// fetchStrategies.js

import { clusterApiUrl, Connection } from '@solana/web3.js';
import {
  createDefaultRpcTransport,
  createRpc,
  createSolanaRpcApi,
  address 
} from '@solana/kit';
import { Kamino } from '@kamino-finance/kliquidity-sdk';

async function main() {
  // 1) Define the RPC endpoint. Using the same public mainnet-beta endpoint here.
  const RPC_URL = "https://rpc-proxy.segfaultx0.workers.dev";

  // 2) Create a "legacy" Connection for any SPL-token or on-chain calls Kamino makes.
  const legacyConnection = new Connection(RPC_URL, 'confirmed');

  // 3) Wrap that endpoint in an Rpc<SolanaRpcApi> so Kamino's internal calls to
  //    getProgramAccounts(...).send() will work.
  const rpc = createRpc({
    api: createSolanaRpcApi({ defaultCommitment: 'confirmed' }),
    transport: createDefaultRpcTransport({ url: RPC_URL }),
  });

  // 4) Instantiate Kamino with (cluster, rpc, legacyConnection).
  const kamino = new Kamino('mainnet-beta', rpc, legacyConnection);

  const walletPubkey = address("4Qmf2vs3CS93xQgyfNbaPv5KqY7rqWJ1njyJpHHKSY1P") 

   // ─── STEP A: Fetch your “basic” positions first ────────────────────────────────────
  // This is equivalent to what you already did:
  //    const positions = await kamino.getUserPositions(walletPubkey);
  //
  // Each KaminoPosition has: { shareMint, strategy, sharesAmount, strategyDex }
  //
  let basicPositions;
  try {
    basicPositions = await kamino.getUserPositions(walletPubkey);
  } catch (err) {
    console.error('Error fetching basic user positions:', err);
    process.exit(1);
  }

  if (basicPositions.length === 0) {
    console.log('No Kamino positions found for this wallet.');
    return;
  }

  console.log('=== Basic Positions (already fetched) ===');
  for (const pos of basicPositions) {
    console.log(`• shareMint:   ${pos.shareMint.toString()}`);
    console.log(`  strategy:    ${pos.strategy.toString()}`);
    console.log(`  sharesHeld:  ${pos.sharesAmount.toString()}  (DEX: ${pos.strategyDex})`);
    console.log('────────────────────────────────────────');
  }

  // ─── STEP B: Extract the unique strategy addresses ─────────────────────────────────
  const uniqueStrategyAddrs = [walletPubkey];

  // ─── STEP C: Fetch only those strategies’ full states ─────────────────────────────
  //
  // Instead of doing getAllStrategiesWithFilters, we call getStrategies([..]) on just these.
  // getStrategies([...]) returns an array of WhirlpoolStrategy | null, in the same order.
  //
  let fetchedStrategyStates;
  try {
    fetchedStrategyStates = await kamino.getStrategies(uniqueStrategyAddrs);
  } catch (err) {
    console.error('Error fetching strategy states:', err);
    process.exit(1);
  }

  // ─── STEP D: Build both maps from the fetched strategies ──────────────────────────
  //
  // 1) strategiesWithShareMintsMap: Map<shareMint (PublicKey), KaminoStrategyWithShareMint>
  //      - We need: { address, type, shareMint, status, tokenAMint, tokenBMint } for each strategy
  //
  // 2) strategiesWithAddressMap: Map<strategyAddress (PublicKey), WhirlpoolStrategy>
  //
  const strategiesWithShareMintsMap = new Map();
  const strategiesWithAddressMap = new Map();

  for (let i = 0; i < uniqueStrategyAddrs.length; i++) {
    const stratAddr = uniqueStrategyAddrs[i];
    const stratState = fetchedStrategyStates[i];

    if (!stratState) {
      // If a strategy address failed to decode, skip it
      console.warn(`⚠️  Strategy at ${stratAddr.toString()} not found or invalid. Skipping.`);
      continue;
    }

    // Build the KaminoStrategyWithShareMint shape:
    //   { address, type, shareMint, status, tokenAMint, tokenBMint }
    const ksws = {
      address: stratAddr,
      type:      stratState.strategyType.toString(),        // e.g. "NON_PEGGED", "STABLE"
      shareMint: stratState.sharesMint,
      status:    stratState.creationStatus.toString(),       // e.g. "LIVE", "IGNORED"
      tokenAMint: stratState.tokenAMint,
      tokenBMint: stratState.tokenBMint,
    };
    // Key by the shareMint’s string form:
    strategiesWithShareMintsMap.set(stratState.sharesMint.toString(), ksws);

    // Populate the address→WhirlpoolStrategy map:
    strategiesWithAddressMap.set(stratAddr.toString(), stratState);
  }

  // ─── STEP E: Convert string‐keyed maps back to PublicKey‐keyed maps ───────────────
  //
  // getUserPositionsByStrategiesMap expects:
  //   strategiesWithShareMintsMap: Map<Address (PublicKey), KaminoStrategyWithShareMint>
  //   strategiesWithAddressMap:    Map<Address (PublicKey), WhirlpoolStrategy>
  //
  // But we built them with string keys. Let’s re‐map to PublicKey:
  //
  const shareMintMapPK = new Map();
  for (const [mintString, ksws] of strategiesWithShareMintsMap.entries()) {
    shareMintMapPK.set(new PublicKey(mintString), ksws);
  }

  const addressMapPK = new Map();
  for (const [addrString, whirlState] of strategiesWithAddressMap.entries()) {
    addressMapPK.set(new PublicKey(addrString), whirlState);
  }

  // ─── STEP F: Call getUserPositionsByStrategiesMap using our two maps ──────────────
  //
  let positionsByMap;
  try {
    positionsByMap = await kamino.getUserPositionsByStrategiesMap(
      walletPubkey,
      shareMintMapPK,
      addressMapPK
    );
  } catch (err) {
    console.error('Error in getUserPositionsByStrategiesMap:', err);
    process.exit(1);
  }

  // ─── STEP G: Print out the results ────────────────────────────────────────────────
  console.log('\n=== Positions via getUserPositionsByStrategiesMap ===');
  for (const pos of positionsByMap) {
    console.log(`• shareMint:   ${pos.shareMint.toString()}`);
    console.log(`  strategy:    ${pos.strategy.toString()}`);
    console.log(`  sharesHeld:  ${pos.sharesAmount.toString()}  (DEX: ${pos.strategyDex})`);
    console.log('────────────────────────────────────────');
  }
}

main();
