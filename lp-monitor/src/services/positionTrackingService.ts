// src/services/positionTrackingService.ts
import fs from 'fs/promises';
import path from 'path';
import { createObjectCsvWriter } from 'csv-writer';

// Paths relative to project root
const PROJECT_ROOT = path.resolve(__dirname, '../../..');
const OPEN_POSITIONS_CSV_PATH = path.join(PROJECT_ROOT, 'lp-data', 'open_positions.csv');
const CLOSED_POSITIONS_CSV_PATH = path.join(PROJECT_ROOT, 'lp-data', 'closed_positions.csv');

interface OpenPosition {
  positionKey: string;
  poolAddress: string;
  entryTime: string;
  tokenXAmount: string;
  tokenYAmount: string;
  initialValueUsd: number;
  tokenXSymbol: string;
  tokenYSymbol: string;
  chain: string;
  protocol: string;
}

interface ClosedPosition {
  positionKey: string;
  poolAddress: string;
  entryTime: string;
  exitTime: string;
  entryValueUsd: number;
  exitValueUsd: number;
  tokenXSymbol: string;
  tokenYSymbol: string;
  chain: string;
  protocol: string;
}

function formatUnixTimestamp(unixTimestamp: number): string {
  return new Date(unixTimestamp * 1000).toISOString().replace('T', ' ').slice(0, 19);
}

async function initializeCsvFiles(): Promise<void> {
  const openHeaders = [
    { id: 'chain', title: 'Chain' },
    { id: 'protocol', title: 'Protocol' },
    { id: 'tokenXSymbol', title: 'Token X Symbol' },
    { id: 'tokenYSymbol', title: 'Token Y Symbol' },
    { id: 'positionKey', title: 'Position Key' },
    { id: 'poolAddress', title: 'Pool Address' },
    { id: 'entryTime', title: 'Entry Time' },
    { id: 'tokenXAmount', title: 'Token X Amount' },
    { id: 'tokenYAmount', title: 'Token Y Amount' },
    { id: 'initialValueUsd', title: 'Initial Value USD' },
  
  ];
  const closedHeaders = [
    { id: 'chain', title: 'Chain' },
    { id: 'protocol', title: 'Protocol' },
    { id: 'tokenXSymbol', title: 'Token X Symbol' },
    { id: 'tokenYSymbol', title: 'Token Y Symbol' },
    { id: 'positionKey', title: 'Position Key' },
    { id: 'poolAddress', title: 'Pool Address' },
    { id: 'entryTime', title: 'Entry Time' },
    { id: 'exitTime', title: 'Exit Time' },
    { id: 'entryValueUsd', title: 'Entry Value USD' },
    { id: 'exitValueUsd', title: 'Exit Value USD' },

  ];

  const fileConfigs: [string, { id: string; title: string }[]][] = [
    [OPEN_POSITIONS_CSV_PATH, openHeaders],
    [CLOSED_POSITIONS_CSV_PATH, closedHeaders],
  ];

  for (const [filePath, headers] of fileConfigs) {
    try {
      await fs.access(filePath);
    } catch {
      const csvWriter = createObjectCsvWriter({ path: filePath, header: headers, append: false });
      await csvWriter.writeRecords([]);
    }
  }
}

async function loadOpenPositions(): Promise<Map<string, OpenPosition>> {
  const positions = new Map<string, OpenPosition>();
  try {
    const data = await fs.readFile(OPEN_POSITIONS_CSV_PATH, 'utf8');
    const lines = data.trim().split('\n').slice(1); // Skip header
    for (const line of lines) {
      if (!line.trim()) continue; // Skip empty lines
      const [ chain, protocol, tokenXSymbol, tokenYSymbol, positionKey, poolAddress, entryTime, tokenXAmount, tokenYAmount, initialValueUsd] = line.split(',').map(val => val.trim());
      if (!positionKey || isNaN(parseFloat(initialValueUsd))) {
        console.warn(`Skipping invalid open position entry: ${line}`);
        continue;
      }
      positions.set(positionKey, {
        chain: chain || 'unknown',
        protocol: protocol || 'unknown',
        tokenXSymbol: tokenXSymbol || 'unknown',
        tokenYSymbol: tokenYSymbol || 'unknown',
        positionKey,
        poolAddress: poolAddress || '',
        entryTime: entryTime || '',
        tokenXAmount: tokenXAmount || '0',
        tokenYAmount: tokenYAmount || '0',
        initialValueUsd: parseFloat(initialValueUsd),

      });
    }
  } catch (error) {
    await initializeCsvFiles();
  }
  return positions;
}

async function loadClosedPositions(): Promise<Map<string, ClosedPosition>> {
  const positions = new Map<string, ClosedPosition>();
  try {
    const data = await fs.readFile(CLOSED_POSITIONS_CSV_PATH, 'utf8');
    const lines = data.trim().split('\n').slice(1);
    for (const line of lines) {
      if (!line.trim()) continue; // Skip empty lines
      const [chain, protocol, tokenXSymbol, tokenYSymbol, positionKey, poolAddress, entryTime, exitTime, entryValueUsd, exitValueUsd ] = line.split(',').map(val => val.trim());
      if (!positionKey || isNaN(parseFloat(entryValueUsd)) || isNaN(parseFloat(exitValueUsd))) {
        console.warn(`Skipping invalid closed position entry: ${line}`);
        continue;
      }
      positions.set(positionKey, {
        chain: chain || 'unknown',
        protocol: protocol || 'unknown',
        tokenXSymbol: tokenXSymbol || 'unknown',
        tokenYSymbol: tokenYSymbol || 'unknown',
        positionKey,
        poolAddress: poolAddress || '',
        entryTime: entryTime || '',
        exitTime: exitTime || '',
        entryValueUsd: parseFloat(entryValueUsd),
        exitValueUsd: parseFloat(exitValueUsd),

      });
    }
  } catch (error) {
    await initializeCsvFiles();
  }
  return positions;
}

async function saveOpenPosition(position: OpenPosition): Promise<void> {
  const csvWriter = createObjectCsvWriter({
    path: OPEN_POSITIONS_CSV_PATH,
    header: [
      { id: 'chain', title: 'Chain' },
      { id: 'protocol', title: 'Protocol' },
      { id: 'tokenXSymbol', title: 'Token X Symbol' },
      { id: 'tokenYSymbol', title: 'Token Y Symbol' },
      { id: 'positionKey', title: 'Position Key' },
      { id: 'poolAddress', title: 'Pool Address' },
      { id: 'entryTime', title: 'Entry Time' },
      { id: 'tokenXAmount', title: 'Token X Amount' },
      { id: 'tokenYAmount', title: 'Token Y Amount' },
      { id: 'initialValueUsd', title: 'Initial Value USD' },

    ],
    append: true,
  });
  await csvWriter.writeRecords([position]);
}

async function closePosition(positionKey: string, exitTime: string, exitValueUsd: number): Promise<void> {
  const openPositions = await loadOpenPositions();
  const closedPositions = await loadClosedPositions();
  const position = openPositions.get(positionKey);
  if (!position) return;

  openPositions.delete(positionKey);
  const closedPosition: ClosedPosition = {
    chain: position.chain,
    protocol: position.protocol,
    tokenXSymbol: position.tokenXSymbol,
    tokenYSymbol: position.tokenYSymbol,
    positionKey,
    poolAddress: position.poolAddress,
    entryTime: position.entryTime,
    exitTime,
    entryValueUsd: position.initialValueUsd,
    exitValueUsd,


  };
  closedPositions.set(positionKey, closedPosition);

  const openCsvWriter = createObjectCsvWriter({
    path: OPEN_POSITIONS_CSV_PATH,
    header: [
      { id: 'chain', title: 'Chain' },
      { id: 'protocol', title: 'Protocol' },
      { id: 'tokenXSymbol', title: 'Token X Symbol' },
      { id: 'tokenYSymbol', title: 'Token Y Symbol' },
      { id: 'positionKey', title: 'Position Key' },
      { id: 'poolAddress', title: 'Pool Address' },
      { id: 'entryTime', title: 'Entry Time' },
      { id: 'tokenXAmount', title: 'Token X Amount' },
      { id: 'tokenYAmount', title: 'Token Y Amount' },
      { id: 'initialValueUsd', title: 'Initial Value USD' }

    ],
    append: false,
  });
  await openCsvWriter.writeRecords(Array.from(openPositions.values()));

  const closedCsvWriter = createObjectCsvWriter({
    path: CLOSED_POSITIONS_CSV_PATH,
    header: [
      { id: 'chain', title: 'Chain' },
      { id: 'protocol', title: 'Protocol' },
      { id: 'tokenXSymbol', title: 'Token X Symbol' },
      { id: 'tokenYSymbol', title: 'Token Y Symbol' },
      { id: 'positionKey', title: 'Position Key' },
      { id: 'poolAddress', title: 'Pool Address' },
      { id: 'entryTime', title: 'Entry Time' },
      { id: 'exitTime', title: 'Exit Time' },
      { id: 'entryValueUsd', title: 'Entry Value USD' },
      { id: 'exitValueUsd', title: 'Exit Value USD' },


    ],
    append: true,
  });
  await closedCsvWriter.writeRecords([closedPosition]);
}

export { loadOpenPositions, loadClosedPositions, saveOpenPosition, closePosition, OpenPosition, ClosedPosition, formatUnixTimestamp };