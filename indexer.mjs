// indexer.mjs — Compound v3 unclaimed COMP rewards batch indexer
// Usage: node indexer.mjs [chainKey ...] [--max-blocks N]
// Zero deps beyond js-sha3 (already installed in same dir)
//
// MAINNET TENDERLY PROBE RESULTS (2026-06-12):
//   mainnet.gateway.tenderly.co works for up to ~200k block ranges reliably (500k times out).
//   Recommended mainnet log strategy: logRpcs=['https://mainnet.gateway.tenderly.co'], logChunk=150000,
//   concurrency 4. USDC comet ~5.5M blocks → ~37 chunks → ~10 parallel batches.
//   getLogsAdaptive auto-splits on "too many results" errors — handles result-count caps.

import { createRequire } from 'module';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const require = createRequire(import.meta.url);
const { keccak_256 } = require('js-sha3');
const keccak = (s) => '0x' + keccak_256(s);

const __dir = dirname(fileURLToPath(import.meta.url));
const CACHE_DIR = join(__dir, 'cache');
if (!existsSync(CACHE_DIR)) mkdirSync(CACHE_DIR, { recursive: true });

// ─── constants ────────────────────────────────────────────────────────────────
const MULTICALL3 = '0xcA11bde05977b3631167028862bE2a173976CA11';
const SEL_AGGREGATE3 = '0x82ad56cb';
const SEL_BALANCE_OF = '0x70a08231';
const SEL_REWARD_CONFIG = '0x2289b6b8';
const SEL_GET_REWARD_OWED = '0x41e0cad6';    // getRewardOwed(address,address)
const SEL_REWARDS_CLAIMED = '0x65e12392';    // rewardsClaimed(address,address)
const SEL_BASE_TRACKING_ACCRUED = '0xab9ba7f4'; // baseTrackingAccrued(address)

// Event topics
const TOPIC_SUPPLY   = keccak('Supply(address,address,uint256)');
const TOPIC_WITHDRAW = keccak('Withdraw(address,address,uint256)');
const TOPIC_TRANSFER = keccak('Transfer(address,address,uint256)');
const TOPIC_ABSORB   = keccak('AbsorbDebt(address,address,uint256,uint256)');

// Sanity check Transfer matches canonical
const TRANSFER_CANONICAL = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
if (TOPIC_TRANSFER !== TRANSFER_CANONICAL) throw new Error('Transfer topic mismatch: ' + TOPIC_TRANSFER);

// ─── chain config ─────────────────────────────────────────────────────────────
const CHAINS = [
  { key: 'mainnet', logChunk: 150000, name: 'Ethereum', chainId: 1, blockTime: 12,
    logRpcs: ['https://mainnet.gateway.tenderly.co'],
    rpcs: ['https://ethereum-rpc.publicnode.com', 'https://eth.drpc.org', 'https://eth.llamarpc.com'],
    rewards: '0x1B0e765F6224C21223AeA2af16c1C46E38885a40',
    comets: [
      { name: 'USDC',   addr: '0xc3d688B66703497DAA19211EEdff47f25384cdc3' },
      { name: 'WETH',   addr: '0xA17581A9E3356d9A858b789D68B4d866e593aE94' },
      { name: 'USDT',   addr: '0x3Afdc9BCA9213A35503b077a6072F3D0d5AB0840' },
      { name: 'wstETH', addr: '0x3D0bb1ccaB520A66e607822fC55BC921738fAFE3' },
      { name: 'USDS',   addr: '0x5D409e56D886231aDAf00c8775665AD0f9897b56' },
      { name: 'WBTC',   addr: '0xe85Dc543813B8c2CFEaAc371517b925a166a9293' },
    ]},
  { key: 'arbitrum', logRpcs: ['https://arb1.arbitrum.io/rpc'], logChunk: 2600000, name: 'Arbitrum', chainId: 42161, blockTime: 0.25,
    rpcs: ['https://arbitrum-one-rpc.publicnode.com', 'https://arb1.arbitrum.io/rpc', 'https://arbitrum.drpc.org'],
    rewards: '0x88730d254A2f7e6AC8388c3198aFd694bA9f7fae',
    comets: [
      { name: 'USDC',   addr: '0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf' },
      { name: 'USDC.e', addr: '0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA' },
      { name: 'WETH',   addr: '0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486' },
      { name: 'USDT',   addr: '0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07' },
    ]},
  { key: 'base', logRpcs: ['https://base.gateway.tenderly.co'], logChunk: 350000, name: 'Base', chainId: 8453, blockTime: 2,
    rpcs: ['https://base-rpc.publicnode.com', 'https://mainnet.base.org', 'https://base.drpc.org'],
    rewards: '0x123964802e6ABabBE1Bc9547D72Ef1B69B00A6b1',
    comets: [
      { name: 'USDC',  addr: '0xb125E6687d4313864e53df431d5425969c15Eb2F' },
      { name: 'WETH',  addr: '0x46e6b214b524310239732D51387075E0e70970bf' },
      { name: 'USDbC', addr: '0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf' },
      { name: 'AERO',  addr: '0x784efeB622244d2348d4F2522f8860B96fbEcE89' },
      { name: 'USDS',  addr: '0x2c776041CCFe903071AF44aa147368a9c8EEA518' },
    ]},
  { key: 'polygon', logRpcs: ['https://polygon.gateway.tenderly.co'], logChunk: 350000, name: 'Polygon', chainId: 137, blockTime: 2.1,
    rpcs: ['https://polygon-bor-rpc.publicnode.com', 'https://polygon-rpc.com', 'https://polygon.drpc.org'],
    rewards: '0x45939657d1CA34A8FA39A924B71D28Fe8431e581',
    comets: [
      { name: 'USDC.e', addr: '0xF25212E676D1F7F89Cd72fFEe66158f541246445' },
      { name: 'USDT',   addr: '0xaeB318360f27748Acb200CE616E389A6C9409a07' },
    ]},
  { key: 'optimism', logRpcs: ['https://optimism.gateway.tenderly.co'], logChunk: 350000, name: 'Optimism', chainId: 10, blockTime: 2,
    rpcs: ['https://optimism-rpc.publicnode.com', 'https://mainnet.optimism.io', 'https://optimism.drpc.org'],
    rewards: '0x443EA0340cb75a160F31A440722dec7b5bc3C2E9',
    comets: [
      { name: 'USDC', addr: '0x2e44e174f7D53F0212823acC11C01A11d58c5bCB' },
      { name: 'USDT', addr: '0x995E394b8B2437aC8Ce61Ee0bC610D617962B214' },
      { name: 'WETH', addr: '0xE36A30D249f7761327fd973001A32010b521b6Fd' },
    ]},
  { key: 'unichain', logRpcs: ['https://unichain.gateway.tenderly.co'], logChunk: 350000, name: 'Unichain', chainId: 130, blockTime: 1,
    rpcs: ['https://unichain-rpc.publicnode.com', 'https://mainnet.unichain.org', 'https://unichain.drpc.org'],
    rewards: '0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486',
    comets: [
      { name: 'USDC', addr: '0x2c7118c4C88B9841FCF839074c26Ae8f035f2921' },
      { name: 'WETH', addr: '0x6C987dDE50dB1dcDd32Cd4175778C2a291978E2a' },
    ]},
  { key: 'scroll', logChunk: 9900, name: 'Scroll', chainId: 534352, blockTime: 3,
    rpcs: ['https://scroll-rpc.publicnode.com', 'https://rpc.scroll.io', 'https://scroll.drpc.org'],
    rewards: '0x70167D30964cbFDc315ECAe02441Af747bE0c5Ee',
    comets: [
      { name: 'USDC', addr: '0xB2f97c1Bd3bf02f5e74d13f02E3e26F93D77CE44' },
    ]},
  { key: 'mantle', logRpcs: ['https://mantle.gateway.tenderly.co'], logChunk: 350000, name: 'Mantle', chainId: 5000, blockTime: 2,
    rpcs: ['https://mantle-rpc.publicnode.com', 'https://rpc.mantle.xyz', 'https://mantle.drpc.org'],
    rewards: '0xCd83CbBFCE149d141A5171C3D6a0F0fCCeE225Ab',
    comets: [
      { name: 'USDe', addr: '0x606174f62cd968d8e684c645080fa694c1D7786E' },
    ]},
  { key: 'linea', logRpcs: ['https://rpc.linea.build'], logChunk: 350000, name: 'Linea', chainId: 59144, blockTime: 2,
    rpcs: ['https://linea-rpc.publicnode.com', 'https://rpc.linea.build', 'https://linea.drpc.org'],
    rewards: '0x2c7118c4C88B9841FCF839074c26Ae8f035f2921',
    comets: [
      { name: 'USDC', addr: '0x8D38A3d6B3c3B7d96D6536DA7Eef94A9d7dbC991' },
      { name: 'WETH', addr: '0x60F2058379716A64a7A5d29219397e79bC552194' },
    ]},
  { key: 'ronin', logRpcs: ['https://api.roninchain.com/rpc', 'https://ronin.drpc.org'], logChunk: 250000, name: 'Ronin', chainId: 2020, blockTime: 3,
    rpcs: ['https://api.roninchain.com/rpc', 'https://ronin.drpc.org'],
    rewards: '0x31CdEe8609Bc15fD33cc525f101B70a81b2B1E59',
    comets: [
      { name: 'WETH', addr: '0x4006eD4097Ee51c09A04c3B0951D28CCf19e6DFE' },
      { name: 'WRON', addr: '0xc0Afdbd1cEB621Ef576BA969ce9D4ceF78Dbc0c0' },
    ]},
];

// ─── ABI / hex helpers ────────────────────────────────────────────────────────
const strip0x = (s) => (s && s.startsWith('0x') ? s.slice(2) : s || '');
const pad32   = (h) => strip0x(h).padStart(64, '0');
const padAddr = (a) => '0'.repeat(24) + strip0x(a).toLowerCase();
const hexToBig = (h) => BigInt(!h || h === '0x' ? 0 : h);
const wordAddr = (hex, i) => '0x' + strip0x(hex).slice(i * 64 + 24, (i + 1) * 64);

function encodeAggregate3(calls) {
  const n = calls.length;
  const tuples = [];
  for (const c of calls) {
    const data = strip0x(c.callData);
    const dataLen = data.length / 2;
    const padded = data.padEnd(Math.ceil(dataLen / 32) * 64, '0');
    tuples.push(padAddr(c.target) + pad32('1') + pad32('60') + pad32(dataLen.toString(16)) + padded);
  }
  let offsets = [], cur = n * 32;
  for (const t of tuples) { offsets.push(pad32(cur.toString(16))); cur += t.length / 2; }
  return SEL_AGGREGATE3 + pad32('20') + pad32(n.toString(16)) + offsets.join('') + tuples.join('');
}

function decodeAggregate3(hex) {
  const h = strip0x(hex);
  const rd = (i) => BigInt('0x' + h.slice(i * 64, (i + 1) * 64));
  const arrOff = Number(rd(0)) / 32;
  const n = Number(rd(arrOff));
  const base = arrOff + 1;
  const out = [];
  for (let i = 0; i < n; i++) {
    const tupOff = base + Number(rd(base + i)) / 32;
    const success = rd(tupOff) === 1n;
    const bytesOff = tupOff + Number(rd(tupOff + 1)) / 32;
    const len = Number(rd(bytesOff));
    const data = '0x' + h.slice((bytesOff + 1) * 64, (bytesOff + 1) * 64 + len * 2);
    out.push({ success, data });
  }
  return out;
}

// ─── RPC ─────────────────────────────────────────────────────────────────────
async function rpcCall(rpcs, method, params, attempt = 0) {
  const rpcList = Array.isArray(rpcs) ? rpcs : [rpcs];
  const url = rpcList[attempt % rpcList.length];
  try {
    const res = await fetch(url, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
      signal: AbortSignal.timeout(20000),
    });
    const j = await res.json();
    if (j.error) throw new Error(j.error.message || JSON.stringify(j.error));
    return j.result;
  } catch (e) {
    const maxAttempts = rpcList.length * 2;
    if (attempt + 1 < maxAttempts) return rpcCall(rpcs, method, params, attempt + 1);
    throw e;
  }
}

const ethCall = (rpcs, to, data) => rpcCall(rpcs, 'eth_call', [{ to, data }, 'latest']);

async function multicall(rpcs, calls) {
  const raw = await ethCall(rpcs, MULTICALL3, encodeAggregate3(calls));
  return decodeAggregate3(raw);
}

// ─── adaptive log fetching ────────────────────────────────────────────────────
function isSplittableError(msg) {
  const m = (msg || '').toLowerCase();
  return m.includes('too many') || m.includes('limit') || m.includes('exceed') ||
         m.includes('range') || m.includes('response') || m.includes('maximum') ||
         m.includes('block range') || m.includes('query') || m.includes('result');
}

async function getLogsAdaptive(rpcs, filter, fromBlock, toBlock, depth = 0) {
  try {
    return await rpcCall(rpcs, 'eth_getLogs', [{
      ...filter,
      fromBlock: '0x' + fromBlock.toString(16),
      toBlock: '0x' + toBlock.toString(16),
    }]);
  } catch (e) {
    if (depth >= 6 || toBlock - fromBlock < 500) throw e;
    if (!isSplittableError(e.message)) throw e;
    const mid = Math.floor((fromBlock + toBlock) / 2);
    const [a, b] = await Promise.all([
      getLogsAdaptive(rpcs, filter, fromBlock, mid, depth + 1),
      getLogsAdaptive(rpcs, filter, mid + 1, toBlock, depth + 1),
    ]);
    return a.concat(b);
  }
}

// getLogsChunked with optional mid-scan checkpoint callback
// onBatch(lastBlock, newLogs) called after each concurrency batch — return false to abort
async function getLogsChunked(chain, filter, fromBlock, toBlock, label, onBatch) {
  const rpcs = chain.logRpcs || chain.rpcs;
  const chunk = chain.logChunk || 9900;
  const ranges = [];
  for (let f = fromBlock; f <= toBlock; f += chunk)
    ranges.push([f, Math.min(f + chunk - 1, toBlock)]);
  const out = [];
  const CONC = 4;
  let done = 0;
  for (let i = 0; i < ranges.length; i += CONC) {
    const slice = ranges.slice(i, i + CONC);
    const batch = await Promise.all(slice.map(([f, t]) => getLogsAdaptive(rpcs, filter, f, t)));
    const batchLogs = [];
    for (const b of batch) { out.push(...b); batchLogs.push(...b); }
    done += slice.length;
    const lastBlock = slice[slice.length - 1][1];
    if (label && (done % 8 === 0 || done === ranges.length))
      process.stderr.write(`  ${label}: ${done}/${ranges.length} chunks, ${out.length} logs\r`);
    if (onBatch) { const cont = await onBatch(lastBlock, batchLogs); if (cont === false) break; }
  }
  if (label) process.stderr.write('\n');
  return out;
}

// ─── deployment block binary search ──────────────────────────────────────────
async function findDeployBlock(rpcs, addr, latestBlock, maxBlocks) {
  let lo = maxBlocks ? Math.max(1, latestBlock - maxBlocks) : 1;
  let hi = latestBlock;
  // Quick probe: find a rough upper bound where code exists
  for (const frac of [0.1, 0.3, 0.5, 0.7, 0.9]) {
    const blk = Math.floor(lo + (hi - lo) * frac);
    const code = await rpcCall(rpcs, 'eth_getCode', [addr, '0x' + blk.toString(16)]);
    if (code && code.length > 4) { hi = blk; break; }
    else lo = blk;
  }
  // Binary search
  let result = hi;
  while (hi - lo > 200) {
    const mid = Math.floor((lo + hi) / 2);
    const code = await rpcCall(rpcs, 'eth_getCode', [addr, '0x' + mid.toString(16)]);
    if (code && code.length > 4) { result = mid; hi = mid - 1; }
    else lo = mid + 1;
  }
  return result;
}

// ─── cache helpers ────────────────────────────────────────────────────────────
function cacheRead(path) {
  try { return JSON.parse(readFileSync(path, 'utf8')); } catch { return null; }
}
function cacheWrite(path, data) {
  writeFileSync(path, JSON.stringify(data, null, 2));
}

// ─── address scan (with incremental checkpointing every CONC*4 batches) ────────
function extractAddrs(logs, addrSet, lastBlockMap) {
  for (const log of logs) {
    const blk = log.blockNumber ? parseInt(log.blockNumber, 16) : 0;
    for (const idx of [1, 2]) {
      const t = log.topics[idx];
      if (t && t !== '0x' + '0'.repeat(64)) {
        const a = '0x' + t.slice(-40).toLowerCase();
        if (a !== '0x' + '0'.repeat(40)) {
          addrSet.add(a);
          if (lastBlockMap && blk > (lastBlockMap[a] || 0)) lastBlockMap[a] = blk;
        }
      }
    }
  }
}

async function scanComet(chain, comet, latestBlock, maxBlocks) {
  const cacheFile = join(CACHE_DIR, chain.key + '-' + comet.name.replace(/\./g, '-') + '.json');
  let cache = cacheRead(cacheFile) || { scannedTo: 0, addresses: [], deployBlock: 0 };
  const addrSet = new Set(cache.addresses);
  // lastBlock is optional; absent in old caches — continue without it
  const lastBlockMap = cache.lastBlock ? { ...cache.lastBlock } : {};

  const rpcs = chain.rpcs;
  let deployBlock = cache.deployBlock;
  if (!deployBlock) {
    process.stderr.write('[' + chain.key + '/' + comet.name + '] finding deploy block...\n');
    deployBlock = await findDeployBlock(rpcs, comet.addr, latestBlock, maxBlocks);
    process.stderr.write('[' + chain.key + '/' + comet.name + '] deploy block: ' + deployBlock + '\n');
    cache.deployBlock = deployBlock;
    cacheWrite(cacheFile, cache);
  }

  const scanFrom = Math.max(deployBlock, cache.scannedTo + 1);
  const scanTo = maxBlocks ? Math.min(latestBlock, deployBlock + maxBlocks - 1) : latestBlock;

  if (scanFrom > scanTo) {
    process.stderr.write('[' + chain.key + '/' + comet.name + '] up to date (scannedTo=' + cache.scannedTo + ', ' + addrSet.size + ' addrs)\n');
    return { addresses: addrSet, scannedTo: cache.scannedTo, deployBlock, lastBlockMap };
  }

  // If the new range is smaller than one chunk and we already have addresses, skip the tiny delta
  // to keep the owed-cache key stable during multi-slice owed queries
  if (addrSet.size > 0 && (scanTo - scanFrom) < (chain.logChunk || 9900)) {
    process.stderr.write('[' + chain.key + '/' + comet.name + '] tiny delta (' + (scanTo-scanFrom) + ' blocks), skipping to preserve owed key\n');
    return { addresses: addrSet, scannedTo: cache.scannedTo, deployBlock, lastBlockMap };
  }

  process.stderr.write('[' + chain.key + '/' + comet.name + '] scanning ' + scanFrom + '->' + scanTo + ' (' + (scanTo - scanFrom) + ' blocks)\n');

  const topics = [TOPIC_SUPPLY, TOPIC_WITHDRAW, TOPIC_TRANSFER, TOPIC_ABSORB];
  const label = chain.key + '/' + comet.name;
  // Checkpoint every 16 batches of CONC=4 → every 64 chunks
  const CKPT_EVERY = 16;
  let batchCount = 0;
  let totalLogs = 0;

  const onBatch = async (lastBlock, newLogs) => {
    extractAddrs(newLogs, addrSet, lastBlockMap);
    totalLogs += newLogs.length;
    batchCount++;
    if (batchCount % CKPT_EVERY === 0) {
      cache.scannedTo = lastBlock;
      cache.addresses = [...addrSet];
      cache.lastBlock = { ...lastBlockMap };
      cacheWrite(cacheFile, cache);
    }
  };

  try {
    await getLogsChunked(chain, { address: comet.addr, topics: [topics] }, scanFrom, scanTo, label, onBatch);
  } catch (e) {
    process.stderr.write('[' + label + '] OR-topics failed (' + e.message.slice(0, 60) + '), fetching per-event\n');
    batchCount = 0; totalLogs = 0;
    for (const topic of topics) {
      await getLogsChunked(chain, { address: comet.addr, topics: [topic] }, scanFrom, scanTo,
        label + '/' + topic.slice(0, 10), onBatch);
    }
  }

  process.stderr.write('[' + label + '] ' + totalLogs + ' logs -> ' + addrSet.size + ' unique addresses\n');

  cache.scannedTo = scanTo;
  cache.deployBlock = deployBlock;
  cache.addresses = [...addrSet];
  cache.lastBlock = { ...lastBlockMap };
  cacheWrite(cacheFile, cache);

  return { addresses: addrSet, scannedTo: scanTo, deployBlock, lastBlockMap };
}

// ─── getRewardOwed batch ──────────────────────────────────────────────────────
// batchGetRewardOwed: startIdx resumes mid-query; onSave(i, userOwed, totalOwed) called every CKPT batches
async function batchGetRewardOwed(chain, comet, decimals, addresses, startIdx, onSave) {
  if (addresses.length === 0) return { totalOwed: 0n, userOwed: {} };

  const rpcs = chain.rpcs;
  const addrs = [...addresses];
  let batchSize = 150;
  const userOwed = {};
  let totalOwed = 0n;
  let validated = false;
  let i = startIdx || 0;
  const CKPT_BATCHES = 20; // save every 20*150 = 3000 addresses
  let batchNum = 0;

  process.stderr.write('[' + chain.key + '/' + comet.name + '] querying ' + addrs.length + ' addresses from idx=' + i + ' (batch=' + batchSize + ')\n');

  while (i < addrs.length) {
    const batch = addrs.slice(i, i + batchSize);
    const calls = batch.map(addr => ({
      target: chain.rewards,
      callData: SEL_GET_REWARD_OWED + padAddr(comet.addr) + padAddr(addr),
    }));

    let results;
    try {
      results = await multicall(rpcs, calls);
    } catch (e) {
      if (batchSize > 10) {
        batchSize = Math.max(10, Math.floor(batchSize / 2));
        process.stderr.write('[' + chain.key + '/' + comet.name + '] batch failed, retry batch=' + batchSize + ': ' + e.message.slice(0, 80) + '\n');
        continue;
      }
      throw e;
    }

    // Validate first batch against individual eth_calls
    if (!validated && i === 0) {
      const checkCount = Math.min(3, results.length);
      let mismatches = 0;
      for (let v = 0; v < checkCount; v++) {
        if (!results[v].success) continue;
        try {
          const single = await ethCall(rpcs, chain.rewards,
            SEL_GET_REWARD_OWED + padAddr(comet.addr) + padAddr(batch[v]));
          const mcOwed = hexToBig('0x' + strip0x(results[v].data).slice(64, 128));
          const singleOwed = hexToBig('0x' + strip0x(single).slice(64, 128));
          if (mcOwed !== singleOwed) {
            mismatches++;
            process.stderr.write('  MISMATCH addr ' + batch[v] + ': mc=' + mcOwed + ' single=' + singleOwed + '\n');
          } else {
            process.stderr.write('  validate ok ' + batch[v].slice(0, 12) + ': owed=' + (Number(mcOwed) / 10 ** decimals).toFixed(6) + ' COMP\n');
          }
        } catch (ve) {
          process.stderr.write('  validate skip: ' + ve.message.slice(0, 50) + '\n');
        }
      }
      if (mismatches) process.stderr.write('  WARNING: ' + mismatches + ' validation mismatches\n');
      validated = true;
    }

    for (let j = 0; j < results.length; j++) {
      const r = results[j];
      if (!r.success || !r.data || r.data === '0x') continue;
      const raw = strip0x(r.data);
      if (raw.length < 128) continue;
      const owed = hexToBig('0x' + raw.slice(64, 128));
      if (owed > 0n) {
        userOwed[batch[j]] = owed.toString();
        totalOwed += owed;
      }
    }

    i += batch.length;
    batchNum++;
    if (batchNum % CKPT_BATCHES === 0 && onSave) await onSave(i, userOwed, totalOwed);
    if (batchNum % CKPT_BATCHES === 0 || i >= addrs.length)
      process.stderr.write('  ' + chain.key + '/' + comet.name + ': ' + i + '/' + addrs.length + ' queried, ' + Object.keys(userOwed).length + ' nonzero\r');
  }
  process.stderr.write('\n');
  return { totalOwed, userOwed };
}

// ─── validation ───────────────────────────────────────────────────────────────
async function validateAddress(chain, comet, decimals, addr) {
  try {
    const accrued = hexToBig(await ethCall(chain.rpcs, comet.addr, SEL_BASE_TRACKING_ACCRUED + padAddr(addr)));
    const claimed = hexToBig(await ethCall(chain.rpcs, chain.rewards, SEL_REWARDS_CLAIMED + padAddr(comet.addr) + padAddr(addr)));
    const configRaw = strip0x(await ethCall(chain.rpcs, chain.rewards, SEL_REWARD_CONFIG + padAddr(comet.addr)));
    const rescaleFactor = hexToBig('0x' + configRaw.slice(64, 128));
    const shouldUpscale = hexToBig('0x' + configRaw.slice(128, 192)) !== 0n;
    const accrualAtScale = shouldUpscale ? accrued * rescaleFactor : (rescaleFactor > 0n ? accrued / rescaleFactor : accrued);
    const expectedOwed = accrualAtScale > claimed ? accrualAtScale - claimed : 0n;
    const owedRaw = await ethCall(chain.rpcs, chain.rewards, SEL_GET_REWARD_OWED + padAddr(comet.addr) + padAddr(addr));
    const actualOwed = hexToBig('0x' + strip0x(owedRaw).slice(64, 128));
    process.stderr.write('\n  IDENTITY CHECK ' + chain.key + '/' + comet.name + ' ' + addr.slice(0, 12) + '...\n');
    process.stderr.write('    baseTrackingAccrued = ' + accrued + '\n');
    process.stderr.write('    rescaleFactor=' + rescaleFactor + ' shouldUpscale=' + shouldUpscale + '\n');
    process.stderr.write('    accrualAtScale = ' + accrualAtScale + '\n');
    process.stderr.write('    rewardsClaimed = ' + claimed + '\n');
    process.stderr.write('    formula owed = ' + Number(expectedOwed) / 10**decimals + ' COMP (' + expectedOwed + ' raw)\n');
    process.stderr.write('    getRewardOwed = ' + Number(actualOwed) / 10**decimals + ' COMP (' + actualOwed + ' raw)\n');
    // NOTE: formula reads baseTrackingAccrued storage (last checkpointed via accrue()).
    // getRewardOwed simulates accrue() internally, catching up to current block.
    // So formula may be LOWER than actual for addresses that haven't interacted recently.
    // This is expected and correct — our batch sum uses getRewardOwed, not the formula.
    const match = expectedOwed === actualOwed || (actualOwed > 0n && expectedOwed <= actualOwed);
    process.stderr.write('    match: ' + (match ? 'YES (formula<=actual, expected for stale checkpoints)' : 'WARNING formula>actual') + '\n');
    return { accrued, rescaleFactor, shouldUpscale, claimed, expectedOwed, actualOwed };
  } catch (e) {
    process.stderr.write('  validation error: ' + e.message + '\n');
    return null;
  }
}

// ─── token helpers ────────────────────────────────────────────────────────────
async function getRewardToken(chain, cometAddr) {
  const data = await ethCall(chain.rpcs, chain.rewards, SEL_REWARD_CONFIG + padAddr(cometAddr));
  const raw = strip0x(data);
  if (!raw || raw.length < 64) return null;
  const token = wordAddr(data, 0);
  if (token === '0x' + '0'.repeat(40)) return null;
  return token;
}

async function getDecimals(rpcs, token) {
  try { return Number(hexToBig(await ethCall(rpcs, token, '0x313ce567'))); } catch { return 18; }
}

async function getBalance(rpcs, token, owner) {
  try { return hexToBig(await ethCall(rpcs, token, SEL_BALANCE_OF + padAddr(owner))); } catch { return 0n; }
}


// ─── rewardsClaimed sweep ─────────────────────────────────────────────────────
async function sweepClaimed(chain, decimals, claimedCacheFile) {
  let claimed = {};
  try { claimed = JSON.parse(require('fs').readFileSync(claimedCacheFile, 'utf8')); } catch (_) {}

  for (const comet of chain.comets) {
    const cometName = comet.name;
    const cometFile = join(CACHE_DIR, chain.key + '-' + cometName.replace(/[^a-zA-Z0-9]/g, '-') + '.json');
    if (!existsSync(cometFile)) continue;
    let addresses;
    try { addresses = JSON.parse(readFileSync(cometFile, 'utf8')).addresses || []; } catch (_) { continue; }
    if (!addresses.length) continue;

    if (!claimed[cometName]) claimed[cometName] = {};
    const existing = new Set(Object.keys(claimed[cometName]).map(a => a.toLowerCase()));
    const todo = addresses.filter(a => !existing.has(a.toLowerCase()));
    if (!todo.length) { process.stderr.write('  [claimed/' + cometName + '] all cached (' + addresses.length + ')\n'); continue; }
    process.stderr.write('  [claimed/' + cometName + '] fetching ' + todo.length + '/' + addresses.length + '\n');

    let batchSize = 500;
    let i = 0, batchCount = 0;
    while (i < todo.length) {
      const slice = todo.slice(i, i + batchSize);
      const calls = slice.map(addr => ({
        target: chain.rewards,
        callData: SEL_REWARDS_CLAIMED + padAddr(comet.addr) + padAddr(addr),
      }));
      try {
        const results = await multicall(chain.rpcs, calls);
        for (let j = 0; j < slice.length; j++) {
          const r = results[j];
          const val = (r.success && r.data && r.data !== '0x' && r.data.length >= 66)
            ? BigInt(r.data).toString() : '0';
          claimed[cometName][slice[j].toLowerCase()] = val;
        }
        i += slice.length;
        batchCount++;
        if (batchCount % 10 === 0) writeFileSync(claimedCacheFile, JSON.stringify(claimed));
      } catch (e) {
        if (batchSize > 25) { batchSize = Math.max(25, batchSize >> 1); continue; }
        i += slice.length; // skip on persistent failure
      }
    }
    writeFileSync(claimedCacheFile, JSON.stringify(claimed));
  }
  return claimed;
}

// ─── per-user file emission ───────────────────────────────────────────────────
function emitUsersFile(chain, owedCache, claimedData, decimals, usersIndexFile) {
  const DIV = BigInt('1' + '0'.repeat(decimals));
  const rawToNum = wei => {
    if (!wei || wei === '0') return 0;
    const b = BigInt(wei);
    return Number(b / DIV) + Number(b % DIV) / 10 ** decimals;
  };

  // pick best owed key per comet (largest numeric suffix)
  function bestOwedKey(owedData, cometName) {
    const prefix = cometName + '-';
    const keys = Object.keys(owedData).filter(k => k === cometName || k.startsWith(prefix));
    let best = null, bestN = -1;
    for (const k of keys) {
      const m = k.match(/n?(\d+)$/);
      const n = m ? Number(m[1]) : 0;
      if (n > bestN) { bestN = n; best = k; }
    }
    return best;
  }

  const owedMap = {}, claimedMap = {}, lastBlockByAddr = {};
  for (const comet of chain.comets) {
    const owedKey = bestOwedKey(owedCache, comet.name);
    if (owedKey && owedCache[owedKey] && owedCache[owedKey].userOwed) {
      for (const [addr, wei] of Object.entries(owedCache[owedKey].userOwed)) {
        const a = addr.toLowerCase();
        owedMap[a] = (owedMap[a] || 0) + rawToNum(wei);
      }
    }
    const cometClaimed = (claimedData && claimedData[comet.name]) || {};
    for (const [addr, wei] of Object.entries(cometClaimed)) {
      const a = addr.toLowerCase();
      claimedMap[a] = (claimedMap[a] || 0) + rawToNum(wei);
    }
    // lastBlock from address cache
    try {
      const cf = join(CACHE_DIR, chain.key + '-' + comet.name.replace(/[^a-zA-Z0-9]/g, '-') + '.json');
      const lb = JSON.parse(readFileSync(cf, 'utf8')).lastBlock || {};
      for (const [addr, blk] of Object.entries(lb)) {
        const a = addr.toLowerCase();
        if (!lastBlockByAddr[a] || blk > lastBlockByAddr[a]) lastBlockByAddr[a] = blk;
      }
    } catch (_) {}
  }

  const allAddrs = new Set([...Object.keys(owedMap), ...Object.keys(claimedMap)]);
  const rows = [];
  for (const addr of allAddrs) {
    const owed = owedMap[addr] || 0;
    const clm = claimedMap[addr] || 0;
    if (owed <= 0 && clm <= 0) continue;
    const row = [addr, +owed.toFixed(6), +clm.toFixed(6)];
    if (lastBlockByAddr[addr]) row.push(lastBlockByAddr[addr]);
    rows.push(row);
  }
  rows.sort((a, b) => b[1] - a[1] || b[2] - a[2]);

  const outFile = join(__dir, 'users-' + chain.key + '.json');
  const outObj = { generatedAt: new Date().toISOString(), chain: chain.key, symbol: 'COMP', users: rows };
  const outStr = JSON.stringify(outObj);
  writeFileSync(outFile, outStr);

  // update users-index.json
  let idx = {};
  try { idx = JSON.parse(readFileSync(usersIndexFile, 'utf8')); } catch (_) {}
  const sumOwed = rows.reduce((s, r) => s + r[1], 0);
  const sumClaimed = rows.reduce((s, r) => s + r[2], 0);
  idx[chain.key] = { users: rows.length, owed: +sumOwed.toFixed(6), claimed: +sumClaimed.toFixed(6), file: 'users-' + chain.key + '.json', bytes: Buffer.byteLength(outStr) };
  writeFileSync(usersIndexFile, JSON.stringify(idx, null, 2));
  process.stderr.write('  [users] ' + rows.length + ' rows, sumOwed=' + sumOwed.toFixed(4) + ', sumClaimed=' + sumClaimed.toFixed(4) + ' -> ' + outFile + '\n');
}

// ─── per-chain orchestration ──────────────────────────────────────────────────
async function indexChain(chain, maxBlocks) {
  process.stderr.write('\n=== ' + chain.name + ' (' + chain.key + ') ===\n');
  const t0 = Date.now();

  const owedCacheFile = join(CACHE_DIR, chain.key + '-owed.json');
  let owedCache = cacheRead(owedCacheFile) || {};

  const latestBlock = parseInt(await rpcCall(chain.rpcs, 'eth_blockNumber', []), 16);
  process.stderr.write('  latest block: ' + latestBlock + '\n');

  let chainToken = null;
  let chainDecimals = 18;
  const perComet = {};
  let totalUnclaimedRaw = 0n;
  const allUsers = new Set();
  let complete = true;
  let allSkipped = true;

  for (const comet of chain.comets) {
    const rewardToken = await getRewardToken(chain, comet.addr);
    if (!rewardToken) {
      process.stderr.write('  [' + comet.name + '] rewardConfig.token=0x0 -> skip\n');
      perComet[comet.name] = { unclaimed: 0, users: 0, note: 'no reward token configured (address(0))' };
      continue;
    }
    allSkipped = false;

    if (!chainToken) {
      chainToken = rewardToken;
      chainDecimals = await getDecimals(chain.rpcs, rewardToken);
    }

    const { addresses, scannedTo, deployBlock, lastBlockMap } = await scanComet(chain, comet, latestBlock, maxBlocks);
    if (maxBlocks && (latestBlock - scannedTo) > chain.logChunk) complete = false;

    // Check owed cache — supports partial resume via owedCache[owedKey].queryIdx
    // Use address count in key so partial resumes survive minor block-tip advances
    const owedKey = comet.name + '-n' + addresses.size;
    let totalOwedRaw, userOwed;
    if (owedCache[owedKey] && owedCache[owedKey].complete) {
      process.stderr.write('  [' + comet.name + '] cached owed complete (scannedTo=' + scannedTo + ')\n');
      totalOwedRaw = BigInt(owedCache[owedKey].totalOwedRaw);
      userOwed = owedCache[owedKey].userOwed;
    } else {
      // Resume from partial progress if available
      const partial = owedCache[owedKey] && !owedCache[owedKey].complete ? owedCache[owedKey] : null;
      const startIdx = partial ? (partial.queryIdx || 0) : 0;
      const resumeOwed = partial ? partial.userOwed : {};
      const addrList = [...addresses];
      // Merge partial results into accumulator
      const onSave = async (idx, partialOwed, partialTotal) => {
        // Merge new findings into resumeOwed
        Object.assign(resumeOwed, partialOwed);
        const mergedTotal = Object.values(resumeOwed).reduce((s, v) => s + BigInt(v), 0n);
        owedCache[owedKey] = { totalOwedRaw: mergedTotal.toString(), userOwed: { ...resumeOwed }, queryIdx: idx, complete: false, scannedTo };
        cacheWrite(owedCacheFile, owedCache);
      };
      const res = await batchGetRewardOwed(chain, comet, chainDecimals, addresses, startIdx, onSave);
      // Merge final results with any previously saved partial
      Object.assign(resumeOwed, res.userOwed);
      totalOwedRaw = Object.values(resumeOwed).reduce((s, v) => s + BigInt(v), 0n);
      userOwed = resumeOwed;
      owedCache[owedKey] = { totalOwedRaw: totalOwedRaw.toString(), userOwed, queryIdx: addrList.length, complete: true, scannedTo };
      cacheWrite(owedCacheFile, owedCache);
    }

    const cometUsers = Object.keys(userOwed).length;
    const cometUnclaimed = Number(totalOwedRaw) / 10 ** chainDecimals;
    perComet[comet.name] = { unclaimed: cometUnclaimed, users: cometUsers, scannedToBlock: scannedTo };
    totalUnclaimedRaw += BigInt(totalOwedRaw);
    for (const a of Object.keys(userOwed)) allUsers.add(a);

    process.stderr.write('  [' + comet.name + '] unclaimed=' + cometUnclaimed.toFixed(4) + ' COMP, users=' + cometUsers + '\n');

    // Identity check on first nonzero address
    const firstAddr = Object.keys(userOwed)[0];
    if (firstAddr) await validateAddress(chain, comet, chainDecimals, firstAddr);
  }

  if (allSkipped) {
    return { symbol: 'COMP', rewardsBalance: 0, totalUnclaimed: 0, shortfall: 0,
      users: 0, perComet, scannedToBlock: latestBlock, complete: true,
      note: 'all comets: rewardConfig.token=address(0), no rewards configured' };
  }

  // ── claimed sweep (rewardsClaimed per comet) ──
  const claimedCacheFile = join(CACHE_DIR, chain.key + '-claimed.json');
  const usersIndexFile = join(__dir, 'users-index.json');
  let claimedData = {};
  try {
    claimedData = await sweepClaimed(chain, chainDecimals, claimedCacheFile);
  } catch (e) {
    process.stderr.write('  [claimed] sweep error (non-fatal): ' + e.message + '\n');
  }

  // ── per-user file ──
  try {
    emitUsersFile(chain, owedCache, claimedData, chainDecimals, usersIndexFile);
  } catch (e) {
    process.stderr.write('  [users] emit error (non-fatal): ' + e.message + '\n');
  }

  let rewardsBalance = 0;
  if (chainToken) {
    const balRaw = await getBalance(chain.rpcs, chainToken, chain.rewards);
    rewardsBalance = Number(balRaw) / 10 ** chainDecimals;
  }

  const totalUnclaimed = Number(totalUnclaimedRaw) / 10 ** chainDecimals;
  const shortfall = Math.max(0, totalUnclaimed - rewardsBalance);
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  process.stderr.write('  RESULT: unclaimed=' + totalUnclaimed.toFixed(4) + ' balance=' + rewardsBalance.toFixed(4) + ' shortfall=' + shortfall.toFixed(4) + ' users=' + allUsers.size + ' time=' + elapsed + 's\n');

  return { symbol: 'COMP', rewardsBalance, totalUnclaimed, shortfall,
    users: allUsers.size, perComet, scannedToBlock: latestBlock, complete };
}

// ─── CLI ──────────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let maxBlocks = null;
const chainKeys = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--max-blocks') maxBlocks = parseInt(args[++i]);
  else chainKeys.push(args[i]);
}

const targetChains = chainKeys.length ? CHAINS.filter(c => chainKeys.includes(c.key)) : CHAINS;
if (!targetChains.length) {
  process.stderr.write('No matching chains. Keys: ' + CHAINS.map(c => c.key).join(', ') + '\n');
  process.exit(1);
}

process.stderr.write('Indexing: ' + targetChains.map(c => c.key).join(', ') + (maxBlocks ? ' (max-blocks=' + maxBlocks + ')' : '') + '\n');

const outFile = join(__dir, 'unclaimed.json');
const output = { generatedAt: new Date().toISOString(), chains: {} };
try { if (existsSync(outFile)) output.chains = JSON.parse(readFileSync(outFile, 'utf8')).chains || {}; } catch (_) {}

function saveOutput() {
  output.generatedAt = new Date().toISOString();
  writeFileSync(outFile, JSON.stringify(output, null, 2));
}

for (const chain of targetChains) {
  try {
    const result = await indexChain(chain, maxBlocks);
    output.chains[chain.key] = { ...result, name: chain.name };
  } catch (e) {
    process.stderr.write('ERROR ' + chain.key + ': ' + e.message + '\n');
    if (!output.chains[chain.key] || !output.chains[chain.key].complete) {
      output.chains[chain.key] = { error: e.message, complete: false, name: chain.name };
    }
  }
  saveOutput();
}

process.stderr.write('\nWrote ' + outFile + '\n');
for (const [k, v] of Object.entries(output.chains)) {
  console.log(k.padEnd(10), v.complete ? ('unclaimed ' + (v.totalUnclaimed || 0).toFixed(2) + ' | users ' + (v.users || 0) + ' | shortfall ' + (v.shortfall || 0).toFixed(2)) : ('INCOMPLETE' + (v.error ? ' err: ' + String(v.error).slice(0, 60) : '')));
}
