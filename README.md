# COMP Rewards Runway

Live dashboard + backlog indexer for Compound v3 COMP rewards, all chains. As of 2026-06-12: the protocol owes users ~86,408 COMP of earned-but-unclaimed rewards while rewards contracts hold ~3,718 — 7 of 8 chains with liabilities cannot cover them.

## Files
- `index.html` — dashboard. Static, client-side, no keys. Live-reads all 29 comets + rewards contracts via public RPCs; overlays `unclaimed.json` (insolvency detection).
- `indexer.mjs` — computes the true earned-but-unclaimed backlog per chain (enumerates all historical users from logs, simulates `getRewardOwed` via Multicall3). Resumable via `./cache`. Merges into `unclaimed.json` per chain.
- `unclaimed.json` — current snapshot (all 10 chains, 2026-06-12).
- `users.html` + `users-<chain>.json` / `users-index.json` — per-user explorer: every address ever owed or paid rewards, owed vs lifetime-claimed, searchable, CSV export. (Last-activity column appears automatically once the nightly Action regenerates snapshots with the patched indexer.)
- `.github/workflows/update-backlog.yml` — nightly re-index + auto-commit; GitHub Pages redeploys automatically.

## Host on GitHub Pages (free for public repos)
1. Create a **public** repo, push these files.
2. Settings → Pages → Source: *Deploy from a branch* → `main` / root.
3. Settings → Actions → General → Workflow permissions: *Read and write*.
4. Actions tab → `update-backlog` → *Run workflow* once (first run rebuilds caches, ~2-4h; later runs are incremental).

Site: `https://<user>.github.io/<repo>/`. Alternatives: Netlify Drop / Cloudflare Pages / any static host; `unclaimed.json` must sit next to `index.html`.

## Run the indexer locally
```
npm install js-sha3
node indexer.mjs                # all chains
node indexer.mjs mainnet base   # specific chains
```

## Reading the dashboard
- **in rewards contract** vs **owed but unclaimed**: owed > balance ⇒ INSOLVENT — claims revert — flags red regardless of emissions.
- **effective runway** = (balance − indexed backlog) ÷ max(accrual, trailing claim rate); without a backlog snapshot it is an upper bound.
- Red threshold = governance lead time (default 8d ≈ 2d review + 3d vote + 2d timelock + bridging), configurable.

Validation: per-address identity `accrued × rescale − claimed == getRewardOwed` exact on all sampled accounts; totals independently recomputed on second RPCs (mainnet ≤0.05% drift from live accrual, base exact); cache re-sums match reported totals to the cent.
