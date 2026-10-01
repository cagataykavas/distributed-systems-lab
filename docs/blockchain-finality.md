# Blockchain fork and finality lab

This increment treats blockchain as distributed state reconciliation, not as a token demo. The
implementation admits independently valid branches, selects strictly greater cumulative work, and
rebuilds canonical UTXO state when a heavier branch wins.

## Invariants

- A block hash binds its parent, height, work, ordered transactions, inputs, outputs, owners, and
  integer amounts through canonical JSON and SHA-256.
- A transaction consumes each input at most once, cannot spend a missing output, and must conserve
  value exactly.
- Competing branches are validated against their own parent UTXO state.
- Equal-work branches do not displace the first-seen tip, preventing deterministic tie churn.
- A heavier branch may reorganize pending state, but its fork point cannot precede the configured
  finalized height.
- Duplicate blocks are idempotent; unknown parents, malformed evidence, and exhausted block
  capacity fail explicitly.

Run the deterministic fork scenario:

```bash
python -m resilience.blockchain --output artifacts/blockchain-reorg.json
```

The scenario first commits an Alice/Bob branch, then receives a competing Alice/Dave/Erin branch
with greater cumulative work. The JSON artifact records the fork height, removed and added block
hashes, canonical balances, finality boundary, and transaction states. It contains no private key,
wallet address, RPC credential, fabricated market metric, or claim about a live network.

## Confirmation semantics

The containing block counts as the first confirmation. With `confirmation_depth=3`, a transaction
in height 1 becomes finalized when the canonical tip reaches height 3. A reorganization may remove
only blocks above the current finalized height. This is an application safety policy, not a claim
that proof-of-work chains provide absolute finality.

## Deliberate boundaries

The lab uses an in-memory UTXO set and caller-supplied work. It does not implement peer discovery,
proof-of-work verification, signatures, scripts, difficulty adjustment, persistence, orphan-block
staging, network-specific consensus, or Byzantine-fault tolerance. SHA-256 here provides evidence
integrity; it does not authenticate a sender.

The next production-minded increment is an append-only journal with crash recovery: persist block
admission and canonical-tip changes atomically, replay them after interruption, and prove that a
reorg cannot leave a partially applied UTXO state.
