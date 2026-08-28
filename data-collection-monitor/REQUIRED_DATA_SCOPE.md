# Robinhood Chain NFT wallet-intelligence required data scope

Status: data collection only. DeepSeek handoff disabled.

## Required and collectible

A data-stage release is incomplete until every item below has immutable evidence and a machine-readable PASS gate.

1. **NFT contract universe**
   - Exhausted ERC-721 and ERC-1155 token-list pagination from public Robinhood Chain indexers.
   - Contract address de-duplication and source-set comparison.

2. **Primary NFT mint universe**
   - All outbound zero-address ERC-721 and ERC-1155 mint transfers through one finalized fixed head.
   - Complete identifiers: block, transaction, log, contract, recipient, token ID or amount.

3. **Canonical SeaDrop history**
   - All `SeaDropMint` events from block 0 through the same finalized fixed head.
   - No 1,000-row truncation or uncovered block range.

4. **Canonical Seaport execution history**
   - All `OrderFulfilled` events from block 0 through the same finalized fixed head.
   - No 1,000-row truncation or uncovered block range.

5. **Every primary mint transaction**
   - Transaction, receipt, block timestamp, receipt status, calldata and selector.
   - Native value, ERC-20 sender outflows, internal native transfers, entry gas.
   - Mint recipients and NFT contracts linked to the transaction.

6. **Every Seaport execution transaction**
   - Transaction, receipt, block timestamp, receipt status, gas and complete receipt logs.
   - OrderFulfilled events grouped at transaction/order level to prevent bundle double counting.
   - Internal transfers preserved for cash-flow reconstruction.

7. **Project metadata and observed mint terms**
   - NFT Trencher, MintGo, GUAP and on-chain observations kept as source-tagged evidence.
   - Free, paid, Public, GTD, WL, team and custom-route states must not be conflated.
   - Source conflicts remain explicit until resolved on-chain.

8. **Wallet candidate sources**
   - Existing public candidate registry and source observations retained.
   - P0/P1/P2 canonical address evidence collected.
   - P3 is not omitted: every P3 wallet that has Robinhood Chain NFT activity is covered by the complete mint and sale universes; the external P3 registry remains a cross-match source rather than a separate definition of strength.

9. **Project opportunity and outcome tables**
   - Every comparable project, including unsuccessful and illiquid projects.
   - Time-indexed outcomes at 15m, 30m, 2h and 24h from actual executions.
   - No floor-only success label and no single bundle counted as multiple independent wins.

10. **Selection, Execution and Copy evidence remain separate**
    - Selection: predictive lift against comparable opportunities available at the time.
    - Execution: exact acquisition cost through proven seller proceeds, with gas.
    - Copy: only observed delayed availability and subsequent executable/actual exits; otherwise `NOT_AVAILABLE`.

## Explicitly unavailable or not required for the historical ground truth

The following must not be fabricated and do not block the collectible on-chain dataset:

- Private or deleted Notion databases without access.
- AlphaGate authenticated feed coverage without an account or credential.
- Historical off-chain OpenSea orders, bids, cancellations or depth that were never archived.
- Deleted X posts that were not captured before deletion.
- Provider-generated wallet scores whose formula and source rows cannot be audited.

These may be added later as source-tagged supplements, but absence is represented as unavailable, not zero.

## DeepSeek gate

DeepSeek implementation work is prohibited until the raw and normalized data release has:

- all required evidence gates PASS;
- immutable manifests and SHA-256 hashes;
- no unresolved coverage gap or known truncation;
- deterministic project opportunity/outcome generation;
- `production_approved_wallets = 0` unless a separate evidence-based approval is completed.
