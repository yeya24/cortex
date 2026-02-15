# DynamoDB Ring KV – Schema and Token Traffic

## Current schema

- **Table**: One partition key (`RingKey`), one sort key (`InstanceKey` = instance ID).
- **Per item**: `Data` (snappy-compressed protobuf `InstanceDesc`), `version`, optional `ttl`.
- **InstanceDesc** (proto): `addr`, `timestamp`, `state`, `tokens` (repeated uint32), `zone`, `registered_timestamp`.

So the ring is sharded by instance: one item per instance, each holding the full `InstanceDesc` including the token list.

## Problem: tokens dominate read traffic

- **Tokens are large**: e.g. 128 tokens × 4 bytes = 512 bytes per instance; with many instances, token data dominates the payload.
- **Tokens change rarely**: they are set at join, and only change on token renewal, claim, or rebalance.
- **Most reads don’t need tokens**: heartbeats and readiness only need metadata (addr, timestamp, state, zone, registered_timestamp) to decide liveness and replication. Only a few operations need the full ring with tokens (e.g. autoJoin, replication set computation, token generation).

Because the whole ring is stored as one protobuf blob per instance, every full read (and every single-item read with the current codec) fetches the full `InstanceDesc` including tokens. So we pay the cost of transferring all tokens on every Get/CAS, even when we only need metadata. That makes tokens a large fraction (e.g. ~90%) of read traffic even though they rarely change.

## Possible evolution: separate metadata from tokens

Goal: allow “metadata-only” reads for the common path (heartbeats, readiness) and full reads only when tokens are needed.

### Option A: Two attributes per item

- Keep one item per instance.
- Add a second attribute, e.g. `Meta`: snappy+proto of a **metadata-only** struct (addr, timestamp, state, zone, registered_timestamp; no tokens).
- Keep `Data` as the full `InstanceDesc` (backward compatibility or phased migration).
- **Reads**: use `ProjectionExpression` to read only `Meta` when we don’t need tokens; read `Data` when we need full state (e.g. autoJoin, replication).
- **Writes**: every write (heartbeat, CAS) updates both `Data` and `Meta` so they stay in sync.
- **Trade-off**: halves (or more) read traffic for metadata-only paths; doubles write size and storage per instance.

### Option B: Two items per instance (metadata vs tokens)

- **Metadata item**: sort key = instance ID; body = metadata only (addr, timestamp, state, zone, registered_timestamp). Small, updated on every heartbeat.
- **Tokens item**: sort key = e.g. `{instanceID}#tokens`; body = token list only. Updated only when tokens change (join, renewal, claim).
- **Reads**:
  - Metadata-only (heartbeats, readiness): query partition with condition “sort key = instance ID” (single item) or “sort key NOT begins_with '#'” to get all metadata items only. No token data transferred.
  - Full ring (autoJoin, replication, token ops): query all items for the partition (metadata + token items) and merge in the client.
- **Writes**:
  - Heartbeat: write only the metadata item (small).
  - Token change: write both metadata and token items (or at least the tokens item and any metadata that changed).
- **Trade-off**: no redundant storage; heartbeat write traffic is small; token traffic happens only when tokens are actually needed. Requires codec/schema and client changes (split encode/decode for metadata vs tokens, and merge logic).

Option B aligns with “tokens don’t change often”: metadata is read/written often and kept small; tokens are separate and read only when necessary.

## Implementation notes (for a future change)

- **Proto/codec**: introduce a “metadata-only” message (e.g. `InstanceMetadata` without `tokens`) and/or a `Tokens` message; extend the codec to encode/decode per-instance metadata and tokens separately and to merge them into `InstanceDesc` when doing a full read.
- **DDB client**: add a notion of “metadata-only” Get/Query (e.g. hint or option) that only fetches metadata items (or only the `Meta` attribute in Option A). CAS that only updates our instance’s metadata (e.g. heartbeat) would write only metadata.
- **Lifecycler**: use metadata-only reads for heartbeat and readiness where possible; use full reads for initRing when we need to know if we have tokens, for autoJoin, for verifyTokens, and for any path that calls `GenerateTokens` or needs the full replication set.
