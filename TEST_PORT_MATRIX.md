# TypeScript Client Test Port Matrix

This note tracks how the current upstream TypeScript client behavioral tests map onto `ElectricSwift`.

Upstream test directory: <https://github.com/electric-sql/electric/tree/main/packages/typescript-client/test>

## Status Legend

- **Full**: Swift has equivalent behavioral coverage for the parts that apply to this package.
- **Partial**: Swift covers the shared protocol/runtime behavior, but some TypeScript client surface, browser behavior, or integration breadth is not represented.
- **Not ported**: The behavior appears relevant, but we do not currently have equivalent Swift coverage.

## Matrix

| Upstream test | Swift status | Swift coverage | Notes |
| --- | --- | --- | --- |
| [`204-no-content.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/204-no-content.test.ts) | Full | `ShapeStreamTests` | Covers `204` up-to-date handling, repeated `204`, and long-poll fallback rather than immediately probing SSE. |
| [`client.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/client.test.ts) | Partial | `ShapeStreamTests`, `ShapeTests`, `FetchSupportTests` | Shared runtime behavior is covered: retry/onError paths, stale CDN loop prevention, snapshots, request snapshots, parser errors, and typed wrapper updates. TypeScript-specific public client lifecycle/status/subscription surface is not exposed by Swift. |
| [`column-mapper.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/column-mapper.test.ts) | Partial | `ShapeRequestBuilderTests`, `ElectricParserTests`, `ShapeStreamTests` | Swift covers request-field encoding and decode-before-transform behavior. Upstream TypeScript column-mapper helper surface is broader than our Swift API. |
| [`error.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/error.test.ts) | Full | `FetchSupportTests` | Covers custom, JSON response-derived, text response-derived, and missing-content-type fetch error formatting. |
| [`expired-shapes-cache.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/expired-shapes-cache.test.ts) | Full | `TrackersTests`, `ShapeStreamTests` | Covers mark/get/delete/clear/LRU plus stale cached response handling, cache-buster retry, retry exhaustion, and 409-expired-handle behavior. Browser/localStorage persistence is intentionally not applicable. |
| [`fetch.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/fetch.test.ts) | Full | `FetchSupportTests`, `ShapeStreamTests` | Covers retry eligibility, `Retry-After`, delay caps, cancellation/terminal errors, response parsing, automatic transient retry, and request-level retry behavior. |
| [`helpers.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/helpers.test.ts) | Partial | `ElectricProtocol` runtime helpers, indirectly via stream/materialization tests | Swift has `ElectricMessage.isChangeMessage` and `isControlMessage`, but no dedicated helper test file currently asserts the positive/negative helper cases directly. |
| [`integration.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/integration.test.ts) | Partial | `ElectricIntegrationTests` | Swift has env-gated real-endpoint coverage, but it is intentionally minimal and does not mirror the full TypeScript integration suite. |
| [`parser.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/parser.test.ts) | Full | `ElectricParserTests`, `ElectricMessageDecoderTests`, `ShapeStreamTests`, `ShapeTests` | Covers scalar parsing, arrays, nested arrays, quoted array tokens, nullability errors, value/oldValue parsing, parser errors surfacing through streams and shapes, custom parser overrides, and transform ordering. |
| [`shape-stream-state.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/shape-stream-state.test.ts) | Full, behaviorally | `ShapeStreamTests`, `ShapeRequestBuilderTests` | Swift covers the applicable state-machine behavior: initial/sync/live boundaries, replay suppression and cursor preservation, schema first-write-wins, `204`/`200` `lastSyncedAt`, SSE offset update, SSE fallback/abort handling, stale retry from initial/sync/live states, cache busters, retry exhaustion, and `must-refetch` reset. TS-only immutable class identity, delegation wrappers, truth-table, fuzz, and mutation scaffolding are not ported because Swift uses an actor runtime rather than the TS state-class DSL. |
| [`snapshot-tracker.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/snapshot-tracker.test.ts) | Full | `SnapshotTrackerTests`, `ShapeStreamTests` | Covers transaction visibility rules, multiple snapshots, cleanup, messages without txids, highest-txid handling, `lastSeenUpdate`, and stream-level snapshot duplicate filtering. |
| [`stream.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/stream.test.ts) | Partial | `ShapeStreamTests`, `ShapeRequestBuilderTests`, `ShapeTests` | Shared stream runtime behavior is covered: request parameters, SSE, long-polling, retries, snapshots, duplicate filtering, refresh, pause/resume, and materialization. TS stream wrapper lifecycle details such as subscription-triggered request start are not exposed by the Swift API. |
| [`up-to-date-tracker.test.ts`](https://github.com/electric-sql/electric/blob/main/packages/typescript-client/test/up-to-date-tracker.test.ts) | Full | `TrackersTests`, `ShapeStreamTests` | Covers recent cursor replay, TTL expiry, max-entry eviction, clear/delete, per-shape independence, and replay suppression at stream level. Browser/localStorage persistence is intentionally not applicable. |

## Current Gaps Worth Considering

- Add a small dedicated Swift helper test for `ElectricMessage.isChangeMessage` and `isControlMessage` if we want `helpers.test.ts` to be marked **Full**.
- Extend env-gated integration coverage only if we want parity with the TypeScript integration suite rather than the current lightweight smoke test.
