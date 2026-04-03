# TypeScript Test Parity

This ledger tracks which TypeScript reference tests were ported to Swift, which required
Swift-specific adaptations, and which are intentionally out of scope for this package.

## Relevant Reference Files

| TypeScript file | Swift target | Status |
| --- | --- | --- |
| `typescript-client/test/snapshot-tracker.test.ts` | `Tests/ElectricSwiftTests/SnapshotTrackerTests.swift` | Ported 1:1 where applicable |
| `typescript-client/test/up-to-date-tracker.test.ts` | `Tests/ElectricSwiftTests/TrackersTests.swift`, `Tests/ElectricSwiftTests/ShapeStreamTests.swift` | Ported with Swift adaptation |
| `typescript-client/test/expired-shapes-cache.test.ts` | `Tests/ElectricSwiftTests/TrackersTests.swift`, `Tests/ElectricSwiftTests/ShapeStreamTests.swift` | Ported with Swift adaptation |
| `typescript-client/test/parser.test.ts` | `Tests/ElectricSwiftTests/ElectricParserTests.swift`, `Tests/ElectricSwiftTests/ShapeStreamTests.swift`, `Tests/ElectricSwiftTests/ShapeTests.swift` | Ported with Swift adaptation |
| `typescript-client/test/fetch.test.ts` | `Tests/ElectricSwiftTests/FetchSupportTests.swift`, `Tests/ElectricSwiftTests/ShapeStreamTests.swift` | Ported with Swift adaptation |
| `typescript-client/test/error.test.ts` | `Tests/ElectricSwiftTests/FetchSupportTests.swift` | Ported 1:1 where applicable |
| `typescript-client/test/integration.test.ts` | `Tests/ElectricSwiftTests/ElectricIntegrationTests.swift` | Partially covered; env-gated expansion deferred |

## Classification

### `snapshot-tracker.test.ts`

`ported 1:1`
- xid `< xmin`
- xid `< xmax` and not in `xip`
- xid `< xmax` and in `xip`
- xid `>= xmax`
- key not in any snapshot
- key in snapshot and rejectable
- multiple snapshots where any snapshot can reject
- multiple snapshots where none reject
- snapshot cleanup when xid passes `xmax`
- cleanup of one snapshot without removing others
- cleanup of multiple snapshots sharing the same boundary
- messages with no txids
- messages with multiple txids using the max
- `lastSeenUpdate`

### `up-to-date-tracker.test.ts`

`ported 1:1`
- no recent up-to-date means no replay cursor
- recent up-to-date enables replay cursor
- TTL expiry disables replay cursor
- oldest entries are evicted once `maxEntries` is exceeded
- `clear()`
- `delete(shapeKey:)`
- replay suppression behavior is covered at stream level

`ported with Swift adaptation`
- per-shape independence is asserted with `replayCursorIfRecent(for:)` rather than TS `shouldEnterReplayMode`

`not applicable to Swift`
- localStorage persistence
- loading persisted entries on initialization
- cleanup of persisted entries on initialization
- localStorage unavailable handling

### `expired-shapes-cache.test.ts`

`ported 1:1`
- mark/get expired handle
- oldest entry eviction under max-entry pressure
- `clear()`
- `delete(shapeKey:)`
- stale response handling covered through `ShapeStream` runtime tests
- `409` storing expired handles covered through `ShapeStream` runtime tests
- no-infinite-loop replay guard covered through `ShapeStream` runtime tests

`not applicable to Swift`
- localStorage persistence or recovery paths

### `parser.test.ts`

`ported 1:1`
- `int2`, `int4`
- `bool`
- `float4`, `float8`
- `json`, `jsonb`
- one-dimensional arrays
- nested arrays
- array `NULL` handling
- quoted-array token handling
- parsing of both `value` and `oldValue`
- non-nullability errors
- text `"NULL"` remains a string when quoted

`ported with Swift adaptation`
- `int8` asserts `Int64`-backed `.integer` values instead of JS `BigInt`
- parser assertions use `ElectricValue`
- malformed built-in scalar values preserve `.string(raw)` instead of throwing
- parser error surfacing is verified through `ShapeStream.poll()`, `ShapeStream.batches()`, and `Shape.rows()`

`swift-only additions`
- custom scalar parser override wins over defaults
- unknown types fall back to `.string`
- row transform is applied after coercion to both `value` and `oldValue`

### `fetch.test.ts`

`ported 1:1`
- retry eligibility for `429`
- retry eligibility for `5xx`
- no retry for `4xx`
- no retry for cancellation
- retry delay honors `Retry-After`
- retry delay capping
- text response parsing
- JSON response parsing

`ported with Swift adaptation`
- missing-header terminal behavior is asserted against Swift's `FetchError.missingHeaders`
- retry loops and replay suppression are also validated at `ShapeStream` level

### `error.test.ts`

`ported 1:1`
- custom error message formatting
- response-derived message formatting
- fallback text formatting when `content-type` is missing

### `integration.test.ts`

`ported with Swift adaptation`
- existing env-gated real-endpoint coverage remains in `Tests/ElectricSwiftTests/ElectricIntegrationTests.swift`

`deferred`
- real snapshot fetch coverage
- real `409` / must-refetch recovery coverage
- live update after initial sync with parser-default behavior intact

These integration cases remain deferred because the current env-gated integration setup is intentionally minimal and the parser/test-port work does not depend on extending it.
