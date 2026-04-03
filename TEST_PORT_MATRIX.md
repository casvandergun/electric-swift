# TypeScript Test Port Matrix

This note tracks how the local `typescript-client/test` suite maps onto `ElectricSwift`.

## Ported / Port Now

- `204-no-content.test.ts`
  - Covered by `ShapeStreamTests` around `204` up-to-date and long-poll fallback semantics.
- `shape-stream-state.test.ts`
  - Covered in Swift by `ShapeStreamTests` scenario coverage plus invariant-style tests for replay, schema adoption, `204`, stale cache, pause/resume, fast-loop recovery, and conflict reset.
- `fetch.test.ts`
  - Covered in Swift by `FetchSupportTests` and `ShapeStreamTests` for `Retry-After`, retryability, cancellation, and transient failure recovery.
- `expired-shapes-cache.test.ts`
  - Covered in Swift by `TrackersTests` and `ShapeStreamTests` stale-cache handling, retry, ignored stale responses, cache busters, and retry exhaustion.
- `up-to-date-tracker.test.ts`
  - Covered in Swift by `TrackersTests` and replay-related `ShapeStreamTests`.
- `snapshot-tracker.test.ts`
  - Covered in Swift by `SnapshotTrackerTests` and snapshot filtering `ShapeStreamTests`.
- Relevant runtime parts of `stream.test.ts`
  - Covered in Swift by `ShapeRequestBuilderTests` and `ShapeStreamTests`.
- Relevant runtime, error, and snapshot parts of `client.test.ts`
  - Covered in Swift by `ShapeStreamTests` for retries, `onError`, snapshot fetch/request, duplicate filtering, and resume behavior.

## Deferred / Needs Feature Work

- `client.test.ts`
  - Higher-level `Shape` consumer semantics, connection/loading status, subscriptions, unsubscribe, and public client APIs not exposed by Swift.
  - Advanced snapshot behavior not yet implemented in Swift, including automatic re-executed sub-snapshots after `must-refetch`.
- `stream.test.ts`
  - Request-start timing based on first subscription and other stream-wrapper lifecycle semantics that depend on a higher-level client API.

## Out Of Scope For This Pass

- `column-mapper.test.ts`
- `expression-compiler.test.ts`
- `pause-lock.test.ts`
- `wake-detection.test.ts`
- `cache.test.ts`
- TS type tests: `client.test-d.ts`, `helpers.test-d.ts`
- Browser/localStorage-specific persistence behavior
- Helper utilities that do not map to a Swift public surface
