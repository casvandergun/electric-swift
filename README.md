# ElectricSwift

`ElectricSwift` is a Swift package for consuming [ElectricSQL](https://electric-sql.com/) shape streams on Apple platforms. It gives you both a low-level stream runtime for working directly with Electric protocol batches and a higher-level typed `Shape<Model>` wrapper for in-memory materialized views. The package currently supports iOS 17+ and macOS 14+.

## Features

- Electric protocol models, message decoding, and schema-aware row coercion
- `ShapeStream` for catch-up, long-poll, and SSE-backed live streaming
- Retry policy and `onError` recovery hooks for transient failures
- Snapshot APIs with `fetchSnapshot(_:)` and `requestSnapshot(_:)`
- Typed in-memory materialization with `Shape<Model>`
- Lower-level `MaterializedShape<Model>` for custom consumers

## Installation

Add `ElectricSwift` with Swift Package Manager:

```swift
dependencies: [
    .package(url: "https://github.com/casvandergun/electric-swift.git", from: "0.1.0")
]
```

Then add the product to your target:

```swift
targets: [
    .target(
        name: "YourApp",
        dependencies: [
            .product(name: "ElectricSwift", package: "electric-swift")
        ]
    )
]
```

## Quick Start

### Low-level streaming with `ShapeStream`

Use `ShapeStream` when you want direct access to Electric batches, checkpoints, and control messages.

```swift
import ElectricSwift
import Foundation

let shape = ElectricShape(
    url: URL(string: "https://example.com/v1/shape")!,
    table: "todos"
)

let stream = ShapeStream(
    shape: shape,
    configuration: .init(subscribe: true)
)

for try await batch in await stream.batches() {
    if batch.reachedUpToDate {
        print("Shape is up to date at offset \(batch.checkpoint.offset)")
    }

    for message in batch.messages {
        print(message)
    }
}
```

### Typed in-memory access with `Shape<Model>`

Use `Shape<Model>` when you want a materialized, typed view of the current rows.

```swift
import ElectricSwift
import Foundation

struct Todo: Codable, Sendable {
    let id: UUID
    let title: String
    let completed: Bool?
}

let stream = ShapeStream(
    shape: ElectricShape(
        url: URL(string: "https://example.com/v1/shape")!,
        table: "todos"
    ),
    configuration: .init(subscribe: true)
)

let shape = Shape<Todo>(stream: stream)

let initialRows = try await shape.rows()
print("Loaded \(initialRows.count) rows")

for try await change in shape.updates() {
    print("Status:", change.status)
    print("Rows:", change.rows)
}
```

## API Overview

### `ElectricShape`

`ElectricShape` describes the stream you want to open: the Electric endpoint URL, table, optional column selection, optional `whereClause`, replica mode, extra query parameters, and request headers.

### `ShapeStream`

`ShapeStream` is the low-level runtime. It owns the current handle, offset, cursor, schema, retry state, and live transport mode. Use it when you need direct control over:

- raw `ElectricShapeBatch` values
- checkpoint inspection and restoration
- pause, resume, refresh, and stop lifecycle control
- snapshot fetch/injection behavior
- transport-level debugging and recovery

### `Shape<Model>`

`Shape<Model>` is a typed wrapper over `ShapeStream`. It consumes stream batches, materializes rows in memory, merges partial updates, clears state on `must-refetch`, and exposes:

- `rows()` / `value()` for first-ready access
- `currentRows()` / `currentValue()` for immediate in-memory access
- `updates()` for observing typed state changes

### `MaterializedShape<Model>`

`MaterializedShape<Model>` is the lower-level merge engine used by `Shape<Model>`. Use it directly if you want to manage stream consumption yourself while still decoding fully materialized Swift models.

### Snapshots and Recovery

The package supports both:

- `fetchSnapshot(_:)` for fetching a subset without mutating the running stream
- `requestSnapshot(_:)` for injecting snapshot data into the live stream/session

For failure handling, `ShapeStreamConfiguration` includes a retry policy and `ShapeStream` accepts an async `onError` handler that can stop, retry, or retry with an updated `ElectricShape`.

## Advanced Usage

### Custom headers and extra parameters

```swift
let shape = ElectricShape(
    url: URL(string: "https://example.com/v1/shape")!,
    table: "todos",
    extraParameters: ["tenant_id": "acme"],
    headers: ["Authorization": "Bearer <token>"]
)
```

### Dynamic headers

```swift
let stream = ShapeStream(
    shape: shape,
    headersProvider: {
        ["Authorization": "Bearer \(await tokenStore.currentToken())"]
    }
)
```

`headersProvider` is resolved for every outgoing poll, SSE connect, and snapshot request. Static `shape.headers` remain the baseline, and dynamic headers override static ones when they share the same key.

### Retry policy and `onError`

```swift
let stream = ShapeStream(
    shape: shape,
    configuration: .init(
        retryPolicy: .init(isEnabled: true)
    ),
    onError: { context in
        switch context.failure {
        case .url:
            return .retry
        default:
            return .stop
        }
    }
)
```

### Restore from a previous checkpoint

```swift
let configuration = ShapeStreamConfiguration(
    initialState: savedState
)

let stream = ShapeStream(
    shape: shape,
    configuration: configuration
)
```

`ShapeStreamState` is `Codable`, so applications can persist and rehydrate stream state if they want to resume from a previous checkpoint.

### Request a snapshot

```swift
let snapshot = try await stream.fetchSnapshot(
    ShapeSubsetRequest(
        whereClause: "completed = false",
        limit: 50
    )
)

let injected = try await shape.requestSnapshot(
    ShapeSubsetRequest(
        whereClause: "completed = false",
        limit: 50
    )
)
```

Use `fetchSnapshot(_:)` when you want the raw snapshot result. Use `requestSnapshot(_:)` when you want the subset to flow into the active in-memory materialized state.

### Choosing `ShapeStream` vs `Shape<Model>`

Use `ShapeStream` if you need protocol-level access, custom consumption logic, or you want to manage materialization yourself.

Use `Shape<Model>` if you want a typed, ergonomic, in-memory view of the shape with minimal application-side merge logic.

## Development

Run the test suite with:

```bash
swift test
```

One integration test requires these environment variables:

- `ELECTRIC_SHAPE_URL`
- `ELECTRIC_TEST_TABLE`

Most of the suite is self-contained, but if you are working on stream lifecycle or wrapper behavior it is worth running targeted tests in addition to the full package suite.

## Notes and Limitations

- `Shape<Model>` and `MaterializedShape<Model>` are in-memory only by default.
- Persistence strategy is application-managed.
- The package is focused on Electric shape streams and typed materialization, not on providing a full ORM or local database layer.
- The public API is intentionally Swift-native and does not attempt to mirror every TypeScript client surface exactly.
