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

let options = ShapeStreamOptions(
    url: URL(string: "https://example.com/v1/shape")!,
    table: "todos"
)

let stream = ShapeStream(
    options: options,
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
    options: ShapeStreamOptions(
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

### `ShapeStreamOptions`

`ShapeStreamOptions` describes the stream you want to open: the Electric endpoint URL, table, optional column selection, optional `whereClause`, positional `whereParams`, `replica`, `log`, additional query `params`, and request headers.

```swift
let options = ShapeStreamOptions(
    url: URL(string: "https://example.com/v1/shape")!,
    table: "todos",
    columns: ["id", "title", "completed"],
    whereClause: "tenant_id = $1",
    whereParams: ["1": "acme"],
    params: ["source_id": "ios-client"],
    headers: ["X-Client": "ios"]
)
```

Use `whereParams` for `$1`, `$2`, and similar placeholders in `whereClause`. Use `params` for additional URL parameters that should be sent alongside Electric's standard shape parameters.

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
- `requestSnapshot(_:)` for changes-only subset materialization

### `MaterializedShape<Model>`

`MaterializedShape<Model>` is the lower-level merge engine used by `Shape<Model>`. Use it directly if you want to manage stream consumption yourself while still decoding fully materialized Swift models.

### Snapshots and Recovery

The package supports both:

- `fetchSnapshot(_:)` for fetching a subset without mutating the running stream
- `requestSnapshot(_:)` for injecting snapshot data into the live stream/session

For failure handling, `ShapeStreamConfiguration` includes a retry policy and `ShapeStream` accepts an async `onError` handler that can stop, retry, or retry with an updated `ShapeStreamOptions`.

## Advanced Usage

### Static Headers, Params, and Log Mode

```swift
let options = ShapeStreamOptions(
    url: URL(string: "https://example.com/v1/shape")!,
    table: "todos",
    whereClause: "tenant_id = $1",
    whereParams: ["1": "acme"],
    params: ["source_id": "ios-client"],
    log: .full,
    headers: ["Authorization": "Bearer <token>"]
)
```

Additional `params` can be a single string, an array serialized as a comma-separated value, or an object serialized as bracketed query parameters:

```swift
let options = ShapeStreamOptions(
    url: URL(string: "https://example.com/v1/shape")!,
    table: "todos",
    params: [
        "source_id": "ios-client",
        "tags": .strings(["inbox", "today"]),
        "metadata": .object(["client": "ios"]),
    ]
)
```

### Dynamic Headers and Params

```swift
let stream = ShapeStream(
    options: options,
    dynamicHeaders: {
        ["Authorization": "Bearer \(await tokenStore.currentToken())"]
    },
    dynamicParams: {
        ["source_id": await sourceStore.currentSourceID()]
    }
)
```

`dynamicHeaders` and `dynamicParams` are resolved for every outgoing poll, SSE connect, and snapshot request. Static `options.headers` and `options.params` remain the baseline, and dynamic values override static values when they share the same key.

Dynamic params are for additional URL parameters, not for changing `whereClause`. A different `whereClause` should be treated as a different shape stream; use `fetchSnapshot(_:)` or `requestSnapshot(_:)` for dynamic subset filters.

### Column mapping and transformation

```swift
let stream = ShapeStream(
    options: ShapeStreamOptions(
        url: URL(string: "https://example.com/v1/shape")!,
        table: "todos",
        columns: ["createdAt"],
        whereClause: "createdAt > $1"
    ),
    columnMapper: snakeCamelMapper(),
    transformer: { row in
        var row = row
        row["loadedLocally"] = .boolean(true)
        return row
    }
)
```

`columnMapper.decode` runs before `transformer`, matching the TypeScript client. `columnMapper.encode` is applied to selected columns, `whereClause`, subset `whereClause`, and subset `orderBy`.

### Retry policy and `onError`

```swift
let stream = ShapeStream(
    options: options,
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
    options: options,
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

let changesOnlyStream = ShapeStream(
    options: ShapeStreamOptions(
        url: URL(string: "https://example.com/v1/shape")!,
        table: "todos",
        log: .changesOnly
    )
)
let shape = Shape<Todo>(stream: changesOnlyStream)

let injected = try await shape.requestSnapshot(
    ShapeSubsetRequest(
        whereClause: "completed = false",
        limit: 50
    )
)
```

Use `fetchSnapshot(_:)` when you want the raw snapshot result in any log mode. Use `requestSnapshot(_:)` in `.changesOnly` mode when you want the subset to flow into the active in-memory materialized state.

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
- The public API follows the TypeScript client’s shape-stream naming and behavior where it maps cleanly to Swift.
