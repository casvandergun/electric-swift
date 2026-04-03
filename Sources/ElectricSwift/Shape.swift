import Foundation

public enum ShapeStatus: String, Sendable, Hashable, Codable {
    case syncing
    case upToDate = "up-to-date"
}

public struct ShapeChange<Model: Sendable>: Sendable {
    public let value: [String: Model]
    public let rows: [Model]
    public let status: ShapeStatus
    public let lastOffset: String
    public let lastSyncedAt: Date?

    public init(
        value: [String: Model],
        rows: [Model],
        status: ShapeStatus,
        lastOffset: String,
        lastSyncedAt: Date?
    ) {
        self.value = value
        self.rows = rows
        self.status = status
        self.lastOffset = lastOffset
        self.lastSyncedAt = lastSyncedAt
    }
}

public enum ShapeError: Error, Sendable, Hashable {
    case stopped
    case failed(String)
}

public actor Shape<Model: Decodable & Sendable> {
    private let stream: ShapeStream
    private let materialized: MaterializedShape<Model>

    private var status: ShapeStatus = .syncing
    private var lastError: ShapeError?
    private var isStopped = false
    private var consumerTask: Task<Void, Never>?
    private var updateContinuations: [UUID: AsyncThrowingStream<ShapeChange<Model>, Error>.Continuation] = [:]
    private var readyContinuations: [UUID: CheckedContinuation<Void, Error>] = [:]

    public init(
        stream: ShapeStream,
        materialized: MaterializedShape<Model> = .init()
    ) {
        self.stream = stream
        self.materialized = materialized
    }

    deinit {
        consumerTask?.cancel()
        guard isStopped == false else { return }
        let stream = stream
        Task {
            await stream.stop()
        }
    }

    public func start() {
        ensureStarted()
    }

    public func stop() async {
        isStopped = true
        let task = consumerTask
        consumerTask = nil
        await stream.stop()
        task?.cancel()
        await task?.value
        finishUpdates()
        failReadyWaiters(with: ShapeError.stopped)
    }

    public func pause() async {
        guard isStopped == false else { return }
        ensureStarted()
        await stream.pause()
    }

    public func resume() async {
        guard isStopped == false else { return }
        await stream.resume()
    }

    public func refresh() async {
        guard isStopped == false else { return }
        ensureStarted()
        await stream.refresh()
    }

    public func requestSnapshot(_ request: ShapeSubsetRequest) async throws -> ShapeSnapshotResult {
        try ensureCanRun()
        ensureStarted()
        let result = try await stream.requestSnapshot(request)
        ensureStarted()
        return result
    }

    public func rows() async throws -> [Model] {
        try await waitUntilReady()
        return try await currentRows()
    }

    public func currentRows() async throws -> [Model] {
        Array(try await currentValue().values)
    }

    public func value() async throws -> [String: Model] {
        try await waitUntilReady()
        return try await currentValue()
    }

    public func currentValue() async throws -> [String: Model] {
        try await materialized.values()
    }

    public func isUpToDate() -> Bool {
        status == .upToDate
    }

    public func lastOffset() async -> String {
        await stream.checkpoint().offset
    }

    public func lastSyncedAt() async -> Date? {
        await stream.checkpoint().lastSyncedAt
    }

    public func error() -> Error? {
        lastError
    }

    public nonisolated func updates() -> AsyncThrowingStream<ShapeChange<Model>, Error> {
        AsyncThrowingStream { continuation in
            let id = UUID()
            Task {
                await self.registerUpdateContinuation(id: id, continuation: continuation)
            }
            continuation.onTermination = { _ in
                Task {
                    await self.removeUpdateContinuation(id: id)
                }
            }
        }
    }

    private func ensureCanRun() throws {
        if let lastError {
            throw lastError
        }
        if isStopped {
            throw ShapeError.stopped
        }
    }

    private func ensureStarted() {
        guard consumerTask == nil else { return }
        guard isStopped == false else { return }

        consumerTask = Task {
            await consumeStream()
        }
    }

    private func consumeStream() async {
        defer {
            consumerTask = nil
        }

        do {
            for try await batch in await stream.batches() {
                await materialized.apply(batch)
                switch batch.boundaryKind {
                case .mustRefetch:
                    status = .syncing
                case .upToDate:
                    status = .upToDate
                case .liveUpdate:
                    let streamState = await stream.currentState()
                    status = streamState.isUpToDate ? .upToDate : .syncing
                }

                let value = try await materialized.values()
                let change = ShapeChange(
                    value: value,
                    rows: Array(value.values),
                    status: status,
                    lastOffset: batch.checkpoint.offset,
                    lastSyncedAt: batch.checkpoint.lastSyncedAt
                )
                yield(change)

                if status == .upToDate {
                    fulfillReadyWaiters()
                }
            }

            if isStopped {
                finishUpdates()
                failReadyWaiters(with: ShapeError.stopped)
            } else if status == .upToDate {
                fulfillReadyWaiters()
                finishUpdates()
            } else {
                finishUpdates()
                failReadyWaiters(with: ShapeError.stopped)
            }
        } catch {
            let wrappedError = wrap(error)
            lastError = wrappedError
            finishUpdates(throwing: wrappedError)
            failReadyWaiters(with: wrappedError)
        }
    }

    private func waitUntilReady() async throws {
        try ensureCanRun()
        ensureStarted()

        if status == .upToDate {
            return
        }

        try await withCheckedThrowingContinuation { continuation in
            readyContinuations[UUID()] = continuation
        }
    }

    private func registerUpdateContinuation(
        id: UUID,
        continuation: AsyncThrowingStream<ShapeChange<Model>, Error>.Continuation
    ) {
        if let lastError {
            continuation.finish(throwing: lastError)
            return
        }
        if isStopped {
            continuation.finish()
            return
        }

        updateContinuations[id] = continuation
        ensureStarted()
    }

    private func removeUpdateContinuation(id: UUID) {
        updateContinuations.removeValue(forKey: id)
    }

    private func yield(_ change: ShapeChange<Model>) {
        for continuation in updateContinuations.values {
            continuation.yield(change)
        }
    }

    private func finishUpdates(throwing error: Error? = nil) {
        let continuations = updateContinuations.values
        updateContinuations.removeAll(keepingCapacity: false)
        for continuation in continuations {
            if let error {
                continuation.finish(throwing: error)
            } else {
                continuation.finish()
            }
        }
    }

    private func fulfillReadyWaiters() {
        let continuations = readyContinuations.values
        readyContinuations.removeAll(keepingCapacity: false)
        for continuation in continuations {
            continuation.resume()
        }
    }

    private func failReadyWaiters(with error: Error) {
        let continuations = readyContinuations.values
        readyContinuations.removeAll(keepingCapacity: false)
        for continuation in continuations {
            continuation.resume(throwing: error)
        }
    }

    private func wrap(_ error: Error) -> ShapeError {
        if let shapeError = error as? ShapeError {
            return shapeError
        }
        return .failed(String(describing: error))
    }
}
