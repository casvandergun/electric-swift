import Foundation

public enum ShapeStreamError: Error, Sendable, Hashable, Codable {
    case invalidResponse
    case invalidStatusCode(Int)
    case missingHeaders([String], url: String)
    case staleCacheLoopExceeded(shapeKey: String, retries: Int)
    case fastLoopDetected(shapeKey: String, offset: String, attempts: Int)
}

internal enum ElectricShapeRequestMode {
    case catchUp
    case liveLongPoll
    case liveSSE

    var isLive: Bool {
        switch self {
        case .catchUp:
            false
        case .liveLongPoll, .liveSSE:
            true
        }
    }
}

public actor ShapeStream {
    private static let manualPauseReason = "manual"
    private static let snapshotPauseReason = "snapshot"
    private static let sseRetryBaseDelay: TimeInterval = 0.1
    private static let sseRetryMaxDelay: TimeInterval = 5
    private static let fastLoopWindow: TimeInterval = 0.5
    private static let fastLoopRequestThreshold = 5
    private static let fastLoopMaxRecoveries = 5
    private static let fastLoopRetryBaseDelay: TimeInterval = 0.1
    private static let fastLoopRetryMaxDelay: TimeInterval = 5

    private var shape: ElectricShape
    private let configuration: ShapeStreamConfiguration
    private let transport: any ElectricShapeTransport
    private let parser: ElectricParser
    private let headersProvider: ShapeStreamHeadersProvider?
    private let debugLogger: ElectricDebugLogger
    private let recoveryPolicy: ShapeStreamRecoveryPolicy
    private let onError: ShapeStreamErrorHandler?
    private let decoder = JSONDecoder()
    private var shapeKey: String

    private var state: ShapeStreamState
    private var bufferedMessages: [ElectricMessage] = []
    private var pendingInjectedBatches: [ElectricShapeBatch] = []
    private var isStopped = false
    private var replayCursor: String?
    private var consecutiveShortSSEConnections = 0
    private var consecutiveSSEBackoffAttempts = 0
    private var useLongPollFallback = false
    private var preferLongPollAfter204 = false
    private var staleCacheRetryCount = 0
    private var staleCacheBuster: String?
    private var pauseReasons: Set<String> = []
    private var forceCatchUpBoundary = false
    private var currentPollTask: Task<ElectricShapeBatch?, Error>?
    private var requestGeneration = 0
    private var fastLoopOffset: String?
    private var fastLoopRequestTimes: [Date] = []
    private var consecutiveFastLoopRecoveries = 0
    private var snapshotTracker = SnapshotTracker()

    public init(
        shape: ElectricShape,
        configuration: ShapeStreamConfiguration = .init(),
        session: URLSession = .shared,
        parser: ElectricParser = .default,
        headersProvider: ShapeStreamHeadersProvider? = nil,
        debugLogger: ElectricDebugLogger = .disabled,
        onError: ShapeStreamErrorHandler? = nil
    ) {
        self.init(
            shape: shape,
            configuration: configuration,
            transport: URLSessionElectricShapeTransport(session: session),
            parser: parser,
            headersProvider: headersProvider,
            debugLogger: debugLogger,
            recoveryPolicy: .live,
            onError: onError
        )
    }

    init(
        shape: ElectricShape,
        configuration: ShapeStreamConfiguration = .init(),
        transport: any ElectricShapeTransport,
        parser: ElectricParser = .default,
        headersProvider: ShapeStreamHeadersProvider? = nil,
        debugLogger: ElectricDebugLogger = .disabled,
        recoveryPolicy: ShapeStreamRecoveryPolicy = .live,
        onError: ShapeStreamErrorHandler? = nil
    ) {
        self.shape = shape
        self.configuration = configuration
        self.transport = transport
        self.parser = parser
        self.headersProvider = headersProvider
        self.debugLogger = debugLogger
        self.recoveryPolicy = recoveryPolicy
        self.onError = onError
        self.shapeKey = ShapeRequestBuilder.canonicalShapeKey(shape: shape)

        var initialState = configuration.initialState
        if initialState.phase == .initial && initialState.isLive {
            initialState.phase = .liveLongPoll
        }
        self.state = initialState
    }

    deinit {
        currentPollTask?.cancel()
    }

    public func stop() {
        isStopped = true
        pauseReasons.removeAll(keepingCapacity: false)
        forceCatchUpBoundary = false
        clearTransientRequestState()
        pendingInjectedBatches.removeAll(keepingCapacity: true)
        resetFastLoopState()
        resetSSERecoveryState()
        preferLongPollAfter204 = false
        requestGeneration += 1
        currentPollTask?.cancel()
        currentPollTask = nil
        state.phase = .stopped
        debugLogger.log(.info, category: "ShapeStream", message: "stop requested", metadata: shapeMetadata())
    }

    public func pause() {
        acquirePause(reason: Self.manualPauseReason)
    }

    public func resume() {
        releasePause(reason: Self.manualPauseReason)
    }

    public func refresh() async {
        guard isStopped == false else { return }
        guard pauseReasons.isEmpty else { return }

        let shouldCancelCurrentPoll = currentPollTask != nil
        forceCatchUpBoundary = true
        clearTransientRequestState()
        pendingInjectedBatches.removeAll(keepingCapacity: true)
        resetFastLoopState()
        resetSSERecoveryState()
        preferLongPollAfter204 = false
        state.isUpToDate = false
        requestGeneration += 1
        if configuration.subscribe {
            state.phase = .syncing
        }

        if shouldCancelCurrentPoll {
            currentPollTask?.cancel()
        }

        debugLogger.log(
            .info,
            category: "ShapeStream",
            message: "refresh requested",
            metadata: shapeMetadata()
        )
    }

    public func currentState() -> ShapeStreamState {
        state
    }

    public func checkpoint() -> ElectricShapeCheckpoint {
        state.checkpoint
    }

    public func phase() -> ElectricShapePhase {
        state.phase
    }

    public func fetchSnapshot(_ request: ShapeSubsetRequest) async throws -> ShapeSnapshotResult {
        try await fetchSnapshotInternal(request, updateStateAfterSuccess: false)
    }

    public func requestSnapshot(_ request: ShapeSubsetRequest) async throws -> ShapeSnapshotResult {
        guard isStopped == false else {
            return try await fetchSnapshotInternal(request, updateStateAfterSuccess: false)
        }

        acquirePause(reason: Self.snapshotPauseReason)
        requestGeneration += 1
        currentPollTask?.cancel()
        currentPollTask = nil

        do {
            let result = try await fetchSnapshotInternal(request, updateStateAfterSuccess: true)
            let snapshotMessages = result.messages + [
                ElectricMessage(
                    headers: .init(
                        control: .snapshotEnd,
                        snapshotMark: result.metadata.snapshotMark,
                        databaseLSN: result.metadata.databaseLSN,
                        xmin: result.metadata.xmin,
                        xmax: result.metadata.xmax,
                        xipList: result.metadata.xipList
                    )
                ),
                ElectricMessage(headers: .init(control: .subsetEnd)),
            ]
            snapshotTracker.addSnapshot(result.metadata, keys: Set(result.messages.compactMap(\.key)))
            let batch = ElectricShapeBatch(
                messages: snapshotMessages,
                checkpoint: state.checkpoint,
                schema: state.schema,
                phase: state.phase,
                boundaryKind: .liveUpdate
            )
            pendingInjectedBatches.append(batch)
            releasePause(reason: Self.snapshotPauseReason)
            return result
        } catch {
            releasePause(reason: Self.snapshotPauseReason)
            throw error
        }
    }

    public func batches() -> AsyncThrowingStream<ElectricShapeBatch, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    while Task.isCancelled == false {
                        if self.isStopped {
                            continuation.finish()
                            return
                        }

                        if let batch = try await self.poll() {
                            self.debugLogger.log(
                                .info,
                                category: "ShapeStream",
                                message: "yielding batch",
                                metadata: self.shapeMetadata([
                                    "messages": String(batch.messages.count),
                                    "offset": batch.checkpoint.offset,
                                    "phase": String(describing: batch.phase),
                                    "boundary": batch.boundaryKind.rawValue,
                                ])
                            )
                            continuation.yield(batch)
                        }

                        if self.isStopped {
                            continuation.finish()
                            return
                        }

                        if self.configuration.subscribe == false, self.state.isUpToDate {
                            self.state.phase = .stopped
                            continuation.finish()
                            return
                        }
                    }

                    continuation.finish()
                } catch {
                    self.state.phase = .failed(String(describing: error))
                    self.debugLogger.log(
                        .error,
                        category: "ShapeStream",
                        message: "session failed",
                        metadata: self.shapeMetadata(["error": String(describing: error)])
                    )
                    continuation.finish(throwing: error)
                }
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }

    public func poll() async throws -> ElectricShapeBatch? {
        while true {
            if let injected = pendingInjectedBatches.first {
                pendingInjectedBatches.removeFirst()
                return injected
            }

            if isStopped {
                return nil
            }

            await waitWhilePaused()
            if isStopped || Task.isCancelled {
                return nil
            }

            enterReplayIfNeeded()

            let mode = currentRequestMode()
            let generation = requestGeneration
            debugLogger.log(
                .debug,
                category: "ShapeStream",
                message: "starting poll",
                metadata: shapeMetadata(["mode": requestModeName(mode)])
            )
            let task = Task { [mode, generation] in
                switch mode {
                case .liveSSE:
                    return try await self.pollSSE(generation: generation)
                case .catchUp, .liveLongPoll:
                    return try await self.pollHTTP(mode: mode, generation: generation)
                }
            }
            currentPollTask = task

            do {
                let batch = try await task.value
                currentPollTask = nil
                return batch
            } catch is CancellationError {
                currentPollTask = nil
                if isStopped || pauseReasons.isEmpty == false || forceCatchUpBoundary {
                    return nil
                }
                throw CancellationError()
            } catch {
                currentPollTask = nil
                if await recoverFromError(error) {
                    continue
                }
                throw error
            }
        }
    }

    private func currentRequestMode() -> ElectricShapeRequestMode {
        if forceCatchUpBoundary {
            return .catchUp
        }
        if configuration.subscribe && state.isUpToDate {
            if configuration.preferSSE && useLongPollFallback == false && preferLongPollAfter204 == false {
                return .liveSSE
            }
            return .liveLongPoll
        }
        return .catchUp
    }

    private func enterReplayIfNeeded() {
        guard state.phase == .initial else { return }
        guard state.offset == "-1", state.cursor == nil else { return }
        guard let replayCursor = ElectricTrackers.upToDate.replayCursorIfRecent(for: shapeKey) else { return }

        self.replayCursor = replayCursor
        state.cursor = replayCursor
        state.phase = .replaying
    }

    private func pollHTTP(mode: ElectricShapeRequestMode, generation: Int) async throws -> ElectricShapeBatch? {
        if mode == .catchUp, try await recoverFromFastLoopIfNeeded() {
            return try await pollHTTP(mode: .catchUp, generation: generation)
        }

        let request = try await makeRequest(
            shape: shape,
            state: state,
            timeout: configuration.timeout,
            mode: mode,
            staleCacheBuster: staleCacheBuster
        )
        let result = try await performHTTPFetchWithRetry(request: request)
        try checkRequestGeneration(generation)
        try Task.checkCancellation()
        logHTTPResponse(result.response, mode: mode)

        guard try handleInitialResponse(response: result.response, request: request, mode: mode) else {
            return nil
        }

        switch result.response.statusCode {
        case 200:
            return try handleChunkResponse(data: result.data, response: result.response, mode: mode, isSSE: false)
        case 204:
            return handleEmptyResponse()
        case 409:
            return handleConflictResponse(response: result.response)
        default:
            throw FetchError.from(response: result.response, data: result.data, url: request.url?.absoluteString ?? shape.url.absoluteString)
        }
    }

    private func pollSSE(generation: Int) async throws -> ElectricShapeBatch? {
        let request = try await makeRequest(
            shape: shape,
            state: state,
            timeout: configuration.timeout,
            mode: .liveSSE,
            staleCacheBuster: staleCacheBuster
        )
        let startedAt = Date()

        do {
            let result = try await performSSEOpenWithRetry(request: request)
            try checkRequestGeneration(generation)
            try Task.checkCancellation()
            logHTTPResponse(result.response, mode: .liveSSE)
            guard try handleInitialResponse(response: result.response, request: request, mode: .liveSSE) else {
                _ = handleSSEClosure(startedAt: startedAt, wasAborted: false)
                return nil
            }

            switch result.response.statusCode {
            case 200:
                var pendingData = Data()
                for try await chunk in result.chunks {
                    if isStopped {
                        _ = handleSSEClosure(startedAt: startedAt, wasAborted: true)
                        return nil
                    }

                    let parsed = ElectricSSEParser.parse(data: chunk, pendingData: pendingData)
                    pendingData = parsed.remaining
                    debugLogger.log(
                        .trace,
                        category: "ShapeStream",
                        message: "parsed SSE chunk",
                        metadata: shapeMetadata([
                            "mode": requestModeName(.liveSSE),
                            "bytes": String(chunk.count),
                            "events": String(parsed.events.count),
                            "remainingBytes": String(pendingData.count),
                        ])
                    )
                    for event in parsed.events where event.data.isEmpty == false {
                        debugLogger.log(
                            .debug,
                            category: "ShapeStream",
                            message: "received SSE event",
                            metadata: shapeMetadata([
                                "event": event.effectiveEvent,
                                "eventID": event.id ?? "",
                                "bytes": String(event.data.utf8.count),
                            ])
                        )
                        let data = Data(event.data.utf8)
                        let decoded = try decoder.decode(ElectricMessage.self, from: data)
                        let message = try PostgresValueParser.coerce(
                            messages: [decoded],
                            schema: state.schema,
                            parser: parser
                        ).first ?? decoded
                        logMessage(message, source: "sse", eventName: event.effectiveEvent)
                        bufferedMessages.append(message)

                        if message.headers.control == .upToDate {
                            if let lsn = message.headers.globalLastSeenLSN {
                                state.offset = "\(lsn)_0"
                            }
                            resetSSERecoveryState()
                            let batch = finalizeBufferedMessages(boundaryKind: state.isUpToDate ? .liveUpdate : .upToDate, isSSE: true)
                            if batch != nil {
                                return batch
                            }
                        }
                    }
                }

                try checkRequestGeneration(generation)
                try Task.checkCancellation()
                if let retryDelay = handleSSEClosure(startedAt: startedAt, wasAborted: false) {
                    try await recoveryPolicy.sleep(retryDelay)
                }
                return nil
            case 204:
                _ = handleSSEClosure(startedAt: startedAt, wasAborted: false)
                return handleEmptyResponse()
            case 409:
                _ = handleSSEClosure(startedAt: startedAt, wasAborted: false)
                return handleConflictResponse(response: result.response)
            default:
                throw FetchError.from(response: result.response, data: Data(), url: request.url?.absoluteString ?? shape.url.absoluteString)
            }
        } catch is CancellationError {
            _ = handleSSEClosure(startedAt: startedAt, wasAborted: true)
            return nil
        } catch {
            _ = handleSSEClosure(startedAt: startedAt, wasAborted: false)
            throw error
        }
    }

    private func handleInitialResponse(
        response: HTTPURLResponse,
        request: URLRequest,
        mode: ElectricShapeRequestMode
    ) throws -> Bool {
        if response.statusCode == 409 {
            return true
        }

        if let action = staleCacheAction(for: response) {
            switch action {
            case .ignore:
                return false
            case .retry:
                throw ShapeStreamError.staleCacheLoopExceeded(shapeKey: shapeKey, retries: staleCacheRetryCount)
            }
        }

        try validateResponseHeaders(response: response, mode: mode, url: request.url?.absoluteString ?? shape.url.absoluteString)
        updateState(from: response, mode: mode)
        staleCacheRetryCount = 0
        staleCacheBuster = nil
        return true
    }

    private enum StaleCacheAction {
        case ignore
        case retry
    }

    private func staleCacheAction(for response: HTTPURLResponse) -> StaleCacheAction? {
        guard let expiredHandle = ElectricCaches.expiredShapes.getExpiredHandle(for: shapeKey) else {
            return nil
        }
        guard let responseHandle = response.value(forHTTPHeaderField: ElectricProtocolValues.handleHeader) else {
            return nil
        }
        guard responseHandle == expiredHandle else {
            return nil
        }

        if state.handle == nil || state.handle == expiredHandle {
            staleCacheRetryCount += 1
            staleCacheBuster = UUID().uuidString
            state.phase = .staleRetry
            if staleCacheRetryCount > configuration.maxStaleCacheRetries {
                return .retry
            }
            debugLogger.log(
                .info,
                category: "ShapeStream",
                message: "stale cached response detected; retrying with cache buster",
                metadata: shapeMetadata([
                    "expiredHandle": expiredHandle,
                    "retry": String(staleCacheRetryCount),
                ])
            )
            return .ignore
        }

        debugLogger.log(
            .info,
            category: "ShapeStream",
            message: "ignored stale cached response with expired handle",
            metadata: shapeMetadata([
                "expiredHandle": expiredHandle,
                "currentHandle": state.handle ?? "",
            ])
        )
        return .ignore
    }

    private func validateResponseHeaders(
        response: HTTPURLResponse,
        mode: ElectricShapeRequestMode,
        url: String
    ) throws {
        guard response.statusCode == 200 || response.statusCode == 204 else {
            return
        }

        var required = [
            ElectricProtocolValues.handleHeader,
            ElectricProtocolValues.offsetHeader,
        ]
        if mode.isLive {
            required.append(ElectricProtocolValues.cursorHeader)
        } else if state.schema.isEmpty {
            required.append(ElectricProtocolValues.schemaHeader)
        }

        let missing = required.filter { response.value(forHTTPHeaderField: $0)?.isEmpty != false }
        if missing.isEmpty == false {
            throw ShapeStreamError.missingHeaders(missing, url: url)
        }
    }

    private func handleChunkResponse(
        data: Data,
        response: HTTPURLResponse,
        mode: ElectricShapeRequestMode,
        isSSE: Bool
    ) throws -> ElectricShapeBatch? {
        let messages = try decodeMessages(from: data, schema: state.schema, source: isSSE || mode == .liveSSE ? "sse" : "http")
        bufferedMessages.append(contentsOf: messages)
        let reachedUpToDate =
            response.value(forHTTPHeaderField: ElectricProtocolValues.upToDateHeader) != nil
            || messages.last?.headers.control == .upToDate

        guard reachedUpToDate else {
            state.phase = if state.phase == .replaying { .replaying } else { .syncing }
            debugLogger.log(
                .debug,
                category: "ShapeBatch",
                message: "buffered messages waiting for boundary",
                metadata: shapeMetadata([
                    "messages": String(bufferedMessages.count),
                    "mode": requestModeName(mode),
                ])
            )
            return nil
        }

        return finalizeBufferedMessages(
            boundaryKind: state.isUpToDate ? .liveUpdate : .upToDate,
            isSSE: isSSE || mode == .liveSSE
        )
    }

    private func finalizeBufferedMessages(
        boundaryKind: ElectricShapeBoundaryKind,
        isSSE: Bool
    ) -> ElectricShapeBatch? {
        guard bufferedMessages.isEmpty == false else {
            return nil
        }

        let resolvedBoundaryKind = forceCatchUpBoundary ? .upToDate : boundaryKind
        let previousPhase = state.phase
        state.isUpToDate = resolvedBoundaryKind != .mustRefetch
        state.lastSyncedAt = Date()
        state.phase = nextPhaseAfterBoundary(boundaryKind: resolvedBoundaryKind, isSSE: isSSE)
        forceCatchUpBoundary = false
        resetFastLoopState()

        if let cursor = state.cursor {
            ElectricTrackers.upToDate.recordUpToDate(shapeKey: shapeKey, cursor: cursor)
        }

        if previousPhase == .replaying, replayCursor == state.cursor {
            debugLogger.log(
                .info,
                category: "ShapeBatch",
                message: "suppressed replayed batch",
                metadata: shapeMetadata([
                    "messages": String(bufferedMessages.count),
                    "boundary": resolvedBoundaryKind.rawValue,
                ])
            )
            bufferedMessages.removeAll(keepingCapacity: true)
            return nil
        }

        let batch = ElectricShapeBatch(
            messages: filteredMessages(bufferedMessages),
            checkpoint: state.checkpoint,
            schema: state.schema,
            phase: state.phase,
            boundaryKind: resolvedBoundaryKind
        )
        debugLogger.log(
            .info,
            category: "ShapeBatch",
            message: "finalized batch",
            metadata: shapeMetadata([
                "messages": String(bufferedMessages.count),
                "boundary": resolvedBoundaryKind.rawValue,
                "phase": String(describing: state.phase),
                "isSSE": String(isSSE),
            ])
        )
        bufferedMessages.removeAll(keepingCapacity: true)
        return batch
    }

    private func nextPhaseAfterBoundary(
        boundaryKind: ElectricShapeBoundaryKind,
        isSSE: Bool
    ) -> ElectricShapePhase {
        if boundaryKind == .mustRefetch {
            return .initial
        }
        guard configuration.subscribe else {
            return .syncing
        }
        if configuration.preferSSE && useLongPollFallback == false && preferLongPollAfter204 == false {
            return isSSE ? .liveSSE : .liveSSE
        }
        return .liveLongPoll
    }

    private func handleEmptyResponse() -> ElectricShapeBatch? {
        let becameUpToDate = state.isUpToDate == false
        let forcedBoundary = forceCatchUpBoundary
        state.isUpToDate = true
        state.lastSyncedAt = Date()
        state.phase = configuration.subscribe ? .liveLongPoll : .syncing
        preferLongPollAfter204 = configuration.subscribe
        forceCatchUpBoundary = false
        resetFastLoopState()

        guard becameUpToDate || forcedBoundary else {
            return nil
        }

        debugLogger.log(
            .info,
            category: "ShapeBatch",
            message: "received empty up-to-date response",
            metadata: shapeMetadata([
                "forcedBoundary": String(forcedBoundary),
                "phase": String(describing: state.phase),
            ])
        )

        return ElectricShapeBatch(
            messages: [.upToDate()],
            checkpoint: state.checkpoint,
            schema: state.schema,
            phase: state.phase,
            boundaryKind: .upToDate
        )
    }

    private func handleConflictResponse(response: HTTPURLResponse) -> ElectricShapeBatch {
        if let expiredHandle = state.handle {
            ElectricCaches.expiredShapes.markExpired(shapeKey: shapeKey, handle: expiredHandle)
        }
        ElectricTrackers.upToDate.delete(shapeKey: shapeKey)

        let replacementHandle = extractHandle(from: response)
        state.reset(handle: replacementHandle)
        state.phase = .initial
        replayCursor = nil
        staleCacheRetryCount = 0
        staleCacheBuster = replacementHandle == nil ? UUID().uuidString : nil
        forceCatchUpBoundary = false
        preferLongPollAfter204 = false
        bufferedMessages.removeAll(keepingCapacity: true)
        pendingInjectedBatches.removeAll(keepingCapacity: true)
        resetFastLoopState()
        resetSSERecoveryState()
        debugLogger.log(
            .info,
            category: "ShapeBatch",
            message: "shape invalidated and requires refetch",
            metadata: shapeMetadata([
                "replacementHandle": replacementHandle ?? "",
                "cacheBusterArmed": String(replacementHandle == nil),
            ])
        )

        return ElectricShapeBatch(
            messages: [.mustRefetch()],
            checkpoint: state.checkpoint,
            schema: state.schema,
            phase: state.phase,
            boundaryKind: .mustRefetch
        )
    }

    private func handleSSEClosure(startedAt: Date, wasAborted: Bool) -> TimeInterval? {
        let duration = Date().timeIntervalSince(startedAt)
        var retryDelay: TimeInterval?
        if duration < configuration.minSSEConnectionDuration && !wasAborted {
            consecutiveShortSSEConnections += 1
            if consecutiveShortSSEConnections >= configuration.maxShortSSEConnections {
                useLongPollFallback = true
                consecutiveSSEBackoffAttempts = 0
                state.phase = .liveLongPoll
            } else {
                consecutiveSSEBackoffAttempts += 1
                retryDelay = fullJitterDelay(
                    attempt: consecutiveSSEBackoffAttempts,
                    base: Self.sseRetryBaseDelay,
                    max: Self.sseRetryMaxDelay
                )
                state.phase = .liveSSE
            }
        } else if duration >= configuration.minSSEConnectionDuration {
            resetSSERecoveryState()
            state.phase = .liveSSE
        }
        debugLogger.log(
            .debug,
            category: "ShapeStream",
            message: "handled SSE closure",
            metadata: shapeMetadata([
                "durationMS": String(Int(duration * 1000)),
                "aborted": String(wasAborted),
                "shortConnections": String(consecutiveShortSSEConnections),
                "retryDelayMS": retryDelay.map { String(Int($0 * 1000)) } ?? "0",
                "fallbackToLongPoll": String(useLongPollFallback),
                "phase": String(describing: state.phase),
            ])
        )
        return retryDelay
    }

    private func updateState(from response: HTTPURLResponse, mode: ElectricShapeRequestMode) {
        let previousHandle = state.handle
        let previousOffset = state.offset
        if let handle = response.value(forHTTPHeaderField: ElectricProtocolValues.handleHeader) {
            state.handle = handle
        }

        if let offset = response.value(forHTTPHeaderField: ElectricProtocolValues.offsetHeader) {
            state.offset = offset
        }

        if let cursor = response.value(forHTTPHeaderField: ElectricProtocolValues.cursorHeader),
           cursor.isEmpty == false {
            state.cursor = cursor
        }

        if let schemaValue = response.value(forHTTPHeaderField: ElectricProtocolValues.schemaHeader),
           schemaValue.isEmpty == false,
           state.schema.isEmpty,
           let schemaData = schemaValue.data(using: .utf8),
           let schema = try? decoder.decode(ElectricSchema.self, from: schemaData) {
            state.schema = schema
        }

        if mode == .catchUp, state.phase != .replaying {
            state.phase = .syncing
        } else if mode == .liveLongPoll {
            state.phase = .liveLongPoll
        } else if mode == .liveSSE {
            state.phase = .liveSSE
        }

        if previousHandle != state.handle || previousOffset != state.offset {
            resetFastLoopRequestHistory()
        }
        if previousOffset != state.offset {
            consecutiveFastLoopRecoveries = 0
        }
        if previousHandle != state.handle {
            resetSSERecoveryState()
        }
    }

    private func decodeMessages(from data: Data, schema: ElectricSchema, source: String) throws -> [ElectricMessage] {
        if data.isEmpty {
            return []
        }
        let messages = try decoder.decode([ElectricMessage].self, from: data)
        let coercedMessages = try PostgresValueParser.coerce(messages: messages, schema: schema, parser: parser)
        for message in coercedMessages {
            logMessage(message, source: source)
        }
        return coercedMessages
    }

    private func extractHandle(from response: HTTPURLResponse) -> String? {
        if let location = response.value(forHTTPHeaderField: ElectricProtocolValues.locationHeader),
           let components = URLComponents(string: location),
           let handle = components.queryItems?.first(where: { $0.name == "handle" })?.value {
            return handle
        }

        return response.value(forHTTPHeaderField: ElectricProtocolValues.handleHeader)
    }

    private func shapeMetadata(_ extra: [String: String] = [:]) -> [String: String] {
        var metadata: [String: String] = [
            "table": shape.table ?? "",
            "where": shape.whereClause ?? "",
            "offset": state.offset,
            "phase": String(describing: state.phase),
        ]
        for (key, value) in extra {
            metadata[key] = value
        }
        return metadata
    }

    private func requestModeName(_ mode: ElectricShapeRequestMode) -> String {
        switch mode {
        case .catchUp:
            "catchUp"
        case .liveLongPoll:
            "liveLongPoll"
        case .liveSSE:
            "liveSSE"
        }
    }

    private func logHTTPResponse(_ response: HTTPURLResponse, mode: ElectricShapeRequestMode) {
        debugLogger.log(
            .debug,
            category: "ShapeStream",
            message: "received shape response",
            metadata: shapeMetadata([
                "mode": requestModeName(mode),
                "statusCode": String(response.statusCode),
                "responseHandle": response.value(forHTTPHeaderField: ElectricProtocolValues.handleHeader) ?? "",
                "responseOffset": response.value(forHTTPHeaderField: ElectricProtocolValues.offsetHeader) ?? "",
                "responseCursor": response.value(forHTTPHeaderField: ElectricProtocolValues.cursorHeader) ?? "",
            ])
        )
    }

    private func logMessage(
        _ message: ElectricMessage,
        source: String,
        eventName: String? = nil
    ) {
        var metadata = shapeMetadata([
            "source": source,
            "key": message.normalizedKey ?? message.key ?? "",
            "operation": message.headers.operation?.rawValue ?? "",
            "control": message.headers.control?.rawValue ?? "",
            "event": eventName ?? message.headers.event ?? "",
            "hasValue": String(message.value != nil),
            "hasOldValue": String(message.oldValue != nil),
        ])
        if let txids = message.headers.txids, txids.isEmpty == false {
            metadata["txids"] = txids.map(String.init).joined(separator: ",")
        }

        debugLogger.log(
            .debug,
            category: "ShapeMessage",
            message: "processed incoming message",
            metadata: metadata
        )
    }

    private func acquirePause(reason: String) {
        guard isStopped == false else { return }
        guard pauseReasons.insert(reason).inserted else { return }

        clearTransientRequestState()
        resetFastLoopState()
        resetSSERecoveryState()
        requestGeneration += 1
        state.phase = .paused
        currentPollTask?.cancel()

        debugLogger.log(
            .info,
            category: "ShapeStream",
            message: "pause requested",
            metadata: shapeMetadata(["reason": reason])
        )
    }

    private func releasePause(reason: String) {
        guard pauseReasons.remove(reason) != nil else { return }
        guard pauseReasons.isEmpty else { return }

        if configuration.subscribe {
            forceCatchUpBoundary = true
            state.isUpToDate = false
        }
        resetFastLoopState()
        if state.phase == .paused {
            state.phase = .syncing
        }

        debugLogger.log(
            .info,
            category: "ShapeStream",
            message: "resume requested",
            metadata: shapeMetadata(["reason": reason])
        )
    }

    private func fetchSnapshotInternal(
        _ request: ShapeSubsetRequest,
        updateStateAfterSuccess: Bool
    ) async throws -> ShapeSnapshotResult {
        let originalShape = shape
        var attempt = 0

        while true {
            let snapshotRequest = try await makeSnapshotRequest(
                shape: shape,
                state: state,
                timeout: configuration.timeout,
                subset: request,
                staleCacheBuster: staleCacheBuster
            )

            do {
                let result = try await performHTTPFetchWithRetry(request: snapshotRequest)
                switch result.response.statusCode {
                case 200:
                    let payload = try decoder.decode(ShapeSnapshotPayload.self, from: result.data)
                    let effectiveSchema = snapshotSchema(from: result.response)
                    let messages = try PostgresValueParser.coerce(
                        messages: payload.data,
                        schema: effectiveSchema,
                        parser: parser
                    )
                    let responseHandle = result.response.value(forHTTPHeaderField: ElectricProtocolValues.handleHeader)
                    let responseOffset = result.response.value(forHTTPHeaderField: ElectricProtocolValues.offsetHeader)

                    if updateStateAfterSuccess {
                        if let responseHandle {
                            state.handle = responseHandle
                        }
                        if let responseOffset {
                            state.offset = responseOffset
                        }
                        if state.schema.isEmpty {
                            state.schema = effectiveSchema
                        }
                    }

                    return ShapeSnapshotResult(
                        metadata: payload.metadata,
                        messages: messages,
                        responseOffset: responseOffset,
                        responseHandle: responseHandle
                    )
                case 409:
                    if let currentHandle = state.handle {
                        ElectricCaches.expiredShapes.markExpired(shapeKey: shapeKey, handle: currentHandle)
                    }
                    let nextHandle = extractHandle(from: result.response)
                    state.handle = nextHandle
                    staleCacheBuster = nextHandle == nil ? UUID().uuidString : nil
                    attempt += 1
                    if attempt > configuration.maxStaleCacheRetries {
                        throw FetchError.from(
                            response: result.response,
                            data: result.data,
                            url: snapshotRequest.url?.absoluteString ?? originalShape.url.absoluteString
                        )
                    }
                    continue
                default:
                    throw FetchError.from(
                        response: result.response,
                        data: result.data,
                        url: snapshotRequest.url?.absoluteString ?? originalShape.url.absoluteString
                    )
                }
            } catch {
                if await recoverFromError(error) {
                    attempt += 1
                    continue
                }
                throw error
            }
        }
    }

    private func recoverFromError(_ error: Error) async -> Bool {
        if let streamError = error as? ShapeStreamError,
           case .missingHeaders = streamError {
            return false
        }
        guard let onError else { return false }

        let decision = await onError(
            ShapeStreamErrorContext(
                failure: ShapeStreamFailure.wrap(error),
                shape: shape,
                state: state
            )
        )

        switch decision {
        case .stop:
            return false
        case .retry:
            resetFastLoopState()
            return true
        case .retryWithShape(let newShape):
            resetFastLoopState()
            shape = newShape
            shapeKey = ShapeRequestBuilder.canonicalShapeKey(shape: newShape)
            return true
        }
    }

    private func resolveRequestHeaders() async throws -> [String: String] {
        var headers = shape.headers
        if let headersProvider {
            let dynamicHeaders = try await headersProvider()
            for (key, value) in dynamicHeaders {
                headers[key] = value
            }
        }
        return headers
    }

    private func makeRequest(
        shape: ElectricShape,
        state: ShapeStreamState,
        timeout: TimeInterval,
        mode: ElectricShapeRequestMode,
        staleCacheBuster: String? = nil
    ) async throws -> URLRequest {
        let headers = try await resolveRequestHeaders()
        return ShapeRequestBuilder.makeRequest(
            shape: shape,
            state: state,
            timeout: timeout,
            mode: mode,
            headers: headers,
            staleCacheBuster: staleCacheBuster
        )
    }

    private func makeSnapshotRequest(
        shape: ElectricShape,
        state: ShapeStreamState,
        timeout: TimeInterval,
        subset: ShapeSubsetRequest,
        staleCacheBuster: String? = nil
    ) async throws -> URLRequest {
        let headers = try await resolveRequestHeaders()
        return try ShapeRequestBuilder.makeSnapshotRequest(
            shape: shape,
            state: state,
            timeout: timeout,
            subset: subset,
            headers: headers,
            staleCacheBuster: staleCacheBuster
        )
    }

    private func performHTTPFetchWithRetry(request: URLRequest) async throws -> ElectricShapeHTTPResponse {
        guard configuration.retryPolicy.isEnabled else {
            return try await transport.fetch(request)
        }

        var attempt = 0
        var delay = configuration.retryPolicy.backoff.initialDelayMilliseconds

        while true {
            do {
                let result = try await transport.fetch(request)
                if result.response.statusCode == 429 || result.response.statusCode >= 500 {
                    let error = FetchError.from(
                        response: result.response,
                        data: result.data,
                        url: request.url?.absoluteString ?? shape.url.absoluteString
                    )
                    guard FetchSupport.shouldRetry(
                        error: error,
                        attempt: attempt,
                        options: configuration.retryPolicy.backoff
                    ) else {
                        throw error
                    }
                    let retryDelay = FetchSupport.retryDelayMilliseconds(
                        error: error,
                        currentDelayMilliseconds: delay,
                        options: configuration.retryPolicy.backoff,
                        randomUnit: recoveryPolicy.randomUnit
                    )
                    try await recoveryPolicy.sleep(TimeInterval(retryDelay) / 1_000)
                    delay = min(
                        Int(Double(delay) * configuration.retryPolicy.backoff.multiplier),
                        configuration.retryPolicy.backoff.maxDelayMilliseconds
                    )
                    attempt += 1
                    continue
                }
                return result
            } catch {
                guard FetchSupport.shouldRetry(
                    error: error,
                    attempt: attempt,
                    options: configuration.retryPolicy.backoff
                ) else {
                    throw error
                }
                let retryDelay = FetchSupport.retryDelayMilliseconds(
                    error: error,
                    currentDelayMilliseconds: delay,
                    options: configuration.retryPolicy.backoff,
                    randomUnit: recoveryPolicy.randomUnit
                )
                try await recoveryPolicy.sleep(TimeInterval(retryDelay) / 1_000)
                delay = min(
                    Int(Double(delay) * configuration.retryPolicy.backoff.multiplier),
                    configuration.retryPolicy.backoff.maxDelayMilliseconds
                )
                attempt += 1
            }
        }
    }

    private func performSSEOpenWithRetry(request: URLRequest) async throws -> ElectricShapeStreamingResponse {
        guard configuration.retryPolicy.isEnabled else {
            return try await transport.openSSE(request)
        }

        var attempt = 0
        var delay = configuration.retryPolicy.backoff.initialDelayMilliseconds

        while true {
            do {
                let result = try await transport.openSSE(request)
                if result.response.statusCode == 429 || result.response.statusCode >= 500 {
                    let error = FetchError.from(
                        response: result.response,
                        data: Data(),
                        url: request.url?.absoluteString ?? shape.url.absoluteString
                    )
                    guard FetchSupport.shouldRetry(
                        error: error,
                        attempt: attempt,
                        options: configuration.retryPolicy.backoff
                    ) else {
                        throw error
                    }
                    let retryDelay = FetchSupport.retryDelayMilliseconds(
                        error: error,
                        currentDelayMilliseconds: delay,
                        options: configuration.retryPolicy.backoff,
                        randomUnit: recoveryPolicy.randomUnit
                    )
                    try await recoveryPolicy.sleep(TimeInterval(retryDelay) / 1_000)
                    delay = min(
                        Int(Double(delay) * configuration.retryPolicy.backoff.multiplier),
                        configuration.retryPolicy.backoff.maxDelayMilliseconds
                    )
                    attempt += 1
                    continue
                }
                return result
            } catch {
                guard FetchSupport.shouldRetry(
                    error: error,
                    attempt: attempt,
                    options: configuration.retryPolicy.backoff
                ) else {
                    throw error
                }
                let retryDelay = FetchSupport.retryDelayMilliseconds(
                    error: error,
                    currentDelayMilliseconds: delay,
                    options: configuration.retryPolicy.backoff,
                    randomUnit: recoveryPolicy.randomUnit
                )
                try await recoveryPolicy.sleep(TimeInterval(retryDelay) / 1_000)
                delay = min(
                    Int(Double(delay) * configuration.retryPolicy.backoff.multiplier),
                    configuration.retryPolicy.backoff.maxDelayMilliseconds
                )
                attempt += 1
            }
        }
    }

    private func filteredMessages(_ messages: [ElectricMessage]) -> [ElectricMessage] {
        messages.filter { message in
            if message.isChangeMessage {
                return snapshotTracker.shouldRejectMessage(message) == false
            }
            if message.headers.control == .upToDate,
               let databaseLSN = message.headers.databaseLSN,
               let lsn = UInt64(databaseLSN) {
                snapshotTracker.lastSeenUpdate(databaseLSN: lsn)
            }
            return true
        }
    }

    private func snapshotSchema(from response: HTTPURLResponse) -> ElectricSchema {
        if state.schema.isEmpty == false {
            return state.schema
        }
        guard let schemaValue = response.value(forHTTPHeaderField: ElectricProtocolValues.schemaHeader),
              let schemaData = schemaValue.data(using: .utf8),
              let schema = try? decoder.decode(ElectricSchema.self, from: schemaData) else {
            return [:]
        }
        return schema
    }

    private func waitWhilePaused() async {
        while pauseReasons.isEmpty == false && isStopped == false {
            if Task.isCancelled {
                return
            }
            try? await Task.sleep(for: .milliseconds(50))
        }
    }

    private func clearTransientRequestState() {
        bufferedMessages.removeAll(keepingCapacity: true)
        replayCursor = nil
    }

    private func resetSSERecoveryState() {
        consecutiveShortSSEConnections = 0
        consecutiveSSEBackoffAttempts = 0
        useLongPollFallback = false
    }

    private func resetFastLoopRequestHistory() {
        fastLoopOffset = nil
        fastLoopRequestTimes.removeAll(keepingCapacity: false)
    }

    private func resetFastLoopState() {
        resetFastLoopRequestHistory()
        consecutiveFastLoopRecoveries = 0
    }

    private func recoverFromFastLoopIfNeeded() async throws -> Bool {
        let now = Date()
        if fastLoopOffset != state.offset {
            fastLoopOffset = state.offset
            fastLoopRequestTimes.removeAll(keepingCapacity: false)
        }

        fastLoopRequestTimes.append(now)
        fastLoopRequestTimes.removeAll {
            now.timeIntervalSince($0) > Self.fastLoopWindow
        }

        guard fastLoopRequestTimes.count >= Self.fastLoopRequestThreshold else {
            return false
        }

        consecutiveFastLoopRecoveries += 1
        let recoveryAttempt = consecutiveFastLoopRecoveries

        debugLogger.log(
            .info,
            category: "ShapeStream",
            message: "detected fast catch-up retry loop",
            metadata: shapeMetadata([
                "offset": state.offset,
                "attempt": String(recoveryAttempt),
                "requestCount": String(fastLoopRequestTimes.count),
            ])
        )

        if recoveryAttempt >= Self.fastLoopMaxRecoveries {
            throw ShapeStreamError.fastLoopDetected(
                shapeKey: shapeKey,
                offset: state.offset,
                attempts: recoveryAttempt
            )
        }

        ElectricCaches.expiredShapes.delete(shapeKey: shapeKey)
        ElectricTrackers.upToDate.delete(shapeKey: shapeKey)

        if recoveryAttempt > 1 {
            let delay = fullJitterDelay(
                attempt: recoveryAttempt - 1,
                base: Self.fastLoopRetryBaseDelay,
                max: Self.fastLoopRetryMaxDelay
            )
            debugLogger.log(
                .info,
                category: "ShapeStream",
                message: "backing off after repeated fast-loop recovery",
                metadata: shapeMetadata([
                    "offset": state.offset,
                    "attempt": String(recoveryAttempt),
                    "delayMS": String(Int(delay * 1000)),
                ])
            )
            try await recoveryPolicy.sleep(delay)
        }

        state.reset()
        clearTransientRequestState()
        pendingInjectedBatches.removeAll(keepingCapacity: true)
        staleCacheRetryCount = 0
        staleCacheBuster = nil
        forceCatchUpBoundary = false
        preferLongPollAfter204 = false
        resetSSERecoveryState()
        fastLoopOffset = state.offset
        fastLoopRequestTimes.removeAll(keepingCapacity: false)
        return true
    }

    private func fullJitterDelay(
        attempt: Int,
        base: TimeInterval,
        max: TimeInterval
    ) -> TimeInterval {
        let exponent = Swift.max(0, attempt - 1)
        let exponentialCap = min(base * pow(2, Double(exponent)), max)
        return recoveryPolicy.randomUnit() * exponentialCap
    }

    private func checkRequestGeneration(_ generation: Int) throws {
        if generation != requestGeneration {
            throw CancellationError()
        }
    }
}

struct ShapeStreamRecoveryPolicy: Sendable {
    let sleep: @Sendable (TimeInterval) async throws -> Void
    let randomUnit: @Sendable () -> Double

    static let live = ShapeStreamRecoveryPolicy(
        sleep: { delay in
            let nanoseconds = UInt64((max(0, delay) * 1_000_000_000).rounded())
            try await Task.sleep(nanoseconds: nanoseconds)
        },
        randomUnit: {
            Double.random(in: 0...1)
        }
    )
}

private struct ShapeSnapshotPayload: Decodable {
    let metadata: SnapshotMetadata
    let data: [ElectricMessage]
    let schemaFallback: ElectricSchema?

    enum CodingKeys: String, CodingKey {
        case metadata
        case data
    }

    init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        metadata = try container.decode(SnapshotMetadata.self, forKey: .metadata)
        data = try container.decode([ElectricMessage].self, forKey: .data)
        schemaFallback = nil
    }
}
