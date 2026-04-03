@testable import ElectricSwift
import Foundation
import Testing

@Suite("Shape Stream", .serialized)
struct ShapeStreamTests {
    @Test("204 response marks stream up to date")
    func noContentMarksUpToDate() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            session: session
        )

        let batch = try #require(await stream.poll())
        #expect(batch.reachedUpToDate == true)
        #expect(batch.messages == [.upToDate()])

        let state = await stream.currentState()
        #expect(state.isUpToDate == true)
        #expect(state.handle == "h1")
        #expect(state.offset == "0_0")
    }

    @Test("Buffers messages until an up-to-date marker arrives")
    func buffersUntilUpToDate() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "1_0",
                    "electric-schema": #"{"id":{"type":"int8"},"title":{"type":"text"}}"#,
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "\"public\".\"todos\"/1",
                    value: ["id": .string("1"), "title": .string("First")],
                    headers: .init(operation: .insert)
                ),
            ])
        )
        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "2_0",
                    "electric-schema": #"{"id":{"type":"int8"},"title":{"type":"text"}}"#,
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "\"public\".\"todos\"/2",
                    value: ["id": .string("2"), "title": .string("Second")],
                    headers: .init(operation: .insert)
                ),
                .upToDate(),
            ])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            session: session
        )

        let first = try await stream.poll()
        #expect(first == nil)

        let second = try #require(await stream.poll())
        #expect(second.messages.count == 3)
        #expect(second.messages[0].value?["id"] == .integer(1))
        #expect(second.messages[1].value?["id"] == .integer(2))
        #expect(second.messages[2].headers.control == .upToDate)
    }

    @Test("409 response resets stream state and emits must-refetch")
    func conflictResetsState() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 409,
                headers: [
                    "Location": "https://example.com/v1/shape?handle=replaced-handle",
                ]
            )
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(initialState: .init(handle: "old", offset: "9_0", isLive: true)),
            session: session
        )

        let batch = try await stream.poll()
        #expect(batch?.messages == [.mustRefetch()])

        let state = await stream.currentState()
        #expect(state.handle == "replaced-handle")
        #expect(state.offset == "-1")
        #expect(state.isUpToDate == false)
    }

    @Test("Unexpected HTTP status throws")
    func invalidStatusThrows() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(url: url, statusCode: 500)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                retryPolicy: .init(isEnabled: false)
            ),
            session: session
        )

        await #expect(throws: FetchError.self) {
            try await stream.poll()
        }
    }

    @Test("Parses array values using schema dimensions")
    func parsesArrayValues() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "1_0",
                    "electric-schema": #"{"tags":{"type":"text","dims":1},"scores":{"type":"int4","dims":1}}"#,
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "\"public\".\"todos\"/1",
                    value: [
                        "tags": .string(#"{"one","two"}"#),
                        "scores": .string("{1,2,3}"),
                    ],
                    headers: .init(operation: .insert)
                ),
                .upToDate(),
            ])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            session: session
        )

        let batch = try #require(await stream.poll())
        let row = try #require(batch.messages.first?.value)
        #expect(row["tags"] == .array([.string("one"), .string("two")]))
        #expect(row["scores"] == .array([.integer(1), .integer(2), .integer(3)]))
    }

    @Test("SSE buffers events until up-to-date and yields a live batch")
    func sseBuffersUntilUpToDate() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let schema = #"{"id":{"type":"int8"},"title":{"type":"text"}}"#
        let response = httpResponse(
            url: url,
            statusCode: 200,
            headers: [
                "electric-handle": "h-live",
                "electric-offset": "2_0",
                "electric-cursor": "cursor-live",
                "electric-schema": schema,
            ]
        )

        let insert = try jsonData(
            ElectricMessage(
                key: "\"public\".\"todos\"/1",
                value: ["id": .string("1"), "title": .string("Live")],
                headers: .init(operation: .insert)
            )
        )
        let upToDate = try jsonData(
            ElectricMessage(
                headers: .init(control: .upToDate, globalLastSeenLSN: "8")
            )
        )

        await transport.enqueueSSE(
            response: response,
            chunks: [
                Data("data: ".utf8) + insert + Data("\n\n".utf8),
                Data("data: ".utf8) + upToDate + Data("\n\n".utf8),
            ]
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: true,
                initialState: .init(
                    handle: "h-live",
                    offset: "2_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: try JSONDecoder().decode(ElectricSchema.self, from: Data(schema.utf8))
                )
            ),
            transport: transport
        )

        let batch = try #require(await stream.poll())
        #expect(batch.phase == .liveSSE)
        #expect(batch.boundaryKind == .liveUpdate)
        #expect(batch.checkpoint.offset == "8_0")
        #expect(batch.messages.count == 2)
        let requests = await transport.requests()
        #expect(requests.last?.value(forHTTPHeaderField: "Accept") == "text/event-stream")
        let requestURL = try #require(requests.last?.url)
        let components = try #require(URLComponents(url: requestURL, resolvingAgainstBaseURL: false))
        #expect(components.queryItems?.contains(.init(name: "live_sse", value: "true")) == true)
    }

    @Test("Repeated short SSE connections fall back to live long-poll")
    func shortSSEConnectionsFallbackToLongPoll() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let schema = #"{"id":{"type":"int8"}}"#

        for _ in 0..<3 {
            await transport.enqueueSSE(
                response: httpResponse(
                    url: url,
                    statusCode: 200,
                    headers: [
                        "electric-handle": "h-live",
                        "electric-offset": "2_0",
                        "electric-cursor": "cursor-live",
                        "electric-schema": schema,
                    ]
                ),
                chunks: []
            )
        }

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: true,
                timeout: 30,
                initialState: .init(
                    handle: "h-live",
                    offset: "2_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: try JSONDecoder().decode(ElectricSchema.self, from: Data(schema.utf8))
                ),
                preferSSE: true,
                minSSEConnectionDuration: 60,
                maxShortSSEConnections: 3
            ),
            transport: transport
        )

        _ = try await stream.poll()
        _ = try await stream.poll()
        _ = try await stream.poll()

        #expect(await stream.phase() == .liveLongPoll)
    }

    @Test("Short-lived SSE closures back off before retrying and stay in SSE mode")
    func shortSSEConnectionsBackoffBeforeFallback() async throws {
        let transport = TestShapeTransport()
        let recovery = TestRecoveryPolicyRecorder(randomValue: 1)
        let url = URL(string: "https://example.com/v1/shape")!
        let schema = #"{"id":{"type":"int8"}}"#

        await transport.enqueueSSE(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h-live",
                    "electric-offset": "2_0",
                    "electric-cursor": "cursor-live",
                    "electric-schema": schema,
                ]
            ),
            chunks: []
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: true,
                initialState: .init(
                    handle: "h-live",
                    offset: "2_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: try JSONDecoder().decode(ElectricSchema.self, from: Data(schema.utf8))
                ),
                preferSSE: true,
                minSSEConnectionDuration: 60,
                maxShortSSEConnections: 3
            ),
            transport: transport,
            recoveryPolicy: recovery.policy()
        )

        let batch = try await stream.poll()
        #expect(batch == nil)
        #expect(await stream.phase() == .liveSSE)
        let sleeps = recovery.sleeps()
        #expect(sleeps.count == 1)
        let sleep = try #require(sleeps.first)
        #expect(abs(sleep - 0.1) < 0.001)
    }

    @Test("Aborted SSE closures do not increment the short-connection counter")
    func abortedSSEConnectionsDoNotIncrementCounter() async throws {
        let transport = TestShapeTransport()
        let recovery = TestRecoveryPolicyRecorder(randomValue: 1)
        let url = URL(string: "https://example.com/v1/shape")!
        let schema = #"{"id":{"type":"int8"}}"#
        let headers = [
            "electric-handle": "h-live",
            "electric-offset": "2_0",
            "electric-cursor": "cursor-live",
            "electric-schema": schema,
        ]

        await transport.enqueueSSE(
            response: httpResponse(url: url, statusCode: 200, headers: headers),
            chunks: [],
            delayMilliseconds: 5_000
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: true,
                initialState: .init(
                    handle: "h-live",
                    offset: "2_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: try JSONDecoder().decode(ElectricSchema.self, from: Data(schema.utf8))
                ),
                preferSSE: true,
                minSSEConnectionDuration: 60,
                maxShortSSEConnections: 2
            ),
            transport: transport,
            recoveryPolicy: recovery.policy()
        )

        let firstPoll = Task { try await stream.poll() }

        let requestDeadline = Date().addingTimeInterval(1)
        while await transport.requests().isEmpty && Date() < requestDeadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }

        await stream.pause()
        let abortedPoll = try await firstPoll.value
        #expect(abortedPoll == nil)
        await stream.resume()

        let upToDate = try jsonData([ElectricMessage.upToDate()])
        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 200, headers: headers),
            data: upToDate
        )
        _ = try #require(await stream.poll())

        await transport.enqueueSSE(
            response: httpResponse(url: url, statusCode: 200, headers: headers),
            chunks: []
        )

        let nextPoll = try await stream.poll()
        #expect(nextPoll == nil)
        #expect(await stream.phase() == .liveSSE)
        let sleeps = recovery.sleeps()
        #expect(sleeps.count == 1)
    }

    @Test("Successful SSE live batches reset short-connection backoff state")
    func successfulSSEBoundaryResetsShortConnectionState() async throws {
        let transport = TestShapeTransport()
        let recovery = TestRecoveryPolicyRecorder(randomValue: 1)
        let url = URL(string: "https://example.com/v1/shape")!
        let schema = #"{"id":{"type":"int8"}}"#
        let headers = [
            "electric-handle": "h-live",
            "electric-offset": "2_0",
            "electric-cursor": "cursor-live",
            "electric-schema": schema,
        ]

        let upToDate = try jsonData(
            ElectricMessage(
                headers: .init(control: .upToDate, globalLastSeenLSN: "8")
            )
        )

        await transport.enqueueSSE(
            response: httpResponse(url: url, statusCode: 200, headers: headers),
            chunks: []
        )
        await transport.enqueueSSE(
            response: httpResponse(url: url, statusCode: 200, headers: headers),
            chunks: [
                Data("data: ".utf8) + upToDate + Data("\n\n".utf8),
            ]
        )
        await transport.enqueueSSE(
            response: httpResponse(url: url, statusCode: 200, headers: headers),
            chunks: []
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: true,
                initialState: .init(
                    handle: "h-live",
                    offset: "2_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: try JSONDecoder().decode(ElectricSchema.self, from: Data(schema.utf8))
                ),
                preferSSE: true,
                minSSEConnectionDuration: 60,
                maxShortSSEConnections: 2
            ),
            transport: transport,
            recoveryPolicy: recovery.policy()
        )

        _ = try await stream.poll()
        let liveBatch = try #require(await stream.poll())
        #expect(liveBatch.boundaryKind == .liveUpdate)

        let shortAfterSuccess = try await stream.poll()
        #expect(shortAfterSuccess == nil)
        #expect(await stream.phase() == .liveSSE)
        let sleeps = recovery.sleeps()
        #expect(sleeps.count == 2)
        #expect(abs(sleeps[0] - 0.1) < 0.001)
        #expect(abs(sleeps[1] - 0.1) < 0.001)
    }

    @Test("Shape rotation resets SSE fallback state and tries SSE again")
    func conflictResetClearsSSEFallback() async throws {
        let transport = TestShapeTransport()
        let recovery = TestRecoveryPolicyRecorder(randomValue: 1)
        let url = URL(string: "https://example.com/v1/shape")!
        let schema = #"{"id":{"type":"int8"}}"#
        let initialHeaders = [
            "electric-handle": "h-live",
            "electric-offset": "2_0",
            "electric-cursor": "cursor-live",
            "electric-schema": schema,
        ]
        let rotatedHeaders = [
            "electric-handle": "h-rotated",
            "electric-offset": "2_0",
            "electric-cursor": "cursor-rotated",
            "electric-schema": schema,
        ]
        let upToDate = try jsonData(
            ElectricMessage(
                headers: .init(control: .upToDate, globalLastSeenLSN: "9")
            )
        )

        for _ in 0..<3 {
            await transport.enqueueSSE(
                response: httpResponse(url: url, statusCode: 200, headers: initialHeaders),
                chunks: []
            )
        }
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 409,
                headers: ["electric-handle": "h-rotated"]
            )
        )
        let rotatedUpToDate = try jsonData([ElectricMessage.upToDate()])
        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 200, headers: rotatedHeaders),
            data: rotatedUpToDate
        )
        await transport.enqueueSSE(
            response: httpResponse(url: url, statusCode: 200, headers: rotatedHeaders),
            chunks: [
                Data("data: ".utf8) + upToDate + Data("\n\n".utf8),
            ]
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: true,
                initialState: .init(
                    handle: "h-live",
                    offset: "2_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: try JSONDecoder().decode(ElectricSchema.self, from: Data(schema.utf8))
                ),
                preferSSE: true,
                minSSEConnectionDuration: 60,
                maxShortSSEConnections: 3
            ),
            transport: transport,
            recoveryPolicy: recovery.policy()
        )

        _ = try await stream.poll()
        _ = try await stream.poll()
        _ = try await stream.poll()
        #expect(await stream.phase() == .liveLongPoll)

        let mustRefetch = try #require(await stream.poll())
        #expect(mustRefetch.messages == [.mustRefetch()])

        let catchUp = try #require(await stream.poll())
        #expect(catchUp.reachedUpToDate == true)

        let liveBatch = try #require(await stream.poll())
        #expect(liveBatch.boundaryKind == .liveUpdate)

        let requests = await transport.requests()
        let sseRequests = requests.compactMap { request -> URL? in
            guard
                let requestURL = request.url,
                let components = URLComponents(url: requestURL, resolvingAgainstBaseURL: false),
                components.queryItems?.contains(.init(name: "live_sse", value: "true")) == true
            else {
                return nil
            }
            return requestURL
        }
        #expect(sseRequests.count == 4)
        let lastSSEURL = try #require(sseRequests.last)
        let lastComponents = try #require(
            URLComponents(url: lastSSEURL, resolvingAgainstBaseURL: false)
        )
        #expect(lastComponents.queryItems?.first(where: { $0.name == "handle" })?.value == "h-rotated")
    }

    @Test("Pause and resume preserve checkpoint and reconnect without live long-polling")
    func pauseAndResumeReconnectFromCheckpoint() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let headers = [
            "electric-handle": "h-live",
            "electric-offset": "2_0",
            "electric-cursor": "cursor-live",
            "electric-schema": "{}",
        ]

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 204, headers: headers),
            delayMilliseconds: 200
        )
        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 204, headers: headers)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: true,
                initialState: .init(
                    handle: "h-live",
                    offset: "2_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: [:]
                ),
                preferSSE: false
            ),
            transport: transport
        )

        let firstPoll = Task { try await stream.poll() }

        let requestDeadline = Date().addingTimeInterval(1)
        while await transport.requests().isEmpty && Date() < requestDeadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        await stream.pause()
        #expect(await stream.phase() == .paused)
        let pausedPoll = try await firstPoll.value
        #expect(pausedPoll == nil)

        try await Task.sleep(nanoseconds: 50_000_000)
        await stream.resume()

        let batch = try await stream.poll()
        if let batch {
            #expect(batch.reachedUpToDate == true)
            #expect(batch.checkpoint.handle == "h-live")
            #expect(batch.checkpoint.offset == "2_0")
        }

        let requests = await transport.requests()
        #expect(requests.count == 2)
        let firstURL = try #require(requests.first?.url)
        let secondURL = try #require(requests.last?.url)
        let firstComponents = try #require(URLComponents(url: firstURL, resolvingAgainstBaseURL: false))
        let secondComponents = try #require(URLComponents(url: secondURL, resolvingAgainstBaseURL: false))
        #expect(firstComponents.queryItems?.contains(.init(name: "live", value: "true")) == true)
        #expect(secondComponents.queryItems?.contains(where: { $0.name == "live" }) == false)
        #expect(secondComponents.queryItems?.first(where: { $0.name == "handle" })?.value == "h-live")
        #expect(secondComponents.queryItems?.first(where: { $0.name == "offset" })?.value == "2_0")
    }

    @Test("Refresh aborts a live request and reconnects without live long-polling")
    func refreshReconnectsWithoutLiveLongPolling() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let headers = [
            "electric-handle": "h-live",
            "electric-offset": "2_0",
            "electric-cursor": "cursor-live",
            "electric-schema": "{}",
        ]

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 204, headers: headers),
            delayMilliseconds: 200
        )
        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 204, headers: headers)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: true,
                initialState: .init(
                    handle: "h-live",
                    offset: "2_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: [:]
                ),
                preferSSE: false
            ),
            transport: transport
        )

        let firstPoll = Task { try await stream.poll() }

        let requestDeadline = Date().addingTimeInterval(1)
        while await transport.requests().isEmpty && Date() < requestDeadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        await stream.refresh()
        let refreshedPoll = try await firstPoll.value
        #expect(refreshedPoll == nil)

        let batch = try await stream.poll()
        if let batch {
            #expect(batch.reachedUpToDate == true)
        }

        let requests = await transport.requests()
        #expect(requests.count == 2)
        let firstURL = try #require(requests.first?.url)
        let secondURL = try #require(requests.last?.url)
        let firstComponents = try #require(URLComponents(url: firstURL, resolvingAgainstBaseURL: false))
        let secondComponents = try #require(URLComponents(url: secondURL, resolvingAgainstBaseURL: false))
        #expect(firstComponents.queryItems?.contains(.init(name: "live", value: "true")) == true)
        #expect(secondComponents.queryItems?.contains(where: { $0.name == "live" }) == false)
    }

    @Test("Fast-loop recovery clears per-shape caches and retries from scratch")
    func fastLoopRecoveryClearsCachesAndRetriesFromScratch() async throws {
        ElectricCaches.expiredShapes.clear()
        ElectricTrackers.upToDate.clear()

        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let shape = ElectricShape(url: url, table: "todos")
        let shapeKey = ShapeRequestBuilder.canonicalShapeKey(shape: shape)
        ElectricCaches.expiredShapes.markExpired(shapeKey: shapeKey, handle: "expired-handle")
        ElectricTrackers.upToDate.recordUpToDate(shapeKey: shapeKey, cursor: "cursor-replay")

        let loopingHeaders = [
            "electric-handle": "loop-handle",
            "electric-offset": "9_0",
            "electric-schema": "{}",
        ]

        for _ in 0..<4 {
            await transport.enqueueHTTP(
                response: httpResponse(url: url, statusCode: 200, headers: loopingHeaders),
                data: Data("[]".utf8)
            )
        }
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "fresh-handle",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: shape,
            configuration: .init(
                subscribe: false,
                initialState: .init(handle: "loop-handle", offset: "9_0", isLive: false, isUpToDate: false, schema: [:])
            ),
            transport: transport
        )

        for _ in 0..<4 {
            let batch = try await stream.poll()
            #expect(batch == nil)
        }

        let recovered = try #require(await stream.poll())
        #expect(recovered.reachedUpToDate == true)
        #expect(ElectricCaches.expiredShapes.getExpiredHandle(for: shapeKey) == nil)
        #expect(ElectricTrackers.upToDate.replayCursorIfRecent(for: shapeKey) == nil)

        let requests = await transport.requests()
        #expect(requests.count == 5)
        let recoveryURL = try #require(requests.last?.url)
        let recoveryQuery = try #require(
            URLComponents(url: recoveryURL, resolvingAgainstBaseURL: false)?.queryItems
        )
        #expect(recoveryQuery.first(where: { $0.name == "offset" })?.value == "-1")
        #expect(recoveryQuery.contains(where: { $0.name == "handle" }) == false)
    }

    @Test("Persistent fast-loop eventually throws a diagnostic error")
    func persistentFastLoopThrowsDiagnosticError() async throws {
        ElectricTrackers.upToDate.clear()
        let transport = TestShapeTransport()
        let recovery = TestRecoveryPolicyRecorder(randomValue: 1)
        let url = URL(string: "https://example.com/v1/shape")!

        let loopingHeaders = [
            "electric-handle": "loop-handle",
            "electric-offset": "-1",
            "electric-schema": "{}",
        ]

        for _ in 0..<24 {
            await transport.enqueueHTTP(
                response: httpResponse(url: url, statusCode: 200, headers: loopingHeaders),
                data: Data("[]".utf8)
            )
        }

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: false,
                initialState: .init(handle: "loop-handle", offset: "-1", isLive: false, isUpToDate: false, schema: [:])
            ),
            transport: transport,
            recoveryPolicy: recovery.policy()
        )

        await #expect {
            for _ in 0..<25 {
                _ = try await stream.poll()
            }
        } throws: { error in
            guard case let ShapeStreamError.fastLoopDetected(shapeKey, offset, attempts) = error else {
                return false
            }
            return shapeKey == ShapeRequestBuilder.canonicalShapeKey(shape: ElectricShape(url: url, table: "todos"))
                && offset == "-1"
                && attempts == 5
        }
    }

    @Test("409 without replacement handle arms a one-shot cache buster on the next request")
    func conflictWithoutReplacementHandleUsesCacheBuster() async throws {
        ElectricTrackers.upToDate.clear()
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 409)
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "fresh-handle",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: false,
                initialState: .init(handle: "old-handle", offset: "9_0", isLive: false, isUpToDate: false, schema: [:])
            ),
            transport: transport
        )

        let mustRefetch = try #require(await stream.poll())
        #expect(mustRefetch.messages == [.mustRefetch()])

        let state = await stream.currentState()
        #expect(state.handle == nil)
        #expect(state.offset == "-1")
        #expect(state.isUpToDate == false)

        _ = try #require(await stream.poll())

        let requests = await transport.requests()
        #expect(requests.count == 2)
        let secondURL = try #require(requests.last?.url)
        let secondQuery = try #require(
            URLComponents(url: secondURL, resolvingAgainstBaseURL: false)?.queryItems
        )
        #expect(secondQuery.first(where: { $0.name == "offset" })?.value == "-1")
        #expect(secondQuery.first(where: { $0.name == "cache-buster" })?.value?.isEmpty == false)
    }

    @Test("Replay mode suppresses duplicated cached up-to-date batches")
    func replayModeSuppressesDuplicateUpToDateBatch() async throws {
        ElectricTrackers.upToDate.clear()
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let shape = ElectricShape(url: url, table: "todos")
        let shapeKey = ShapeRequestBuilder.canonicalShapeKey(shape: shape)
        ElectricTrackers.upToDate.recordUpToDate(shapeKey: shapeKey, cursor: "cursor-replay")

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "1_0",
                    "electric-cursor": "cursor-replay",
                    "electric-schema": #"{"id":{"type":"int8"}}"#,
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "\"public\".\"todos\"/1",
                    value: ["id": .string("1")],
                    headers: .init(operation: .insert)
                ),
                .upToDate(),
            ])
        )

        let stream = ShapeStream(
            shape: shape,
            configuration: .init(subscribe: true, preferSSE: false),
            transport: transport
        )

        let batch = try await stream.poll()
        #expect(batch == nil)
        let state = await stream.currentState()
        #expect(state.isUpToDate == true)
        #expect(state.cursor == "cursor-replay")
    }

    @Test("Emits message-level debug events for the read path")
    func emitsReadTraceEvents() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let recorder = TestDebugRecorder()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "1_0",
                    "electric-schema": #"{"id":{"type":"int8"},"title":{"type":"text"}}"#,
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "\"public\".\"todos\"/todo-1",
                    value: ["id": .string("1"), "title": .string("First")],
                    headers: .init(operation: .insert, txids: [101])
                ),
                .upToDate(),
            ])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            session: session,
            debugLogger: recorder.logger()
        )

        _ = try #require(await stream.poll())

        let events = recorder.events
        let pollEvent = try #require(events.first(where: { $0.message == "starting poll" }))
        #expect(pollEvent.metadata["mode"] == "catchUp")

        let responseEvent = try #require(events.first(where: { $0.message == "received shape response" }))
        #expect(responseEvent.metadata["statusCode"] == "200")
        #expect(responseEvent.metadata["responseOffset"] == "1_0")

        let shapeMessageEvents = events.filter { $0.category == "ShapeMessage" }
        let insertEvent = try #require(
            shapeMessageEvents.first(where: { $0.metadata["key"] == "todo-1" })
        )
        #expect(insertEvent.metadata["txids"] == "101")
        #expect(insertEvent.metadata["source"] == "http")
        #expect(insertEvent.metadata["operation"] == "insert")

        let controlEvent = try #require(
            shapeMessageEvents.first(where: { $0.metadata["control"] == ElectricControl.upToDate.rawValue })
        )
        #expect(controlEvent.metadata["source"] == "http")

        let batchEvent = try #require(
            events.first(where: { $0.category == "ShapeBatch" && $0.message == "finalized batch" })
        )
        #expect(batchEvent.metadata["messages"] == "2")
        #expect(batchEvent.metadata["boundary"] == ElectricShapeBoundaryKind.upToDate.rawValue)
    }

    @Test("Ignored stale cached response does not mutate state when local handle differs")
    func ignoredStaleResponseDoesNotMutateState() async throws {
        ElectricCaches.expiredShapes.clear()
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let shape = ElectricShape(url: url, table: "todos")
        let shapeKey = ShapeRequestBuilder.canonicalShapeKey(shape: shape)
        ElectricCaches.expiredShapes.markExpired(shapeKey: shapeKey, handle: "expired-handle")

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "expired-handle",
                    "electric-offset": "99_0",
                    "electric-schema": #"{"id":{"type":"text"}}"#,
                ]
            ),
            data: Data("[]".utf8)
        )

        let stream = ShapeStream(
            shape: shape,
            configuration: .init(
                subscribe: false,
                initialState: .init(
                    handle: "current-handle",
                    offset: "5_0",
                    isLive: false,
                    isUpToDate: false,
                    schema: ["id": .init(type: "int8", dims: nil)]
                )
            ),
            transport: transport
        )

        let batch = try await stream.poll()
        #expect(batch == nil)
        let state = await stream.currentState()
        #expect(state.handle == "current-handle")
        #expect(state.offset == "5_0")
        #expect(state.schema["id"]?.type == "int8")
        #expect(await stream.phase() == .initial)
    }

    @Test("Stale cached response retries with cache buster when local handle is nil")
    func staleResponseRetriesWhenLocalHandleMissing() async throws {
        ElectricCaches.expiredShapes.clear()
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let shape = ElectricShape(url: url, table: "todos")
        let shapeKey = ShapeRequestBuilder.canonicalShapeKey(shape: shape)
        ElectricCaches.expiredShapes.markExpired(shapeKey: shapeKey, handle: "expired-handle")

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "expired-handle",
                    "electric-offset": "9_0",
                    "electric-schema": "{}",
                ]
            ),
            data: Data("[]".utf8)
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "fresh-handle",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: shape,
            configuration: .init(subscribe: false),
            transport: transport
        )

        let first = try await stream.poll()
        #expect(first == nil)
        let second = try #require(await stream.poll())
        #expect(second.reachedUpToDate == true)

        let requests = await transport.requests()
        #expect(requests.count == 2)
        let retryURL = try #require(requests.last?.url)
        let retryQuery = try #require(URLComponents(url: retryURL, resolvingAgainstBaseURL: false)?.queryItems)
        #expect(retryQuery.first(where: { $0.name == "cache-buster" })?.value?.isEmpty == false)
    }

    @Test("Stale cached response retries when local handle matches expired handle")
    func staleResponseRetriesWhenLocalHandleMatchesExpiredHandle() async throws {
        ElectricCaches.expiredShapes.clear()
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let shape = ElectricShape(url: url, table: "todos")
        let shapeKey = ShapeRequestBuilder.canonicalShapeKey(shape: shape)
        ElectricCaches.expiredShapes.markExpired(shapeKey: shapeKey, handle: "expired-handle")

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "expired-handle",
                    "electric-offset": "9_0",
                    "electric-schema": "{}",
                ]
            ),
            data: Data("[]".utf8)
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "fresh-handle",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: shape,
            configuration: .init(
                subscribe: false,
                initialState: .init(handle: "expired-handle", offset: "9_0", isLive: false, isUpToDate: false, schema: [:])
            ),
            transport: transport
        )

        let first = try await stream.poll()
        #expect(first == nil)
        let second = try #require(await stream.poll())
        #expect(second.reachedUpToDate == true)
    }

    @Test("Stale cache retry exhaustion throws when stale responses keep repeating")
    func staleCacheRetryExhaustionThrows() async throws {
        ElectricCaches.expiredShapes.clear()
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let shape = ElectricShape(url: url, table: "todos")
        let shapeKey = ShapeRequestBuilder.canonicalShapeKey(shape: shape)
        ElectricCaches.expiredShapes.markExpired(shapeKey: shapeKey, handle: "expired-handle")

        for _ in 0..<2 {
            await transport.enqueueHTTP(
                response: httpResponse(
                    url: url,
                    statusCode: 200,
                    headers: [
                        "electric-handle": "expired-handle",
                        "electric-offset": "9_0",
                        "electric-schema": "{}",
                    ]
                ),
                data: Data("[]".utf8)
            )
        }

        let stream = ShapeStream(
            shape: shape,
            configuration: .init(subscribe: false, maxStaleCacheRetries: 1),
            transport: transport
        )

        _ = try await stream.poll()
        await #expect {
            _ = try await stream.poll()
        } throws: { error in
            guard case let ShapeStreamError.staleCacheLoopExceeded(shapeKey, retries) = error else {
                return false
            }
            return shapeKey == ShapeRequestBuilder.canonicalShapeKey(shape: shape) && retries == 2
        }
    }

    @Test("200 without a boundary does not set lastSyncedAt")
    func incompleteBatchDoesNotSetLastSyncedAt() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "1_0",
                    "electric-schema": #"{"id":{"type":"int8"}}"#,
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "todo:1",
                    value: ["id": .string("1")],
                    headers: .init(operation: .insert)
                ),
            ])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            session: session
        )

        let batch = try await stream.poll()
        #expect(batch == nil)
        let state = await stream.currentState()
        #expect(state.lastSyncedAt == nil)
    }

    @Test("204 sets lastSyncedAt")
    func noContentSetsLastSyncedAt() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            session: session
        )

        _ = try #require(await stream.poll())
        let state = await stream.currentState()
        #expect(state.lastSyncedAt != nil)
    }

    @Test("Non-SSE up-to-date preserves response offset and ignores message LSN")
    func nonSSEUpToDatePreservesResponseOffset() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "6_0",
                    "electric-schema": "{}",
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    headers: .init(control: .upToDate, globalLastSeenLSN: "99")
                ),
            ])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: false,
                initialState: .init(handle: "h1", offset: "5_0", isLive: false, isUpToDate: false, schema: [:])
            ),
            session: session
        )

        let batch = try #require(await stream.poll())
        #expect(batch.checkpoint.offset == "6_0")
        let state = await stream.currentState()
        #expect(state.offset == "6_0")
    }

    @Test("Replay does not loop when the server keeps returning the same cursor")
    func replayDoesNotLoopWhenCursorStaysTheSame() async throws {
        ElectricTrackers.upToDate.clear()
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let shape = ElectricShape(url: url, table: "todos")
        let shapeKey = ShapeRequestBuilder.canonicalShapeKey(shape: shape)
        ElectricTrackers.upToDate.recordUpToDate(shapeKey: shapeKey, cursor: "cursor-replay")

        for offset in ["1_0", "2_0"] {
            await transport.enqueueHTTP(
                response: httpResponse(
                    url: url,
                    statusCode: 200,
                    headers: [
                        "electric-handle": "h1",
                        "electric-offset": offset,
                        "electric-cursor": "cursor-replay",
                        "electric-schema": #"{"id":{"type":"int8"}}"#,
                    ]
                ),
                data: try jsonData([ElectricMessage.upToDate()])
            )
        }

        let stream = ShapeStream(
            shape: shape,
            configuration: .init(subscribe: true, preferSSE: false),
            transport: transport
        )

        let first = try await stream.poll()
        #expect(first == nil)
        let second = try #require(await stream.poll())
        #expect(second.boundaryKind == .liveUpdate)
        #expect(second.checkpoint.cursor == "cursor-replay")
        let requests = await transport.requests()
        #expect(requests.count == 2)
    }

    @Test("onError stop is terminal")
    func onErrorStopIsTerminal() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 401)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false, retryPolicy: .init(backoff: .init(maxRetries: 0))),
            transport: transport,
            onError: { _ in .stop }
        )

        await #expect(throws: FetchError.self) {
            _ = try await stream.poll()
        }
    }

    @Test("Async onError handlers are supported")
    func asyncOnErrorHandlersAreSupported() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 401)
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos", headers: ["Authorization": "stale"]),
            configuration: .init(subscribe: false, retryPolicy: .init(backoff: .init(maxRetries: 0))),
            transport: transport,
            onError: { context in
                try? await Task.sleep(nanoseconds: 5_000_000)
                var nextShape = context.shape
                nextShape.headers["Authorization"] = "fresh"
                return .retryWithShape(nextShape)
            }
        )

        let batch = try #require(await stream.poll())
        #expect(batch.reachedUpToDate == true)
        let requests = await transport.requests()
        #expect(requests.last?.value(forHTTPHeaderField: "Authorization") == "fresh")
    }

    @Test("Repeated retries preserve the current checkpoint")
    func repeatedRetriesPreserveCheckpoint() async throws {
        let transport = TestShapeTransport()
        let recovery = TestRecoveryPolicyRecorder(randomValue: 0)
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(response: httpResponse(url: url, statusCode: 503))
        await transport.enqueueHTTP(response: httpResponse(url: url, statusCode: 503))
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "9_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: false,
                initialState: .init(handle: "h1", offset: "9_0", isLive: false, isUpToDate: false, schema: [:]),
                retryPolicy: .init(backoff: .init(initialDelayMilliseconds: 100, maxDelayMilliseconds: 100, multiplier: 2, maxRetries: 2))
            ),
            transport: transport,
            recoveryPolicy: recovery.policy()
        )

        let batch = try #require(await stream.poll())
        #expect(batch.checkpoint.handle == "h1")
        #expect(batch.checkpoint.offset == "9_0")
        let requests = await transport.requests()
        #expect(requests.count == 3)
        for request in requests {
            let url = try #require(request.url)
            let queryItems = try #require(URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems)
            #expect(queryItems.first(where: { $0.name == "handle" })?.value == "h1")
            #expect(queryItems.first(where: { $0.name == "offset" })?.value == "9_0")
        }
    }

    @Test("Successful onError recovery resets fast-loop detection")
    func onErrorRecoveryResetsFastLoopDetection() async throws {
        ElectricTrackers.upToDate.clear()
        let transport = TestShapeTransport()
        let recovery = TestRecoveryPolicyRecorder(randomValue: 0)
        let url = URL(string: "https://example.com/v1/shape")!

        for _ in 0..<4 {
            await transport.enqueueHTTP(
                response: httpResponse(
                    url: url,
                    statusCode: 200,
                    headers: [
                        "electric-handle": "loop-handle",
                        "electric-offset": "-1",
                        "electric-schema": "{}",
                    ]
                ),
                data: Data("[]".utf8)
            )
        }
        await transport.enqueueHTTP(response: httpResponse(url: url, statusCode: 401))
        for _ in 0..<4 {
            await transport.enqueueHTTP(
                response: httpResponse(
                    url: url,
                    statusCode: 200,
                    headers: [
                        "electric-handle": "loop-handle",
                        "electric-offset": "-1",
                        "electric-schema": "{}",
                    ]
                ),
                data: Data("[]".utf8)
            )
        }
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "fresh-handle",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: false,
                initialState: .init(handle: "loop-handle", offset: "-1", isLive: false, isUpToDate: false, schema: [:]),
                retryPolicy: .init(backoff: .init(maxRetries: 0))
            ),
            transport: transport,
            recoveryPolicy: recovery.policy(),
            onError: { _ in .retry }
        )

        for _ in 0..<3 {
            let batch = try await stream.poll()
            #expect(batch == nil)
        }

        let recoveryPoll = try await stream.poll()
        #expect(recoveryPoll == nil)

        for _ in 0..<4 {
            let batch = try await stream.poll()
            #expect(batch == nil)
        }

        let finalBatch = try #require(await stream.poll())
        #expect(finalBatch.reachedUpToDate == true)
    }

    @Test("Missing headers remain terminal even if onError asks to retry")
    func missingHeadersStayTerminalEvenWithOnErrorRetry() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 200, headers: [:]),
            data: Data("[]".utf8)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false, retryPolicy: .init(backoff: .init(maxRetries: 0))),
            transport: transport,
            onError: { _ in .retry }
        )

        await #expect(throws: ShapeStreamError.self) {
            _ = try await stream.poll()
        }
        let requests = await transport.requests()
        #expect(requests.count == 1)
    }

    @Test("Schema is first-write-wins across later responses")
    func schemaIsFirstWriteWins() async throws {
        MockURLProtocol.reset()
        let session = makeMockSession()
        let url = URL(string: "https://example.com/v1/shape")!

        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "1_0",
                    "electric-schema": #"{"id":{"type":"int8"}}"#,
                ]
            ),
            data: try jsonData([ElectricMessage]())
        )
        MockURLProtocol.enqueue(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "2_0",
                    "electric-schema": #"{"id":{"type":"text"}}"#,
                ]
            ),
            data: try jsonData([ElectricMessage.upToDate()])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            session: session
        )

        _ = try await stream.poll()
        _ = try await stream.poll()

        let state = await stream.currentState()
        #expect(state.schema["id"]?.type == "int8")
    }

    @Test("204 keeps the stream on live long-poll instead of immediately switching to SSE")
    func noContentKeepsLongPollFallback() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let headers = [
            "electric-handle": "h1",
            "electric-offset": "0_0",
            "electric-schema": "{}",
            "electric-cursor": "cursor-1",
        ]

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 204, headers: headers)
        )
        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 204, headers: headers)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: true, preferSSE: true),
            transport: transport
        )

        let first = try #require(await stream.poll())
        #expect(first.boundaryKind == .upToDate)
        _ = try await stream.poll()

        let requests = await transport.requests()
        #expect(requests.count == 2)
        #expect(requests.last?.value(forHTTPHeaderField: "Accept") == nil)
        let secondURL = try #require(requests.last?.url)
        let secondComponents = try #require(
            URLComponents(url: secondURL, resolvingAgainstBaseURL: false)
        )
        #expect(secondComponents.queryItems?.contains(where: { $0.name == "live_sse" }) == false)
        #expect(await stream.phase() == .liveLongPoll)
    }

    @Test("Transient failures retry automatically and honor Retry-After")
    func transientFailuresRetryAutomatically() async throws {
        let transport = TestShapeTransport()
        let recovery = TestRecoveryPolicyRecorder(randomValue: 0)
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 429,
                headers: ["retry-after": "2"]
            )
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: false,
                retryPolicy: .init(
                    backoff: .init(initialDelayMilliseconds: 100, maxDelayMilliseconds: 1_000, multiplier: 2, maxRetries: 3)
                )
            ),
            transport: transport,
            recoveryPolicy: recovery.policy()
        )

        let batch = try #require(await stream.poll())
        #expect(batch.reachedUpToDate == true)
        let sleeps = recovery.sleeps()
        #expect(sleeps.count == 1)
        #expect(abs(sleeps[0] - 2.0) < 0.001)
    }

    @Test("onError can retry with an updated shape")
    func onErrorCanRetryWithUpdatedShape() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 401)
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "0_0",
                    "electric-schema": "{}",
                ]
            )
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos", headers: ["Authorization": "old-token"]),
            configuration: .init(
                subscribe: false,
                retryPolicy: .init(
                    backoff: .init(maxRetries: 0)
                )
            ),
            transport: transport,
            onError: { context in
                guard case let .fetch(error) = context.failure, error.status == 401 else {
                    return .stop
                }
                var nextShape = context.shape
                nextShape.headers["Authorization"] = "new-token"
                return .retryWithShape(nextShape)
            }
        )

        let batch = try #require(await stream.poll())
        #expect(batch.reachedUpToDate == true)
        let requests = await transport.requests()
        #expect(requests.count == 2)
        #expect(requests.last?.value(forHTTPHeaderField: "Authorization") == "new-token")
    }

    @Test("Missing headers remain terminal and do not auto-retry")
    func missingHeadersRemainTerminal() async throws {
        let transport = TestShapeTransport()
        let recovery = TestRecoveryPolicyRecorder(randomValue: 0)
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 200, headers: [:]),
            data: Data("[]".utf8)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: false,
                retryPolicy: .init(
                    backoff: .init(initialDelayMilliseconds: 100, maxDelayMilliseconds: 1_000, multiplier: 2, maxRetries: 3)
                )
            ),
            transport: transport,
            recoveryPolicy: recovery.policy()
        )

        await #expect(throws: ShapeStreamError.self) {
            _ = try await stream.poll()
        }
        #expect(recovery.sleeps().isEmpty)
    }

    @Test("fetchSnapshot GET returns metadata and rows without mutating stream state")
    func fetchSnapshotGetReturnsDataWithoutMutatingState() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let payload = """
        {
          "metadata": {
            "snapshot_mark": 1,
            "xmin": "100",
            "xmax": "200",
            "xip_list": [],
            "database_lsn": "123"
          },
          "data": [
            {
              "key": "todo:1",
              "value": { "id": "1" },
              "headers": { "operation": "insert" }
            }
          ]
        }
        """

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-schema": #"{"id":{"type":"int8"}}"#,
                    "electric-handle": "h1",
                    "electric-offset": "5_0",
                ]
            ),
            data: Data(payload.utf8)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            transport: transport
        )

        let result = try await stream.fetchSnapshot(.init(limit: 1))
        #expect(result.metadata.snapshotMark == 1)
        #expect(result.messages.count == 1)
        #expect(result.messages[0].value?["id"] == .integer(1))
        let state = await stream.currentState()
        #expect(state.handle == nil)
        #expect(state.offset == "-1")
    }

    @Test("fetchSnapshot POST sends subset params in request body")
    func fetchSnapshotPostSendsSubsetBody() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let payload = """
        {
          "metadata": {
            "snapshot_mark": 1,
            "xmin": "100",
            "xmax": "200",
            "xip_list": [],
            "database_lsn": "123"
          },
          "data": []
        }
        """

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: ["electric-schema": "{}"]
            ),
            data: Data(payload.utf8)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            transport: transport
        )

        _ = try await stream.fetchSnapshot(
            .init(
                whereClause: "title = 'B'",
                params: ["1": .string("B")],
                limit: 100,
                orderBy: "title ASC",
                method: .post
            )
        )

        let request = try #require((await transport.requests()).last)
        #expect(request.httpMethod == "POST")
        let body = try #require(request.httpBody)
        let json = try #require(JSONSerialization.jsonObject(with: body) as? [String: Any])
        #expect(json["where"] as? String == "title = 'B'")
        #expect(json["order_by"] as? String == "title ASC")
        #expect(json["limit"] as? Int == 100)
    }

    @Test("requestSnapshot queues an injected batch and filters overlapping live changes")
    func requestSnapshotQueuesBatchAndFiltersDuplicates() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let snapshotPayload = """
        {
          "metadata": {
            "snapshot_mark": 1,
            "xmin": "100",
            "xmax": "200",
            "xip_list": [],
            "database_lsn": "123"
          },
          "data": [
            {
              "key": "todo:1",
              "value": { "id": "1", "title": "snap" },
              "headers": { "operation": "insert" }
            }
          ]
        }
        """

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-schema": #"{"id":{"type":"int8"},"title":{"type":"text"}}"#,
                    "electric-handle": "h1",
                    "electric-offset": "5_0",
                ]
            ),
            data: Data(snapshotPayload.utf8)
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-schema": #"{"id":{"type":"int8"},"title":{"type":"text"}}"#,
                    "electric-handle": "h1",
                    "electric-offset": "6_0",
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "todo:1",
                    value: ["id": .string("1"), "title": .string("dup")],
                    headers: .init(operation: .update, txids: [50])
                ),
                ElectricMessage(
                    key: "todo:2",
                    value: ["id": .string("2"), "title": .string("fresh")],
                    headers: .init(operation: .insert, txids: [50])
                ),
                .upToDate(),
            ])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: true),
            transport: transport
        )

        let snapshot = try await stream.requestSnapshot(.init(limit: 1))
        #expect(snapshot.messages.count == 1)

        let injected = try #require(await stream.poll())
        #expect(injected.messages.contains(where: { $0.headers.control == .snapshotEnd }))
        #expect(injected.messages.contains(where: { $0.headers.control == .subsetEnd }))

        let live = try #require(await stream.poll())
        #expect(live.messages.contains(where: { $0.key == "todo:2" }))
        #expect(live.messages.contains(where: { $0.key == "todo:1" }) == false)
    }

    @Test("requestSnapshot can complete while initial sync is in progress")
    func requestSnapshotCompletesDuringInitialSync() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let snapshotPayload = """
        {
          "metadata": {
            "snapshot_mark": 1,
            "xmin": "100",
            "xmax": "200",
            "xip_list": [],
            "database_lsn": "123"
          },
          "data": [
            {
              "key": "todo:1",
              "value": { "id": "1" },
              "headers": { "operation": "insert" }
            }
          ]
        }
        """

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "sync-handle",
                    "electric-offset": "1_0",
                    "electric-schema": #"{"id":{"type":"int8"}}"#,
                ]
            ),
            data: Data("[]".utf8),
            delayMilliseconds: 5_000
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "snapshot-handle",
                    "electric-offset": "5_0",
                    "electric-schema": #"{"id":{"type":"int8"}}"#,
                ]
            ),
            data: Data(snapshotPayload.utf8)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: true),
            transport: transport
        )

        let firstPoll = Task { try await stream.poll() }
        let requestDeadline = Date().addingTimeInterval(1)
        while await transport.requests().isEmpty && Date() < requestDeadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }

        let snapshot = try await stream.requestSnapshot(.init(limit: 1))
        #expect(snapshot.messages.count == 1)
        let cancelledPoll = try await firstPoll.value
        #expect(cancelledPoll == nil)

        let injected = try #require(await stream.poll())
        #expect(injected.messages.contains(where: { $0.headers.control == .snapshotEnd }))
    }

    @Test("Snapshot does not miss live updates committed after the snapshot boundary")
    func snapshotDoesNotMissLiveUpdatesAfterSnapshotBoundary() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let snapshotPayload = """
        {
          "metadata": {
            "snapshot_mark": 1,
            "xmin": "100",
            "xmax": "200",
            "xip_list": [],
            "database_lsn": "123"
          },
          "data": [
            {
              "key": "todo:1",
              "value": { "id": "1", "title": "snap" },
              "headers": { "operation": "insert" }
            }
          ]
        }
        """

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-schema": #"{"id":{"type":"int8"},"title":{"type":"text"}}"#,
                    "electric-handle": "h1",
                    "electric-offset": "5_0",
                ]
            ),
            data: Data(snapshotPayload.utf8)
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-schema": #"{"id":{"type":"int8"},"title":{"type":"text"}}"#,
                    "electric-handle": "h1",
                    "electric-offset": "6_0",
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "todo:1",
                    value: ["id": .string("1"), "title": .string("after")],
                    headers: .init(operation: .update, txids: [250])
                ),
                .upToDate(),
            ])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: true),
            transport: transport
        )

        _ = try await stream.requestSnapshot(.init(limit: 1))
        _ = try #require(await stream.poll())
        let live = try #require(await stream.poll())
        #expect(live.messages.contains(where: { $0.key == "todo:1" && $0.value?["title"] == .string("after") }))
    }

    @Test("Unseen-key updates and deletes are not filtered by snapshot tracking")
    func unseenKeyChangesAreNotFilteredBySnapshotTracking() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let snapshotPayload = """
        {
          "metadata": {
            "snapshot_mark": 1,
            "xmin": "100",
            "xmax": "200",
            "xip_list": [],
            "database_lsn": "123"
          },
          "data": [
            {
              "key": "todo:1",
              "value": { "id": "1" },
              "headers": { "operation": "insert" }
            }
          ]
        }
        """

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-schema": #"{"id":{"type":"int8"}}"#,
                    "electric-handle": "h1",
                    "electric-offset": "5_0",
                ]
            ),
            data: Data(snapshotPayload.utf8)
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-schema": #"{"id":{"type":"int8"}}"#,
                    "electric-handle": "h1",
                    "electric-offset": "6_0",
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "todo:2",
                    value: ["id": .string("2")],
                    headers: .init(operation: .update, txids: [50])
                ),
                ElectricMessage(
                    key: "todo:3",
                    headers: .init(operation: .delete, txids: [50])
                ),
                .upToDate(),
            ])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: true),
            transport: transport
        )

        _ = try await stream.requestSnapshot(.init(limit: 1))
        _ = try #require(await stream.poll())
        let live = try #require(await stream.poll())
        let hasUnseenUpdate = live.messages.contains {
            $0.key == "todo:2" && $0.headers.operation == .update
        }
        let hasUnseenDelete = live.messages.contains {
            $0.key == "todo:3" && $0.headers.operation == .delete
        }
        #expect(hasUnseenUpdate)
        #expect(hasUnseenDelete)
    }

    @Test("fetchSnapshot retries 409 without resetting live stream state")
    func fetchSnapshotRetriesConflictWithoutResettingLiveState() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let payload = """
        {
          "metadata": {
            "snapshot_mark": 2,
            "xmin": "100",
            "xmax": "200",
            "xip_list": [],
            "database_lsn": "123"
          },
          "data": []
        }
        """

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 409,
                headers: ["Location": "https://example.com/v1/shape?handle=rotated-handle"]
            )
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "rotated-handle",
                    "electric-offset": "9_0",
                    "electric-schema": #"{"id":{"type":"int8"}}"#,
                ]
            ),
            data: Data(payload.utf8)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(
                subscribe: true,
                initialState: .init(
                    handle: "h-live",
                    offset: "7_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: ["id": .init(type: "int8", dims: nil)]
                )
            ),
            transport: transport
        )

        let result = try await stream.fetchSnapshot(.init(limit: 1))
        #expect(result.responseHandle == "rotated-handle")
        let state = await stream.currentState()
        #expect(state.offset == "7_0")
        #expect(state.phase == .liveLongPoll)
        #expect(state.isUpToDate == true)
        #expect(state.schema["id"]?.type == "int8")
    }

    @Test("Invariant matrix preserves expected phase and checkpoint changes")
    func invariantMatrixPreservesExpectedPhaseAndCheckpointChanges() async throws {
        struct Case {
            let name: String
            let initialState: ShapeStreamState
            let response: HTTPURLResponse
            let data: Data
            let expectedPhase: ElectricShapePhase
            let expectedOffset: String
            let expectsBatch: Bool
        }

        let url = URL(string: "https://example.com/v1/shape")!
        let cases = try [
            Case(
                name: "initial 200 without boundary",
                initialState: .init(),
                response: httpResponse(
                    url: url,
                    statusCode: 200,
                    headers: [
                        "electric-handle": "h1",
                        "electric-offset": "1_0",
                        "electric-schema": "{}",
                    ]
                ),
                data: jsonData([ElectricMessage]()),
                expectedPhase: .syncing,
                expectedOffset: "1_0",
                expectsBatch: false
            ),
            Case(
                name: "initial 204",
                initialState: .init(),
                response: httpResponse(
                    url: url,
                    statusCode: 204,
                    headers: [
                        "electric-handle": "h1",
                        "electric-offset": "0_0",
                        "electric-schema": "{}",
                    ]
                ),
                data: Data(),
                expectedPhase: .syncing,
                expectedOffset: "0_0",
                expectsBatch: true
            ),
            Case(
                name: "live sse up-to-date advances offset",
                initialState: .init(
                    handle: "h1",
                    offset: "2_0",
                    cursor: "cursor-live",
                    isLive: true,
                    isUpToDate: true,
                    schema: ["id": .init(type: "int8", dims: nil)]
                ),
                response: httpResponse(
                    url: url,
                    statusCode: 200,
                    headers: [
                        "electric-handle": "h1",
                        "electric-offset": "2_0",
                        "electric-cursor": "cursor-live",
                    ]
                ),
                data: Data(),
                expectedPhase: .liveSSE,
                expectedOffset: "8_0",
                expectsBatch: true
            ),
        ]

        for testCase in cases {
            let transport = TestShapeTransport()
            if testCase.expectedPhase == .liveSSE {
                let upToDate = try jsonData(
                    ElectricMessage(headers: .init(control: .upToDate, globalLastSeenLSN: "8"))
                )
                await transport.enqueueSSE(
                    response: testCase.response,
                    chunks: [Data("data: ".utf8) + upToDate + Data("\n\n".utf8)]
                )
            } else {
                await transport.enqueueHTTP(response: testCase.response, data: testCase.data)
            }

            let stream = ShapeStream(
                shape: ElectricShape(url: url, table: "todos"),
                configuration: .init(
                    subscribe: testCase.expectedPhase.isLive,
                    initialState: testCase.initialState,
                    preferSSE: true
                ),
                transport: transport
            )

            let batch = try await stream.poll()
            #expect((batch != nil) == testCase.expectsBatch, Comment(rawValue: testCase.name))
            let state = await stream.currentState()
            #expect(state.phase == testCase.expectedPhase, Comment(rawValue: testCase.name))
            #expect(state.offset == testCase.expectedOffset, Comment(rawValue: testCase.name))
        }
    }
}

@Suite("Materialized Shape", .serialized)
struct MaterializedShapeTests {
    @Test("Applies insert update delete and must-refetch")
    func appliesBatchLifecycle() async throws {
        struct TodoValue: Codable, Sendable, Equatable {
            let id: Int
            let title: String
        }

        let shape = MaterializedShape<TodoValue>()

        await shape.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/1",
                        value: ["id": .integer(1), "title": .string("Initial")],
                        headers: .init(operation: .insert)
                    ),
                ],
                state: testShapeState(),
                schema: [:],
                reachedUpToDate: false
            )
        )

        await shape.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/1",
                        value: ["title": .string("Updated")],
                        headers: .init(operation: .update)
                    ),
                ],
                state: testShapeState(),
                schema: [:],
                reachedUpToDate: false
            )
        )

        let updated = try await shape.values()
        #expect(updated["1"] == TodoValue(id: 1, title: "Updated"))

        await shape.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/1",
                        headers: .init(operation: .delete)
                    ),
                ],
                state: testShapeState(),
                schema: [:],
                reachedUpToDate: false
            )
        )

        let deleted = await shape.snapshotRows()
        #expect(deleted.isEmpty)

        await shape.apply(
            ShapeBatch(
                messages: [
                    ElectricMessage(
                        key: "\"public\".\"todos\"/2",
                        value: ["id": .integer(2), "title": .string("Refetch me")],
                        headers: .init(operation: .insert)
                    ),
                ],
                state: testShapeState(),
                schema: [:],
                reachedUpToDate: false
            )
        )
        await shape.apply(
            ShapeBatch(
                messages: [.mustRefetch()],
                state: testShapeState(isUpToDate: false),
                schema: [:],
                reachedUpToDate: false
            )
        )

        let afterRefetch = await shape.snapshotRows()
        #expect(afterRefetch.isEmpty)
    }
}
