@testable import ElectricSwift
import Foundation
import Testing

final class MockURLProtocol: URLProtocol, @unchecked Sendable {
    struct Stub {
        let response: URLResponse
        let data: Data
        let error: Error?
    }

    private static let lock = NSLock()
    private nonisolated(unsafe) static var stubsByKey: [String: [Stub]] = [:]
    private(set) nonisolated(unsafe) static var requests: [URLRequest] = []

    static func reset() {
        lock.lock()
        defer { lock.unlock() }
        stubsByKey.removeAll()
        requests.removeAll()
        ElectricCaches.expiredShapes.clear()
        ElectricTrackers.upToDate.clear()
    }

    static func enqueue(
        response: URLResponse,
        data: Data = Data(),
        error: Error? = nil
    ) {
        lock.lock()
        defer { lock.unlock() }
        let key = stubKey(for: response.url)
        stubsByKey[key, default: []].append(
            Stub(response: response, data: data, error: error)
        )
    }

    static func nextStub(for request: URLRequest) -> Stub? {
        lock.lock()
        defer { lock.unlock() }
        requests.append(request)
        let key = stubKey(for: request.url)
        guard var stubs = stubsByKey[key], stubs.isEmpty == false else {
            return nil
        }
        let stub = stubs.removeFirst()
        if stubs.isEmpty {
            stubsByKey.removeValue(forKey: key)
        } else {
            stubsByKey[key] = stubs
        }
        return stub
    }

    override class func canInit(with request: URLRequest) -> Bool {
        true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let stub = Self.nextStub(for: request) else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
            return
        }

        if let error = stub.error {
            client?.urlProtocol(self, didFailWithError: error)
            return
        }

        client?.urlProtocol(self, didReceive: stub.response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: stub.data)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}

    private static func stubKey(for url: URL?) -> String {
        guard let url else { return "" }
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url.absoluteString
        }
        components.query = nil
        components.fragment = nil
        return components.url?.absoluteString ?? url.absoluteString
    }
}

func makeMockSession() -> URLSession {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [MockURLProtocol.self]
    return URLSession(configuration: configuration)
}

actor TestShapeTransport: ElectricShapeTransport {
    struct HTTPStub {
        let matches: @Sendable (URLRequest) -> Bool
        let response: HTTPURLResponse
        let data: Data
        let error: Error?
        let delayMilliseconds: UInt64?
    }

    struct SSEStub {
        let response: HTTPURLResponse
        let chunks: [Data]
        let error: Error?
        let delayMilliseconds: UInt64?
    }

    private var httpStubs: [HTTPStub] = []
    private var sseStubs: [SSEStub] = []
    private var storedRequests: [URLRequest] = []

    func enqueueHTTP(
        matching matches: @escaping @Sendable (URLRequest) -> Bool = { _ in true },
        response: HTTPURLResponse,
        data: Data = Data(),
        error: Error? = nil,
        delayMilliseconds: UInt64? = nil
    ) {
        httpStubs.append(
            HTTPStub(
                matches: matches,
                response: response,
                data: data,
                error: error,
                delayMilliseconds: delayMilliseconds
            )
        )
    }

    func enqueueSSE(
        response: HTTPURLResponse,
        chunks: [Data],
        error: Error? = nil,
        delayMilliseconds: UInt64? = nil
    ) {
        sseStubs.append(
            SSEStub(
                response: response,
                chunks: chunks,
                error: error,
                delayMilliseconds: delayMilliseconds
            )
        )
    }

    func requests() -> [URLRequest] {
        storedRequests
    }

    func fetch(_ request: URLRequest) async throws -> ElectricShapeHTTPResponse {
        storedRequests.append(request)
        guard let stubIndex = httpStubs.firstIndex(where: { $0.matches(request) }) else {
            throw URLError(.badServerResponse)
        }
        let stub = httpStubs.remove(at: stubIndex)
        if let error = stub.error {
            throw error
        }
        if let delayMilliseconds = stub.delayMilliseconds {
            do {
                try await Task.sleep(nanoseconds: delayMilliseconds * 1_000_000)
            } catch is CancellationError {
                throw CancellationError()
            }
            try Task.checkCancellation()
        }
        return ElectricShapeHTTPResponse(data: stub.data, response: stub.response)
    }

    func openSSE(_ request: URLRequest) async throws -> ElectricShapeStreamingResponse {
        storedRequests.append(request)
        guard sseStubs.isEmpty == false else {
            throw URLError(.badServerResponse)
        }
        let stub = sseStubs.removeFirst()
        if let error = stub.error {
            throw error
        }

        let chunks = AsyncThrowingStream<Data, Error> { continuation in
            let task = Task {
                do {
                    if let delayMilliseconds = stub.delayMilliseconds {
                        try await Task.sleep(nanoseconds: delayMilliseconds * 1_000_000)
                    }
                    for chunk in stub.chunks {
                        continuation.yield(chunk)
                    }
                    continuation.finish()
                } catch is CancellationError {
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }

        return ElectricShapeStreamingResponse(response: stub.response, chunks: chunks)
    }
}

final class TestRecoveryPolicyRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var recordedSleeps: [TimeInterval] = []
    private let randomValue: Double

    init(randomValue: Double = 1) {
        self.randomValue = randomValue
    }

    func policy() -> ShapeStreamRecoveryPolicy {
        let randomValue = self.randomValue
        return ShapeStreamRecoveryPolicy(
            sleep: { [weak self] delay in
                self?.record(delay)
            },
            randomUnit: {
                randomValue
            }
        )
    }

    private func record(_ delay: TimeInterval) {
        lock.lock()
        recordedSleeps.append(delay)
        lock.unlock()
    }

    func sleeps() -> [TimeInterval] {
        lock.lock()
        defer { lock.unlock() }
        return recordedSleeps
    }
}

func httpResponse(
    url: URL = URL(string: "https://example.com/v1/shape")!,
    statusCode: Int,
    headers: [String: String] = [:]
) -> HTTPURLResponse {
    HTTPURLResponse(url: url, statusCode: statusCode, httpVersion: nil, headerFields: headers)!
}

func jsonData(_ value: some Encodable) throws -> Data {
    try JSONEncoder().encode(value)
}

func testShapeState(
    handle: String? = "shape-1",
    offset: String = "0_0",
    isLive: Bool = false,
    isUpToDate: Bool = true
) -> ShapeStreamState {
    ShapeStreamState(
        handle: handle,
        offset: offset,
        cursor: nil,
        isLive: isLive,
        isUpToDate: isUpToDate,
        schema: [:],
        lastSyncedAt: Date()
    )
}

final class TestDebugRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [ElectricDebugEvent] = []

    func logger() -> ElectricDebugLogger {
        ElectricDebugLogger { [weak self] event in
            self?.record(event)
        }
    }

    var events: [ElectricDebugEvent] {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }

    private func record(_ event: ElectricDebugEvent) {
        lock.lock()
        storage.append(event)
        lock.unlock()
    }
}

struct TestTimeoutError: Error, CustomStringConvertible, Sendable {
    let operation: String
    let timeoutSeconds: Double

    var description: String {
        "Timed out after \(timeoutSeconds)s while \(operation)"
    }
}

func testLog(_ message: @autoclosure () -> String) {
    print("[ElectricSwiftTests] \(message())")
}

func withTestTimeout<T: Sendable>(
    operation: String,
    timeoutSeconds: Double = 10,
    _ work: @escaping @Sendable () async throws -> T
) async throws -> T {
    let timeoutNanoseconds = UInt64(timeoutSeconds * 1_000_000_000)

    return try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            try await work()
        }

        group.addTask {
            try await Task.sleep(nanoseconds: timeoutNanoseconds)
            testLog("timeout after \(timeoutSeconds)s while \(operation)")
            throw TestTimeoutError(operation: operation, timeoutSeconds: timeoutSeconds)
        }

        let result = try await group.next()
        group.cancelAll()
        return try #require(result)
    }
}

func loggedPoll(
    _ stream: ShapeStream,
    label: String,
    timeoutSeconds: Double = 10
) async throws -> ElectricShapeBatch? {
    let before = await stream.currentState()
    testLog(
        "poll start [\(label)] phase=\(String(describing: before.phase)) handle=\(before.handle ?? "nil") offset=\(before.offset) upToDate=\(before.isUpToDate)"
    )

    do {
        let batch = try await withTestTimeout(
            operation: "poll \(label)",
            timeoutSeconds: timeoutSeconds
        ) {
            try await stream.poll()
        }
        let after = await stream.currentState()
        testLog(
            "poll end [\(label)] phase=\(String(describing: after.phase)) handle=\(after.handle ?? "nil") offset=\(after.offset) upToDate=\(after.isUpToDate) messages=\(batch?.messages.count ?? 0)"
        )
        return batch
    } catch {
        let after = await stream.currentState()
        testLog(
            "poll failed [\(label)] error=\(error) phase=\(String(describing: after.phase)) handle=\(after.handle ?? "nil") offset=\(after.offset) upToDate=\(after.isUpToDate)"
        )
        throw error
    }
}
