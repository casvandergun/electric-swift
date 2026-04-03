import Foundation

struct ElectricShapeHTTPResponse: Sendable {
    let data: Data
    let response: HTTPURLResponse
}

struct ElectricShapeStreamingResponse: Sendable {
    let response: HTTPURLResponse
    let chunks: AsyncThrowingStream<Data, Error>
}

protocol ElectricShapeTransport: Sendable {
    func fetch(_ request: URLRequest) async throws -> ElectricShapeHTTPResponse
    func openSSE(_ request: URLRequest) async throws -> ElectricShapeStreamingResponse
}

struct URLSessionElectricShapeTransport: ElectricShapeTransport {
    let session: URLSession

    init(session: URLSession = .shared) {
        self.session = session
    }

    func fetch(_ request: URLRequest) async throws -> ElectricShapeHTTPResponse {
        let (data, response) = try await session.data(for: request)
        guard let response = response as? HTTPURLResponse else {
            throw ShapeStreamError.invalidResponse
        }
        return ElectricShapeHTTPResponse(data: data, response: response)
    }

    func openSSE(_ request: URLRequest) async throws -> ElectricShapeStreamingResponse {
        let (bytes, response) = try await session.bytes(for: request)
        guard let response = response as? HTTPURLResponse else {
            throw ShapeStreamError.invalidResponse
        }

        let chunks = AsyncThrowingStream<Data, Error> { continuation in
            let task = Task {
                do {
                    for try await byte in bytes {
                        if Task.isCancelled {
                            break
                        }
                        continuation.yield(Data([byte]))
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            continuation.onTermination = { _ in
                task.cancel()
            }
        }

        return ElectricShapeStreamingResponse(response: response, chunks: chunks)
    }
}
