@testable import ElectricSwift
import Foundation
import Testing

@Suite("Shape", .serialized)
struct ShapeTests {
    struct Todo: Codable, Sendable, Equatable {
        let id: Int
        let title: String?
        let completed: Bool?
    }

    @Test("rows waits for first up-to-date and current access is in-memory")
    func rowsWaitForUpToDateAndCurrentAccessIsImmediate() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(
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
                    key: "todo:1",
                    value: ["id": .string("1"), "title": .string("A")],
                    headers: .init(operation: .insert)
                ),
            ])
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "2_0",
                    "electric-schema": #"{"id":{"type":"int8"},"title":{"type":"text"}}"#,
                ]
            ),
            data: try jsonData([ElectricMessage.upToDate()])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            transport: transport
        )
        let shape = Shape<Todo>(stream: stream)

        let beforeStart = try await shape.currentRows()
        #expect(beforeStart.isEmpty)

        let rows = try await shape.rows()
        #expect(rows.count == 1)
        #expect(rows.first?.id == 1)
        #expect(rows.first?.title == "A")

        let currentRows = try await shape.currentRows()
        #expect(currentRows.count == 1)
        let currentValue = try await shape.currentValue()
        #expect(currentValue.values.first?.title == "A")
        await shape.stop()
    }

    @Test("updates emits a typed change for the first up-to-date batch")
    func updatesEmitsTypedChangeForInitialBatch() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let schema = #"{"id":{"type":"int8"},"title":{"type":"text"}}"#

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "1_0",
                    "electric-schema": schema,
                ]
            ),
            data: try jsonData([
                ElectricMessage(
                    key: "todo:1",
                    value: ["id": .string("1"), "title": .string("Initial")],
                    headers: .init(operation: .insert)
                ),
                .upToDate(),
            ])
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            transport: transport
        )
        let shape = Shape<Todo>(stream: stream)

        do {
            var iterator = shape.updates().makeAsyncIterator()
            let first = try #require(await iterator.next())
            #expect(first.rows.count == 1)
            #expect(first.rows.first?.id == 1)
            #expect(first.rows.first?.title == "Initial")
            #expect(first.status == .upToDate)
            let finished = try await iterator.next()
            #expect(finished == nil)
        }
        let currentRows = try await shape.currentRows()
        #expect(currentRows.count == 1)
        await shape.stop()
    }

    @Test("requestSnapshot through the wrapper populates typed rows")
    func requestSnapshotPopulatesTypedRows() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!
        let schema = #"{"id":{"type":"int8"},"title":{"type":"text"}}"#
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
              "value": { "id": "1", "title": "Snap" },
              "headers": { "operation": "insert" }
            }
          ]
        }
        """

        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 204,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "0_0",
                    "electric-schema": schema,
                ]
            )
        )
        await transport.enqueueHTTP(
            response: httpResponse(
                url: url,
                statusCode: 200,
                headers: [
                    "electric-handle": "h1",
                    "electric-offset": "5_0",
                    "electric-schema": schema,
                ]
            ),
            data: Data(snapshotPayload.utf8)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            transport: transport
        )
        let shape = Shape<Todo>(stream: stream)

        let initialRows = try await shape.rows()
        #expect(initialRows.isEmpty)

        _ = try await shape.requestSnapshot(.init(limit: 1))
        let deadline = Date().addingTimeInterval(1)
        var rows: [Todo] = []
        while rows.isEmpty && Date() < deadline {
            rows = try await shape.currentRows()
            if rows.isEmpty {
                try await Task.sleep(nanoseconds: 20_000_000)
            }
        }

        #expect(rows.count == 1)
        #expect(rows.first?.title == "Snap")
        await shape.stop()
    }

    @Test("start is idempotent and exposes offset sync metadata")
    func startIsIdempotent() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!

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
            configuration: .init(subscribe: false),
            transport: transport
        )
        let shape = Shape<Todo>(stream: stream)

        await shape.start()
        await shape.start()
        _ = try await shape.rows()

        let requests = await transport.requests()
        #expect(requests.count == 1)
        #expect(await shape.isUpToDate())
        #expect(await shape.lastOffset() == "0_0")
        #expect(await shape.lastSyncedAt() != nil)
        await shape.stop()
    }

    @Test("rows and updates surface wrapper errors from the stream")
    func rowsAndUpdatesSurfaceErrors() async throws {
        let transport = TestShapeTransport()
        let url = URL(string: "https://example.com/v1/shape")!

        await transport.enqueueHTTP(
            response: httpResponse(url: url, statusCode: 200, headers: [:]),
            data: Data("[]".utf8)
        )

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: "todos"),
            configuration: .init(subscribe: false),
            transport: transport
        )
        let shape = Shape<Todo>(stream: stream)

        await #expect(throws: ShapeError.self) {
            _ = try await shape.rows()
        }

        do {
            var iterator = shape.updates().makeAsyncIterator()
            await #expect(throws: ShapeError.self) {
                _ = try await iterator.next()
            }
        }
        #expect(await shape.error() != nil)
        await shape.stop()
    }
}
