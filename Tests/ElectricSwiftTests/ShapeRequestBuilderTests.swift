@testable import ElectricSwift
import Testing

@Suite("Shape Request Builder", .serialized)
struct ShapeRequestBuilderTests {
    @Test("Builds Electric protocol URL parameters")
    func buildsURLParameters() throws {
        let shape = ShapeStreamOptions(
            url: URL(string: "https://example.com/v1/shape")!,
            table: "issues",
            columns: ["id", "title"],
            whereClause: "priority = 'high'",
            params: ["1": "high"],
            replica: .full,
            extraParameters: ["source_id": "ios-client"]
        )
        let state = ShapeStreamState(
            handle: "shape-1",
            offset: "123_0",
            cursor: "cursor-1",
            isLive: true
        )

        let url = ShapeRequestBuilder.makeURL(options: shape, state: state)
        let components = try #require(URLComponents(url: url, resolvingAgainstBaseURL: false))
        let queryItems = try #require(components.queryItems)

        #expect(queryItems.contains(.init(name: "table", value: "issues")))
        #expect(queryItems.contains(.init(name: "offset", value: "123_0")))
        #expect(queryItems.contains(.init(name: "handle", value: "shape-1")))
        #expect(queryItems.contains(.init(name: "cursor", value: "cursor-1")))
        #expect(queryItems.contains(.init(name: "live", value: "true")))
        #expect(queryItems.contains(.init(name: "replica", value: "full")))
        #expect(queryItems.contains(.init(name: "log", value: "full")))
        #expect(queryItems.contains(.init(name: "columns", value: #""id","title""#)))
        #expect(queryItems.contains(.init(name: "params[1]", value: "high")))
        #expect(queryItems.contains(.init(name: "source_id", value: "ios-client")))
    }

    @Test("Column mapper encodes request fields")
    func columnMapperEncodesRequestFields() throws {
        let shape = ShapeStreamOptions(
            url: URL(string: "https://example.com/v1/shape")!,
            table: "issues",
            columns: ["userId", "createdAt"],
            whereClause: "userId = $1 AND status = 'open'",
            params: ["1": "42"],
            log: .changesOnly
        )
        let state = ShapeStreamState(offset: "-1", isLive: false, isUpToDate: false)

        let url = ShapeRequestBuilder.makeURL(
            options: shape,
            state: state,
            columnMapper: snakeCamelMapper()
        )
        let components = try #require(URLComponents(url: url, resolvingAgainstBaseURL: false))
        let queryItems = try #require(components.queryItems)

        #expect(queryItems.contains(.init(name: "where", value: "user_id = $1 AND status = 'open'")))
        #expect(queryItems.contains(.init(name: "columns", value: #""user_id","created_at""#)))
        #expect(queryItems.contains(.init(name: "params[1]", value: "42")))
        #expect(queryItems.contains(.init(name: "log", value: "changes_only")))
    }

    @Test("Builds snapshot GET parameters without live cursor state")
    func buildsSnapshotGETParameters() throws {
        let shape = ShapeStreamOptions(
            url: URL(string: "https://example.com/v1/shape")!,
            table: "issues"
        )
        let state = ShapeStreamState(
            handle: "shape-1",
            offset: "123_0",
            cursor: "cursor-1",
            isLive: true
        )

        let subset = ShapeSubsetRequest(
            whereClause: "title = 'hello'",
            params: ["1": .string("hello")],
            limit: 10,
            offset: 5,
            orderBy: "title ASC"
        )

        let request = try ShapeRequestBuilder.makeSnapshotRequest(
            options: shape,
            state: state,
            timeout: 30,
            subset: subset,
            columnMapper: snakeCamelMapper()
        )
        let requestURL = try #require(request.url)
        let components = try #require(URLComponents(url: requestURL, resolvingAgainstBaseURL: false))
        let queryItems = try #require(components.queryItems)

        #expect(queryItems.contains(.init(name: "handle", value: "shape-1")))
        #expect(queryItems.contains(.init(name: "offset", value: "123_0")))
        #expect(queryItems.contains(.init(name: ShapeRequestBuilder.subsetWhereParam, value: "title = 'hello'")))
        #expect(queryItems.contains(.init(name: ShapeRequestBuilder.subsetLimitParam, value: "10")))
        #expect(queryItems.contains(.init(name: ShapeRequestBuilder.subsetOffsetParam, value: "5")))
        #expect(queryItems.contains(.init(name: ShapeRequestBuilder.subsetOrderByParam, value: "title ASC")))
        #expect(queryItems.contains(where: { $0.name == "cursor" }) == false)
        #expect(queryItems.contains(where: { $0.name == "live" }) == false)
        #expect(queryItems.contains(where: { $0.name == "live_sse" }) == false)
    }

    @Test("Builds snapshot POST body")
    func buildsSnapshotPOSTBody() throws {
        let shape = ShapeStreamOptions(
            url: URL(string: "https://example.com/v1/shape")!,
            table: "issues"
        )
        let state = ShapeStreamState(handle: "shape-1", offset: "123_0", cursor: "cursor-1", isLive: true)
        let subset = ShapeSubsetRequest(
            whereClause: "title = 'hello'",
            params: ["1": .string("hello"), "2": .integer(2)],
            limit: 10,
            offset: 5,
            orderBy: "title ASC",
            method: .post
        )

        let request = try ShapeRequestBuilder.makeSnapshotRequest(
            options: shape,
            state: state,
            timeout: 30,
            subset: subset,
            columnMapper: snakeCamelMapper()
        )

        #expect(request.httpMethod == "POST")
        let body = try #require(request.httpBody)
        let json = try #require(JSONSerialization.jsonObject(with: body) as? [String: Any])
        #expect(json["where"] as? String == "title = 'hello'")
        #expect(json["order_by"] as? String == "title ASC")
        #expect(json["limit"] as? Int == 10)
        #expect(json["offset"] as? Int == 5)
        let params = try #require(json["params"] as? [String: Any])
        #expect(params["1"] as? String == "hello")
        #expect(params["2"] as? NSNumber == 2)
    }

    @Test("Omits handle query parameter when initial state has no handle")
    func omitsHandleForInitialRequests() throws {
        let shape = ShapeStreamOptions(
            url: URL(string: "https://example.com/v1/shape")!,
            table: "issues"
        )
        let state = ShapeStreamState(offset: "-1", isLive: false, isUpToDate: false)

        let url = ShapeRequestBuilder.makeURL(options: shape, state: state)
        let components = try #require(URLComponents(url: url, resolvingAgainstBaseURL: false))
        let queryItems = try #require(components.queryItems)

        #expect(queryItems.contains(.init(name: "table", value: "issues")))
        #expect(queryItems.contains(.init(name: "offset", value: "-1")))
        #expect(queryItems.contains(.init(name: "log", value: "full")))
        #expect(queryItems.contains(where: { $0.name == "handle" }) == false)
    }
}
