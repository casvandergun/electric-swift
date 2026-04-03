import Foundation

enum ElectricProtocolValues {
    static let handleHeader = "electric-handle"
    static let offsetHeader = "electric-offset"
    static let cursorHeader = "electric-cursor"
    static let schemaHeader = "electric-schema"
    static let upToDateHeader = "electric-up-to-date"
    static let locationHeader = "Location"
}

struct ShapeRequestBuilder {
    static let subsetWhereParam = "subset__where"
    static let subsetParamsParam = "subset__params"
    static let subsetLimitParam = "subset__limit"
    static let subsetOffsetParam = "subset__offset"
    static let subsetOrderByParam = "subset__order_by"

    static func makeRequest(
        shape: ElectricShape,
        state: ShapeStreamState,
        timeout: TimeInterval,
        mode: ElectricShapeRequestMode,
        staleCacheBuster: String? = nil
    ) -> URLRequest {
        var request = URLRequest(url: makeURL(shape: shape, state: state, mode: mode, staleCacheBuster: staleCacheBuster))
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        request.cachePolicy = .reloadIgnoringLocalCacheData
        for (header, value) in shape.headers {
            request.setValue(value, forHTTPHeaderField: header)
        }
        if mode == .liveSSE {
            request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        }
        return request
    }

    static func makeSnapshotRequest(
        shape: ElectricShape,
        state: ShapeStreamState,
        timeout: TimeInterval,
        subset: ShapeSubsetRequest,
        staleCacheBuster: String? = nil
    ) throws -> URLRequest {
        switch subset.method {
        case .get:
            var request = URLRequest(
                url: makeURL(
                    shape: shape,
                    state: state,
                    mode: .catchUp,
                    staleCacheBuster: staleCacheBuster,
                    subset: subset,
                    includeCursor: false,
                    includeLiveParams: false
                )
            )
            request.httpMethod = SnapshotMethod.get.rawValue
            request.timeoutInterval = timeout
            request.cachePolicy = .reloadIgnoringLocalCacheData
            for (header, value) in shape.headers {
                request.setValue(value, forHTTPHeaderField: header)
            }
            return request
        case .post:
            var request = URLRequest(
                url: makeURL(
                    shape: shape,
                    state: state,
                    mode: .catchUp,
                    staleCacheBuster: staleCacheBuster,
                    subset: nil,
                    includeCursor: false,
                    includeLiveParams: false
                )
            )
            request.httpMethod = SnapshotMethod.post.rawValue
            request.timeoutInterval = timeout
            request.cachePolicy = .reloadIgnoringLocalCacheData
            for (header, value) in shape.headers {
                request.setValue(value, forHTTPHeaderField: header)
            }
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONEncoder().encode(SnapshotRequestBody(subset))
            return request
        }
    }

    static func makeURL(
        shape: ElectricShape,
        state: ShapeStreamState,
        mode: ElectricShapeRequestMode = .catchUp,
        staleCacheBuster: String? = nil,
        subset: ShapeSubsetRequest? = nil,
        includeCursor: Bool = true,
        includeLiveParams: Bool = true
    ) -> URL {
        var components = URLComponents(url: shape.url, resolvingAgainstBaseURL: false)
            ?? URLComponents()
        var queryItems = components.queryItems ?? []
        let shapeKey = canonicalShapeKey(shape: shape)

        set(&queryItems, name: "table", value: shape.table)
        set(&queryItems, name: "offset", value: state.offset)
        set(&queryItems, name: "where", value: shape.whereClause)
        set(&queryItems, name: "replica", value: shape.replica.rawValue)
        set(&queryItems, name: "columns", value: shape.columns.isEmpty ? nil : shape.columns.joined(separator: ","))
        set(&queryItems, name: "handle", value: state.handle)
        set(&queryItems, name: "cursor", value: includeCursor ? state.cursor : nil)
        set(&queryItems, name: "live", value: includeLiveParams && (mode.isLive || state.isLive) ? "true" : nil)
        set(&queryItems, name: "live_sse", value: includeLiveParams && mode == .liveSSE ? "true" : nil)
        set(&queryItems, name: "experimental_live_sse", value: includeLiveParams && mode == .liveSSE ? "true" : nil)
        set(&queryItems, name: "expired_handle", value: ElectricCaches.expiredShapes.getExpiredHandle(for: shapeKey))
        set(&queryItems, name: "cache-buster", value: staleCacheBuster)

        for (key, value) in shape.extraParameters {
            set(&queryItems, name: key, value: value)
        }
        if let subset {
            applySubset(subset, to: &queryItems)
        }

        components.queryItems = queryItems
        return components.url ?? shape.url
    }

    static func canonicalShapeKey(shape: ElectricShape) -> String {
        var components = URLComponents(url: shape.url, resolvingAgainstBaseURL: false)
            ?? URLComponents()
        var queryItems: [URLQueryItem] = []
        set(&queryItems, name: "table", value: shape.table)
        set(&queryItems, name: "where", value: shape.whereClause)
        set(&queryItems, name: "replica", value: shape.replica.rawValue)
        set(&queryItems, name: "columns", value: shape.columns.isEmpty ? nil : shape.columns.joined(separator: ","))
        for key in shape.extraParameters.keys.sorted() {
            set(&queryItems, name: key, value: shape.extraParameters[key])
        }
        components.queryItems = queryItems
        return components.url?.absoluteString ?? shape.url.absoluteString
    }

    private static func set(_ items: inout [URLQueryItem], name: String, value: String?) {
        items.removeAll { $0.name == name }
        guard let value else { return }
        items.append(URLQueryItem(name: name, value: value))
    }

    private static func applySubset(_ subset: ShapeSubsetRequest, to items: inout [URLQueryItem]) {
        set(&items, name: subsetWhereParam, value: subset.whereClause)
        if subset.params.isEmpty == false,
           let data = try? JSONEncoder().encode(subset.params),
           let json = String(data: data, encoding: .utf8) {
            set(&items, name: subsetParamsParam, value: json)
        } else {
            set(&items, name: subsetParamsParam, value: nil)
        }
        set(&items, name: subsetLimitParam, value: subset.limit.map(String.init))
        set(&items, name: subsetOffsetParam, value: subset.offset.map(String.init))
        set(&items, name: subsetOrderByParam, value: subset.orderBy)
    }
}

private struct SnapshotRequestBody: Encodable {
    let whereClause: String?
    let params: [String: ElectricValue]?
    let limit: Int?
    let offset: Int?
    let orderBy: String?

    init(_ subset: ShapeSubsetRequest) {
        whereClause = subset.whereClause
        params = subset.params.isEmpty ? nil : subset.params
        limit = subset.limit
        offset = subset.offset
        orderBy = subset.orderBy
    }

    enum CodingKeys: String, CodingKey {
        case whereClause = "where"
        case params
        case limit
        case offset
        case orderBy = "order_by"
    }
}
