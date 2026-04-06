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
        options: ShapeStreamOptions,
        state: ShapeStreamState,
        timeout: TimeInterval,
        mode: ElectricShapeRequestMode,
        headers: [String: String] = [:],
        staleCacheBuster: String? = nil,
        columnMapper: ColumnMapper? = nil
    ) -> URLRequest {
        var request = URLRequest(url: makeURL(options: options, state: state, mode: mode, staleCacheBuster: staleCacheBuster, columnMapper: columnMapper))
        request.httpMethod = "GET"
        request.timeoutInterval = timeout
        request.cachePolicy = .reloadIgnoringLocalCacheData
        for (header, value) in headers {
            request.setValue(value, forHTTPHeaderField: header)
        }
        if mode == .liveSSE {
            request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        }
        return request
    }

    static func makeSnapshotRequest(
        options: ShapeStreamOptions,
        state: ShapeStreamState,
        timeout: TimeInterval,
        subset: ShapeSubsetRequest,
        headers: [String: String] = [:],
        staleCacheBuster: String? = nil,
        columnMapper: ColumnMapper? = nil
    ) throws -> URLRequest {
        switch subset.method {
        case .get:
            var request = URLRequest(
                url: makeURL(
                    options: options,
                    state: state,
                    mode: .catchUp,
                    staleCacheBuster: staleCacheBuster,
                    subset: subset,
                    includeCursor: false,
                    includeLiveParams: false,
                    columnMapper: columnMapper
                )
            )
            request.httpMethod = SnapshotMethod.get.rawValue
            request.timeoutInterval = timeout
            request.cachePolicy = .reloadIgnoringLocalCacheData
            for (header, value) in headers {
                request.setValue(value, forHTTPHeaderField: header)
            }
            return request
        case .post:
            var request = URLRequest(
                url: makeURL(
                    options: options,
                    state: state,
                    mode: .catchUp,
                    staleCacheBuster: staleCacheBuster,
                    subset: nil,
                    includeCursor: false,
                    includeLiveParams: false,
                    columnMapper: columnMapper
                )
            )
            request.httpMethod = SnapshotMethod.post.rawValue
            request.timeoutInterval = timeout
            request.cachePolicy = .reloadIgnoringLocalCacheData
            for (header, value) in headers {
                request.setValue(value, forHTTPHeaderField: header)
            }
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONEncoder().encode(SnapshotRequestBody(subset, columnMapper: columnMapper))
            return request
        }
    }

    static func makeURL(
        options: ShapeStreamOptions,
        state: ShapeStreamState,
        mode: ElectricShapeRequestMode = .catchUp,
        staleCacheBuster: String? = nil,
        subset: ShapeSubsetRequest? = nil,
        includeCursor: Bool = true,
        includeLiveParams: Bool = true,
        columnMapper: ColumnMapper? = nil
    ) -> URL {
        var components = URLComponents(url: options.url, resolvingAgainstBaseURL: false)
            ?? URLComponents()
        var queryItems = components.queryItems ?? []
        let shapeKey = canonicalShapeKey(options: options, columnMapper: columnMapper)

        set(&queryItems, name: "table", value: options.table)
        set(&queryItems, name: "offset", value: state.offset)
        set(&queryItems, name: "where", value: ColumnMappingSupport.encodeWhereClause(options.whereClause, using: columnMapper))
        setWhereParams(&queryItems, params: options.params)
        set(&queryItems, name: "replica", value: options.replica.rawValue)
        set(&queryItems, name: "log", value: options.log.rawValue)
        set(&queryItems, name: "columns", value: serializedColumns(options.columns, columnMapper: columnMapper))
        set(&queryItems, name: "handle", value: state.handle)
        set(&queryItems, name: "cursor", value: includeCursor ? state.cursor : nil)
        set(&queryItems, name: "live", value: includeLiveParams && (mode.isLive || state.isLive) ? "true" : nil)
        set(&queryItems, name: "live_sse", value: includeLiveParams && mode == .liveSSE ? "true" : nil)
        set(&queryItems, name: "experimental_live_sse", value: includeLiveParams && mode == .liveSSE ? "true" : nil)
        set(&queryItems, name: "expired_handle", value: ElectricCaches.expiredShapes.getExpiredHandle(for: shapeKey))
        set(&queryItems, name: "cache-buster", value: staleCacheBuster)

        for (key, value) in options.extraParameters {
            set(&queryItems, name: key, value: value)
        }
        if let subset {
            applySubset(subset, to: &queryItems, columnMapper: columnMapper)
        }

        components.queryItems = queryItems
        return components.url ?? options.url
    }

    static func canonicalShapeKey(options: ShapeStreamOptions, columnMapper: ColumnMapper? = nil) -> String {
        var components = URLComponents(url: options.url, resolvingAgainstBaseURL: false)
            ?? URLComponents()
        var queryItems: [URLQueryItem] = []
        set(&queryItems, name: "table", value: options.table)
        set(&queryItems, name: "where", value: ColumnMappingSupport.encodeWhereClause(options.whereClause, using: columnMapper))
        setWhereParams(&queryItems, params: options.params)
        set(&queryItems, name: "replica", value: options.replica.rawValue)
        set(&queryItems, name: "log", value: options.log.rawValue)
        set(&queryItems, name: "columns", value: serializedColumns(options.columns, columnMapper: columnMapper))
        for key in options.extraParameters.keys.sorted() {
            set(&queryItems, name: key, value: options.extraParameters[key])
        }
        components.queryItems = queryItems
        return components.url?.absoluteString ?? options.url.absoluteString
    }

    private static func set(_ items: inout [URLQueryItem], name: String, value: String?) {
        items.removeAll { $0.name == name }
        guard let value else { return }
        items.append(URLQueryItem(name: name, value: value))
    }

    private static func applySubset(_ subset: ShapeSubsetRequest, to items: inout [URLQueryItem], columnMapper: ColumnMapper?) {
        set(&items, name: subsetWhereParam, value: ColumnMappingSupport.encodeWhereClause(subset.whereClause, using: columnMapper))
        if subset.params.isEmpty == false,
           let data = try? JSONEncoder().encode(subset.params),
           let json = String(data: data, encoding: .utf8) {
            set(&items, name: subsetParamsParam, value: json)
        } else {
            set(&items, name: subsetParamsParam, value: nil)
        }
        set(&items, name: subsetLimitParam, value: subset.limit.map(String.init))
        set(&items, name: subsetOffsetParam, value: subset.offset.map(String.init))
        set(&items, name: subsetOrderByParam, value: ColumnMappingSupport.encodeWhereClause(subset.orderBy, using: columnMapper))
    }

    private static func setWhereParams(_ items: inout [URLQueryItem], params: [String: String]) {
        for key in params.keys {
            items.removeAll { $0.name == "params[\(key)]" }
        }
        for key in params.keys.sorted() {
            items.append(URLQueryItem(name: "params[\(key)]", value: params[key]))
        }
    }

    private static func serializedColumns(_ columns: [String], columnMapper: ColumnMapper?) -> String? {
        guard columns.isEmpty == false else { return nil }
        return columns
            .map { columnMapper?.encode($0) ?? $0 }
            .map(ColumnMappingSupport.quoteIdentifier)
            .joined(separator: ",")
    }
}

private struct SnapshotRequestBody: Encodable {
    let whereClause: String?
    let params: [String: ElectricValue]?
    let limit: Int?
    let offset: Int?
    let orderBy: String?

    init(_ subset: ShapeSubsetRequest, columnMapper: ColumnMapper?) {
        whereClause = ColumnMappingSupport.encodeWhereClause(subset.whereClause, using: columnMapper)
        params = subset.params.isEmpty ? nil : subset.params
        limit = subset.limit
        offset = subset.offset
        orderBy = ColumnMappingSupport.encodeWhereClause(subset.orderBy, using: columnMapper)
    }

    enum CodingKeys: String, CodingKey {
        case whereClause = "where"
        case params
        case limit
        case offset
        case orderBy = "order_by"
    }
}
