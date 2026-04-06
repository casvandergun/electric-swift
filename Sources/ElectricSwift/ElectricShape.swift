import Foundation

public enum ElectricReplica: String, Sendable, Codable, Hashable {
    case `default`
    case full
}

public enum ShapeLogMode: String, Sendable, Codable, Hashable {
    case changesOnly = "changes_only"
    case full
}

public enum ShapeRequestParam: Sendable, Hashable, Codable {
    case string(String)
    case strings([String])
    case object([String: String])
}

extension ShapeRequestParam: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self = .string(value)
    }
}

public struct ShapeStreamOptions: Sendable, Hashable, Codable {
    public var url: URL
    public var table: String?
    public var columns: [String]
    public var whereClause: String?
    public var whereParams: [String: String]
    public var params: [String: ShapeRequestParam]
    public var replica: ElectricReplica
    public var log: ShapeLogMode
    public var offset: String?
    public var handle: String?
    public var headers: [String: String]

    public init(
        url: URL,
        table: String? = nil,
        columns: [String] = [],
        whereClause: String? = nil,
        whereParams: [String: String] = [:],
        params: [String: ShapeRequestParam] = [:],
        replica: ElectricReplica = .default,
        log: ShapeLogMode = .full,
        offset: String? = nil,
        handle: String? = nil,
        headers: [String: String] = [:]
    ) {
        self.url = url
        self.table = table
        self.columns = columns
        self.whereClause = whereClause
        self.whereParams = whereParams
        self.params = params
        self.replica = replica
        self.log = log
        self.offset = offset
        self.handle = handle
        self.headers = headers
    }
}

public struct ShapeStreamConfiguration: Sendable, Hashable, Codable {
    public var subscribe: Bool
    public var timeout: TimeInterval
    public var initialState: ShapeStreamState
    public var preferSSE: Bool
    public var minSSEConnectionDuration: TimeInterval
    public var maxShortSSEConnections: Int
    public var maxStaleCacheRetries: Int
    public var retryPolicy: ShapeStreamRetryPolicy

    public init(
        subscribe: Bool = true,
        timeout: TimeInterval = 30,
        initialState: ShapeStreamState = .init(),
        preferSSE: Bool = true,
        minSSEConnectionDuration: TimeInterval = 1,
        maxShortSSEConnections: Int = 3,
        maxStaleCacheRetries: Int = 3,
        retryPolicy: ShapeStreamRetryPolicy = .init()
    ) {
        self.subscribe = subscribe
        self.timeout = timeout
        self.initialState = initialState
        self.preferSSE = preferSSE
        self.minSSEConnectionDuration = minSSEConnectionDuration
        self.maxShortSSEConnections = maxShortSSEConnections
        self.maxStaleCacheRetries = maxStaleCacheRetries
        self.retryPolicy = retryPolicy
    }
}

public struct ShapeStreamRetryPolicy: Sendable, Hashable, Codable {
    public var isEnabled: Bool
    public var backoff: BackoffOptions

    public init(
        isEnabled: Bool = true,
        backoff: BackoffOptions = .init()
    ) {
        self.isEnabled = isEnabled
        self.backoff = backoff
    }
}

public enum ShapeStreamFailure: Error, Sendable {
    case fetch(FetchError)
    case stream(ShapeStreamError)
    case url(URLError.Code)
    case other(String)

    static func wrap(_ error: Error) -> ShapeStreamFailure {
        if let fetchError = error as? FetchError {
            return .fetch(fetchError)
        }
        if let streamError = error as? ShapeStreamError {
            return .stream(streamError)
        }
        if let urlError = error as? URLError {
            return .url(urlError.code)
        }
        return .other(String(describing: error))
    }
}

public struct ShapeStreamErrorContext: Sendable {
    public let failure: ShapeStreamFailure
    public let options: ShapeStreamOptions
    public let state: ShapeStreamState

    public init(
        failure: ShapeStreamFailure,
        options: ShapeStreamOptions,
        state: ShapeStreamState
    ) {
        self.failure = failure
        self.options = options
        self.state = state
    }
}

public enum ShapeStreamErrorDecision: Sendable {
    case stop
    case retry
    case retryWithOptions(ShapeStreamOptions)
}

public typealias ShapeStreamErrorHandler =
    @Sendable (ShapeStreamErrorContext) async -> ShapeStreamErrorDecision

public typealias ShapeStreamDynamicHeaders =
    @Sendable () async throws -> [String: String]

public typealias ShapeStreamDynamicParams =
    @Sendable () async throws -> [String: ShapeRequestParam]

public enum SnapshotMethod: String, Sendable, Hashable, Codable {
    case get = "GET"
    case post = "POST"
}

public struct ShapeSubsetRequest: Sendable, Hashable, Codable {
    public var whereClause: String?
    public var params: [String: ElectricValue]
    public var limit: Int?
    public var offset: Int?
    public var orderBy: String?
    public var method: SnapshotMethod

    public init(
        whereClause: String? = nil,
        params: [String: ElectricValue] = [:],
        limit: Int? = nil,
        offset: Int? = nil,
        orderBy: String? = nil,
        method: SnapshotMethod = .get
    ) {
        self.whereClause = whereClause
        self.params = params
        self.limit = limit
        self.offset = offset
        self.orderBy = orderBy
        self.method = method
    }
}

public struct ShapeSnapshotResult: Sendable, Hashable {
    public let metadata: SnapshotMetadata
    public let messages: [ElectricMessage]
    public let responseOffset: String?
    public let responseHandle: String?

    public init(
        metadata: SnapshotMetadata,
        messages: [ElectricMessage],
        responseOffset: String?,
        responseHandle: String?
    ) {
        self.metadata = metadata
        self.messages = messages
        self.responseOffset = responseOffset
        self.responseHandle = responseHandle
    }
}
