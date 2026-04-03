import Foundation

public enum ElectricOperation: String, Sendable, Codable, Hashable {
    case insert
    case update
    case delete
}

public enum ElectricControl: String, Sendable, Codable, Hashable {
    case upToDate = "up-to-date"
    case mustRefetch = "must-refetch"
    case snapshotEnd = "snapshot-end"
    case subsetEnd = "subset-end"
}

public struct ElectricMoveOutPattern: Sendable, Codable, Hashable {
    public let pos: Int
    public let value: String

    public init(pos: Int, value: String) {
        self.pos = pos
        self.value = value
    }
}

public struct ElectricMessageHeaders: Sendable, Codable, Hashable {
    public let operation: ElectricOperation?
    public let control: ElectricControl?
    public let event: String?
    public let txids: [Int64]?
    public let tags: [String]?
    public let removedTags: [String]?
    public let patterns: [ElectricMoveOutPattern]?
    public let globalLastSeenLSN: String?
    public let snapshotMark: Int?
    public let databaseLSN: String?
    public let xmin: String?
    public let xmax: String?
    public let xipList: [String]?

    enum CodingKeys: String, CodingKey {
        case operation
        case control
        case event
        case txids
        case tags
        case removedTags = "removed_tags"
        case patterns
        case globalLastSeenLSN = "global_last_seen_lsn"
        case snapshotMark = "snapshot_mark"
        case databaseLSN = "database_lsn"
        case xmin
        case xmax
        case xipList = "xip_list"
    }

    public init(
        operation: ElectricOperation? = nil,
        control: ElectricControl? = nil,
        event: String? = nil,
        txids: [Int64]? = nil,
        tags: [String]? = nil,
        removedTags: [String]? = nil,
        patterns: [ElectricMoveOutPattern]? = nil,
        globalLastSeenLSN: String? = nil,
        snapshotMark: Int? = nil,
        databaseLSN: String? = nil,
        xmin: String? = nil,
        xmax: String? = nil,
        xipList: [String]? = nil
    ) {
        self.operation = operation
        self.control = control
        self.event = event
        self.txids = txids
        self.tags = tags
        self.removedTags = removedTags
        self.patterns = patterns
        self.globalLastSeenLSN = globalLastSeenLSN
        self.snapshotMark = snapshotMark
        self.databaseLSN = databaseLSN
        self.xmin = xmin
        self.xmax = xmax
        self.xipList = xipList
    }
}

public struct ElectricMessage: Sendable, Codable, Hashable {
    public let key: String?
    public var value: ElectricRow?
    public var oldValue: ElectricRow?
    public let headers: ElectricMessageHeaders

    enum CodingKeys: String, CodingKey {
        case key
        case value
        case oldValue = "old_value"
        case headers
    }

    public init(
        key: String? = nil,
        value: ElectricRow? = nil,
        oldValue: ElectricRow? = nil,
        headers: ElectricMessageHeaders
    ) {
        self.key = key
        self.value = value
        self.oldValue = oldValue
        self.headers = headers
    }

    public var normalizedKey: String? {
        guard let key else { return nil }
        let trimmed = key.replacingOccurrences(of: "\"", with: "")
        return trimmed.split(separator: "/").last.map(String.init) ?? trimmed
    }

    public var isChangeMessage: Bool {
        headers.operation != nil
    }

    public var isControlMessage: Bool {
        headers.control != nil
    }

    public static func mustRefetch() -> Self {
        Self(headers: .init(control: .mustRefetch))
    }

    public static func upToDate() -> Self {
        Self(headers: .init(control: .upToDate))
    }
}

public struct ElectricColumnDefinition: Sendable, Codable, Hashable {
    public let type: String
    public let dims: Int?
    public let notNull: Bool?
    public let maxLength: Int?
    public let length: Int?
    public let precision: Int?
    public let scale: Int?
    public let fields: String?

    enum CodingKeys: String, CodingKey {
        case type
        case dims
        case notNull = "not_null"
        case maxLength = "max_length"
        case length
        case precision
        case scale
        case fields
    }

    public init(
        type: String,
        dims: Int? = nil,
        notNull: Bool? = nil,
        maxLength: Int? = nil,
        length: Int? = nil,
        precision: Int? = nil,
        scale: Int? = nil,
        fields: String? = nil
    ) {
        self.type = type
        self.dims = dims
        self.notNull = notNull
        self.maxLength = maxLength
        self.length = length
        self.precision = precision
        self.scale = scale
        self.fields = fields
    }
}

public typealias ElectricSchema = [String: ElectricColumnDefinition]

public enum ElectricShapeBoundaryKind: String, Sendable, Codable, Hashable {
    case upToDate
    case liveUpdate
    case mustRefetch
}

public struct ElectricShapeBatch: Sendable, Hashable {
    public let messages: [ElectricMessage]
    public let schema: ElectricSchema
    public let checkpoint: ElectricShapeCheckpoint
    public let phase: ElectricShapePhase
    public let boundaryKind: ElectricShapeBoundaryKind

    public init(
        messages: [ElectricMessage],
        checkpoint: ElectricShapeCheckpoint,
        schema: ElectricSchema,
        phase: ElectricShapePhase,
        boundaryKind: ElectricShapeBoundaryKind
    ) {
        self.messages = messages
        self.schema = schema
        self.checkpoint = checkpoint
        self.phase = phase
        self.boundaryKind = boundaryKind
    }

    public init(
        messages: [ElectricMessage],
        state: ShapeStreamState,
        schema: ElectricSchema,
        reachedUpToDate: Bool
    ) {
        self.messages = messages
        self.schema = schema
        self.checkpoint = state.checkpoint
        self.phase = state.phase
        if messages.contains(where: { $0.headers.control == .mustRefetch }) {
            self.boundaryKind = .mustRefetch
        } else {
            self.boundaryKind = reachedUpToDate ? .upToDate : .liveUpdate
        }
    }

    public var state: ShapeStreamState {
        ShapeStreamState(
            checkpoint: checkpoint,
            phase: phase,
            isUpToDate: boundaryKind != .mustRefetch,
            schema: schema
        )
    }

    public var reachedUpToDate: Bool {
        boundaryKind == .upToDate
    }
}

public typealias ShapeBatch = ElectricShapeBatch
