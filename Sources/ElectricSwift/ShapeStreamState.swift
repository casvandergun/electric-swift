import Foundation

public struct ElectricShapeCheckpoint: Sendable, Hashable, Codable {
    public var handle: String?
    public var offset: String
    public var cursor: String?
    public var lastSyncedAt: Date?

    public init(
        handle: String? = nil,
        offset: String = "-1",
        cursor: String? = nil,
        lastSyncedAt: Date? = nil
    ) {
        self.handle = handle
        self.offset = offset
        self.cursor = cursor
        self.lastSyncedAt = lastSyncedAt
    }
}

public enum ElectricShapePhase: Sendable, Hashable, Codable {
    case initial
    case syncing
    case replaying
    case liveSSE
    case liveLongPoll
    case paused
    case staleRetry
    case stopped
    case failed(String)

    public var isLive: Bool {
        switch self {
        case .liveSSE, .liveLongPoll:
            true
        case .initial, .syncing, .replaying, .paused, .staleRetry, .stopped, .failed:
            false
        }
    }
}

public struct ShapeStreamState: Sendable, Hashable, Codable {
    public var checkpoint: ElectricShapeCheckpoint
    public var phase: ElectricShapePhase
    public var isUpToDate: Bool
    public var schema: ElectricSchema

    public init(
        handle: String? = nil,
        offset: String = "-1",
        cursor: String? = nil,
        isLive: Bool = false,
        isUpToDate: Bool = false,
        schema: ElectricSchema = [:],
        lastSyncedAt: Date? = nil
    ) {
        self.checkpoint = ElectricShapeCheckpoint(
            handle: handle,
            offset: offset,
            cursor: cursor,
            lastSyncedAt: lastSyncedAt
        )
        self.phase = isLive ? .liveLongPoll : .initial
        self.isUpToDate = isUpToDate
        self.schema = schema
    }

    public init(
        checkpoint: ElectricShapeCheckpoint = .init(),
        phase: ElectricShapePhase = .initial,
        isUpToDate: Bool = false,
        schema: ElectricSchema = [:]
    ) {
        self.checkpoint = checkpoint
        self.phase = phase
        self.isUpToDate = isUpToDate
        self.schema = schema
    }

    public var handle: String? {
        get { checkpoint.handle }
        set { checkpoint.handle = newValue }
    }

    public var offset: String {
        get { checkpoint.offset }
        set { checkpoint.offset = newValue }
    }

    public var cursor: String? {
        get { checkpoint.cursor }
        set { checkpoint.cursor = newValue }
    }

    public var isLive: Bool {
        phase.isLive
    }

    public var lastSyncedAt: Date? {
        get { checkpoint.lastSyncedAt }
        set { checkpoint.lastSyncedAt = newValue }
    }

    public mutating func reset(handle: String? = nil, preserveLastSyncedAt: Bool = false) {
        self.checkpoint = ElectricShapeCheckpoint(
            handle: handle,
            lastSyncedAt: preserveLastSyncedAt ? checkpoint.lastSyncedAt : nil
        )
        self.phase = .initial
        self.isUpToDate = false
        self.schema = [:]
    }
}
