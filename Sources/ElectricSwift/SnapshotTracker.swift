import Foundation

public struct SnapshotMetadata: Sendable, Hashable, Codable {
    public let snapshotMark: Int
    public let xmin: String
    public let xmax: String
    public let xipList: [String]
    public let databaseLSN: String

    public init(
        snapshotMark: Int,
        xmin: String,
        xmax: String,
        xipList: [String],
        databaseLSN: String
    ) {
        self.snapshotMark = snapshotMark
        self.xmin = xmin
        self.xmax = xmax
        self.xipList = xipList
        self.databaseLSN = databaseLSN
    }

    enum CodingKeys: String, CodingKey {
        case snapshotMark = "snapshot_mark"
        case xmin
        case xmax
        case xipList = "xip_list"
        case databaseLSN = "database_lsn"
    }
}

public final class SnapshotTracker: @unchecked Sendable {
    private struct ActiveSnapshot: Sendable {
        let xmin: UInt64
        let xmax: UInt64
        let xipList: Set<UInt64>
        let keys: Set<String>
        let databaseLSN: UInt64
    }

    private let lock = NSLock()
    private var activeSnapshots: [Int: ActiveSnapshot] = [:]

    public init() {}

    public func addSnapshot(_ metadata: SnapshotMetadata, keys: Set<String>) {
        lock.lock()
        defer { lock.unlock() }
        activeSnapshots[metadata.snapshotMark] = ActiveSnapshot(
            xmin: UInt64(metadata.xmin) ?? 0,
            xmax: UInt64(metadata.xmax) ?? 0,
            xipList: Set(metadata.xipList.compactMap(UInt64.init)),
            keys: keys,
            databaseLSN: UInt64(metadata.databaseLSN) ?? 0
        )
    }

    public func removeSnapshot(snapshotMark: Int) {
        lock.lock()
        defer { lock.unlock() }
        activeSnapshots.removeValue(forKey: snapshotMark)
    }

    public func shouldRejectMessage(_ message: ElectricMessage) -> Bool {
        guard let key = message.key,
              let txids = message.headers.txids,
              let xid = txids.max().map(UInt64.init) ?? nil else {
            return false
        }

        lock.lock()
        defer { lock.unlock() }

        for (mark, snapshot) in activeSnapshots where xid >= snapshot.xmax {
            activeSnapshots.removeValue(forKey: mark)
        }

        return activeSnapshots.values.contains { snapshot in
            snapshot.keys.contains(key) && SnapshotTracker.isVisibleInSnapshot(xid: xid, snapshot: snapshot)
        }
    }

    public func lastSeenUpdate(databaseLSN: UInt64) {
        lock.lock()
        defer { lock.unlock() }
        activeSnapshots = activeSnapshots.filter { $0.value.databaseLSN > databaseLSN }
    }

    private static func isVisibleInSnapshot(xid: UInt64, snapshot: ActiveSnapshot) -> Bool {
        xid < snapshot.xmin || (xid < snapshot.xmax && snapshot.xipList.contains(xid) == false)
    }
}
