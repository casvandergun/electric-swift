import Foundation

public final class UpToDateTracker: @unchecked Sendable {
    private struct Entry: Sendable, Codable, Hashable {
        var timestamp: Date
        var cursor: String
    }

    private let lock = NSLock()
    private var data: [String: Entry]
    private let cacheTTL: TimeInterval
    private let maxEntries: Int

    public init(
        cacheTTL: TimeInterval = 60,
        maxEntries: Int = 250
    ) {
        self.data = [:]
        self.cacheTTL = cacheTTL
        self.maxEntries = maxEntries
    }

    public func recordUpToDate(shapeKey: String, cursor: String) {
        lock.lock()
        defer { lock.unlock() }
        data[shapeKey] = Entry(timestamp: Date(), cursor: cursor)
        if data.count > maxEntries,
           let oldestKey = data.min(by: { $0.value.timestamp < $1.value.timestamp })?.key {
            data.removeValue(forKey: oldestKey)
        }
    }

    public func replayCursorIfRecent(for shapeKey: String) -> String? {
        lock.lock()
        defer { lock.unlock() }
        cleanupLocked()
        guard let entry = data[shapeKey] else { return nil }
        return entry.cursor
    }

    public func clear() {
        lock.lock()
        defer { lock.unlock() }
        data.removeAll(keepingCapacity: false)
    }

    public func delete(shapeKey: String) {
        lock.lock()
        defer { lock.unlock() }
        data.removeValue(forKey: shapeKey)
    }

    private func cleanupLocked() {
        let cutoff = Date().addingTimeInterval(-cacheTTL)
        data = data.filter { $0.value.timestamp >= cutoff }
    }
}

enum ElectricTrackers {
    static let upToDate = UpToDateTracker()
}
