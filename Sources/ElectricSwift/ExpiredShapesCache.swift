import Foundation

public final class ExpiredShapesCache: @unchecked Sendable {
    private struct Entry: Sendable, Codable, Hashable {
        var expiredHandle: String
        var lastUsed: Date
    }

    private let lock = NSLock()
    private var data: [String: Entry]
    private let maxEntries: Int

    public init(maxEntries: Int = 250) {
        self.data = [:]
        self.maxEntries = maxEntries
    }

    public func getExpiredHandle(for shapeKey: String) -> String? {
        lock.lock()
        defer { lock.unlock() }
        guard var entry = data[shapeKey] else { return nil }
        entry.lastUsed = Date()
        data[shapeKey] = entry
        return entry.expiredHandle
    }

    public func markExpired(shapeKey: String, handle: String) {
        lock.lock()
        defer { lock.unlock() }

        data[shapeKey] = Entry(expiredHandle: handle, lastUsed: Date())
        if data.count > maxEntries,
           let oldestKey = data.min(by: { $0.value.lastUsed < $1.value.lastUsed })?.key {
            data.removeValue(forKey: oldestKey)
        }
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
}

public enum ElectricCaches {
    public static let expiredShapes = ExpiredShapesCache()
}
