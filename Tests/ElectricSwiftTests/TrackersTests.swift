@testable import ElectricSwift
import Foundation
import Testing

@Suite("Trackers", .serialized)
struct TrackersTests {
    @Test("Expired shapes cache returns marked handle")
    func expiredShapesCacheTracksHandles() {
        let cache = ExpiredShapesCache()
        #expect(cache.getExpiredHandle(for: "shape-1") == nil)
        cache.markExpired(shapeKey: "shape-1", handle: "handle-1")
        #expect(cache.getExpiredHandle(for: "shape-1") == "handle-1")
        #expect(cache.getExpiredHandle(for: "shape-2") == nil)
    }

    @Test("Expired shapes cache enforces LRU eviction")
    func expiredShapesCacheEnforcesLRU() async throws {
        let cache = ExpiredShapesCache(maxEntries: 2)
        cache.markExpired(shapeKey: "shape-1", handle: "handle-1")
        try await Task.sleep(nanoseconds: 2_000_000)
        cache.markExpired(shapeKey: "shape-2", handle: "handle-2")
        try await Task.sleep(nanoseconds: 2_000_000)
        _ = cache.getExpiredHandle(for: "shape-1")
        try await Task.sleep(nanoseconds: 2_000_000)
        cache.markExpired(shapeKey: "shape-3", handle: "handle-3")

        #expect(cache.getExpiredHandle(for: "shape-1") == "handle-1")
        #expect(cache.getExpiredHandle(for: "shape-2") == nil)
        #expect(cache.getExpiredHandle(for: "shape-3") == "handle-3")
    }

    @Test("Expired shapes cache can clear and delete entries")
    func expiredShapesCacheClearsAndDeletes() {
        let cache = ExpiredShapesCache()
        cache.markExpired(shapeKey: "shape-1", handle: "handle-1")
        cache.markExpired(shapeKey: "shape-2", handle: "handle-2")

        cache.delete(shapeKey: "shape-1")
        #expect(cache.getExpiredHandle(for: "shape-1") == nil)
        #expect(cache.getExpiredHandle(for: "shape-2") == "handle-2")

        cache.clear()
        #expect(cache.getExpiredHandle(for: "shape-2") == nil)
    }

    @Test("Up-to-date tracker returns recent replay cursor")
    func upToDateTrackerReturnsRecentCursor() {
        let tracker = UpToDateTracker(cacheTTL: 60)
        #expect(tracker.replayCursorIfRecent(for: "shape-1") == nil)
        tracker.recordUpToDate(shapeKey: "shape-1", cursor: "cursor-1")
        #expect(tracker.replayCursorIfRecent(for: "shape-1") == "cursor-1")
    }

    @Test("Up-to-date tracker expires old replay cursors after TTL")
    func upToDateTrackerExpiresOldCursors() async throws {
        let tracker = UpToDateTracker(cacheTTL: 0.01)
        tracker.recordUpToDate(shapeKey: "shape-1", cursor: "cursor-1")
        #expect(tracker.replayCursorIfRecent(for: "shape-1") == "cursor-1")

        try await Task.sleep(nanoseconds: 30_000_000)

        #expect(tracker.replayCursorIfRecent(for: "shape-1") == nil)
    }

    @Test("Up-to-date tracker keeps different shapes independent")
    func upToDateTrackerTracksShapesIndependently() {
        let tracker = UpToDateTracker(cacheTTL: 60)

        tracker.recordUpToDate(shapeKey: "shape-1", cursor: "cursor-1")
        tracker.recordUpToDate(shapeKey: "shape-2", cursor: "cursor-2")

        #expect(tracker.replayCursorIfRecent(for: "shape-1") == "cursor-1")
        #expect(tracker.replayCursorIfRecent(for: "shape-2") == "cursor-2")
    }

    @Test("Up-to-date tracker evicts the oldest recorded entries once max entries is exceeded")
    func upToDateTrackerEvictsOldestEntries() async throws {
        let tracker = UpToDateTracker(cacheTTL: 60, maxEntries: 2)
        tracker.recordUpToDate(shapeKey: "shape-1", cursor: "cursor-1")
        try await Task.sleep(nanoseconds: 2_000_000)
        tracker.recordUpToDate(shapeKey: "shape-2", cursor: "cursor-2")
        try await Task.sleep(nanoseconds: 2_000_000)
        tracker.recordUpToDate(shapeKey: "shape-3", cursor: "cursor-3")

        #expect(tracker.replayCursorIfRecent(for: "shape-1") == nil)
        #expect(tracker.replayCursorIfRecent(for: "shape-2") == "cursor-2")
        #expect(tracker.replayCursorIfRecent(for: "shape-3") == "cursor-3")
    }

    @Test("Up-to-date tracker can clear and delete entries")
    func upToDateTrackerClearsAndDeletes() {
        let tracker = UpToDateTracker(cacheTTL: 60)
        tracker.recordUpToDate(shapeKey: "shape-1", cursor: "cursor-1")
        tracker.recordUpToDate(shapeKey: "shape-2", cursor: "cursor-2")

        tracker.delete(shapeKey: "shape-1")
        #expect(tracker.replayCursorIfRecent(for: "shape-1") == nil)
        #expect(tracker.replayCursorIfRecent(for: "shape-2") == "cursor-2")

        tracker.clear()
        #expect(tracker.replayCursorIfRecent(for: "shape-2") == nil)
    }
}
