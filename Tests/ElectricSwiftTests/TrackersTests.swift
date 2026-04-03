@testable import ElectricSwift
import Foundation
import Testing

@Suite("Trackers")
struct TrackersTests {
    @Test("Expired shapes cache returns marked handle")
    func expiredShapesCacheTracksHandles() {
        let cache = ExpiredShapesCache()
        #expect(cache.getExpiredHandle(for: "shape-1") == nil)
        cache.markExpired(shapeKey: "shape-1", handle: "handle-1")
        #expect(cache.getExpiredHandle(for: "shape-1") == "handle-1")
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
}
