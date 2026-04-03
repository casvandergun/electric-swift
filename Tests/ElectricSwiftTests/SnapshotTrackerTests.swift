@testable import ElectricSwift
import Testing

@Suite("Snapshot Tracker", .serialized)
struct SnapshotTrackerTests {
    @Test("Rejects change already visible in snapshot")
    func rejectsVisibleMessage() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "100", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:1", txids: [50])) == true)
    }

    @Test("Rejects change when xid is below xmax and not in xip")
    func rejectsCommittedTransactionBeforeSnapshotBoundary() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "50", xmax: "200", xipList: ["150", "175"], databaseLSN: "123"),
            keys: ["user:1"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:1", operation: .update, txids: [100])) == true)
    }

    @Test("Does not reject parallel transaction in xip")
    func allowsParallelTransaction() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "50", xmax: "200", xipList: ["100", "150"], databaseLSN: "123"),
            keys: ["user:1"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:1", operation: .update, txids: [100])) == false)
    }

    @Test("Does not reject message when xid is outside the snapshot")
    func allowsMessageOutsideSnapshotBoundary() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "50", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:1", txids: [250])) == false)
    }

    @Test("Does not reject message when key is not in any snapshot")
    func allowsKeysOutsideTrackedSnapshots() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "100", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1", "user:2"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:3", txids: [50])) == false)
    }

    @Test("Rejects message when key is in snapshot and txid condition matches")
    func rejectsTrackedKeysWhenTransactionIsVisible() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "100", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:1", txids: [50])) == true)
    }

    @Test("Rejects message if included in any active snapshot")
    func rejectsMessageIfAnySnapshotContainsIt() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "100", xmax: "200", xipList: ["170"], databaseLSN: "123"),
            keys: ["user:1", "user:2"]
        )
        tracker.addSnapshot(
            metadata(snapshotMark: 2, xmin: "300", xmax: "400", xipList: [], databaseLSN: "456"),
            keys: ["user:1", "user:2"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:1", txids: [170])) == true)
    }

    @Test("Does not reject message if no active snapshot includes it")
    func allowsMessageIfNoSnapshotIncludesIt() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "100", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )
        tracker.addSnapshot(
            metadata(snapshotMark: 2, xmin: "300", xmax: "400", xipList: [], databaseLSN: "456"),
            keys: ["user:3"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:2", txids: [50])) == false)
    }

    @Test("Cleans up snapshots when a later xid passes xmax")
    func cleansUpSnapshotsWhenTransactionAdvancesPastBoundary() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "50", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:2", txids: [250])) == false)
        #expect(tracker.shouldRejectMessage(message(key: "user:1", operation: .update, txids: [60])) == false)
    }

    @Test("Keeps other snapshots active when one is cleaned up")
    func cleanupLeavesOtherSnapshotsActive() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "50", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )
        tracker.addSnapshot(
            metadata(snapshotMark: 2, xmin: "50", xmax: "400", xipList: [], databaseLSN: "456"),
            keys: ["user:2"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "other", txids: [250])) == false)
        #expect(tracker.shouldRejectMessage(message(key: "user:1", txids: [60])) == false)
        #expect(tracker.shouldRejectMessage(message(key: "user:2", txids: [60])) == true)
    }

    @Test("Cleans up all snapshots with the same xmax from one message")
    func cleanupRemovesMultipleSnapshotsSharingBoundary() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "50", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )
        tracker.addSnapshot(
            metadata(snapshotMark: 2, xmin: "60", xmax: "200", xipList: [], databaseLSN: "124"),
            keys: ["user:2"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "other", txids: [250])) == false)
        #expect(tracker.shouldRejectMessage(message(key: "user:1", txids: [70])) == false)
        #expect(tracker.shouldRejectMessage(message(key: "user:2", txids: [70])) == false)
    }

    @Test("Uses the highest txid when filtering snapshot-visible messages")
    func usesHighestTransactionID() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "50", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:1", operation: .update, txids: [10, 150])) == true)
    }

    @Test("Allows messages without txids")
    func allowsMessagesWithoutTransactionIDs() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "50", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )

        #expect(tracker.shouldRejectMessage(message(key: "user:1", operation: .update, txids: nil)) == false)
    }

    @Test("Last seen update removes snapshots whose database LSN has been passed")
    func removesSnapshotsAfterLastSeenUpdate() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            metadata(snapshotMark: 1, xmin: "50", xmax: "200", xipList: [], databaseLSN: "123"),
            keys: ["user:1"]
        )
        tracker.addSnapshot(
            metadata(snapshotMark: 2, xmin: "50", xmax: "300", xipList: [], databaseLSN: "250"),
            keys: ["user:2"]
        )

        tracker.lastSeenUpdate(databaseLSN: 123)

        #expect(tracker.shouldRejectMessage(message(key: "user:1", operation: .update, txids: [60])) == false)
        #expect(tracker.shouldRejectMessage(message(key: "user:2", operation: .update, txids: [60])) == true)
    }

    private func metadata(
        snapshotMark: Int,
        xmin: String,
        xmax: String,
        xipList: [String],
        databaseLSN: String
    ) -> SnapshotMetadata {
        SnapshotMetadata(
            snapshotMark: snapshotMark,
            xmin: xmin,
            xmax: xmax,
            xipList: xipList,
            databaseLSN: databaseLSN
        )
    }

    private func message(
        key: String,
        operation: ElectricOperation = .insert,
        txids: [Int64]?
    ) -> ElectricMessage {
        ElectricMessage(
            key: key,
            value: ["id": .integer(1)],
            headers: .init(operation: operation, txids: txids)
        )
    }
}
