@testable import ElectricSwift
import Testing

@Suite("Snapshot Tracker", .serialized)
struct SnapshotTrackerTests {
    @Test("Rejects change already visible in snapshot")
    func rejectsVisibleMessage() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            SnapshotMetadata(
                snapshotMark: 1,
                xmin: "100",
                xmax: "200",
                xipList: [],
                databaseLSN: "123"
            ),
            keys: ["user:1"]
        )

        let message = ElectricMessage(
            key: "user:1",
            value: ["id": .integer(1)],
            headers: .init(operation: .insert, txids: [50])
        )

        #expect(tracker.shouldRejectMessage(message) == true)
    }

    @Test("Does not reject parallel transaction in xip")
    func allowsParallelTransaction() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            SnapshotMetadata(
                snapshotMark: 1,
                xmin: "50",
                xmax: "200",
                xipList: ["100"],
                databaseLSN: "123"
            ),
            keys: ["user:1"]
        )

        let message = ElectricMessage(
            key: "user:1",
            value: ["id": .integer(1)],
            headers: .init(operation: .update, txids: [100])
        )

        #expect(tracker.shouldRejectMessage(message) == false)
    }

    @Test("Uses the highest txid when filtering snapshot-visible messages")
    func usesHighestTransactionID() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            SnapshotMetadata(
                snapshotMark: 1,
                xmin: "50",
                xmax: "200",
                xipList: [],
                databaseLSN: "123"
            ),
            keys: ["user:1"]
        )

        let message = ElectricMessage(
            key: "user:1",
            value: ["id": .integer(1)],
            headers: .init(operation: .update, txids: [10, 150])
        )

        #expect(tracker.shouldRejectMessage(message) == true)
    }

    @Test("Allows messages without txids")
    func allowsMessagesWithoutTransactionIDs() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            SnapshotMetadata(
                snapshotMark: 1,
                xmin: "50",
                xmax: "200",
                xipList: [],
                databaseLSN: "123"
            ),
            keys: ["user:1"]
        )

        let message = ElectricMessage(
            key: "user:1",
            value: ["id": .integer(1)],
            headers: .init(operation: .update)
        )

        #expect(tracker.shouldRejectMessage(message) == false)
    }

    @Test("Cleans up snapshots when a later xid passes xmax")
    func cleansUpSnapshotsWhenTransactionAdvancesPastBoundary() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            SnapshotMetadata(
                snapshotMark: 1,
                xmin: "50",
                xmax: "200",
                xipList: [],
                databaseLSN: "123"
            ),
            keys: ["user:1"]
        )

        let laterMessage = ElectricMessage(
            key: "user:2",
            value: ["id": .integer(2)],
            headers: .init(operation: .insert, txids: [250])
        )
        #expect(tracker.shouldRejectMessage(laterMessage) == false)

        let oldMessage = ElectricMessage(
            key: "user:1",
            value: ["id": .integer(1)],
            headers: .init(operation: .update, txids: [60])
        )
        #expect(tracker.shouldRejectMessage(oldMessage) == false)
    }

    @Test("Last seen update removes snapshots whose database LSN has been passed")
    func removesSnapshotsAfterLastSeenUpdate() {
        let tracker = SnapshotTracker()
        tracker.addSnapshot(
            SnapshotMetadata(
                snapshotMark: 1,
                xmin: "50",
                xmax: "200",
                xipList: [],
                databaseLSN: "123"
            ),
            keys: ["user:1"]
        )
        tracker.addSnapshot(
            SnapshotMetadata(
                snapshotMark: 2,
                xmin: "50",
                xmax: "300",
                xipList: [],
                databaseLSN: "250"
            ),
            keys: ["user:2"]
        )

        tracker.lastSeenUpdate(databaseLSN: 123)

        let firstMessage = ElectricMessage(
            key: "user:1",
            value: ["id": .integer(1)],
            headers: .init(operation: .update, txids: [60])
        )
        let secondMessage = ElectricMessage(
            key: "user:2",
            value: ["id": .integer(2)],
            headers: .init(operation: .update, txids: [60])
        )

        #expect(tracker.shouldRejectMessage(firstMessage) == false)
        #expect(tracker.shouldRejectMessage(secondMessage) == true)
    }
}
