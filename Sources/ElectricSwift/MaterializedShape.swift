import Foundation

public actor MaterializedShape<Model: Decodable & Sendable> {
    private var rowsByKey: [String: ElectricRow] = [:]
    private var materializedKeys: Set<String> = []
    private let log: ShapeLogMode
    private let rowDecoder: ElectricRowDecoder

    public init(
        log: ShapeLogMode = .full,
        rowDecoder: ElectricRowDecoder = .init()
    ) {
        self.log = log
        self.rowDecoder = rowDecoder
    }

    public func apply(_ batch: ShapeBatch) {
        for message in batch.messages {
            if message.headers.control == .mustRefetch {
                rowsByKey.removeAll()
                materializedKeys.removeAll()
                continue
            }

            guard
                let operation = message.headers.operation,
                let key = message.key
            else {
                continue
            }

            switch operation {
            case .insert:
                guard let row = message.value else { continue }
                materializedKeys.insert(key)
                rowsByKey[key] = row
            case .update:
                guard let row = message.value else { continue }
                guard log == .full || materializedKeys.contains(key) else { continue }
                rowsByKey[key] = rowsByKey[key, default: [:]].merging(row) { _, new in new }
            case .delete:
                guard log == .full || materializedKeys.contains(key) else { continue }
                rowsByKey.removeValue(forKey: key)
                materializedKeys.remove(key)
            }
        }
    }

    public func snapshotRows() -> [String: ElectricRow] {
        rowsByKey
    }

    public func values() throws -> [String: Model] {
        try rowsByKey.mapValues { row in
            try rowDecoder.decode(Model.self, from: row)
        }
    }
}
