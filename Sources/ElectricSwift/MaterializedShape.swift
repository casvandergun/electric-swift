import Foundation

public actor MaterializedShape<Model: Decodable & Sendable> {
    private var rowsByKey: [String: ElectricRow] = [:]
    private let rowDecoder: ElectricRowDecoder

    public init(rowDecoder: ElectricRowDecoder = .init()) {
        self.rowDecoder = rowDecoder
    }

    public func apply(_ batch: ShapeBatch) {
        for message in batch.messages {
            if message.headers.control == .mustRefetch {
                rowsByKey.removeAll()
                continue
            }

            guard
                let operation = message.headers.operation,
                let key = message.normalizedKey
            else {
                continue
            }

            switch operation {
            case .insert:
                guard let row = message.value else { continue }
                rowsByKey[key] = row
            case .update:
                guard let row = message.value else { continue }
                rowsByKey[key] = rowsByKey[key, default: [:]].merging(row) { _, new in new }
            case .delete:
                rowsByKey.removeValue(forKey: key)
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
