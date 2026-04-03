import Foundation

public struct ElectricRowDecoder: Sendable {
    private let makeDecoder: @Sendable () -> JSONDecoder
    private let makeEncoder: @Sendable () -> JSONEncoder

    public init(
        makeDecoder: @escaping @Sendable () -> JSONDecoder = { JSONDecoder() },
        makeEncoder: @escaping @Sendable () -> JSONEncoder = { JSONEncoder() }
    ) {
        self.makeDecoder = makeDecoder
        self.makeEncoder = makeEncoder
    }

    public func decode<T: Decodable>(_ type: T.Type, from row: ElectricRow) throws -> T {
        let encoder = makeEncoder()
        let data = try encoder.encode(row)
        return try makeDecoder().decode(T.self, from: data)
    }
}
