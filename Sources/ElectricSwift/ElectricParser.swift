import Foundation

/// Parses a single scalar Postgres value using its column metadata.
public typealias ElectricParseFunction =
    @Sendable (_ value: String, _ column: ElectricColumnDefinition) throws -> ElectricValue

/// Transforms a fully-coerced row before it is emitted to callers.
public typealias ElectricRowTransform =
    @Sendable (_ row: ElectricRow) throws -> ElectricRow

/// Errors raised while coercing Electric protocol values into Swift runtime values.
public enum ElectricParserError: Error, Sendable, Hashable {
    /// A `null` value was received for a column whose schema marks it as non-nullable.
    case nullInNonNullableColumn(String)
}

/// Configures how incoming Electric rows are coerced from raw Postgres wire values.
///
/// `ElectricParser.default` preserves the package's built-in behavior for standard scalar
/// types and Postgres arrays. Callers can override individual scalar parsers and optionally
/// apply a row transform after schema-based coercion has completed.
public struct ElectricParser: Sendable {
    /// Per-type scalar parsers keyed by Postgres type name.
    public let scalarParsers: [String: ElectricParseFunction]
    /// Optional transform applied to each coerced `value` and `oldValue` row.
    public let rowTransform: ElectricRowTransform?

    public init(
        scalarParsers: [String: ElectricParseFunction] = [:],
        rowTransform: ElectricRowTransform? = nil
    ) {
        self.scalarParsers = scalarParsers
        self.rowTransform = rowTransform
    }

    /// The package's built-in parser configuration.
    public static let `default` = ElectricParser(scalarParsers: builtInScalarParsers)

    private static let builtInScalarParsers: [String: ElectricParseFunction] = [
        "int2": { value, _ in
            Int64(value).map(ElectricValue.integer) ?? .string(value)
        },
        "int4": { value, _ in
            Int64(value).map(ElectricValue.integer) ?? .string(value)
        },
        "int8": { value, _ in
            Int64(value).map(ElectricValue.integer) ?? .string(value)
        },
        "oid": { value, _ in
            Int64(value).map(ElectricValue.integer) ?? .string(value)
        },
        "float4": { value, _ in
            Double(value).map(ElectricValue.double) ?? .string(value)
        },
        "float8": { value, _ in
            Double(value).map(ElectricValue.double) ?? .string(value)
        },
        "numeric": { value, _ in
            Double(value).map(ElectricValue.double) ?? .string(value)
        },
        "bool": { value, _ in
            switch value {
            case "t", "true":
                .boolean(true)
            case "f", "false":
                .boolean(false)
            default:
                .string(value)
            }
        },
        "json": { value, _ in
            guard let data = value.data(using: .utf8) else {
                return .string(value)
            }
            return (try? JSONDecoder().decode(ElectricValue.self, from: data)) ?? .string(value)
        },
        "jsonb": { value, _ in
            guard let data = value.data(using: .utf8) else {
                return .string(value)
            }
            return (try? JSONDecoder().decode(ElectricValue.self, from: data)) ?? .string(value)
        },
    ]
}

extension ElectricParser {
    func scalarParser(for type: String) -> ElectricParseFunction? {
        scalarParsers[type] ?? ElectricParser.default.scalarParsers[type]
    }

    func transform(row: ElectricRow) throws -> ElectricRow {
        guard let rowTransform else { return row }
        return try rowTransform(row)
    }
}
