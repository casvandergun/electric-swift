import Foundation


public enum ElectricValue: Sendable, Hashable, Codable {
    case string(String)
    case integer(Int64)
    case double(Double)
    case boolean(Bool)
    case object([String: ElectricValue])
    case array([ElectricValue])
    case null

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .boolean(value)
        } else if let value = try? container.decode([String: ElectricValue].self) {
            self = .object(value)
        } else if let value = try? container.decode([ElectricValue].self) {
            self = .array(value)
        } else if let value = try? container.decode(Int64.self) {
            self = .integer(value)
        } else if let value = try? container.decode(Double.self) {
            self = .double(value)
        } else {
            self = .string(try container.decode(String.self))
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let value):
            try container.encode(value)
        case .integer(let value):
            try container.encode(value)
        case .double(let value):
            try container.encode(value)
        case .boolean(let value):
            try container.encode(value)
        case .object(let value):
            try container.encode(value)
        case .array(let value):
            try container.encode(value)
        case .null:
            try container.encodeNil()
        }
    }
}

public typealias ElectricRow = [String: ElectricValue]

public extension ElectricValue {
    var stringValue: String? {
        switch self {
        case .string(let value):
            return value
        case .integer(let value):
            return String(value)
        case .double(let value):
            return String(value)
        case .boolean(let value):
            return String(value)
        case .object, .array, .null:
            return nil
        }
    }
}
