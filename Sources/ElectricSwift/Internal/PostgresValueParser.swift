import Foundation

enum PostgresValueParser {
    static func coerce(messages: [ElectricMessage], schema: ElectricSchema) -> [ElectricMessage] {
        guard schema.isEmpty == false else { return messages }

        return messages.map { message in
            var updated = message
            if let value = message.value {
                updated.value = coerce(row: value, schema: schema)
            }
            if let oldValue = message.oldValue {
                updated.oldValue = coerce(row: oldValue, schema: schema)
            }
            return updated
        }
    }

    private static func coerce(row: ElectricRow, schema: ElectricSchema) -> ElectricRow {
        Dictionary(uniqueKeysWithValues: row.map { key, value in
            guard let column = schema[key] else {
                return (key, value)
            }
            return (key, coerce(value: value, column: column))
        })
    }

    private static func coerce(value: ElectricValue, column: ElectricColumnDefinition) -> ElectricValue {
        switch value {
        case .null:
            return .null
        case .array, .object, .boolean, .integer, .double:
            return value
        case .string(let raw):
            if let dims = column.dims, dims > 0 {
                return parseArray(raw, column: column)
            }

            switch column.type {
            case "int2", "int4", "int8", "oid":
                if let parsed = Int64(raw) {
                    return .integer(parsed)
                }
                return .string(raw)
            case "float4", "float8", "numeric":
                if let parsed = Double(raw) {
                    return .double(parsed)
                }
                return .string(raw)
            case "bool":
                if raw == "t" || raw == "true" {
                    return .boolean(true)
                }
                if raw == "f" || raw == "false" {
                    return .boolean(false)
                }
                return .string(raw)
            case "json", "jsonb":
                guard let data = raw.data(using: .utf8) else {
                    return .string(raw)
                }
                return (try? JSONDecoder().decode(ElectricValue.self, from: data)) ?? .string(raw)
            default:
                return .string(raw)
            }
        }
    }

    private static func parseArray(_ input: String, column: ElectricColumnDefinition) -> ElectricValue {
        var index = input.startIndex
        let parsed = parseArrayContents(input, index: &index) {
            guard let token = $0 else {
                return .null
            }
            var scalarColumn = column
            scalarColumn = .init(
                type: column.type,
                dims: nil,
                notNull: column.notNull,
                maxLength: column.maxLength,
                length: column.length,
                precision: column.precision,
                scale: column.scale,
                fields: column.fields
            )
            return coerce(value: .string(token), column: scalarColumn)
        }
        return .array(parsed)
    }

    private static func parseArrayContents(
        _ input: String,
        index: inout String.Index,
        leafParser: (String?) -> ElectricValue
    ) -> [ElectricValue] {
        var results: [ElectricValue] = []
        var current = ""
        var isQuoted = false

        if index < input.endIndex, input[index] == "{" {
            index = input.index(after: index)
        }

        while index < input.endIndex {
            let character = input[index]

            if isQuoted {
                if character == "\\" {
                    let next = input.index(after: index)
                    if next < input.endIndex {
                        current.append(input[next])
                        index = input.index(after: next)
                        continue
                    }
                }

                if character == "\"" {
                    isQuoted = false
                    index = input.index(after: index)
                    continue
                }

                current.append(character)
                index = input.index(after: index)
                continue
            }

            switch character {
            case "\"":
                isQuoted = true
                index = input.index(after: index)
            case "{":
                let nested = parseArrayContents(input, index: &index, leafParser: leafParser)
                results.append(.array(nested))
            case "}":
                if current.isEmpty == false || results.isEmpty {
                    results.append(leafParser(normalizeToken(current)))
                    current.removeAll(keepingCapacity: true)
                }
                index = input.index(after: index)
                return results
            case ",":
                results.append(leafParser(normalizeToken(current)))
                current.removeAll(keepingCapacity: true)
                index = input.index(after: index)
            default:
                current.append(character)
                index = input.index(after: index)
            }
        }

        if current.isEmpty == false {
            results.append(leafParser(normalizeToken(current)))
        }

        return results
    }

    private static func normalizeToken(_ token: String) -> String? {
        let trimmed = token.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed == "NULL" {
            return nil
        }
        return trimmed
    }
}
