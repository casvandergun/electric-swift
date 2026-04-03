import Foundation

enum PostgresValueParser {
    static func coerce(
        messages: [ElectricMessage],
        schema: ElectricSchema,
        parser: ElectricParser
    ) throws -> [ElectricMessage] {
        if schema.isEmpty, parser.rowTransform == nil {
            return messages
        }

        return try messages.map { message in
            var updated = message
            if let value = message.value {
                updated.value = try coerce(row: value, schema: schema, parser: parser)
            }
            if let oldValue = message.oldValue {
                updated.oldValue = try coerce(row: oldValue, schema: schema, parser: parser)
            }
            return updated
        }
    }

    private static func coerce(
        row: ElectricRow,
        schema: ElectricSchema,
        parser: ElectricParser
    ) throws -> ElectricRow {
        let coerced = try Dictionary(uniqueKeysWithValues: row.map { key, value in
            guard let column = schema[key] else {
                return (key, value)
            }
            return (key, try coerce(value: value, column: column, columnName: key, parser: parser))
        })
        return try parser.transform(row: coerced)
    }

    private static func coerce(
        value: ElectricValue,
        column: ElectricColumnDefinition,
        columnName: String,
        parser: ElectricParser
    ) throws -> ElectricValue {
        switch value {
        case .null:
            if column.notNull == true {
                throw ElectricParserError.nullInNonNullableColumn(columnName)
            }
            return .null
        case .array, .object, .boolean, .integer, .double:
            return value
        case .string(let raw):
            if let dims = column.dims, dims > 0 {
                return try parseArray(raw, column: column, columnName: columnName, parser: parser)
            }
            return try parser.scalarParser(for: column.type)?(raw, column) ?? .string(raw)
        }
    }

    private static func parseArray(
        _ input: String,
        column: ElectricColumnDefinition,
        columnName: String,
        parser: ElectricParser
    ) throws -> ElectricValue {
        var index = input.startIndex
        let parsed = try parseArrayContents(input, index: &index) { token in
            guard let token else {
                return try coerce(value: .null, column: scalarColumn(from: column), columnName: columnName, parser: parser)
            }
            return try coerce(
                value: .string(token),
                column: scalarColumn(from: column),
                columnName: columnName,
                parser: parser
            )
        }
        return .array(parsed)
    }

    private static func parseArrayContents(
        _ input: String,
        index: inout String.Index,
        leafParser: (String?) throws -> ElectricValue
    ) throws -> [ElectricValue] {
        var results: [ElectricValue] = []
        var current = ""
        var isQuoted = false
        var tokenWasQuoted = false
        var justParsedNestedArray = false

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
                    tokenWasQuoted = true
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
                let nested = try parseArrayContents(input, index: &index, leafParser: leafParser)
                results.append(.array(nested))
                justParsedNestedArray = true
            case "}":
                if current.isEmpty == false || tokenWasQuoted {
                    results.append(try leafParser(normalizeToken(current, wasQuoted: tokenWasQuoted)))
                    current.removeAll(keepingCapacity: true)
                }
                tokenWasQuoted = false
                justParsedNestedArray = false
                index = input.index(after: index)
                return results
            case ",":
                if current.isEmpty == false || tokenWasQuoted || justParsedNestedArray == false {
                    if current.isEmpty == false || tokenWasQuoted {
                        results.append(try leafParser(normalizeToken(current, wasQuoted: tokenWasQuoted)))
                    }
                }
                current.removeAll(keepingCapacity: true)
                tokenWasQuoted = false
                justParsedNestedArray = false
                index = input.index(after: index)
            default:
                current.append(character)
                justParsedNestedArray = false
                index = input.index(after: index)
            }
        }

        if current.isEmpty == false || tokenWasQuoted {
            results.append(try leafParser(normalizeToken(current, wasQuoted: tokenWasQuoted)))
        }

        return results
    }

    private static func scalarColumn(from column: ElectricColumnDefinition) -> ElectricColumnDefinition {
        .init(
            type: column.type,
            dims: nil,
            notNull: column.notNull,
            maxLength: column.maxLength,
            length: column.length,
            precision: column.precision,
            scale: column.scale,
            fields: column.fields
        )
    }

    private static func normalizeToken(_ token: String, wasQuoted: Bool) -> String? {
        let trimmed = wasQuoted ? token : token.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed == "NULL" {
            if wasQuoted {
                return trimmed
            }
            return nil
        }
        return trimmed
    }
}
