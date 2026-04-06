import Foundation

public struct ColumnMapper: Sendable {
    public let decode: @Sendable (String) -> String
    public let encode: @Sendable (String) -> String

    public init(
        decode: @escaping @Sendable (String) -> String,
        encode: @escaping @Sendable (String) -> String
    ) {
        self.decode = decode
        self.encode = encode
    }
}

public func createColumnMapper(_ mapping: [String: String]) -> ColumnMapper {
    let reverseMapping = Dictionary(uniqueKeysWithValues: mapping.map { ($0.value, $0.key) })
    return ColumnMapper(
        decode: { mapping[$0] ?? $0 },
        encode: { reverseMapping[$0] ?? $0 }
    )
}

public func snakeCamelMapper(schema: ElectricSchema? = nil) -> ColumnMapper {
    if let schema {
        return createColumnMapper(
            Dictionary(uniqueKeysWithValues: schema.keys.map { ($0, snakeToCamel($0)) })
        )
    }
    return ColumnMapper(decode: snakeToCamel, encode: camelToSnake)
}

public func snakeToCamel(_ string: String) -> String {
    let leading = string.prefix { $0 == "_" }
    let withoutLeading = string.dropFirst(leading.count)
    let trailing = withoutLeading.reversed().prefix { $0 == "_" }.reversed()
    let core = withoutLeading.dropLast(trailing.count).lowercased()
    var result = ""
    var uppercaseNext = false
    for character in core {
        if character == "_" {
            uppercaseNext = true
            continue
        }
        if uppercaseNext {
            result.append(String(character).uppercased())
            uppercaseNext = false
        } else {
            result.append(character)
        }
    }
    return String(leading) + result + String(trailing)
}

public func camelToSnake(_ string: String) -> String {
    guard string.isEmpty == false else { return string }
    var result = ""
    let characters = Array(string)
    for index in characters.indices {
        let character = characters[index]
        if character.isUppercase {
            let previousIsLowercase = index > characters.startIndex && characters[characters.index(before: index)].isLowercase
            let nextIsLowercase = index < characters.index(before: characters.endIndex) && characters[characters.index(after: index)].isLowercase
            if result.isEmpty == false && (previousIsLowercase || nextIsLowercase) {
                result.append("_")
            }
            result.append(String(character).lowercased())
        } else {
            result.append(character)
        }
    }
    return result
}

package enum ColumnMappingSupport {
    package static func quoteIdentifier(_ identifier: String) -> String {
        "\"\(identifier.replacingOccurrences(of: "\"", with: "\"\""))\""
    }

    package static func encodeWhereClause(
        _ whereClause: String?,
        using mapper: ColumnMapper?
    ) -> String? {
        guard let whereClause, let mapper else { return whereClause }
        return encodeIdentifiers(in: whereClause, using: mapper.encode)
    }

    package static func encodeIdentifiers(
        in input: String,
        using encode: (String) -> String
    ) -> String {
        let keywords: Set<String> = [
            "AND", "OR", "NOT", "IN", "IS", "NULL", "NULLS", "FIRST", "LAST",
            "TRUE", "FALSE", "LIKE", "ILIKE", "BETWEEN", "ASC", "DESC", "LIMIT",
            "OFFSET", "ORDER", "BY", "GROUP", "HAVING", "DISTINCT", "AS", "ON",
            "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "CASE", "WHEN",
            "THEN", "ELSE", "END", "CAST", "LOWER", "UPPER", "COALESCE", "NULLIF",
        ]
        var output = ""
        var token = ""
        var quote: Character?

        func flushToken() {
            guard token.isEmpty == false else { return }
            if keywords.contains(token.uppercased()) {
                output += token
            } else {
                output += encode(token)
            }
            token.removeAll(keepingCapacity: true)
        }

        for character in input {
            if let activeQuote = quote {
                output.append(character)
                if character == activeQuote {
                    quote = nil
                }
                continue
            }

            if character == "'" || character == "\"" {
                flushToken()
                output.append(character)
                quote = character
            } else if character.isLetter || character == "_" || (token.isEmpty == false && character.isNumber) {
                token.append(character)
            } else {
                flushToken()
                output.append(character)
            }
        }
        flushToken()
        return output
    }
}
