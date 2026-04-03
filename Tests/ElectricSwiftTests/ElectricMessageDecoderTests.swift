@testable import ElectricSwift
import Foundation
import Testing

@Suite("Electric Message Decoding", .serialized)
struct ElectricMessageDecoderTests {
    @Test("Coerces common postgres scalar types using the schema header")
    func coercesCommonTypes() throws {
        let schema: ElectricSchema = [
            "id": .init(type: "int8"),
            "is_active": .init(type: "bool"),
            "payload": .init(type: "jsonb"),
        ]

        let payload = """
        [
          {
            "key": "\\"public\\".\\"items\\"/42",
            "value": {
              "id": "42",
              "is_active": "t",
              "payload": "{\\"name\\":\\"Electric\\"}"
            },
            "headers": { "operation": "insert" }
          },
          {
            "headers": { "control": "up-to-date" }
          }
        ]
        """.data(using: .utf8)!

        let messages = try JSONDecoder().decode([ElectricMessage].self, from: payload)
        let coerced = PostgresValueParser.coerce(messages: messages, schema: schema)

        let row = try #require(coerced.first?.value)
        #expect(row["id"] == .integer(42))
        #expect(row["is_active"] == .boolean(true))
        #expect(row["payload"] == .object(["name": .string("Electric")]))
        #expect(coerced.first?.normalizedKey == "42")
        #expect(coerced.last?.headers.control == .upToDate)
    }

    @Test("Decodes a row into a Swift model")
    func decodesRowsIntoModels() throws {
        struct Todo: Codable, Sendable, Equatable {
            let id: Int
            let title: String
            let done: Bool
        }

        let decoder = ElectricRowDecoder()
        let row: ElectricRow = [
            "id": .integer(7),
            "title": .string("Ship the outline"),
            "done": .boolean(false),
        ]

        let todo = try decoder.decode(Todo.self, from: row)
        #expect(todo == .init(id: 7, title: "Ship the outline", done: false))
    }
}
