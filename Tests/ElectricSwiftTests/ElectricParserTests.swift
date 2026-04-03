@testable import ElectricSwift
import Foundation
import Testing

@Suite("Electric Parser", .serialized)
struct ElectricParserTests {
    @Test("Parses built-in scalar postgres types")
    func parsesBuiltInScalarTypes() throws {
        let schema: ElectricSchema = [
            "small": .init(type: "int2"),
            "big": .init(type: "int8"),
            "flag": .init(type: "bool"),
            "float": .init(type: "float8"),
            "json": .init(type: "json"),
            "jsonb": .init(type: "jsonb"),
        ]

        let messages = try PostgresValueParser.coerce(
            messages: [
                ElectricMessage(
                    key: "row:1",
                    value: [
                        "small": .string("7"),
                        "big": .string("42"),
                        "flag": .string("true"),
                        "float": .string("1.25"),
                        "json": .string(#"{"name":"Electric"}"#),
                        "jsonb": .string(#"[1,2,3]"#),
                    ],
                    headers: .init(operation: .insert)
                ),
            ],
            schema: schema,
            parser: .default
        )

        let row = try #require(messages.first?.value)
        #expect(row["small"] == .integer(7))
        #expect(row["big"] == .integer(42))
        #expect(row["flag"] == .boolean(true))
        #expect(row["float"] == .double(1.25))
        #expect(row["json"] == .object(["name": .string("Electric")]))
        #expect(row["jsonb"] == .array([.integer(1), .integer(2), .integer(3)]))
    }

    @Test("Malformed built-in scalar values preserve raw strings")
    func malformedBuiltInsPreserveRawStrings() throws {
        let schema: ElectricSchema = [
            "int": .init(type: "int8"),
            "float": .init(type: "float8"),
            "flag": .init(type: "bool"),
            "json": .init(type: "jsonb"),
        ]

        let messages = try PostgresValueParser.coerce(
            messages: [
                ElectricMessage(
                    key: "row:1",
                    value: [
                        "int": .string("not-an-int"),
                        "float": .string("not-a-float"),
                        "flag": .string("maybe"),
                        "json": .string("{bad json}"),
                    ],
                    headers: .init(operation: .insert)
                ),
            ],
            schema: schema,
            parser: .default
        )

        let row = try #require(messages.first?.value)
        #expect(row["int"] == .string("not-an-int"))
        #expect(row["float"] == .string("not-a-float"))
        #expect(row["flag"] == .string("maybe"))
        #expect(row["json"] == .string("{bad json}"))
    }

    @Test("Parses one-dimensional postgres arrays and NULL elements")
    func parsesOneDimensionalArrays() throws {
        let schema: ElectricSchema = [
            "ints": .init(type: "int8", dims: 1),
            "jsons": .init(type: "jsonb", dims: 1),
            "strings": .init(type: "text", dims: 1),
        ]

        let messages = try PostgresValueParser.coerce(
            messages: [
                ElectricMessage(
                    key: "row:1",
                    value: [
                        "ints": .string("{1,2,NULL,4}"),
                        "jsons": .string(#"{"{\"a\":1}",NULL}"#),
                        "strings": .string(#"{"foo","bar","NULL",NULL}"#),
                    ],
                    headers: .init(operation: .insert)
                ),
            ],
            schema: schema,
            parser: .default
        )

        let row = try #require(messages.first?.value)
        #expect(row["ints"] == .array([.integer(1), .integer(2), .null, .integer(4)]))
        #expect(row["jsons"] == .array([.object(["a": .integer(1)]), .null]))
        #expect(row["strings"] == .array([.string("foo"), .string("bar"), .string("NULL"), .null]))
    }

    @Test("Parses nested postgres arrays")
    func parsesNestedArrays() throws {
        let schema: ElectricSchema = [
            "matrix": .init(type: "int8", dims: 2),
            "flags": .init(type: "bool", dims: 2),
        ]

        let messages = try PostgresValueParser.coerce(
            messages: [
                ElectricMessage(
                    key: "row:1",
                    value: [
                        "matrix": .string("{{1,2},{3,4}}"),
                        "flags": .string("{{t,f},{f,t}}"),
                    ],
                    headers: .init(operation: .insert)
                ),
            ],
            schema: schema,
            parser: .default
        )

        let row = try #require(messages.first?.value)
        #expect(row["matrix"] == .array([.array([.integer(1), .integer(2)]), .array([.integer(3), .integer(4)])]))
        #expect(row["flags"] == .array([.array([.boolean(true), .boolean(false)]), .array([.boolean(false), .boolean(true)])]))
    }

    @Test("Parses quoted array tokens correctly")
    func parsesQuotedArrayTokens() throws {
        let schema: ElectricSchema = [
            "values": .init(type: "text", dims: 1),
        ]

        let messages = try PostgresValueParser.coerce(
            messages: [
                ElectricMessage(
                    key: "row:1",
                    value: [
                        "values": .string(#"{foo,"}","a\"b"}"#),
                    ],
                    headers: .init(operation: .insert)
                ),
            ],
            schema: schema,
            parser: .default
        )

        let row = try #require(messages.first?.value)
        #expect(row["values"] == .array([.string("foo"), .string("}"), .string("a\"b")]))
    }

    @Test("Parses value and oldValue rows")
    func parsesValueAndOldValue() throws {
        let schema: ElectricSchema = [
            "id": .init(type: "int8"),
            "done": .init(type: "bool"),
        ]

        let messages = try PostgresValueParser.coerce(
            messages: [
                ElectricMessage(
                    key: "row:1",
                    value: ["id": .string("2"), "done": .string("true")],
                    oldValue: ["id": .string("1"), "done": .string("false")],
                    headers: .init(operation: .update)
                ),
            ],
            schema: schema,
            parser: .default
        )

        let message = try #require(messages.first)
        #expect(message.value?["id"] == .integer(2))
        #expect(message.value?["done"] == .boolean(true))
        #expect(message.oldValue?["id"] == .integer(1))
        #expect(message.oldValue?["done"] == .boolean(false))
    }

    @Test("Unknown types use passthrough string behavior")
    func unknownTypesStayAsStrings() throws {
        let schema: ElectricSchema = [
            "custom": .init(type: "my_custom_type"),
        ]

        let messages = try PostgresValueParser.coerce(
            messages: [
                ElectricMessage(
                    key: "row:1",
                    value: ["custom": .string("opaque")],
                    headers: .init(operation: .insert)
                ),
            ],
            schema: schema,
            parser: .default
        )

        #expect(messages.first?.value?["custom"] == .string("opaque"))
    }

    @Test("Custom scalar parser override wins over defaults")
    func customScalarParserOverridesDefault() throws {
        let schema: ElectricSchema = [
            "id": .init(type: "int8"),
        ]
        let parser = ElectricParser(
            scalarParsers: [
                "int8": { value, _ in
                    .string("custom-\(value)")
                },
            ]
        )

        let messages = try PostgresValueParser.coerce(
            messages: [
                ElectricMessage(
                    key: "row:1",
                    value: ["id": .string("42")],
                    headers: .init(operation: .insert)
                ),
            ],
            schema: schema,
            parser: parser
        )

        #expect(messages.first?.value?["id"] == .string("custom-42"))
    }

    @Test("Row transform is applied after coercion to value and oldValue")
    func rowTransformAppliesToValueAndOldValue() throws {
        let schema: ElectricSchema = [
            "id": .init(type: "int8"),
            "title": .init(type: "text"),
        ]
        let parser = ElectricParser(rowTransform: { row in
            var row = row
            if case .integer(let id) = row["id"] {
                row["id"] = .string("id-\(id)")
            }
            if case .string(let title) = row["title"] {
                row["title"] = .string(title.uppercased())
            }
            return row
        })

        let messages = try PostgresValueParser.coerce(
            messages: [
                ElectricMessage(
                    key: "row:1",
                    value: ["id": .string("2"), "title": .string("after")],
                    oldValue: ["id": .string("1"), "title": .string("before")],
                    headers: .init(operation: .update)
                ),
            ],
            schema: schema,
            parser: parser
        )

        let message = try #require(messages.first)
        #expect(message.value?["id"] == .string("id-2"))
        #expect(message.value?["title"] == .string("AFTER"))
        #expect(message.oldValue?["id"] == .string("id-1"))
        #expect(message.oldValue?["title"] == .string("BEFORE"))
    }

    @Test("Null in non-nullable column throws a parser error")
    func nullInNonNullableColumnThrows() {
        let schema: ElectricSchema = [
            "id": .init(type: "int8", notNull: true),
        ]

        do {
            _ = try PostgresValueParser.coerce(
                messages: [
                    ElectricMessage(
                        key: "row:1",
                        value: ["id": .null],
                        headers: .init(operation: .insert)
                    ),
                ],
                schema: schema,
                parser: .default
            )
            Issue.record("Expected ElectricParserError")
        } catch let error as ElectricParserError {
            #expect(error == .nullInNonNullableColumn("id"))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Null array element in non-nullable column throws a parser error")
    func nullArrayElementInNonNullableColumnThrows() {
        let schema: ElectricSchema = [
            "ids": .init(type: "int8", dims: 1, notNull: true),
        ]

        do {
            _ = try PostgresValueParser.coerce(
                messages: [
                    ElectricMessage(
                        key: "row:1",
                        value: ["ids": .string("{1,NULL,3}")],
                        headers: .init(operation: .insert)
                    ),
                ],
                schema: schema,
                parser: .default
            )
            Issue.record("Expected ElectricParserError")
        } catch let error as ElectricParserError {
            #expect(error == .nullInNonNullableColumn("ids"))
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}
