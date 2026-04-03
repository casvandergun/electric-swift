import Foundation

struct ElectricSSEEvent: Sendable, Equatable {
    let event: String?
    let data: String
    let id: String?
    let retry: Int?

    var effectiveEvent: String {
        event ?? "message"
    }
}

struct ElectricSSEParser: Sendable {
    static func parse(data: Data, pendingData: Data = Data()) -> (events: [ElectricSSEEvent], remaining: Data) {
        var combined = pendingData
        combined.append(data)

        guard let text = String(data: combined, encoding: .utf8) else {
            return ([], combined)
        }

        var events: [ElectricSSEEvent] = []
        var eventType: String?
        var dataBuffer: [String] = []
        var eventID: String?
        var retryMS: Int?

        let lines = splitLines(text)
        for (index, line) in lines.enumerated() {
            let isLastLine = index == lines.count - 1
            let textEndsWithTerminator = text.hasSuffix("\n") || text.hasSuffix("\r")
            if isLastLine && !textEndsWithTerminator && !line.isEmpty {
                break
            }

            if line.isEmpty {
                if !dataBuffer.isEmpty {
                    events.append(
                        ElectricSSEEvent(
                            event: eventType,
                            data: dataBuffer.joined(separator: "\n"),
                            id: eventID,
                            retry: retryMS
                        )
                    )
                }
                eventType = nil
                dataBuffer = []
                continue
            }

            if line.hasPrefix(":") {
                continue
            }

            let (field, value) = parseLine(line)
            switch field {
            case "event":
                eventType = value
            case "data":
                dataBuffer.append(value)
            case "id":
                if !value.contains("\0") {
                    eventID = value
                }
            case "retry":
                retryMS = Int(value)
            default:
                break
            }
        }

        let remaining: Data
        let textEndsWithTerminator = text.hasSuffix("\n") || text.hasSuffix("\r")
        if !textEndsWithTerminator {
            if let index = findLastLineTerminator(text) {
                let remainingText = String(text[text.index(after: index)...])
                remaining = remainingText.data(using: .utf8) ?? Data()
            } else {
                remaining = combined
            }
        } else {
            remaining = Data()
        }

        return (events, remaining)
    }

    private static func splitLines(_ text: String) -> [String] {
        var lines: [String] = []
        var currentLine = ""
        var previousWasCR = false

        for character in text {
            if character == "\r" {
                lines.append(currentLine)
                currentLine = ""
                previousWasCR = true
            } else if character == "\n" {
                if previousWasCR {
                    previousWasCR = false
                } else {
                    lines.append(currentLine)
                    currentLine = ""
                }
            } else {
                previousWasCR = false
                currentLine.append(character)
            }
        }

        lines.append(currentLine)
        return lines
    }

    private static func findLastLineTerminator(_ text: String) -> String.Index? {
        var lastIndex: String.Index?
        for (offset, character) in text.enumerated() where character == "\r" || character == "\n" {
            lastIndex = text.index(text.startIndex, offsetBy: offset)
        }
        return lastIndex
    }

    private static func parseLine(_ line: String) -> (String, String) {
        guard let colonIndex = line.firstIndex(of: ":") else {
            return (line, "")
        }

        let field = String(line[..<colonIndex])
        var value = String(line[line.index(after: colonIndex)...])
        if value.hasPrefix(" ") {
            value = String(value.dropFirst())
        }
        return (field, value)
    }
}
