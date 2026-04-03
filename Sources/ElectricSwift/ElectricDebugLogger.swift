import Foundation
#if canImport(OSLog)
import OSLog
#endif

public enum ElectricDebugLevel: String, Sendable, Codable, Hashable {
    case trace
    case debug
    case info
    case error
}

public struct ElectricDebugEvent: Sendable {
    public let timestamp: Date
    public let level: ElectricDebugLevel
    public let category: String
    public let message: String
    public let metadata: [String: String]

    public init(
        timestamp: Date = Date(),
        level: ElectricDebugLevel,
        category: String,
        message: String,
        metadata: [String: String] = [:]
    ) {
        self.timestamp = timestamp
        self.level = level
        self.category = category
        self.message = message
        self.metadata = metadata
    }
}

public struct ElectricDebugLogger: Sendable {
    private let handler: @Sendable (ElectricDebugEvent) -> Void

    public init(handler: @escaping @Sendable (ElectricDebugEvent) -> Void) {
        self.handler = handler
    }

    public func log(
        _ level: ElectricDebugLevel,
        category: String,
        message: String,
        metadata: [String: String] = [:]
    ) {
        handler(
            ElectricDebugEvent(
                level: level,
                category: category,
                message: message,
                metadata: metadata
            )
        )
    }

    public static let disabled = ElectricDebugLogger { _ in }

    public static func console(
        prefix: String = "Electric",
        subsystem: String = "ElectricSwiftData"
    ) -> ElectricDebugLogger {
        ElectricDebugLogger { event in
            let metadata = event.metadata.isEmpty
                ? ""
                : " " + event.metadata
                    .sorted { $0.key < $1.key }
                    .map { "\($0.key)=\($0.value)" }
                    .joined(separator: " ")
            let line = "[\(prefix)] [\(event.level.rawValue)] [\(event.category)] \(event.message)\(metadata)"
            print(line)

            #if canImport(OSLog)
            let logger = Logger(subsystem: subsystem, category: event.category)
            switch event.level {
            case .trace, .debug:
                logger.debug("\(line, privacy: .public)")
            case .info:
                logger.info("\(line, privacy: .public)")
            case .error:
                logger.error("\(line, privacy: .public)")
            }
            #endif
        }
    }

    public static func combining(_ loggers: [ElectricDebugLogger]) -> ElectricDebugLogger {
        ElectricDebugLogger { event in
            for logger in loggers {
                logger.handler(event)
            }
        }
    }
}
