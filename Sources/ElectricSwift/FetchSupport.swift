import Foundation

public struct FetchError: Error, Sendable, Hashable, Codable {
    public let status: Int
    public let text: String?
    public let json: ElectricValue?
    public let headers: [String: String]
    public let url: String

    public init(
        status: Int,
        text: String?,
        json: ElectricValue?,
        headers: [String: String],
        url: String,
        message: String? = nil
    ) {
        self.status = status
        self.text = text
        self.json = json
        self.headers = headers
        self.url = url
        self.message = message
    }

    private let message: String?

    public var localizedDescription: String {
        message ?? "HTTP Error \(status) at \(url): \(text ?? json.map(String.init(describing:)) ?? "unknown error")"
    }

    public static func from(
        response: HTTPURLResponse,
        data: Data,
        url: String
    ) -> FetchError {
        let headers = response.allHeaderFields.reduce(into: [String: String]()) { result, entry in
            result[String(describing: entry.key).lowercased()] = String(describing: entry.value)
        }
        let contentType = headers["content-type"] ?? ""
        let json: ElectricValue?
        let text: String?
        if contentType.contains("application/json"),
           let decoded = try? JSONDecoder().decode(ElectricValue.self, from: data) {
            json = decoded
            text = nil
        } else {
            json = nil
            text = String(data: data, encoding: .utf8)
        }

        return FetchError(
            status: response.statusCode,
            text: text,
            json: json,
            headers: headers,
            url: url
        )
    }
}

extension FetchError: CustomStringConvertible {
    public var description: String {
        localizedDescription
    }
}

public struct BackoffOptions: Sendable, Hashable, Codable {
    public var initialDelayMilliseconds: Int
    public var maxDelayMilliseconds: Int
    public var multiplier: Double
    public var maxRetries: Int

    public init(
        initialDelayMilliseconds: Int = 1_000,
        maxDelayMilliseconds: Int = 32_000,
        multiplier: Double = 2,
        maxRetries: Int = .max
    ) {
        self.initialDelayMilliseconds = initialDelayMilliseconds
        self.maxDelayMilliseconds = maxDelayMilliseconds
        self.multiplier = multiplier
        self.maxRetries = maxRetries
    }
}

enum FetchSupport {
    static func parseRetryAfterHeader(_ retryAfter: String?) -> Int {
        guard let retryAfter, retryAfter.isEmpty == false else { return 0 }

        if let seconds = Double(retryAfter), seconds > 0 {
            return Int(seconds * 1_000)
        }

        let date = DateFormatter.rfc1123.date(from: retryAfter) ?? ISO8601DateFormatter().date(from: retryAfter)
        guard let date else { return 0 }
        let delta = Int(date.timeIntervalSinceNow * 1_000)
        return max(0, min(delta, 3_600_000))
    }

    static func shouldRetry(
        error: Error,
        attempt: Int,
        options: BackoffOptions
    ) -> Bool {
        guard attempt < options.maxRetries else { return false }
        if error is CancellationError {
            return false
        }
        if let streamError = error as? ShapeStreamError,
           case .missingHeaders = streamError {
            return false
        }
        if let fetchError = error as? FetchError {
            if fetchError.status == 429 || fetchError.status >= 500 {
                return true
            }
            return false
        }
        return true
    }

    static func retryDelayMilliseconds(
        error: Error,
        currentDelayMilliseconds: Int,
        options: BackoffOptions,
        randomUnit: @Sendable () -> Double = { Double.random(in: 0...1) }
    ) -> Int {
        let serverMinimum: Int
        if let fetchError = error as? FetchError {
            serverMinimum = parseRetryAfterHeader(fetchError.headers["retry-after"])
        } else {
            serverMinimum = 0
        }

        let jitter = Int(randomUnit() * Double(currentDelayMilliseconds))
        return max(serverMinimum, min(jitter, options.maxDelayMilliseconds))
    }
}

private extension DateFormatter {
    static let rfc1123: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "EEE',' dd MMM yyyy HH':'mm':'ss z"
        return formatter
    }()
}
