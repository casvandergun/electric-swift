@testable import ElectricSwift
import Foundation
import Testing

@Suite("Fetch Support", .serialized)
struct FetchSupportTests {
    @Test("Retry-After parser supports seconds")
    func parsesRetryAfterSeconds() {
        #expect(FetchSupport.parseRetryAfterHeader("5") == 5_000)
    }

    @Test("Retry-After parser ignores invalid values")
    func ignoresInvalidRetryAfter() {
        #expect(FetchSupport.parseRetryAfterHeader("nonsense") == 0)
    }

    @Test("Retry-After parser supports HTTP dates")
    func parsesRetryAfterDate() {
        let future = Date(timeIntervalSinceNow: 120)
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "EEE',' dd MMM yyyy HH':'mm':'ss z"
        let header = formatter.string(from: future)

        #expect(FetchSupport.parseRetryAfterHeader(header) >= 119_000)
    }

    @Test("Retry-After parser caps large HTTP dates")
    func capsLargeRetryAfterDate() {
        let future = Date(timeIntervalSinceNow: 7_200)
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "EEE',' dd MMM yyyy HH':'mm':'ss z"
        let header = formatter.string(from: future)

        #expect(FetchSupport.parseRetryAfterHeader(header) == 3_600_000)
    }

    @Test("Fetch support retries 429 and 5xx responses")
    func retriesRetryableHTTPFailures() {
        let options = BackoffOptions(maxRetries: 3)
        let tooManyRequests = FetchError(status: 429, text: nil, json: nil, headers: [:], url: "https://example.com")
        let serverError = FetchError(status: 503, text: nil, json: nil, headers: [:], url: "https://example.com")

        #expect(FetchSupport.shouldRetry(error: tooManyRequests, attempt: 0, options: options) == true)
        #expect(FetchSupport.shouldRetry(error: serverError, attempt: 0, options: options) == true)
    }

    @Test("Fetch support does not retry client, cancellation, or missing-header failures")
    func doesNotRetryTerminalFailures() {
        let options = BackoffOptions(maxRetries: 3)
        let clientError = FetchError(status: 403, text: nil, json: nil, headers: [:], url: "https://example.com")
        let missingHeaders = ShapeStreamError.missingHeaders(["electric-schema"], url: "https://example.com")

        #expect(FetchSupport.shouldRetry(error: clientError, attempt: 0, options: options) == false)
        #expect(FetchSupport.shouldRetry(error: CancellationError(), attempt: 0, options: options) == false)
        #expect(FetchSupport.shouldRetry(error: missingHeaders, attempt: 0, options: options) == false)
    }

    @Test("Fetch support respects max retries")
    func respectsMaxRetryLimit() {
        let options = BackoffOptions(maxRetries: 1)
        let error = FetchError(status: 503, text: nil, json: nil, headers: [:], url: "https://example.com")

        #expect(FetchSupport.shouldRetry(error: error, attempt: 0, options: options) == true)
        #expect(FetchSupport.shouldRetry(error: error, attempt: 1, options: options) == false)
    }

    @Test("Retry delay honors Retry-After")
    func retryDelayHonorsRetryAfter() {
        let error = FetchError(
            status: 503,
            text: nil,
            json: nil,
            headers: ["retry-after": "2"],
            url: "https://example.com"
        )

        let delay = FetchSupport.retryDelayMilliseconds(
            error: error,
            currentDelayMilliseconds: 100,
            options: BackoffOptions(maxDelayMilliseconds: 500),
            randomUnit: { 0 }
        )

        #expect(delay == 2_000)
    }

    @Test("Retry delay caps jitter at max delay when no server minimum exists")
    func retryDelayCapsJitterAtMaxDelay() {
        let error = FetchError(status: 503, text: nil, json: nil, headers: [:], url: "https://example.com")

        let delay = FetchSupport.retryDelayMilliseconds(
            error: error,
            currentDelayMilliseconds: 2_000,
            options: BackoffOptions(maxDelayMilliseconds: 500),
            randomUnit: { 1 }
        )

        #expect(delay == 500)
    }

    @Test("FetchError builds from text response")
    func buildsFetchErrorFromTextResponse() {
        let response = httpResponse(
            statusCode: 500,
            headers: ["content-type": "text/plain"]
        )
        let error = FetchError.from(
            response: response,
            data: Data("Server error".utf8),
            url: "https://example.com/v1/shape"
        )
        #expect(error.status == 500)
        #expect(error.text == "Server error")
        #expect(error.json == nil)
        #expect(error.localizedDescription == "HTTP Error 500 at https://example.com/v1/shape: Server error")
    }

    @Test("FetchError builds from JSON response")
    func buildsFetchErrorFromJSONResponse() {
        let response = httpResponse(
            statusCode: 429,
            headers: ["content-type": "application/json", "retry-after": "2"]
        )
        let error = FetchError.from(
            response: response,
            data: Data(#"{"error":"rate limited"}"#.utf8),
            url: "https://example.com/v1/shape"
        )
        #expect(error.status == 429)
        #expect(error.json == .object(["error": .string("rate limited")]))
        #expect(error.headers["retry-after"] == "2")
        #expect(error.description == "HTTP Error 429 at https://example.com/v1/shape: object([\"error\": ElectricSwift.ElectricValue.string(\"rate limited\")])")
    }

    @Test("FetchError falls back to text when content type is missing")
    func buildsFetchErrorWithoutContentType() {
        let response = httpResponse(statusCode: 500)
        let error = FetchError.from(
            response: response,
            data: Data("Server error with no content-type".utf8),
            url: "https://example.com/v1/shape"
        )

        #expect(error.text == "Server error with no content-type")
        #expect(error.json == nil)
    }

    @Test("FetchError uses a custom message when provided")
    func usesCustomFetchErrorMessage() {
        let error = FetchError(
            status: 403,
            text: "Forbidden",
            json: nil,
            headers: ["content-type": "text/plain"],
            url: "https://example.com/forbidden",
            message: "Custom Error Message"
        )

        #expect(error.localizedDescription == "Custom Error Message")
        #expect(error.description == "Custom Error Message")
    }
}
