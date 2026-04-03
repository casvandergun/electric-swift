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
    }
}
