@testable import ElectricSwift
import Foundation
import Testing

@Suite(
    "Electric Integration",
    .serialized,
    .enabled(
        if: ProcessInfo.processInfo.environment["ELECTRIC_SHAPE_URL"] != nil &&
            ProcessInfo.processInfo.environment["ELECTRIC_TEST_TABLE"] != nil
    )
)
struct ElectricIntegrationTests {
    @Test("Can poll a real Electric shape endpoint")
    func pollsRealShapeEndpoint() async throws {
        let environment = ProcessInfo.processInfo.environment
        let url = try #require(URL(string: environment["ELECTRIC_SHAPE_URL"] ?? ""))
        let table = try #require(environment["ELECTRIC_TEST_TABLE"])

        testLog("integration start url=\(url.absoluteString) table=\(table)")

        let stream = ShapeStream(
            options: ShapeStreamOptions(url: url, table: table),
            configuration: .init(subscribe: false),
            debugLogger: .console(prefix: "ElectricIntegration")
        )

        var batch: ShapeBatch?
        for attempt in 1...5 {
            batch = try await loggedPoll(
                stream,
                label: "integration attempt \(attempt)",
                timeoutSeconds: 15
            )
            if batch != nil {
                testLog("integration received batch on attempt \(attempt)")
                break
            }
        }

        let state = await stream.currentState()
        testLog(
            "integration final state phase=\(String(describing: state.phase)) handle=\(state.handle ?? "nil") offset=\(state.offset) upToDate=\(state.isUpToDate)"
        )
        #expect(state.handle != nil)
        #expect(state.offset != "-1")
        #expect(batch != nil || state.isUpToDate)
    }
}
