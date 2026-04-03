@testable import ElectricSwift
import Foundation
import Testing

@Suite(
    "Electric Integration",
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

        let stream = ShapeStream(
            shape: ElectricShape(url: url, table: table),
            configuration: .init(subscribe: false)
        )

        var batch: ShapeBatch?
        for _ in 0..<5 {
            batch = try await stream.poll()
            if batch != nil {
                break
            }
        }

        let state = await stream.currentState()
        #expect(state.handle != nil)
        #expect(state.offset != "-1")
        #expect(batch != nil || state.isUpToDate)
    }
}
