import XCTest

#if !os(macOS)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(SQLiteSwORMTests.allTests),
    ]
}
#endif