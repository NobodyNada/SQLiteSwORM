import XCTest
@testable import SQLiteSwORM

final class SQLiteSwORMTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(SQLiteSwORM().text, "Hello, World!")
    }


    static var allTests = [
        ("testExample", testExample),
    ]
}
