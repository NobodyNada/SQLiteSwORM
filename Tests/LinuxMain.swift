import XCTest

import SQLiteSwORMTests

var tests = [XCTestCaseEntry]()
tests += SQLiteSwORMTests.allTests()
XCTMain(tests)