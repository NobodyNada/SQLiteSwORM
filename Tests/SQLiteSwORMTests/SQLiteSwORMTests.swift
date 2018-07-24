import XCTest
import SwORM
import Async
@testable import SQLiteSwORM

final class SQLiteSwORMTests: XCTestCase {
    var db: SQLiteDatabase!
    
    let testText = "The quick brown fox jumps over the lazy dog."
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
        do {
            try db = SQLiteDatabase()
        } catch {
            XCTFail("SQLiteDatabase.init() threw an error: \(error)")
            return
        }
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        db = nil
        super.tearDown()
    }
    
    func testOnDiskDatabase() {
        do {
            let _ = try SQLiteDatabase(URL(fileURLWithPath: "test.sqlite3"))
        } catch {
            XCTFail("SQLiteDatabase.init(_:) threw an error: \(error)")
        }
        
        //clean up
        let _ = try? FileManager.default.removeItem(atPath: "test.sqlite3")
    }
    
    func testBasicQuery() throws {
        let conn = try db.connection(on: MultiThreadedEventLoopGroup(numberOfThreads: 1)).wait()
        
        XCTAssert(try conn.execute(
            "CREATE TABLE test (" +
                "id INTEGER PRIMARY KEY NOT NULL," +
                "someText TEXT" +
            ")"
            ).wait().isEmpty, "CREATE TABLE should not return any rows"
        )
        
        XCTAssert(try conn.execute(
            "INSERT INTO test (someText) VALUES (?)", parameters: testText
            ).wait().isEmpty, "INSERT should not return any rows"
        )
        
        let results = try conn.execute("SELECT id AS id, someText AS someText FROM test").wait()
        XCTAssert(results.count == 1, "results should have exactly one element")
        
        if let result = results.first {
            XCTAssert(result.columnIndices.count == 2)
            XCTAssert(result.columns.count == 2)
            
            XCTAssert(result.columnIndices["id"] == 0)
            XCTAssert(result.columnIndices["someText"] == 1)
            
            guard case .some(.string(let s)) = result.columns.last, s == testText else {
                XCTFail("someText invalid: \(testText)")
                return
            }
            
            XCTAssert(try (result.column(named: "someText") as String?) == testText)
        }
    }
    
    func testNull() throws {
        let conn = try db.connection(on: MultiThreadedEventLoopGroup(numberOfThreads: 1)).wait()
        
        XCTAssert(try conn.execute(
            "CREATE TABLE test (" +
                "id INTEGER PRIMARY KEY NOT NULL," +
                "someText TEXT" +
            ")"
            ).wait().isEmpty, "CREATE TABLE should not return any rows"
        )
        
        XCTAssert(try conn.execute(
            "INSERT INTO test (someText) VALUES (?);", parameters: nil
            ).wait().isEmpty, "INSERT should not return any rows"
        )
        
        let results = try conn.execute("SELECT * FROM test").wait()
        XCTAssert(results.count == 1, "results should have exactly one element")
        
        if let result = results.first {
            XCTAssert(try (result.column(named: "someText") as String?) == nil)
        }
    }
    
    func testTransactions() throws {
        let worker = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let conn = try db.connection(on: worker).wait()
        
        try _ = conn.execute(
            "CREATE TABLE test (" +
                "id INTEGER PRIMARY KEY NOT NULL," +
                "someText TEXT" +
            ");"
            ).wait()
        
        try _ = db.transaction(on: worker) { connection in
            connection.execute("INSERT INTO test (someText) VALUES (?);", parameters: self.testText).flatMap(to: [Row].self) { _ in
                connection.execute("INSERT INTO test (someText) VALUES (?);", parameters: self.testText)
            }
            }.wait()
        
        XCTAssert(try conn.execute("SELECT COUNT(*) FROM test").wait().first?.column(at: 0) == 2, "Transactions should work")
        
        
        
        XCTAssertThrowsError(try db.transaction(on: worker) { connection -> Future<Void> in
            connection.execute("INSERT INTO test (someText) VALUES (?);", parameters: [self.testText]).flatMap(to: [Row].self) { _ in
                connection.execute("HAJSDFLKJAHSDFALSDF;")
                }.map(to: [Row].self) { _ in
                    //return connection.execute("DELETE FROM test;")
                    return []
                }.map(to: Void.self) { _ in }
            }.wait(), "A failed transaction should throw an error"
        )
        
        XCTAssert(try conn.execute("SELECT COUNT(*) FROM test").wait().first?.column(at: 0)  == 2,
                  "An invalid transaction should not have any effects"
        )
        
        
        _ = try db.transaction(on: worker) { connection in connection.execute("DELETE FROM test;") }.wait()
        
        XCTAssert(try conn.execute("SELECT COUNT(*) FROM test").wait()
            .first?.column(at: 0)  == 0, "Transactions should work")
    }
    
    
    func testReadOnlyDatabase() throws {
        let db = try SQLiteDatabase(URL(string: "file::memory:")!, options: [.readOnly])
        let conn = try db.connection(on: MultiThreadedEventLoopGroup(numberOfThreads: 1)).wait()
        
        do {
            try _ = conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT);").wait()
            XCTFail("CREATE TABLE did not throw an error")
        } catch SQLiteError.sqliteError(let error, _){
            XCTAssert(error == .readOnly, "CREATE TABLE should throw a readOnly error")
        }
    }
    
    
    static var allTests : [(String, (SQLiteSwORMTests) -> () throws -> Void)] {
        return [
            ("testOnDiskDatabse", testOnDiskDatabase),
            ("testBasicQuery", testBasicQuery),
            ("testNull", testNull),
            ("testTransactions", testTransactions),
            ("testReadOnlyDatabase", testReadOnlyDatabase)
        ]
    }
}
