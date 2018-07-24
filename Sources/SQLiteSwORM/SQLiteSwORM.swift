//
//  SQLiteSwORM.swift
//  SQLiteSwORM
//
//  Created by NobodyNada on 3/24/18.
//  Adapted from SwiftChatSE's DatabaseConnection.swift
//

import Foundation
import SwORM
import Dispatch
import Async

#if os(Linux)
import CSQLite
#else
import SQLite3
#endif

public enum SQLiteError: Error {
    public enum ErrorCode: Int32 {
        case genericError = 1
        case aborted = 4
        case notAuthenticated = 23
        case busy = 5
        case cantOpen = 14
        case constraint = 19
        case corrupt = 11
        case diskFull = 13
        case internalError = 2
        case interrupt = 9
        case ioError = 10
        case locked = 6
        case datatypeMismatch = 20
        case misuse = 21
        case noLargeFileSupport = 22
        case outOfMemory = 7
        case notADatabase = 12
        case noPermissions = 3
        case protocolError = 15
        case indexOutOfRange = 25
        case readOnly = 8
        case schemaChagned = 17
        case stringOrBlobTooBig = 18
    }
    
    case unknownSQLiteError(code: Int32, message: String?)
    case sqliteError(error: ErrorCode, message: String?)
}

private func throwSQLiteError(code: Int32, db: OpaquePointer?) throws -> Never {
    let messagePtr = sqlite3_errmsg(db)
    let message = messagePtr.map { String(cString: $0) }
    
    if let error = SQLiteError.ErrorCode(rawValue: code) {
        throw SQLiteError.sqliteError(error: error, message: message)
    }
    throw SQLiteError.unknownSQLiteError(code: code, message: message)
}

open class SQLiteDatabase: Database {
    public let url: URL
    public let options: SQLiteDatabase.OpenFlags
    
    private var _busyTimeout: TimeInterval = 1
    private var busyTimeoutQueue = DispatchQueue(label: "com.nobodynada.SQLiteORM.SQLiteDatabase.busyTimeoutQueue", attributes: .concurrent)
    
    
    ///The amount of time to wait before a query will time out with `SQLiteError.busy`.
    ///Default is 1 second.
    public var busyTimeout: TimeInterval {
        get {
            return busyTimeoutQueue.sync { self._busyTimeout }
        } set {
            busyTimeoutQueue.sync(flags: .barrier) {
                self._busyTimeout = newValue
            }
        }
    }
    
    public init(_ url: URL, options: SQLiteDatabase.OpenFlags = []) throws {
        self.url = url
        self.options = options
    }
    
    public convenience init() throws {
        let identifier = UUID().uuidString
        try self.init(URL(string: "file:\(identifier)?mode=memory")!)
    }
    
    public func _newConnection(on worker: Worker) -> Future<Connection> {
        do {
            return worker.eventLoop.newSucceededFuture(result: try SQLiteConnection(database: self, worker: worker))
        } catch {
            return worker.eventLoop.newFailedFuture(error: error)
        }
    }
    
    public struct OpenFlags: OptionSet {
        public var rawValue: Int32
        
        public init(rawValue: Int32) { self.rawValue = rawValue }
        
        public static let readOnly = OpenFlags(rawValue: SQLITE_OPEN_READONLY)
        public static let readWrite = OpenFlags(rawValue: SQLITE_OPEN_READWRITE)
        public static let createIfNotExists = OpenFlags(rawValue: SQLITE_OPEN_CREATE)
        internal static let allowURIFilenames  = OpenFlags(rawValue: SQLITE_OPEN_URI)
        public static let memory = OpenFlags(rawValue: SQLITE_OPEN_MEMORY)
        public static let noMutex = OpenFlags(rawValue: SQLITE_OPEN_NOMUTEX)
        public static let fullMutex = OpenFlags(rawValue: SQLITE_OPEN_FULLMUTEX)
        public static let sharedCache = OpenFlags(rawValue: SQLITE_OPEN_SHAREDCACHE)
        public static let privateCache = OpenFlags(rawValue: SQLITE_OPEN_PRIVATECACHE)
    }
    
    public enum SchemaLoadingError: Error {
        case noResults
    }
    
    public func loadSchemaVersion(on worker: Worker) -> Future<Int> {
        return connection(on: worker)
            .flatMap(to: [Row].self) { $0.execute("PRAGMA user_version") }
            .map(to: Int.self) {
                guard let result = try $0.first?.column(at: 0) as Int? else { throw SchemaLoadingError.noResults }
                return result
        }
    }
    
    public func setSchemaVersion(to version: Int, on worker: Worker) -> Future<Void> {
        //PRAGMA doesn't support bound parameters.  SQL injection isn't an issue, since it's just an integer
        return connection(on: worker).then { $0.execute("PRAGMA user_version = \(version)") }.map { _ in }
    }
}

open class SQLiteConnection: Connection, BasicWorker {
    open let connection: OpaquePointer
    
    open let sqliteDatabase: SQLiteDatabase
    open var database: Database { return sqliteDatabase }
    
    open let worker: Worker
    open var eventLoop: EventLoop { return worker.eventLoop }
    
    //A cache of prepared statements.
    open var statementCache = [String:OpaquePointer]()
    
    internal init(database: SQLiteDatabase, worker: Worker) throws {
        self.sqliteDatabase = database
        self.worker = worker
        var connection: OpaquePointer?
        var flags = database.options
        
        if (flags.contains(.readOnly) && (flags.contains(.readWrite)) || flags.contains(.createIfNotExists)) {
            fatalError("SQLiteOpenFlags.readOnly is incompatible with .readWrite and .createIfNotExists")
        }
        if !flags.contains(.readOnly) && !flags.contains(.readWrite) {
            flags.insert(.readWrite)
            flags.insert(.createIfNotExists)
        }
        if !flags.contains(.privateCache) { flags.insert(.sharedCache) }
        flags.insert(.allowURIFilenames)
        
        let result = sqlite3_open_v2(database.url.absoluteString, &connection, flags.rawValue, nil)
        guard result == SQLITE_OK, let c = connection else {
            try throwSQLiteError(code: result, db: nil)
        }
        
        self.connection = c
    }
    
    deinit {
        for (_, statement) in statementCache {
            sqlite3_finalize(statement)
        }
        sqlite3_close_v2(connection)
    }
    
    ///The primary key inserted by the last `INSERT` statement,
    ///or 0 if no successful `INSERT` statements have been performed by this connection.
    open func lastInsertedRow() -> Future<Int64> {
        return eventLoop.newSucceededFuture(result: sqlite3_last_insert_rowid(connection))
    }
    
    public func execute(_ query: String, parameters: [DatabaseType?]) -> Future<[Row]> {
        do {
            sqlite3_busy_timeout(connection, Int32(sqliteDatabase.busyTimeout * 1000))
            //Compile the query.
            let query = query.trimmingCharacters(in: .whitespacesAndNewlines)
            let statement: OpaquePointer
            
            if let cached = statementCache[query] {
                //cache hit; use the cached query instead of recompiling
                statement = cached
            } else {
                //cache miss; compile the query
                var tail: UnsafePointer<Int8>?
                var stmt: OpaquePointer?
                try query.utf8CString.withUnsafeBufferPointer {
                    let result = sqlite3_prepare_v2(
                        connection,
                        $0.baseAddress,
                        Int32($0.count),
                        &stmt,
                        &tail
                    )
                    
                    guard result == SQLITE_OK, stmt != nil else {
                        try throwSQLiteError(code: result, db: connection)
                    }
                    if tail != nil && tail!.pointee != 0 {
                        //programmer error, so crash instead of throwing
                        fatalError("\(#function) does not accept multiple statements: '\(query)' (tail: \(String(cString: tail!)))")
                    }
                }
                
                statement = stmt!
                
                //Store the compiled query
                statementCache[query] = statement
            }
            
            defer {
                //Reset the query if
                sqlite3_clear_bindings(statement)
                sqlite3_reset(statement)
            }
            
            
            //Bind the parameters.
            for i in parameters.indices {
                let result: Int32
                
                if let param = parameters[i] {
                    result = param.asNative.bind(to: statement, index: Int32(i + 1))
                } else {
                    result = sqlite3_bind_null(statement, Int32(i + 1))
                }
                
                guard result == SQLITE_OK else {
                    try throwSQLiteError(code: result, db: connection)
                }
            }
            
            
            //Run the query.
            var done = false
            var results: [Row] = []
            repeat {
                let result = sqlite3_step(statement)
                
                switch result {
                case SQLITE_DONE:
                    done = true
                case SQLITE_ROW:
                    //we got a row
                    results.append(Row(statement: statement))
                    break
                default:
                    try throwSQLiteError(code: result, db: connection)
                }
            } while !done
            
            return eventLoop.newSucceededFuture(result: results)
        } catch {
            return eventLoop.newFailedFuture(error: error)
        }
    }
}

public extension DatabaseNativeType {
    func bind(to statement: OpaquePointer, index: Int32) -> Int32 {
        switch self {
        case .int(let v): return sqlite3_bind_int64(statement, index, v)
        case .double(let v): return sqlite3_bind_double(statement, index, v)
        case .string(let v):
            let chars = Array(v.utf8)
            let buf = malloc(chars.count).bindMemory(to: Int8.self, capacity: chars.count)
            memcpy(buf, chars, chars.count)
            
            return sqlite3_bind_text(statement, index, buf, Int32(chars.count)) { data in free(data) }
        case .data(let v):
            return v.withUnsafeBytes { (bytes: UnsafePointer<UInt8>) -> Int32 in
                let count = v.count
                let bytesCopy = malloc(count)
                memcpy(bytesCopy!, bytes, count)
                
                return sqlite3_bind_blob(statement, index, bytesCopy, Int32(count)) { data in free(data) }
            }
        case .date(let v): return v.asDatabaseString.bind(to: statement, index: index)
        case .null: return sqlite3_bind_null(statement, index)
        }
    }
}

public extension Row {
    public convenience init(statement: OpaquePointer) {
        var columns = [DatabaseNativeType]()
        var columnNames = [String:Int]()
        
        for i in 0..<sqlite3_column_count(statement) {
            let value: DatabaseNativeType
            let type = sqlite3_column_type(statement, i)
            switch type {
            case SQLITE_INTEGER:
                value = .int(sqlite3_column_int64(statement, i))
            case SQLITE_FLOAT:
                value = .double(sqlite3_column_double(statement, i))
            case SQLITE_TEXT:
                value = .string(String(cString: sqlite3_column_text(statement, i)))
            case SQLITE_BLOB:
                let bytes = sqlite3_column_bytes(statement, i)
                if bytes == 0 {
                    value = .data(Data())
                } else {
                    value = .data(Data(bytes: sqlite3_column_blob(statement, i), count: Int(bytes)))
                }
            case SQLITE_NULL:
                value = .null
            default:
                fatalError("unrecognized SQLite type \(type)")
            }
            
            columns.append(value)
            columnNames[String(cString: sqlite3_column_name(statement, i))] = Int(i)
        }
        
        self.init(columns: columns, columnIndices: columnNames)
    }
}

