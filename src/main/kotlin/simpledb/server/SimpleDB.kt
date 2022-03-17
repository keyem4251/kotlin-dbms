package simpledb.server

import simpledb.buffer.BufferManager
import simpledb.file.FileManager
import simpledb.index.planner.IndexUpdatePlanner
import simpledb.log.LogManager
import simpledb.metadata.MetadataManager
import simpledb.opt.HeuristicQueryPlanner
import simpledb.plan.*
import simpledb.tx.Transaction
import java.io.File

const val BLOCK_SIZE = 400
const val BUFFER_SIZE = 8
const val LOG_FILE = "simpledb.log"

class SimpleDB {
    var fileManager: FileManager
    var logManager: LogManager
    var bufferManager: BufferManager
    lateinit var metadataManager: MetadataManager
    lateinit var planner: Planner

    constructor(directoryName: String,blockSize: Int, bufferSize: Int) {
        val dbDirectory = File(directoryName)
        fileManager = FileManager(dbDirectory, blockSize)
        logManager = LogManager(fileManager, LOG_FILE)
        bufferManager = BufferManager(fileManager, logManager, bufferSize)
    }

    constructor(directoryName: String): this(directoryName, BLOCK_SIZE, BUFFER_SIZE) {
        val transaction = newTransaction()
        val isNew = fileManager.isNew
        if (isNew) {
            println("creating new database")
        } else {
            println("recovering existing database")
            transaction.recover()
        }
        metadataManager = MetadataManager(isNew, transaction)
        val queryPlanner: QueryPlanner = BasicQueryPlanner(metadataManager)
        val updatePlanner: UpdatePlanner = BasicUpdatePlanner(metadataManager)
        // val queryPlanner: QueryPlanner = HeuristicQueryPlanner(metadataManager)
        // val updatePlanner: UpdatePlanner = IndexUpdatePlanner(metadataManager)
        planner = Planner(queryPlanner, updatePlanner)
        transaction.commit()
    }

    fun newTransaction(): Transaction {
        return Transaction(fileManager, bufferManager, logManager)
    }
}