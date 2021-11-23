package simpledb.tx

import simpledb.buffer.Buffer
import simpledb.buffer.BufferManager
import simpledb.file.BlockId
import simpledb.file.FileManager
import simpledb.log.LogManager
import simpledb.tx.concurrency.ConcurrencyManager
import simpledb.tx.recovery.RecoveryManager

class Transaction(
    val fileManager: FileManager,
    val bufferManager: BufferManager,
    val logManager: LogManager,
) {
    private var nextTransactionNumber = 0
    private val enfOfFile = -1
    private lateinit var recoveryManager: RecoveryManager
    private lateinit var concurrencyManager: ConcurrencyManager
    private var transactionNumber: Int
    private lateinit var myBuffers: BufferList

    init {
        transactionNumber = nextTransactionNumber()
        val recoveryManager = RecoveryManager(this, transactionNumber, logManager, bufferManager)
        val concurrencyManager = ConcurrencyManager()
        myBuffers = BufferList(bufferManager)
    }

    fun commit() {
        recoveryManager.commit()
        concurrencyManager.release()
        myBuffers.unpinAll()
        println("transaction $transactionNumber committed")
    }

    fun rollback() {
        recoveryManager.rollback()
        concurrencyManager.release()
        myBuffers.unpinAll()
        println("transaction $transactionNumber rolled back")
    }

    fun  recover() {
        bufferManager.flushAll(transactionNumber)
        recoveryManager.recover()
    }

    fun pin(blockId: BlockId) {
        myBuffers.pin(blockId)
    }

    fun unpin(blockId: BlockId) {
        myBuffers.unpin(blockId)
    }

    fun getInt(blockId: BlockId, offset: Int): Int {
        concurrencyManager.sLock(blockId)
        val buffer = myBuffers.getBuffer(blockId)
        return buffer.contents().getInt(offset)
    }

    fun getString(blockId: BlockId, offset: Int): String {
        concurrencyManager.sLock(blockId)
        val buffer = myBuffers.getBuffer(blockId)
        return buffer.contents().getString(offset)
    }

    fun setInt(blockId: BlockId, offset: Int, value: Int, okToLog: Boolean) {
        concurrencyManager.xLock(blockId)
        val buffer = myBuffers.getBuffer(blockId)
        var lsn = -1
        if (okToLog) lsn = recoveryManager.setInt(buffer, offset)
        val page = buffer.contents()
        page.setString(offset, value)
        buffer.setModified(transactionNumber, lsn)
    }
}