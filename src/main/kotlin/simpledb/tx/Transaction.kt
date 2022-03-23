package simpledb.tx

import simpledb.buffer.BufferManager
import simpledb.file.BlockId
import simpledb.file.FileManager
import simpledb.log.LogManager
import simpledb.tx.concurrency.ConcurrencyManager
import simpledb.tx.recovery.RecoveryManager

/**
 * TransactionはRecoveryManager、ConcurrencyManagerからACID特性を実現するために使用される
 * 3つの種類のメソッドに分けられる
 * 1. life span: init（コンストラクタ）で作成、commit、rollbackで削除、recoverはコミットされてないTransactionをもとに戻す
 * 2. access buffer: クライアントからbufferを隠す。pinでbufferを保存し、getIntでBlockIdへの参照をクライアントへ渡す。
 *    setInt、setString、getStringも同じくクライアントからbufferを隠蔽する
 * 3. related file manager: sizeでファイルマーカーの末尾を読む、appendはファイルマーカーの末尾を修正する。
 *    競合を回避するためにConcurrencyManagerを呼ぶ。blockSizeはファイルマーカーのブロックサイズを取得するときに使用。
 */
class Transaction(
    val fileManager: FileManager,
    val bufferManager: BufferManager,
    val logManager: LogManager,
) {
    private val enfOfFile = -1
    private var recoveryManager: RecoveryManager
    private var concurrencyManager: ConcurrencyManager
    private var transactionNumber: Int = nextTransactionNumber()
    private var myBuffers: BufferList

    /**
     * recover managerとconcurrency managerを関連付けてトランザクションを作成する
     * このクラスの初期化はSimpleDBクラスから取得されるファイル、ログ、バッファーマネージャーに依存している
     * ファイル、ログ、バッファーマネージャーのオブジェクトはシステムの初期化時に作成される
     */
    init {
        recoveryManager = RecoveryManager(this, transactionNumber, logManager, bufferManager)
        concurrencyManager = ConcurrencyManager()
        myBuffers = BufferList(bufferManager)
    }

    /**
     * トランザクションに結び付けられているbufferを開放する
     * recover managerのコミットを呼び、concurrency managerのロックを開放する
     */
    fun commit() {
        recoveryManager.commit()
        concurrencyManager.release()
        myBuffers.unpinAll()
        println("transaction $transactionNumber committed")
    }

    /**
     * 自動的にトランザクションに結び付けられているbufferを開放する
     * recover managerのロールバックを呼び、concurrency managerのロックを開放する
     */
    fun rollback() {
        recoveryManager.rollback()
        concurrencyManager.release()
        myBuffers.unpinAll()
        println("transaction $transactionNumber rolled back")
    }

    /**
     * 修正されたバッファを書き込みrecover managerのrecverを呼び復旧する
     * ログを確認して、コミットされてないトランザクションをロールバックする
     * このメソッドはトランザクションが開始される前のシステムの開始時に呼ばれる。
     */
    fun  recover() {
        bufferManager.flushAll(transactionNumber)
        recoveryManager.recover()
    }

    /**
     * トランザクションがクライアントのためにバッファを管理する
     */
    fun pin(blockId: BlockId) {
        myBuffers.pin(blockId)
    }

    /**
     * トランザクションはブロックに紐付けられた、バッファを管理からはずす
     */
    fun unpin(blockId: BlockId) {
        myBuffers.unpin(blockId)
    }

    /**
     * 共有ロックを獲得し、指定したブロックの値を取得する
     */
    fun getInt(blockId: BlockId, offset: Int): Int? {
        concurrencyManager.sLock(blockId)
        val buffer = myBuffers.getBuffer(blockId) ?: return null
        return buffer.contents().getInt(offset)
    }

    /**
     * 共有ロックを獲得し、指定したブロックの値を取得する
     */
    fun getString(blockId: BlockId, offset: Int): String? {
        concurrencyManager.sLock(blockId)
        val buffer = myBuffers.getBuffer(blockId) ?: return null
        return buffer.contents().getString(offset)
    }

    /**
     * 指定のブロックの指定の位置に値を保存する
     * ブロックの排他ロックを獲得後にブロックのバッファを取得し、
     * recoverManagerで値をセットしログをレコードに書き込む
     * bufferの値を最新の値にセットし、トランザクションの識別子とろうの識別子をbufferにセットする
     */
    fun setInt(blockId: BlockId, offset: Int, value: Int, okToLog: Boolean) {
        concurrencyManager.xLock(blockId)
        val buffer = myBuffers.getBuffer(blockId)
        var lsn = -1
        if (buffer == null) return
        if (okToLog) lsn = recoveryManager.setInt(buffer, offset)
        val page = buffer.contents()
        page.setInt(offset, value)
        buffer.setModified(transactionNumber, lsn)
    }

    /**
     * 指定のブロックの指定の位置に値を保存する
     * ブロックの排他ロックを獲得後にブロックのバッファを取得し、
     * recoverManagerで値をセットしログをレコードに書き込む
     * bufferの値を最新の値にセットし、トランザクションの識別子とろうの識別子をbufferにセットする
     */
    fun setString(blockId: BlockId, offset: Int, value: String, okToLog: Boolean) {
        concurrencyManager.xLock(blockId)
        val buffer = myBuffers.getBuffer(blockId)
        var lsn = -1
        if (buffer == null) return
        if (okToLog) lsn = recoveryManager.setString(buffer, offset)
        val page = buffer.contents()
        page.setString(offset, value)
        buffer.setModified(transactionNumber, lsn)
    }

    /**
     * 共有ロックを獲得しファイルの末尾を-1の値としてダミーのブロックを作成し、ファイル名からファイルの長さを返す
     * @return ファイルの長さ
     */
    fun size(filename: String): Int {
        val dummyBlock = BlockId(filename, enfOfFile)
        concurrencyManager.sLock(dummyBlock)
        return fileManager.length(filename)
    }

    /**
     * 排他ロックを獲得しファイルの末尾を-1の値としてダミーのブロックを作成し、ファイルに新しいブロックを結びつける
     * @return ファイルの長さ
     */
    fun append(filename: String): BlockId {
        val dummyBlock = BlockId(filename, enfOfFile)
        concurrencyManager.xLock(dummyBlock)
        return fileManager.append(filename)
    }

    fun blockSize(): Int {
        return fileManager.blockSize
    }

    fun availableBuffers(): Int {
        return bufferManager.available()
    }

    companion object {
        var nextTransactionNumber = 0

        @Synchronized
        fun nextTransactionNumber(): Int {
            this.nextTransactionNumber++
            println("new transaction $nextTransactionNumber")
            return nextTransactionNumber
        }
    }
}