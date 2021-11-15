package simpledb.log

import simpledb.file.BlockId
import simpledb.file.FileManager
import simpledb.file.Page

/**
 * システムの最初に作成される
 * ログをバイト配列として扱い、FileMangerを通してディスクに保存（メモリにログの内容を保持する場合はPage）
 * クライアントへはLogIteratorとしてログの内容をバイト配列として返す
 *
 * @property fm FileManagerクラス
 * @property logFile ログファイル名
 * @property logPage ログの内容を含むPageクラス
 * @property currentBlock 現在のBlockIdクラス
 * @property latestLSN 最新のログレコードの識別子
 * @property lastSavedLSN 最後に保存されたログレコードの識別子
 */
class LogManager(
    private val fm: FileManager,
    private val logFile: String,
) {
    var logPage: Page
    var currentBlock: BlockId
    private var latestLSN = 0
    private var lastSavedLSN = 0

    /**
     * ログファイルが空の場合は新しい空のブロックを割り当てる
     * 空ではない場合はlogPageにログファイルの最後のディスクの内容を読み取る
     */
    init {
        val b = ByteArray(fm.blockSize)
        logPage = Page(b)

        val logSize = fm.length(logFile)
        if (logSize == 0) {
            currentBlock = appendNewBlock()
        } else {
            currentBlock = BlockId(logFile, logSize - 1)
            fm.read(currentBlock, logPage)
        }
    }

    /**
     * 指定したログレコードの識別子[lsn]が最後に保存したログレコードより大きい場合に
     * 現状のlogPageの内容をディスクに保存する
     */
    fun flush(lsn: Int) {
        if (lsn >= lastSavedLSN) {
            flush()
        }
    }

    /**
     * LogManagerのClientがログの内容を読み取る
     * flush()でメモリの内容をディスクに保存後、ディスク内のログの内容を持つIteratorを返す
     * @return ディスク内のログの内容を持つIterator
     */
    fun iterator(): Iterator<ByteArray> {
        flush()
        return LogIterator(fm, currentBlock)
    }

    /**
     * バイト配列の[logRecord]をログファイルに保存する
     * @return 最新のログレコードの識別子
     */
    @Synchronized
    fun append(logRecord: ByteArray): Int {
        var boundary = logPage.getInt(0)
        val recordSize = logRecord.size
        val bytesNeeded = recordSize + Integer.BYTES
        if (boundary - bytesNeeded < Integer.BYTES) { // It doesn't fit
            flush() // so move to the next block
            currentBlock = appendNewBlock()
            boundary = logPage.getInt(0)
        }
        val recrdPosition = boundary - bytesNeeded
        logPage.setBytes(recrdPosition, logRecord)
        logPage.setInt(0, recrdPosition) // the new boundary
        latestLSN += 1
        return latestLSN
    }

    /**
     * 空のページをログファイルに追加する。
     * ログフィアルに新しい領域を追加する。
     * @return 新しい領域のBlockId
     */
    private fun appendNewBlock(): BlockId {
        val block = fm.append(logFile)
        logPage.setInt(0, fm.blockSize)
        fm.write(block, logPage)
        return block
    }

    /**
     * ログページの内容を現在のブロックに書き込み
     * 最後に保存したログレコードの識別子を、最新の識別子に更新する
     */
    private fun flush() {
        fm.write(currentBlock, logPage)
        lastSavedLSN = latestLSN
    }
}