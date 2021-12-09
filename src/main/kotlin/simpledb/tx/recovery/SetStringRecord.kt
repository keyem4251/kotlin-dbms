package simpledb.tx.recovery

import simpledb.file.BlockId
import simpledb.file.Page
import simpledb.log.LogManager
import simpledb.tx.Transaction

/**
 * @property blockId: 変更されたファイルの名前とブロック番号
 * @property offset: 変更が発生したオフセット
 * @property value: 変更が発生したオフセットでの古い値
 * 変更が発生したオフセットでの新しい値
 */
class SetStringRecord(val page: Page): LogRecord {
    private var transactionNumber: Int
    private var offset: Int
    private var value: String
    private var blockId: BlockId

    /**
     * 特定のトランザクションのIDを設定し、ログの値を含んだバイト配列（page）を元にログの内容を作成
     */
    init {
        val transactionPosition = Integer.BYTES
        transactionNumber = page.getInt(transactionPosition)
        val filePosition = transactionPosition + Integer.BYTES
        val filename = page.getString(filePosition)
        val blockPosition = filePosition + Page.maxLength(filename.length)
        val blockNumber = page.getInt(blockPosition)
        blockId = BlockId(filename, blockNumber)
        val offsetPosition = blockPosition + Integer.BYTES
        offset = page.getInt(offsetPosition)
        val valuePosition = offsetPosition + Integer.BYTES
        value = page.getString(valuePosition)
    }

    override fun op(): Int {
        return Operator.SETSTRING.id
    }

    override fun txNumber(): Int {
        return transactionNumber
    }

    override fun toString(): String {
        return "<SETSTRING $transactionNumber $blockId $offset $value>"
    }

    /**
     * 指定したトランザクションの内容をログレコードに保存されている値に置き換える。
     * やり直しが発生したログの行（Stringをセットする）のブロックにトランザクション処理を固定し、
     * setStringを呼び出し値を元に戻し、トランザクション処理を開放する。
     */
    override fun undo(transaction: Transaction) {
        transaction.pin(blockId)
        transaction.setString(blockId, offset, value, false) // undoの処理なのでログレコードは作成しない
        transaction.unpin(blockId)
    }

    /**
     * ログにsetString(文字列を設定するという動作)の行を書くメソッド
     * このログレコードはSETSTRING Operatorの後にトランザクションID、
     * ブロックのファイル名、修正されたブロックのオフセット（位置）、修正前のの文字列の値とそのオフセット（位置）が含まれている。
     */
    companion object {
        fun writeToLog(logManager: LogManager, txNum: Int, blk: BlockId, offset: Int, value: String): Int {
            val transactionPosition = Integer.BYTES
            val filePosition = transactionPosition + Integer.BYTES
            val blockPosition = filePosition + Page.maxLength(blk.filename.length)
            val offsetPosition = blockPosition + Integer.BYTES
            val valuePosition = offsetPosition + Integer.BYTES
            val recordLength = valuePosition + Page.maxLength(value.length)
            val record = ByteArray(recordLength)
            val p = Page(record)
            p.setInt(0, Operator.SETSTRING.id)
            p.setInt(transactionPosition, txNum)
            p.setString(filePosition, blk.filename)
            p.setInt(blockPosition, blk.number)
            p.setInt(offsetPosition, offset)
            p.setString(valuePosition, value)
            return logManager.append(record)
        }
    }
}