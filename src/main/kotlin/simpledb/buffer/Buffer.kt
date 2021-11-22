package simpledb.buffer

import simpledb.file.BlockId
import simpledb.file.FileManager
import simpledb.file.Page
import simpledb.log.LogManager

/**
 * Pageに関連する4角情報を保持する
 * 1. Pageに関連するBlockId: ディスクの場所
 * 2. Pageが関連付けられた回数。
 * 3. Pageが修正されたかどうかの数値。修正されていない場合は-1、そうでない場合はPageを修正したトランザクションの識別子
 * 4. ログの情報。Pageが修正されば場合は最も最新のログレコードの識別子を保持する。
 *
 * @property fm FileManagerクラス
 * @property lm LogManagerクラス
 * @property contents 関連するPageクラス
 * @property blockId 関連するBlockIdクラス
 * @property pins Pageが関連付けられた回数。新たにブロックを割り当てられた場合は0になる。
 * @property txnum 関連するトランザクションの識別子。Pageが修正されたかどうかの識別子。
 * @property lsn 関連付けられたPageが修正された場合に作成されるログレコードの識別子。
 */
class Buffer(
    val fm: FileManager,
    val lm: LogManager,
) {
    private var contents: Page = Page(fm.blockSize)
    private lateinit var blockId: BlockId
    private var pins = 0
    private var txnum = -1
    private var lsn = -1

    /**
     * @return 現在紐付いているPageを返す
     */
    fun contents(): Page {
        return contents
    }

    fun blockId(): BlockId {
        return blockId
    }

    /**
     * クライアントが現在のPageを修正した場合は適切なログレコードの生成とともにsetModifiedが呼ばれる
     * [newTxnum]修正中のtransactionを識別する数値と生成したログレコードの識別子を受け取る
     * [newLsn]が-1の場合は今回のPageの操作ではログレコードが生成されていない
     */
    fun setModified(newTxnum: Int, newLsn: Int) {
        txnum = newTxnum
        if (lsn >= 0) lsn = newLsn
    }

    fun isPinned(): Boolean {
        return pins > 0
    }

    fun modifyingTx(): Int {
        return txnum
    }

    fun assignToBlock(b: BlockId) {
        flush()
        blockId = b
        fm.read(blockId, contents)
        pins = 0
    }

    fun flush() {
        if (txnum >= 0) {
            lm.flush(lsn)
            fm.write(blockId, contents)
            txnum = -1
        }
    }

    fun pin() {
        pins++
    }

    fun unpin() {
        pins--
    }
}