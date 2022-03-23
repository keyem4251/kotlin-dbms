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
    private var blockId: BlockId? = null
    private var pins = 0
    private var txnum = -1
    private var lsn = -1

    fun contents(): Page {
        return contents
    }

    fun blockId(): BlockId? {
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

    /**
     * バッファをディスクブロックに関連付ける
     * flushを呼び現在のブロックの変更を保存し、新しく渡されたブロック[b]の内容をPageに関連付ける
     * 関連付けられている内容が変わるのでpinsを0にする
     */
    fun assignToBlock(b: BlockId) {
        flush()
        blockId = b
        fm.read(blockId!!, contents)
        pins = 0
    }

    /**
     * バッファに割り当てられたディスクブロックがPageと同じ値になるようにしする。
     * Pageが修正されてない場合は何もしない、
     * 修正されている（txnumが0以上）場合はLogManagerのflushを呼び、
     * 修正内容のログレコードとPageの内容をディスクに書き込み、Pageの修正フラグ（txnum）を-1に設定する
     */
    fun flush() {
        if (txnum >= 0) {
            lm.flush(lsn)
            fm.write(blockId!!, contents)
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