package simpledb.log

import simpledb.file.BlockId
import simpledb.file.FileManager
import simpledb.file.Page

/**
 * ログブロックのコンテンツを保持するためのページを割り当てるクラス
 *
 * @property fm ファイルマネージャー
 * @property blockId イテレータで保持する内容を持つブロック
 * @property page イテレータで保持する内容を持つページ
 * @property currentPosition ページ内の現在の読み込んでいる場所
 * @property boundary
 */
class LogIterator(val fm: FileManager, var blockId: BlockId): Iterator<ByteArray> {
    private var page: Page
    private var currentPosition = 0
    private var boundary = 0

    /**
     * ログファイルの末尾のブロックの最初のログレコードにイテレータを配置する
     */
    init {
        val b = ByteArray(fm.blockSize)
        page = Page(b)
        moveToBlock(blockId)
    }

    /**
     * 現在のログレコードがログファイルの中で最も古いレコードかを判定する
     * -> 次のログレコードがあるか
     * @return ログレコードがあればtrue
     */
    override fun hasNext(): Boolean {
        return currentPosition < fm.blockSize || blockId.number > 0
    }

    /**
     * ページ内の次のログレコードに移動する
     * レコードがない場合は、前のブロックを読み込み、ブロックの最初のレコードを返す
     * @return ログレコードをバイト配列として返す
     */
    override fun next(): ByteArray {
        if (currentPosition == fm.blockSize) {
            blockId = BlockId(blockId.filename, blockId.number-1)
            moveToBlock(blockId)
        }
        val record = page.getBytes(currentPosition)
        currentPosition += Integer.BYTES + record.size
        return record
    }

    /**
     * [blk]のブロックの内容をページに読み込み、現在の場所に設定する
     */
    private fun moveToBlock(blk: BlockId) {
        fm.read(blk, page)
        boundary = page.getInt(0)
        currentPosition = boundary
    }
}