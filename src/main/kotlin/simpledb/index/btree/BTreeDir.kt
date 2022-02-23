package simpledb.index.btree

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.record.Layout
import simpledb.tx.Transaction

/**
 * B Treeへソートされた順序で値を挿入、分割を行う
 * B TreeのLeafのもとになるクラス
 * インデックスの行を持つ、Leafへの参照を持つ
 *
 * @property blockId B Treeブロックへの参照
 * @property layout B Tree directoryファイルのメタデータ
 */
class BTreeDir(
    private val transaction: Transaction,
    private val layout: Layout,
    private val blockId: BlockId,
) {
    private var contents = BTPage(transaction, blockId, layout)
    private val filename = blockId.filename

    /**
     * directory pageを閉じる
     */
    fun close() {
        contents.close()
    }

    /**
     * [searchKey]指定されたキーを持つB TreeのLeafへの参照を持つブロックを返す
     * 0番目の階層から子ノードを探索していく（階層はBTPageのflagで確認）
     */
    fun search(searchKey: Constant): Int {
        var childBlockId = findChildBlock(searchKey)
        while (contents.getFlag() > 0) {
            contents.close()
            contents = BTPage(transaction, childBlockId, layout)
            childBlockId = findChildBlock(searchKey)
        }
        return childBlockId.number
    }

    /**
     * 新しいルートのブロックを作成する（ルートでのinsertを行ったときに結果がnull出ない場合に呼び出される->ルートが分割、階層が0でなくなる）
     * ルートは常に階層構造の0番目のブロックでなければいけないので、新しいブロックを作成し、今の0番目の値を新しいブロックに移動する
     * ブロック0を新しいルート
     * 新しいルートは2つの子を持つ（古いルート、新しく分割されたブロック[entry]）
     */
    fun makeNewRoot(entry: DirEntry) {
        val firstVal = contents.getDataVal(0)
        val level = contents.getFlag()
        val newBlockId = contents.split(0, level) // ie, transfer all the recs
        val oldRoot = DirEntry(firstVal, newBlockId.number)
        insertEntry(oldRoot)
        insertEntry(entry)
        contents.setFlag(level+1)
    }

    /**
     * B Treeブロックに新しいdirectory entryを挿入する
     * 0番目の階層から子ノードを探索していく（階層はBTPageのflagで確認）
     * 0番目の階層なら、entryは0番目に挿入される。
     * そうでなければ、適切な子ノードに挿入され、返り値が検査される。
     * myEntryがnullでなければ、子ノードが分割されたためそのブロックに挿入される。
     */
    fun insert(entry: DirEntry): DirEntry? {
        if (contents.getFlag() == 0) return insertEntry(entry)
        val childBlockId = findChildBlock(entry.dataVal)
        val child = BTreeDir(transaction, layout, childBlockId)
        val myEntry = child.insert(entry)
        child.close()
        return if (myEntry != null) {
            insertEntry(myEntry)
        } else {
            null
        }
    }

    /**
     * BTPageクラスにEntryを挿入する
     * ページが分割された場合は新しいページのためのEntryが返される
     */
    private fun insertEntry(entry: DirEntry): DirEntry? {
        val newSlot = 1 + contents.findSlotBefore(entry.dataVal)
        contents.insertDir(newSlot, entry.dataVal, entry.blockNumber)
        if (!contents.isFull()) return null
        // else page is full, so split it
        val level = contents.getFlag()
        val splitPosition = contents.getNumRecords() / 2
        val splitVal = contents.getDataVal(splitPosition)
        val newBlockId = contents.split(splitPosition, level)
        return DirEntry(splitVal, newBlockId.number)
    }

    private fun findChildBlock(searchKey: Constant): BlockId {
        var slot = contents.findSlotBefore(searchKey)
        if (contents.getDataVal(slot+1).equals(searchKey)) slot += 1
        val blockNumber = contents.getChildNum(slot)
        return BlockId(filename, blockNumber)
    }
}
