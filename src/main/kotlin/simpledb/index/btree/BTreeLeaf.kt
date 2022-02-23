package simpledb.index.btree

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.record.Layout
import simpledb.record.RID
import simpledb.tx.Transaction

/**
 * B Treeへソートされた順序で値を挿入、分割を行う
 * B Treeの末端となるクラス（挿入されると分割する可能性）
 * Indexの行を持つ
 *
 * @property blockId ディスクブロックへの参照
 * @property layout B Tree leafファイルのメタデータ
 * @property searchKey 検索キーの値
 * @property currentSlot 初期化時に指定された検索キーが存在する場合に、キーを持つ最初のレコードの直前に現在のバッファ（スロット）を設定する
 */
class BTreeLeaf(
    private val transaction: Transaction,
    private val layout: Layout,
    private val searchKey: Constant,
    private val blockId: BlockId,
) {
    private var contents: BTPage = BTPage(transaction, blockId, layout)
    private var currentSlot = contents.findSlotBefore(searchKey)
    private val filename = blockId.filename

    /**
     * leaf pageを閉じる
     */
    fun close() {
        contents.close()
    }

    /**
     * 先に指定された検索キーを持つ次のleafの行に移動する
     * leafブロックの中の参照しているスロットをすすめる（BTPageのレコード数と現在のスロットを比較しながら）
     * 該当する行がなければfalseを返す
     */
    fun next(): Boolean {
        currentSlot += 1
        return if (currentSlot >= contents.getNumRecords()) {
            tryOverflow()
        } else if (contents.getDataVal(currentSlot).equals(searchKey)) {
            true
        } else {
            tryOverflow()
        }
    }

    /**
     * leafブロックの中の現在参照しているスロットの行のRIDを返す
     */
    fun getDataRid(): RID {
        return contents.getDataRid(currentSlot)
    }

    /**
     * leafブロックの中の[dataRid]指定されたRIDを持つ行を削除する]
     */
    fun delete(dataRid: RID) {
        while (next()) {
            if (getDataRid().equals(dataRid)) {
                contents.delete(currentSlot)
                return
            }
        }
    }

    /**
     * [dataRid]指定されたRIDを持ち、leafで管理しているのと同じ検索キーを持つ新しいインデックスの行を挿入する
     * 行がこのPageと合わなければ、Pageの分割を行い新しいページに対する参照を持つdirectory entry（ノード）を返す
     * そうでなく分割が行われなければ、nullを返す
     */
   fun insert(dataRid: RID): DirEntry? {
        if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchKey) > 0) {
            val firstVal = contents.getDataVal(0)
            val newBlockInt = contents.split(0, contents.getFlag())
            currentSlot = 0
            contents.setFlag(-1)
            contents.insertLeaf(currentSlot, searchKey, dataRid)
            return DirEntry(firstVal, newBlockInt.number)
        }
        currentSlot += 1
        contents.insertLeaf(currentSlot, searchKey, dataRid)
        if (!contents.isFull()) return null
        // else page is full, so split it
        val firstKey = contents.getDataVal(0)
        val lastKey = contents.getDataVal(contents.getNumRecords()-1)
        if (lastKey.equals(firstKey)) {
            // create an overflow block to hold all but the first record
            val newBlockId = contents.split(1, contents.getFlag())
            contents.setFlag(newBlockId.number)
            return null
        } else {
            var splitPosition = contents.getNumRecords() / 2
            var splitKey = contents.getDataVal(splitPosition)
            if (splitKey.equals(firstKey)) {
                // move right, looking for the next key
                while (contents.getDataVal(splitPosition).equals(splitPosition)) splitPosition += 1
                splitKey = contents.getDataVal(splitPosition)
            } else {
                // move left, looking for first entry having that key
                while (contents.getDataVal(splitPosition-1).equals(splitKey)) splitPosition -= 1
            }
            val newBlockId = contents.split(splitPosition, -1)
            return DirEntry(splitKey, newBlockId.number)
        }
    }

    /**
     * leafブロックがoverflowのchainを含む可能性があるかを判断する
     * 同じ値でridが異なるブロックの連鎖
     * [|pat||ron|] -> [(ron,r27)] -> [(ron,r35), (ron,r59), ...]
     */
    private fun tryOverflow(): Boolean {
        val firstKey = contents.getDataVal(0)
        val flag = contents.getFlag()
        if (searchKey != firstKey || flag < 0) return false
        contents.close()
        val nextBlockId = BlockId(filename, flag)
        contents = BTPage(transaction, nextBlockId, layout)
        currentSlot = 0
        return true
    }
}