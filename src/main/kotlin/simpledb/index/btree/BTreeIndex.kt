package simpledb.index.btree

import simpledb.file.BlockId
import simpledb.index.Index
import simpledb.query.Constant
import simpledb.record.Layout
import simpledb.record.RID
import simpledb.record.Schema
import simpledb.tx.Transaction
import kotlin.math.ln

/**
 * B Treeの操作を行うクラス
 */
class BTreeIndex(
    private val transaction: Transaction,
    private val indexName: String,
    private val leafLayout: Layout,
) : Index {
    private var leafTable = ""
    private var dirLayout: Layout
    private var rootBlockId: BlockId
    private var leaf: BTreeLeaf? = null

    /**
     * leaf、directoryのそれぞれのファイルを決定する
     * schemaオブジェクトからleafを作成する
     * その後leafから情報を取得しdirectoryを作成する
     */
    init {
        // deal with the leaves
        leafTable = "${indexName}leaf"
        if (transaction.size(leafTable) == 0) {
            val blockId = transaction.append(leafTable)
            val leafNode = BTPage(transaction, blockId, leafLayout)
            leafNode.format(blockId, -1)
        }
        // deal with the directory
        val dirSchema = Schema()
        dirSchema.add("block", leafLayout.schema())
        dirSchema.add("dataval", leafLayout.schema())
        val dirTable = "${indexName}dir"
        dirLayout = Layout(dirSchema)
        rootBlockId = BlockId(dirTable, 0)
        if (transaction.size(dirTable) == 0) {
            // create new root block
            transaction.append(dirTable)
            val dirNode = BTPage(transaction, rootBlockId, dirLayout)
            dirNode.format(rootBlockId, 0)
            // insert initial directory entry
            val fieldType = dirSchema.type("dataval")
            val minVal = if (fieldType == java.sql.Types.INTEGER) {
                Constant(Integer.MIN_VALUE)
            } else {
                Constant("")
            }
            dirNode.insertDir(0, minVal, 0)
            dirNode.close()
        }
    }

    /**
     * directoryを探索し、[searchKey]受け取った検索キーに対応するleafブロックを探す
     * leafブロックを開き、検索キーに該当する前の行に移動する
     * leaf pageは開かれ、next、getDataRidメソッドで該当する行に移動、行を返す
     */
    override fun beforeFirst(searchKey: Constant) {
        close()
        val root = BTreeDir(transaction, dirLayout, rootBlockId)
        val blockIdNumber = root.search(searchKey)
        root.close()
        val leafBlockId = BlockId(leafTable, blockIdNumber)
        leaf = BTreeLeaf(transaction, leafLayout, searchKey, leafBlockId)
    }

    /**
     * 検索キーを指定されたleaf pageを進め、次の行に移る
     */
    override fun next(): Boolean {
        return leaf!!.next()
    }

    /**
     * 現在のleafの行からRIDを返す
     */
    override fun getDataRid(): RID {
        return leaf!!.getDataRid()
    }

    /**
     * [dataValue]、[dataRid]受け取った値をindexに挿入する
     * beforeFirstによりdirectoryを探索しleaf pageにinsertを行う。
     * leaf pageが分割を行った場合、ルートdirectoryでinsertを行うことで新しいleaf pageのentryを渡す
     * ルートdirectoryが分割される場合、makeNewRootが呼ばれる
     */
    override fun insert(dataValue: Constant, dataRid: RID) {
        beforeFirst(dataValue)
        val entry = leaf?.insert(dataRid)
        leaf?.close()
        if (entry == null) return
        val root = BTreeDir(transaction, dirLayout, rootBlockId)
        val entry2 = root.insert(entry)
        if (entry2 != null) root.makeNewRoot(entry2)
        root.close()
    }

    /**
     * [dataValue]、[dataRid]受け取った値をindexから削除する
     * beforeFirstでdirectoryを探索し、leaf pageから行を削除する
     */
    override fun delete(dataValue: Constant, dataRid: RID) {
        beforeFirst(dataValue)
        leaf?.delete(dataRid)
        leaf?.close()
    }

    /**
     * leaf pageを閉じる
     */
    override fun close() {
        if (leaf != null) leaf?.close()
    }

    companion object {
        /**
         * [numberBlocks] B tree directoryのブロックの数、[rpb] ブロックあたりのインデックスの行の数をもとに
         * ブロックアクセスの数を推定する
         */
        fun searchCost(numberBlocks: Int, rpb: Int): Int {
            return 1 + (ln(numberBlocks.toDouble()) / ln(rpb.toDouble())) as Int
        }
    }
}