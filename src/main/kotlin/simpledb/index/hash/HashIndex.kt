package simpledb.index.hash

import simpledb.index.Index
import simpledb.query.Constant
import simpledb.record.Layout
import simpledb.record.RID
import simpledb.record.TableScan
import simpledb.tx.Transaction
import java.lang.RuntimeException

/**
 * ハッシュインデックスを実装したクラス
 * bucket: インデックスのレコードの集まり -> テーブル
 * bucketの数は固定サイズ
 * 1つのインデックスに対して、複数のテーブル（バケット）が作られる
 * インデックスのそれぞれの行はRID（ブロックの位置）とdataval（データの値）を持つ
 *
 * @property indexName: 指定されたインデックス名
 */
class HashIndex(
    private val transaction: Transaction,
    private val indexName: String,
    private val layout: Layout,
): Index {
    val numberBuckets = 100
    private var searchKey: Constant? = null
    private var tableScan: TableScan? = null

    /**
     * 検索のキー[searchKey]を受け取り、ハッシュ値があるテーブルの最初の行に移動する
     * バケットに対応するTableScanクラスを作成し読み取りを行う
     */
    override fun beforeFirst(searchKey: Constant) {
        close()
        this.searchKey = searchKey
        val bucket = searchKey.hashCode() % numberBuckets
        val tableName = "$indexName$bucket"
        tableScan = TableScan(transaction, tableName, layout)
    }

    /**
     * 検索のキーを持つバケット（テーブル）の行をすすめる
     */
    override fun next(): Boolean {
        while (tableScan != null && tableScan!!.next()) {
            val dataValue = tableScan?.getVal("dataval")  ?: throw RuntimeException("null error")
            if (dataValue == searchKey) {
                return true
            }
        }
        return false
    }

    /**
     * バケットの行をTableScanクラスを用いて取得し行のRIDを返す
     */
    override fun getDataRid(): RID {
        val blockNumber = tableScan?.getInt("block") ?: throw RuntimeException("null error")
        val id = tableScan?.getInt("id") ?: throw RuntimeException("null error")
        return RID(blockNumber, id)
    }

    /**
     * バケットに行を追加する
     */
    override fun insert(dataValue: Constant, dataRid: RID) {
        beforeFirst(dataValue)
        tableScan?.insert()
        tableScan?.setInt("block", dataRid.blockNumber)
        tableScan?.setInt("id", dataRid.slot)
        tableScan?.setVal("dataval", dataValue)
    }

    /**
     * バケットから行を削除する
     */
    override fun delete(dataValue: Constant, dataRid: RID) {
        beforeFirst(dataValue)
        while (next()) {
            if (getDataRid() == dataRid) {
                tableScan?.delete()
                return
            }
        }
    }

    /**
     * インデックスを閉じる
     */
    override fun close() {
        if (tableScan != null) {
            tableScan?.close()
        }
    }

    /**
     * 指定されたブロックの数[numberBuckets]とバケットの数から検索のコストを計算する
     */
    companion object {
        private const val numberBuckets = 100
        fun searchCost(numberBlocks: Int, recordPerBlock: Int): Int {
            return numberBlocks / HashIndex.numberBuckets
        }
    }
}