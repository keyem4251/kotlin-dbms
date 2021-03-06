package simpledb.materialize

import simpledb.plan.Plan
import simpledb.query.Scan
import simpledb.query.UpdateScan
import simpledb.record.Layout
import simpledb.record.Schema
import simpledb.tx.Transaction
import kotlin.math.ceil

/**
 * 一時テーブルを用いた実行計画のコストを算出する
 * マテリアライズを用いる部分は全体のクエリのコストを計算する前に、TempTableに書き出され、後続の処理ではすべてTempTableから読まれる
 * T1 -> Materialize -> T2
 * T1 -> T2の実行計画のコストを計算する際にまず、T2のMaterialize（TempTable）を作成
 * その後T2へのアクセスは行わなずにTempTableを用いてT1 -> T2の実行計画を計算する
 * MaterializePlanはT2のMaterialize（TempTable）の実行計画を返す
 */
class MaterializePlan(
    private val srcPlan: Plan,
    private val transaction: Transaction,
) : Plan {
    /**
     * 元になるプラン[srcPlan](T2)から一時テーブルを作成し、Materialize用のScanを返す
     */
    override fun open(): Scan {
        val schema = srcPlan.schema()
        val tempTable = TempTable(transaction, schema)
        val srcScan: Scan = srcPlan.open()
        val destScan: UpdateScan = tempTable.open()
        while (srcScan.next()) {
            destScan.insert()
            for (fieldName in schema.fields) {
                destScan.setVal(fieldName, srcScan.getVal(fieldName))
            }
        }
        srcScan.close()
        destScan.beforeFirst()
        return destScan
    }

    /**
     * 一時テーブルのブロックアクセスの回数を推定し返す
     * TempTableを作るための前処理のコストは計算しない
     */
    override fun blocksAccessed(): Int {
        // create a dummy Layout object to calculate slot size
        val dummyLayout = Layout(srcPlan.schema())
        val rpb = (transaction.blockSize() / dummyLayout.slotSize()) as Double
        return ceil(srcPlan.recordsOutput() / rpb) as Int
    }

    /**
     * 一時テーブルの行数を返す
     */
    override fun recordsOutput(): Int {
        return srcPlan.recordsOutput()
    }

    /**
     * 一時テーブルの行のばらつきを返す
     */
    override fun distinctValues(fieldName: String): Int {
        return srcPlan.distinctValues(fieldName)
    }

    /**
     * 一時テーブルのスキーマを返す
     */
    override fun schema(): Schema {
        return srcPlan.schema()
    }
}
