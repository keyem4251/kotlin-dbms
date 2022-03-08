package simpledb.materialize

import simpledb.plan.Plan
import simpledb.query.Scan
import simpledb.query.UpdateScan
import simpledb.record.Schema
import simpledb.tx.Transaction

/**
 * マージソートを行うプラン
 */
class SortPlan(
    private val plan: Plan,
    private val transaction: Transaction,
    private val sortFields: List<String>,
) : Plan {
    private val schema = plan.schema()
    private val comparator = RecordComparator(sortFields)

    /**
     * マージソートアルゴリズムの動きをする
     */
    override fun open(): Scan {
        val srcScan: Scan = plan.open()
        var runs = splitIntoRuns(srcScan)
        srcScan.close()
        while (runs.size > 2) {
           runs = doAMergeIteration(runs)
        }
        return SortScan(comparator, runs)
    }

    override fun blocksAccessed(): Int {
        // 一時テーブルを作るための前処理のコストは計算しなし
        val materializePlan = MaterializePlan(plan, transaction)
        return materializePlan.blocksAccessed()
    }

    override fun recordsOutput(): Int {
        return plan.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return plan.distinctValues(fieldName)
    }

    override fun schema(): Schema {
        return schema
    }

    /**
     * マージソートアルゴリズムの分割部分を担当する
     * 一時テーブルを作成し、一時テーブルからcurrentScanを作成する
     * 入力[srcScan]を繰り返し、[copy]でcurrentScanに書き込む
     * 新しい実行が開始されるたびにcurrentScanを閉じて、一時テーブルを作成し直す
     * 関数の終了時には複数の一時テーブルが作られ、それぞれに1つの実行が格納される
     */
    private fun splitIntoRuns(srcScan: Scan): MutableList<TempTable> {
        val tempTables = mutableListOf<TempTable>()
        srcScan.beforeFirst()
        if (!srcScan.next()) return tempTables
        var currentTempTable = TempTable(transaction, schema)
        tempTables.add(currentTempTable)
        var currentScan: UpdateScan = currentTempTable.open()
        while (copy(srcScan, currentScan)) {
            if (comparator.compare(srcScan, currentScan) < 0) {
                // 新しいrunを開始する
                currentScan.close()
                currentTempTable = TempTable(transaction, schema)
                tempTables.add(currentTempTable)
                currentScan = currentTempTable.open()
            }
        }
        currentScan.close()
        return tempTables
    }

    /**
     * 一時テーブルのリストを受け取り、リスト内の一時テーブルのペアごとにmergeTwoRunsを呼び出し、マージの結果をresultに格納する
     */
    private fun doAMergeIteration(runs: MutableList<TempTable>): MutableList<TempTable> {
        val result = mutableListOf<TempTable>()
        while (runs.size > 1) {
            val tempTable1 = runs.removeAt(0)
            val tempTable2 = runs.removeAt(0)
            result.add(mergeTwoRuns(tempTable1, tempTable2))
        }
        if (runs.size == 1) result.add(runs[0])
        return result
    }

    /**
     * 受け取った2つのテーブルをマージして、その結果をresultに書き込み返す
     */
    private fun mergeTwoRuns(tempTable1: TempTable, tempTable2: TempTable): TempTable {
        val srcScan1: Scan = tempTable1.open()
        val srcScan2: Scan = tempTable2.open()
        val result = TempTable(transaction, schema)
        val destScan: UpdateScan = result.open()
        var hasMore1 = srcScan1.next()
        var hasMore2 = srcScan2.next()
        while (hasMore1 && hasMore2) {
            if (comparator.compare(srcScan1, srcScan2) < 0) {
                hasMore1 = copy(srcScan1, srcScan2 as UpdateScan)
            } else {
                hasMore2 = copy(srcScan2, srcScan1 as UpdateScan)
            }

            if (hasMore1) {
                while (hasMore1) hasMore1 = copy(srcScan1, destScan)
            } else {
                while (hasMore2) hasMore2 = copy(srcScan2, destScan)
            }
        }
        srcScan1.close()
        srcScan2.close()
        destScan.close()
        return result
    }

    /**
     * [srcScan]の内容を[destScan]に書き込む
     */
    private fun copy(srcScan: Scan, destScan: UpdateScan): Boolean {
        destScan.insert()
        for (fieldName in schema.fields) {
            destScan.setVal(fieldName, srcScan.getVal(fieldName))
        }
        return srcScan.next()
    }
}