package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan
import simpledb.query.UpdateScan
import simpledb.record.RID

/**
 * マージソートを行うScan
 * @property runs 1あるいは2つのソートされた集合
 */
class SortScan(
    private val comparator: RecordComparator,
    private val runs: MutableList<TempTable>,
) : Scan {
    private val scan1: UpdateScan = runs[0].open()
    private var scan2: UpdateScan? = null
    private var currentScan: UpdateScan? = null
    private var hasMore1 = scan1.next()
    private var hasMore2 = false
    private lateinit var savedPosition: List<RID>

    /**
     * runsが1つの場合はscan2はnull、hasMore2はfalseになる
     *
     */
    init {
        if (runs.size > 1) {
            scan2 = runs[1].open()
            hasMore2 = scan2?.next() ?: throw RuntimeException("null error")
        }
    }

    /**
     * ソートされた最初のレコードの前に移動する
     * currentScanはnullの状態、内部的なrunsに格納されているテーブルをすすめる
     */
    override fun beforeFirst() {
        scan1.beforeFirst()
        hasMore1 = scan1.next()
        if (scan2 != null) {
            scan2!!.beforeFirst()
            hasMore2 = scan2!!.next()
        }
    }

    /**
     * ソートされている次のレコードに移動する
     * 現在のscanを次のレコードに移動する（currentScanに該当するscan1 or sca2をnextですすめる）
     * sca1、scan2のうち小さい値のレコードをもっている方をcurrentScanに設定する
     *
     */
    override fun next(): Boolean {
        if (currentScan == scan1) {
            hasMore1 = scan1.next()
        } else if (currentScan == scan2) {
            hasMore2 = scan2?.next() ?: throw RuntimeException("null error")
        }

        if (!hasMore1 && !hasMore2) {
            return false
        } else if (hasMore1 && hasMore2) {
            currentScan =  if (comparator.compare(scan1, scan2) < 0) {
                scan1
            } else {
                scan2
            }
        } else if (hasMore1) {
            currentScan = scan1
        } else if (hasMore2) {
            currentScan = scan2
        }
        return true
    }

    override fun close() {
        scan1.close()
        if (scan2 != null) {
            scan2!!.close()
        }
    }

    override fun getVal(fieldName: String): Constant {
        return currentScan?.getVal(fieldName) ?: throw RuntimeException("null error")
    }

    override fun getInt(fieldName: String): Int {
        return currentScan?.getInt(fieldName) ?: throw RuntimeException("null error")
    }

    override fun getString(fieldName: String): String {
        return currentScan?.getString(fieldName) ?: throw RuntimeException("null error")
    }

    override fun hasField(fieldName: String): Boolean {
        return currentScan?.hasField(fieldName) ?: throw RuntimeException("null error")
    }

    /**
     * 復元用に現在のレコードの位置を保存する
     */
    fun savePosition() {
        val rid1 = scan1.getRid()
        val rid2 = scan2?.getRid() ?: throw RuntimeException("null error")
        savedPosition = listOf(rid1, rid2)
    }

    /**
     * 保存されているレコードの位置に移動する
     */
    fun restorePosition() {
        val rid1 = savedPosition[0]
        val rid2 = savedPosition[1]
        scan1.moveToRid(rid1)
        scan2?.moveToRid(rid2)
    }
}