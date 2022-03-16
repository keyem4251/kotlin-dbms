package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

/**
 * merge joinを行うプラン（indexを用いたjoinではなく）
 *
 * @property scan 入力となるテーブル1
 * @property sortScan 入力となるテーブル2 ソートされている
 * @property fieldName1 テーブル1の結合を行うフィールド1
 * @property fieldName2 テーブル2の結合を行うフィールド2
 * @property joinValue 直前に結合を行った値
 */
class MergeJoinScan(
    private val scan: Scan,
    private val sortScan: SortScan,
    private val fieldName1: String,
    private val fieldName2: String,
) : Scan {
    private var joinValue: Constant? = null

    init {
        beforeFirst()
    }

    /**
     * それぞれのテーブルの位置を最初のレコードの前にすすめる
     */
    override fun beforeFirst() {
        scan.beforeFirst()
        sortScan.beforeFirst()
    }

    /**
     * それぞれのテーブルを閉じる
     */
    override fun close() {
        scan.close()
        sortScan.close()
    }

    /**
     * 次のレコードにすすめる
     * テーブル2が直前に結合を行った値[joinValue]と同じ値を持っている場合はテーブル2をすすめる
     * そうではなく、テーブル1の次の値が[joinValue]と同じ値の場合はテーブル2を[joinValue]を持つ位置に再移動する
     * テーブル1、テーブル2がともに[joinValue]と同じ値を持たない場合はそれぞれのテーブルの値が同じになるまで繰り返しテーブルをすすめる
     */
    override fun next(): Boolean {
        var hasMore2 = sortScan.next()
        if (hasMore2 && sortScan.getVal(fieldName2).equals(joinValue)) return true

        var hasMore1 = scan.next()
        if (hasMore1 && scan.getVal(fieldName1).equals(joinValue)) {
            sortScan.restorePosition()
            return true
        }

        while (hasMore1 && hasMore2) {
            val value1 = scan.getVal(fieldName1)
            val value2 = sortScan.getVal(fieldName2)
            if (value1.compareTo(value2) < 0) {
                hasMore1 = scan.next()
            } else if (value1.compareTo(value2) > 0) {
                hasMore2 = sortScan.next()
            } else {
                sortScan.savePosition()
                joinValue = sortScan.getVal(fieldName2)
                return true
            }
        }
        return false
    }

    /**
     * 指定されたフィールド[fieldName]の値を返す
     */
    override fun getInt(fieldName: String): Int {
        return if (scan.hasField(fieldName)) {
            scan.getInt(fieldName)
        } else {
            sortScan.getInt(fieldName)
        }
    }

    /**
     * 指定されたフィールド[fieldName]の値を返す
     */
    override fun getString(fieldName: String): String {
        return if (scan.hasField(fieldName)) {
            scan.getString(fieldName)
        } else {
            sortScan.getString(fieldName)
        }
    }

    /**
     * 指定されたフィールド[fieldName]の値を返す
     */
    override fun getVal(fieldName: String): Constant {
        return if (scan.hasField(fieldName)) {
            scan.getVal(fieldName)
        } else {
            sortScan.getVal(fieldName)
        }
    }

    /**
     * 指定されたフィールド[fieldName]を持っているか判定する
     */
    override fun hasField(fieldName: String): Boolean {
        return scan.hasField(fieldName) || sortScan.hasField(fieldName)
    }
}