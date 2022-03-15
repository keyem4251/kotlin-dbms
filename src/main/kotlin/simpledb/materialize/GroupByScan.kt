package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

/**
 * 入力となるテーブル、集約を行うフィールド、関数の3つをもとにする
 *
 * @property scan 入力レコードのために初期化時にsort scan
 * @property groupFields group by を行うフィールド
 * @property aggregationFns 集約を行う関数
 */
class GroupByScan(
    private val scan: Scan,
    private val groupFields: List<String>,
    private val aggregationFns: List<AggregationFn>,
) : Scan {
    private lateinit var groupValue: GroupValue
    private var moreGroups = false

    init {
        beforeFirst()
    }

    /**
     * 最初のグループの前に移動する
     * グループの中の最初のレコードに位置する
     */
    override fun beforeFirst() {
        scan.beforeFirst()
        moreGroups = scan.next()
    }

    /**
     * グループ化された次のレコードに移動する
     * グループのキーは現在のレコードのGroupValueによって決まる
     * 異なるGroupValueを持つレコードを見つけるまで、scanをすすめる
     * グループ内の各レコードに対して集約関数が呼び出され、集約が行われる
     */
    override fun next(): Boolean {
        if (!moreGroups) return false
        for (aggregationFn in aggregationFns) {
            aggregationFn.processFirst(scan)
        }
        groupValue = GroupValue(scan, groupFields)
        moreGroups = scan.next()
        while (moreGroups) {
            val nextGroupValue = GroupValue(scan, groupFields)
            if (!groupValue.equals(nextGroupValue)) {
                break
            }
            for (aggregationFn in aggregationFns) {
                aggregationFn.processNext(scan)
            }
            moreGroups = scan.next()
        }
        return true
    }

    /**
     * scanを閉じる
     */
    override fun close() {
        scan.close()
    }

    /**
     * [fieldName]指定されたフィールドの値を返す
     * 指定されたフィールドが集約が行われるフィールドの場合、集約した値を返す
     */
    override fun getVal(fieldName: String): Constant {
        if (groupFields.contains(fieldName)) return groupValue.getVal(fieldName)
        for (aggregationFn in aggregationFns) {
            if (aggregationFn.fieldName() == fieldName) return aggregationFn.value()
        }
        throw RuntimeException("no field $fieldName")
    }

    /**
     * [fieldName]指定されたフィールドの値を返す
     * 指定されたフィールドが集約が行われるフィールドの場合、集約した値を返す
     */
    override fun getInt(fieldName: String): Int {
        return getVal(fieldName).asInt()!!
    }

    /**
     * [fieldName]指定されたフィールドの値を返す
     * 指定されたフィールドが集約が行われるフィールドの場合、集約した値を返す
     */
    override fun getString(fieldName: String): String {
        return getVal(fieldName).asString()!!
    }

    /**
     * [fieldName]指定されたフィールドがgroup byのフィールドの場合はtrue
     * また指定されたフィールドが集約が行われるフィールドの場合にもtrue
     * 上記以外はfalseを返す
     */
    override fun hasField(fieldName: String): Boolean {
        if (groupFields.contains(fieldName)) return true
        for (aggregationFn in aggregationFns) {
            if (aggregationFn.fieldName() == fieldName) return true
        }
        return false
    }
}