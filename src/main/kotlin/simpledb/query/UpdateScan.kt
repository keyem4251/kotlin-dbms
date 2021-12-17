package simpledb.query

import simpledb.record.RID

/**
 * テーブル更新scanにより実装されるインターフェイス
 */
interface UpdateScan: Scan {

    /**
     * [fieldName]指定されたフィールドの現在のレコードの値を[value]指定された値で変更する
     */
    fun setVal(fieldName: String, value: Constant)

    /**
     * [fieldName]指定されたフィールドの現在のレコードの値を[value]指定された値で変更する
     */
    fun setInt(fieldName: String, value: Int)

    /**
     * [fieldName]指定されたフィールドの現在のレコードの値を[value]指定された値で変更する
     */
    fun setString(fieldName: String, value: String)

    /**
     * scanのどこかに新しいレコードを挿入する
     */
    fun insert()

    /**
     * scanから現在のレコードを削除する
     */
    fun delete()

    /**
     * 現在のレコードのIDを返す
     * @return 現在のレコードのID
     */
    fun getRid(): RID

    /**
     * [rid]指定されたIDを持つレコードの位置に移動する
     */
    fun moveToRid(rid: RID)
}