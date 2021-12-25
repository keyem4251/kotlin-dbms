package simpledb

import simpledb.query.Constant
import simpledb.record.RID

/**
 * インデックスを管理、更新するためのメソッドを定義したインターフェイス
 */
interface Index {

    /**
     * [searchKey]指定された検索キーを持つ最初のレコードの前にインデックスを配置する
     */
    fun beforeFirst(searchKey: Constant)

    /**
     * beforeFirstメソッドでしてされた検索キーを持つ次のレコードにインデックスを移動させる
     * インデックスに次のレコードがない場合falseを返す
     * @return 検索キーを持つ次のレコードがなければfalse
     */
    fun next(): Boolean

    /**
     * 現在のインデックスのレコードに保存されたデータのレコードIDを返す
     * @return レコードID
     */
    fun getDataRid(): RID

    /**
     * [dataValue]指定された値、[dataRid]指定されたレコードIDを持つインデックスのレコードを挿入する
     */
    fun insert(dataValue: Constant, dataRid: RID)

    /**
     * [dataValue]指定された値、[dataRid]指定されたレコードIDを持つインデックスのレコードを削除する
     */
    fun delete(dataValue: Constant, dataRid: RID)

    /**
     * インデックスを閉じる
     */
    fun close()
}