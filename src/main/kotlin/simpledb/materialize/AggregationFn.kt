package simpledb.materialize

import simpledb.query.Constant
import simpledb.query.Scan

/**
 * 集約を行う関数のインターフェイス
 */
interface AggregationFn {
    /**
     * 現在のレコードを最初のレコードとして使用し新しいグループを開始する
     */
    fun processFirst(scan: Scan)

    /**
     * 既存のグループに次のレコードを追加し、集約を行う
     */
    fun processNext(scan: Scan)

    /**
     * 集約を行うフィールド名を返す
     */
    fun fieldName(): String

    /**
     * 集約した値を返す
     */
    fun value(): Constant
}