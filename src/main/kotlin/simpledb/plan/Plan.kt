package simpledb.plan

import simpledb.query.Scan
import simpledb.record.Schema

/**
 * クエリツリーのコストを計算する
 * クエリで読み取られるテーブルのメタデータにアクセスし、効率のよい方法を探す
 */
interface Plan {

    /**
     * 実行計画に関連するScanクラスを開く
     * Scanはテーブルを読むのに使用される
     */
    fun open(): Scan

    /**
     * Scanが読み込みを完了するまでに起きるブロックアクセスの回数を推定し返す
     */
    fun blocksAccessed(): Int

    /**
     * クエリから出力されるレコード数を推定し返す
     */
    fun recordsOutput(): Int

    /**
     * クエリから出力されるレコードに指定された[fieldName]がどのくらい含まれているかを推定し返す
     */
    fun distinctValues(fieldName: String): Int

    /**
     * クエリのスキーマを返す
     */
    fun schema(): Schema
}
