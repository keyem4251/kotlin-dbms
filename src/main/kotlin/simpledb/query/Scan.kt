package simpledb.query

/**
 * それぞれのクエリスキャンで実装される
 */
interface Scan {

    /**
     * 最初のレコードの前に配置する。
     * 続けてnext()が呼び出されると最初のレコードが返される。
     */
    fun beforeFirst()

    /**
     * scanを次のレコードに動かす
     * @return 次のレコードがなければfalseを返す
     */
    fun next(): Boolean

    /**
     * 現在のレコードの[fieldName]数値フィールドの値を返す
     * @return 現在のレコードの数値の値
     */
    fun getInt(fieldName: String): Int

    /**
     * 現在のレコードの[fieldName]文字列フィールドの値を返す
     * @return 現在のレコードの文字列の値
     */
    fun getString(fieldName: String): String

    /**
     * 現在のレコードの[fieldName]Constantフィールドの値を返す
     * @return 現在のレコードのConstantの値
     */
    fun getVal(fieldName: String): Constant

    /**
     * scanが[fieldName]特定のフィールドを持っていればtrueを返す
     * @return フィールドを持っているかどうか
     */
    fun hasField(fieldName: String): Boolean

    /**
     * scanとsubscansがあれば終了する
     */
    fun close()
}