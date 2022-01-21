package simpledb.query

/**
 * projectオペレータは入力テーブルから行の指定されたフィールドを出力する
 * フィールド操作以外のメソッドは受け取ったscanの操作を実行する
 *
 * @property scan テーブルのレコードを操作する
 * @property fieldList　フィールドのリスト
 */
class ProjectScan(
    private val scan: Scan,
    private val fieldList: List<String>,
):Scan {
    override fun beforeFirst() {
        scan.beforeFirst()
    }

    override fun next(): Boolean {
        return scan.next()
    }

    /**
     * [fieldName]指定されたフィールドを返す
     */
    override fun getInt(fieldName: String): Int {
        if (hasField(fieldName)) {
            return scan.getInt(fieldName)
        } else {
            throw RuntimeException("field not found.")
        }
    }

    /**
     * [fieldName]指定されたフィールドを返す
     */
    override fun getString(fieldName: String): String {
        if (hasField(fieldName)) {
            return scan.getString(fieldName)
        } else {
            throw RuntimeException("field not found.")
        }
    }

    /**
     * [fieldName]指定されたフィールドを返す
     */
    override fun getVal(fieldName: String): Constant {
        if (hasField(fieldName)) {
            return scan.getVal(fieldName)
        } else {
            throw RuntimeException("field not found.")
        }
    }

    /**
     * [fieldName]取得したいフィールドが指定されたフィールドかどうか判定する
     */
    override fun hasField(fieldName: String): Boolean {
        return fieldList.contains(fieldName)
    }

    override fun close() {
        scan.close()
    }
}