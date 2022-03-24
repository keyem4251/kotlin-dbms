package simpledb.query

/**
 * productオペレータは複数の入力テーブルからの行をunion（結合）して出力する
 * 元になるscan1とscan2のレコードの可能な組み合わせのすべてを繰り返し処理できる必要がある
 * scan1、scan2で2重のループとなっているイメージでscan1が外側のループ、scan2が内側のループです
 *
 * @property scan1 結合するテーブル1
 * @property scan2 結合するテーブル2
 */
class ProductScan(
    private val scan1: Scan,
    private val scan2: Scan,
): Scan {
    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        scan1.beforeFirst()
        scan1.next()
        scan2.beforeFirst()
    }

    /**
     * 先にscan2を検査し続けることで入れ子のループのイメージを実現している
     */
    override fun next(): Boolean {
        return if (scan2.next()) {
            true
        } else {
            scan2.beforeFirst()
            scan2.next() && scan1.next()
        }
    }

    /**
     * 複数のテーブルの結合なので先にscan1を見て
     * scan1に[fieldName]指定されたフィールドがあれば返す、なければscan2から返す
     */
    override fun getInt(fieldName: String): Int {
        return if (scan1.hasField(fieldName)) {
            scan1.getInt(fieldName)
        } else {
            scan2.getInt(fieldName)
        }
    }

    /**
     * 複数のテーブルの結合なので先にscan1を見て
     * scan1に[fieldName]指定されたフィールドがあれば返す、なければscan2から返す
     */
    override fun getString(fieldName: String): String {
        return if (scan1.hasField(fieldName)) {
            scan1.getString(fieldName)
        } else {
            scan2.getString(fieldName)
        }
    }

    /**
     * 複数のテーブルの結合なので先にscan1を見て
     * scan1に[fieldName]指定されたフィールドがあれば返す、なければscan2から返す
     */
    override fun getVal(fieldName: String): Constant {
        return if (scan1.hasField(fieldName)) {
            scan1.getVal(fieldName)
        } else {
            scan2.getVal(fieldName)
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return scan1.hasField(fieldName) || scan2.hasField(fieldName)
    }

    override fun close() {
        scan1.close()
        scan2.close()
    }
}