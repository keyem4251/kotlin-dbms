package simpledb.query

/**
 * productオペレータは複数の入力テーブルからの行をunion（結合）して出力する
 */
class ProductScan(
    private val scan1: Scan,
    private val scan2: Scan,
): Scan {
    init {
        scan1.next()
    }

    override fun beforeFirst() {
        scan1.beforeFirst()
        scan1.next()
        scan2.beforeFirst()
    }

    override fun next(): Boolean {
        return if (scan2.next()) {
            true
        } else {
            scan2.beforeFirst()
            scan2.next() && scan1.next()
        }
    }

    override fun getInt(fieldName: String): Int {
        return if (scan1.hasField(fieldName)) {
            scan1.getInt(fieldName)
        } else {
            scan2.getInt(fieldName)
        }
    }

    override fun getString(fieldName: String): String {
        return if (scan1.hasField(fieldName)) {
            scan1.getString(fieldName)
        } else {
            scan2.getString(fieldName)
        }
    }

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