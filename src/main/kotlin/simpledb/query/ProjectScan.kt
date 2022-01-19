package simpledb.query

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

    override fun getInt(fieldName: String): Int {
        if (hasField(fieldName)) {
            return scan.getInt(fieldName)
        } else {
            throw RuntimeException("field not found.")
        }
    }

    override fun getString(fieldName: String): String {
        if (hasField(fieldName)) {
            return scan.getString(fieldName)
        } else {
            throw RuntimeException("field not found.")
        }
    }

    override fun getVal(fieldName: String): Constant {
        if (hasField(fieldName)) {
            return scan.getVal(fieldName)
        } else {
            throw RuntimeException("field not found.")
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return fieldList.contains(fieldName)
    }

    override fun close() {
        scan.close()
    }
}