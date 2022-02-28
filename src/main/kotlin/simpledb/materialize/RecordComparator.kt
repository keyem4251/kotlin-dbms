package simpledb.materialize

import simpledb.query.Scan

class RecordComparator(
    private val fields: Collection<String>
) : Comparator<Scan> {
    override fun compare(o1: Scan?, o2: Scan?): Int {
        for (fieldName in fields) {
            val value1 = o1?.getVal(fieldName) ?: throw RuntimeException("null error")
            val value2 = o2?.getVal(fieldName) ?: throw RuntimeException("null error")
            val result = value1.compareTo(value2)
            if (result != 0) return result
        }
        return 0
    }
}