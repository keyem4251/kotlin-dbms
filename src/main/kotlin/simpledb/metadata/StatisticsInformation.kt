package simpledb.metadata

class StatisticsInformation(
    private val numberBlocks: Int,
    private val numberRecords: Int,
) {
    fun blockAccessed(): Int {
        return numberBlocks
    }

    fun recordsOutput(): Int {
        return numberRecords
    }

    fun distinctValues(fieldName: String): Int {
        return 1 + (numberRecords / 3) // This is wildly inaccurate.
    }
}