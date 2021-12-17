package simpledb.record

class RID(
    val blockNumber: Int,
    val slot: Int
) {
    override fun equals(other: Any?): Boolean {
        val r = other as RID
        return blockNumber == r.blockNumber && slot == r.slot
    }

    override fun toString(): String {
        return "[$blockNumber, $slot]"
    }
}