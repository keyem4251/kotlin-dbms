package simpledb.record

/**
 * レコードIDを表すクラス
 *
 * @property blockNumber レコードが配置されているブロックの位置
 * @property slot スロット（RecordPageの中の何番目のスロットか）
 */
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