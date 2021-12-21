package simpledb.record

import simpledb.file.BlockId
import simpledb.tx.Transaction

/**
 * 1つのブロック（スロットの配列）を表すクラス: サイズとして何バイトか決まっている
 * 1ブロックをスロットの配列として管理（スロットは1レコード+レコードの使用有無のフラグ: 0は空、1は使用済み）
 * Record Pageのイメージ: [slot0|slot1|.....|slot13|空き領域]
 * 1ブロック400バイト
 * スロット使用領域: 378バイト
 * 空き領域: 22バイト
 * slot0: [1|record0]
 * slot1: [0|record1]
 * slot13: [1|record13]
 * (1 record 26バイト（layoutが保持する情報）+ 使用済みかのフラグ 1バイト)* 14 slot
 * transactionを通して値に設定する
 */
class RecordPage(
    private val transaction: Transaction,
    val blockId: BlockId,
    private val layout: Layout,
) {
    init {
        transaction.pin(blockId)
    }

    /**
     * [slot]受け取ったレコードのスロットの位置の[fieldName]フィールド名を返す
     * @return レコードの数値
     */
    fun getInt(slot: Int, fieldName: String): Int {
        val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
        val fieldPosition = offset(slot) + layoutOffset
        return transaction.getInt(blockId, fieldPosition) ?: throw RecordPageException()
    }

    /**
     * [slot]受け取ったレコードのスロットの位置の[fieldName]フィールド名を返す
     * @return レコードの文字列
     */
    fun getString(slot: Int, fieldName: String): String {
        val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
        val fieldPosition = offset(slot) + layoutOffset
        return transaction.getString(blockId, fieldPosition) ?: throw RecordPageException()
    }

    /**
     * [slot]受け取ったレコードのスロットの位置の[fieldName]フィールド名に指定された[value]値を設定する
     */
    fun setInt(slot: Int, fieldName: String, value: Int) {
        val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
        val fieldPosition = offset(slot) + layoutOffset
        transaction.setInt(blockId, fieldPosition, value, true)
    }

    /**
     * [slot]受け取ったレコードのスロットの位置の[fieldName]フィールド名に指定された[value]値を設定する
     */
    fun setString(slot: Int, fieldName: String, value: String) {
        val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
        val fieldPosition = offset(slot) + layoutOffset
        transaction.setString(blockId, fieldPosition, value, true)
    }

    /**
     * [slot]受け取ったレコードのスロットのフラグを空にする
     */
    fun delete(slot: Int) {
        setFlag(slot, RecordPageState.EMPTY.id)
    }

    /**
     * レイアウトの構造を元にして、新しいレコードのブロックをフォーマットする
     * RecordPageのスロット、レコードの配列を初期化する（状態、値を空にする）
     * トランザクションのログには記録しない
     */
    fun format() {
        var slot = 0
        while (isValidSlot(slot)) {
            transaction.setInt(blockId, offset(slot), RecordPageState.EMPTY.id, false)
            val schema = layout.schema()
            for (fieldName in schema.fields) {
                val layoutOffset = layout.offset(fieldName) ?: throw RecordPageException()
                val fieldPosition = offset(slot) + layoutOffset
                if (schema.type(fieldName) == java.sql.Types.INTEGER) {
                    transaction.setInt(blockId, fieldPosition, 0, false)
                } else {
                    transaction.setString(blockId, fieldPosition, "", false)
                }
            }
            slot++
        }
    }

    /**
     * [slot]受け取ったレコードのスロットの後の使用されているスロットを返す
     * @return 指定されたスロットの後ろのスロット、なければ-1
     */
    fun nextAfter(slot: Int): Int {
        return searchAfter(slot, RecordPageState.USED.id)
    }

    /**
     * [slot]受け取ったレコードのスロットの後ろの空のスロットを使用済みのフラグに変えてスロットを返す
     * @return 指定されたスロットの後ろの空のスロット、なければ-1
     */
    fun insertAfter(slot: Int): Int {
        val newSlot = searchAfter(slot, RecordPageState.EMPTY.id)
        if (newSlot >= 0) setFlag(newSlot, RecordPageState.USED.id)
        return newSlot
    }

    /**
     * [slot]受け取ったレコードのスロットの後の[flag]指定されたフラグのスロットを返す
     * @return 指定されたスロットの後ろのスロット、なければ-1
     */
    private fun searchAfter(slot: Int, flag: Int): Int {
        var nextSlot = slot + 1
        while (isValidSlot(nextSlot)) {
            // slot = [state|record]の構造なのでnextSlotの位置はflag
            val transactionInt = transaction.getInt(blockId, offset(nextSlot))
            if (transactionInt != null && transactionInt == flag) {
                return nextSlot
            }
            nextSlot++
        }
        return -1
    }

    /**
     * [slot]受け取ったレコードのスロットが有効か返す
     * @return 有効ならtrue
     */
    private fun isValidSlot(slot: Int): Boolean {
        // 受け取ったスロットの次のスロットの位置がトランザクションのブロックのサイズより小さいならtrue
        return offset(slot+1) <= transaction.blockSize()
    }

    /**
     * [slot]受け取ったレコードのスロットに[flag]指定されたフラグを設定する
     */
    private fun setFlag(slot: Int, flag: Int) {
        transaction.setInt(blockId, offset(slot), flag, true)
    }

    /**
     * [slot]受け取ったレコードのスロットの始まる位置を計算する
     * @return スロットの位置
     */
    private fun offset(slot: Int): Int {
        return slot * layout.slotSize()
    }
}