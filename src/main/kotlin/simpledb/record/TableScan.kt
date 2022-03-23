package simpledb.record

import simpledb.file.BlockId
import simpledb.query.Constant
import simpledb.query.UpdateScan
import simpledb.tx.Transaction
import java.lang.RuntimeException

/**
 * テーブルのレコードを管理するクラス
 * 現在のレコードを記録、変更し内容を読み取る。
 * ファイルの中に複数のrecordPageがあり、TableScanはRecordPage、Slotを指定して値を取得する
 * TableScanはファイル単位で内容を確認（ファイル＝テーブル）なので複数のRecordPage、Slotをまたぐ
 * ファイル1:
 *   RecordPage1: [slot0|slot1|.....|slot13|空き領域]
 *   RecordPage2: [slot0|slot1|.....|slot13|空き領域]
 *
 * @property transaction テーブルを操作するトランザクション
 * @property recordPage スロットを管理する（スロットをブロックに割り当てている: RecordPageごとにブロックが決まってる）
 * @property currentSlot 操作対象となる現在のレコード
 */
class TableScan(
    private val transaction: Transaction,
    private val tableName: String,
    private val layout: Layout,
) : UpdateScan {
    private var recordPage: RecordPage? = null
    private var fileName: String = ""
    private var currentSlot: Int = 0

    init {
        fileName = "$tableName.tbl"
        if (transaction.size(fileName) == 0) {
            // ファイルが作成されていない場合（ファイルのサイズが0）
            // 新たにブロックを割り当てる
            moveToNewBlock()
        } else {
            // ファイルが作成されている場合
            // ファイルの戦闘に移動
            moveToBlock(0)
        }
    }

    // methods that implement Scan
    /**
     * データを操作するブロックをトランザクションの対象から外す
     * 新しいブロックをデータ操作の対象とする（ロックがかかるため）
     */
    override fun close() {
        if (recordPage != null) transaction.unpin(recordPage!!.blockId)
    }

    /**
     * 管理対象となる現在のレコードをファイルの最初のレコードの前の位置にする
     * 現在のファイル名でブロックを0にし、RecordPageを作成しcurrentSlotに-1を設定する
     */
    override fun beforeFirst() {
        moveToBlock(0)
    }

    /**
     * 管理対象となるレコードをの次のレコードに移す
     * 現在のrecordPageに次のレコードがない場合、ファイル末尾に到達するか次のレコードが見つかるまで探す
     * @return ファイル末尾に到達した場合 false、次のレコードが見つかった場合 true
     */
    override fun next(): Boolean {
        currentSlot = recordPage!!.nextAfter(currentSlot)
        while (currentSlot < 0) {
            // 現在のrecordPageに次のレコードがない場合
            if (atLastBlock()) return false // ファイル末尾に到達
            moveToBlock(recordPage!!.blockId.number+1)
            currentSlot = recordPage!!.nextAfter(currentSlot)
        }
        return true
    }

    /**
     * [fieldName]指定したフィールド名の現在のスロットの値を取得する
     * @return 数値
     */
    override fun getInt(fieldName: String): Int {
        return recordPage!!.getInt(currentSlot, fieldName)
    }

    /**
     * [fieldName]指定したフィールド名の現在のスロットの値を取得する
     * @return 文字列
     */
    override fun getString(fieldName: String): String {
        return recordPage!!.getString(currentSlot, fieldName)
    }

    /**
     * [fieldName]指定したフィールド名の現在のスロットの値を取得する
     * @return Constant
     */
    override fun getVal(fieldName: String): Constant {
        return if (layout.schema().type(fieldName) == java.sql.Types.INTEGER) {
            Constant(getInt(fieldName))
        } else {
            Constant(getString(fieldName))
        }
    }

    /**
     * [fieldName]指定したフィールド名を持っているかを返す
     * @return フィールドを持っていればtrue
     */
    override fun hasField(fieldName: String): Boolean {
        return layout.schema().hasField(fieldName)
    }

    // methods that implement UpdateScan
    /**
     * [fieldName]指定したフィールド名、[value]値を現在のスロットに瀬底する
     */
    override fun setInt(fieldName: String, value: Int) {
        recordPage!!.setInt(currentSlot, fieldName, value)
    }

    /**
     * [fieldName]指定したフィールド名、[value]値を現在のスロットに瀬底する
     */
    override fun setString(fieldName: String, value: String) {
        recordPage!!.setString(currentSlot, fieldName, value)
    }

    /**
     * [fieldName]指定したフィールド名、[value]値を現在のスロットに瀬底する
     */
    override fun setVal(fieldName: String, value: Constant) {
        if (layout.schema().type(fieldName) == java.sql.Types.INTEGER) {
            val intValue = value.asInt() ?: throw RuntimeException("null value")
            setInt(fieldName, intValue)
        } else {
            val stringValue = value.asString() ?: throw RuntimeException("null value")
            setString(fieldName, stringValue)
        }
    }

    /**
     * 現在のスロットの後ろに空いているスロットがあれば使用済みに変える
     * なければ新しいブロック（recordPage）を確保する
     */
    override fun insert() {
        currentSlot = recordPage!!.insertAfter(currentSlot)
        while (currentSlot < 0) {
            // 次のスロットが空いてない場合
            if (atLastBlock()) {
                // ファイル末尾なので新しいブロックへ
                moveToNewBlock()
            } else {
                // 現在のrecordPageの次のブロックへ
                moveToBlock(recordPage!!.blockId.number+1)
            }
            currentSlot = recordPage!!.insertAfter(currentSlot)
        }
    }

    /**
     * 現在のスロットを削除する（スロットの状態を空にする）
     */
    override fun delete() {
        recordPage!!.delete(currentSlot)
    }

    /**
     * [rid]指定されたレコードのIDに管理対象となるスロットを移動する
     */
    override fun moveToRid(rid: RID) {
        close()
        val blockId = BlockId(fileName, rid.blockNumber)
        recordPage = RecordPage(transaction, blockId, layout)
        currentSlot = rid.slot
    }

    /**
     * 現在管理対象となっているスロットのレコードIDを返す
     * @return レコードID
     */
    override fun getRid(): RID {
        return RID(recordPage!!.blockId.number, currentSlot)
    }

    /**
     * 現在のトランザクションから対象となっているブロックを外し
     * [blockNumber]指定されたブロックで新しくRecordPageを作成する
     */
    private fun moveToBlock(blockNumber: Int) {
        close()
        val blockId = BlockId(fileName, blockNumber)
        recordPage = RecordPage(transaction, blockId, layout)
        currentSlot = -1
    }

    /**
     * 現在のトランザクションから対象となっているブロックを外し
     * RecordPageを新しく作り直す（formatでRecordPageを初期化）
     */
    private fun moveToNewBlock() {
        close()
        val blockId = transaction.append(fileName)
        recordPage = RecordPage(transaction, blockId, layout)
        recordPage!!.format()
        currentSlot = -1
    }

    /**
     * 現在のファイルがRecordPageのブロックの末尾かどうかを返す
     * @return RecordPageのブロック末尾ならtrue
     */
    private fun atLastBlock(): Boolean {
        return recordPage!!.blockId.number == (transaction.size(fileName) - 1)
    }
}