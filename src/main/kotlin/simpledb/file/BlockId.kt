package simpledb.file

/**
 * 特定のファイル（Disk）の何番目のBlockなのかを保持するクラス
 *
 * @property filename ファイル名
 * @property number Blockの番号
 */
data class BlockId(val filename: String, val number: Int)
