package simpledb.file

/**
 * 特定のファイル（Disk）の何番目のBlockなのかを保持するクラス
 *
 * @property filename ファイル名
 * @property blockNumber Blockの番号
 */
data class BlockId(val filename: String, val blockNumber: Int)
