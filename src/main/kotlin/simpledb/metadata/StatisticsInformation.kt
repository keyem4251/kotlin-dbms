package simpledb.metadata

/**
 * 3つの統計情報を保持するクラス
 * テーブルのブロックの数、レコードの数、各フィールドの値が何種類か
 * 
 * @property numberBlocks それぞれのテーブルでどのくらいの数のブロックが使われているか
 * @property numberRecords それぞれのテーブルにどのくらいのレコードがあるか
 */
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