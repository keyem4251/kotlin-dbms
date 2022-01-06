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
    /**
     * テーブルのブロックの数
     * @return テーブルのブロックの数
     */
    fun blockAccessed(): Int {
        return numberBlocks
    }

     /**
     * テーブルのレコードの数
     * @return テーブルのレコードの数
     */
    fun recordsOutput(): Int {
        return numberRecords
    }

    /**
     * 各フィールドの値が何種類か。フィールドん値のばらつき。
     * この値は推測なため、参考値
     * @return フィールドのばらつきの数
     */
    fun distinctValues(fieldName: String): Int {
        return 1 + (numberRecords / 3) // 推測の値
    }
}