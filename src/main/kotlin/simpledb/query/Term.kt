package simpledb.query

import simpledb.plan.Plan
import simpledb.record.Schema

/**
 * 項は2つの式を比較するためのクラス
 * 「SName = "joe" and MajorId = DId」という条件式の場合、
 *  SName、"joe"が格納されているExpressionを受け取るのがTerm
 *
 *  @property leftSideExpression 左の値
 *  @property rightSideExpression 右の値
 */
class Term(
    private val leftSideExpression: Expression,
    private val rightSideExpression: Expression,
) {
    /**
     * 左右の値をScanクラスを通して取得し評価する
     * 左右の値が同じならtrue、異なればfalseを返す
     * @return 左右の値が同じならtrue、異なればfalseを返す
     */
    fun isSatisfied(scan: Scan): Boolean {
        val leftSideExpressionValue = leftSideExpression.evaluate(scan)
        val rightSideExpressionValue = rightSideExpression.evaluate(scan)
        return rightSideExpressionValue.equals(leftSideExpressionValue)
    }

    /**
     * クエリプランナーのための関数
     * 指定されたスキーマ[schema]に左右の式がフィールドとして含まれているかを判定する
     * @return 含まれていればtrue、そうでなければfalse
     */
    fun appliesTo(schema: Schema): Boolean {
        return leftSideExpression.appliesTo(schema) && rightSideExpression.appliesTo(schema)
    }

    /**
     * クエリプランナーのための関数。Termクラスを条件式として
     * 条件式によってクエリが出力するレコードの数がどの程度減少するかを計算する
     * 例）削除係数が2の場合、条件式は出力のサイズを半分にする
     * @return 削除係数
     */
    fun reductionFactor(plan: Plan): Int {
        val leftSideExpressionName: String
        val rightSideExpressionName: String
        if (leftSideExpression.isFieldName() && rightSideExpression.isFieldName()) {
            leftSideExpressionName = leftSideExpression.asFieldName()
            rightSideExpressionName = leftSideExpression.asFieldName()
            return plan.distinctValues(leftSideExpressionName)
                .coerceAtLeast(plan.distinctValues(rightSideExpressionName))
        }
        if (leftSideExpression.isFieldName()) {
            leftSideExpressionName = leftSideExpression.asFieldName()
            return plan.distinctValues(leftSideExpressionName)
        }
        if (rightSideExpression.isFieldName()) {
            rightSideExpressionName = rightSideExpression.asFieldName()
            return plan.distinctValues(rightSideExpressionName)
        }
        // otherwise, the term equates constants
        return if (leftSideExpression.asConstant().equals(rightSideExpression.asConstant())) {
            1
        } else {
            Integer.MAX_VALUE
        }
    }

    /**
     * クエリプランナーのための関数
     * Termクラスが「F = c」（Fは渡されたフィールド名[fieldName]、cは何らかの値）の形式かを判断する
     * 形式通りなら値を返し、そうでない場合はnullを返す
     * @return 値かnull
     */
    fun equatesWithConstant(fieldName: String): Constant? {
        val hasOnlyLeftSideFieldName = leftSideExpression.isFieldName() &&
                leftSideExpression.asFieldName() == fieldName &&
                !rightSideExpression.isFieldName()
        val hasOnlyRightSideFieldName = rightSideExpression.isFieldName() &&
                rightSideExpression.asFieldName() == fieldName &&
                !leftSideExpression.isFieldName()
        return if (hasOnlyLeftSideFieldName) {
            rightSideExpression.asConstant()
        } else if (hasOnlyRightSideFieldName) {
            leftSideExpression.asConstant()
        } else {
            null
        }
    }

    /**
     * クエリプランナーのための関数
     * Termクラスが「F1 = F2」（F1は渡されたフィールド名、F2は別のフィールド）の形式かを判断する
     * 形式通りならフィールド名を返し、そうでない場合はnullを返す
     * @return フィールド名かnull
     */
    fun equatesWithField(fieldName: String): String? {
        val hasLeftSideFieldName = leftSideExpression.isFieldName() &&
                leftSideExpression.asFieldName() == fieldName &&
                rightSideExpression.isFieldName()
        val hasRightSideFieldName = rightSideExpression.isFieldName() &&
                rightSideExpression.asFieldName() == fieldName &&
                leftSideExpression.isFieldName()
        return if (hasLeftSideFieldName) {
            rightSideExpression.asFieldName()
        } else if (hasRightSideFieldName) {
            leftSideExpression.asFieldName()
        } else {
            null
        }
    }

    override fun toString(): String {
        return "${leftSideExpression.toString()}=${rightSideExpression.toString()}"
    }
}
