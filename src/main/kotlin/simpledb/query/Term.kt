package simpledb.query

import simpledb.plan.Plan
import simpledb.record.Schema

/**
 * 項は2つの式を比較するためのクラス
 * 「SName = "joe" and MajorId = DId」という条件式の場合、
 *  SName、"joe"が格納されているExpressionを受け取るのがTerm
 */
class Term(
    private val leftSideExpression: Expression,
    private val rightSideExpression: Expression,
) {
    fun isSatisfied(scan: Scan): Boolean {
        val leftSideExpressionValue = leftSideExpression.evaluate(scan)
        val rightSideExpressionValue = rightSideExpression.evaluate(scan)
        return rightSideExpressionValue.equals(leftSideExpressionValue)
    }

    fun appliesTo(schema: Schema): Boolean {
        return leftSideExpression.appliesTo(schema) && rightSideExpression.appliesTo(schema)
    }

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
