package simpledb.query

import simpledb.plan.Plan
import simpledb.record.Schema

/**
 * 条件は複数の項を受け取り項の結果を確認するクラス
 * 「SName = "joe" and MajorId = DId」という条件式の場合、
 *  (SName, "joe")というTermと(MajorId, DId)というTermを受け取り評価を行うのがPredicate
 *  Predicateクラスは各Termクラスのメソッドを呼び出し、式を評価していきます。
 *  引数なしとtermを受け取るパターンの2つのコンストラクタを持ちます。
 */
class Predicate {
    private val terms = arrayListOf<Term>()

    constructor() {}

    constructor(term: Term) {
        terms.add(term)
    }

    fun conjoinWith(predicate: Predicate) {
        terms.addAll(predicate.terms)
    }

    fun isSatisfied(scan: Scan): Boolean {
        for (term in terms) {
            if (!term.isSatisfied(scan)) return false
        }
        return true
    }

    fun reductionFactor(plan: Plan): Int {
        var factor = 1
        for (term in terms) {
            factor *= term.reductionFactor(plan)
        }
        return factor
    }

    fun selectSubPredicate(schema: Schema): Predicate? {
        val result = Predicate()
        for (term in terms) {
            if (term.appliesTo(schema)) result.terms.add(term)
        }
        return if (result.terms.size == 0) {
            null
        } else {
            result
        }
    }

    fun joinSubPredicate(schema1: Schema, schema2: Schema): Predicate? {
        val result = Predicate()
        val newSchema = Schema()
        newSchema.addAll(schema1)
        newSchema.addAll(schema2)
        for (term in terms) {
            if (!term.appliesTo(schema1) && !term.appliesTo(schema2) && term.appliesTo(newSchema)) {
                result.terms.add(term)
            }
        }
        return if (result.terms.size == 0) {
            null
        } else {
            result
        }
    }

    fun equatesWithField(fieldName: String): String? {
        for (term in terms) {
            val s = term.equatesWithField(fieldName)
            if (s != null) return s
        }
        return null
    }

    override fun toString(): String {
        val iterator = terms.iterator()
        if (!iterator.hasNext()) return ""
        var result = iterator.next().toString()
        while (iterator.hasNext()) {
            result += " and ${iterator.next().toString()}"
        }
        return result
    }
}
