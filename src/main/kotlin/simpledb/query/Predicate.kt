package simpledb.query

import simpledb.plan.Plan
import simpledb.record.Schema

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
