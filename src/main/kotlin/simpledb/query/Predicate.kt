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

    /**
     * 空の条件式を作成します
     * この場合、条件式は常にtrueを返します
     */
    constructor() {}

    /**
     * 一つの項を含む条件式を作成します
     */
    constructor(term: Term) {
        terms.add(term)
    }

    /**
     * 指定された条件式[predicate]と自身を結合し、まとめた条件式にする
     */
    fun conjoinWith(predicate: Predicate) {
        terms.addAll(predicate.terms)
    }

    /**
     * 指定されたScan[scan]に関して、自身のそれぞれの項を評価して満たすtrueを返し、満たさない場合falseを返す
     */
    fun isSatisfied(scan: Scan): Boolean {
        for (term in terms) {
            if (!term.isSatisfied(scan)) return false
        }
        return true
    }

    /**
     * クエリプランナーのための関数。Predicateクラスを条件式として
     * 条件式によってクエリが出力するレコードの数がどの程度減少するかを計算する
     * 例）削除係数が2の場合、条件式は出力のサイズを半分にする
     * @return 削除係数
     */
    fun reductionFactor(plan: Plan): Int {
        var factor = 1
        for (term in terms) {
            factor *= term.reductionFactor(plan)
        }
        return factor
    }

    /**
     * 指定されたスキーマ[schema]に条件式の中のフィールドが含まれていれば条件式として返す
     */
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

    /**
     * 指定された２つのスキーマを合わせたフィールドに合致する項を条件式として返す
     * 個別のスキーマには対応しない
     */
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

    /**
     * クエリプランナーのための関数
     * 「F = c」（Fは渡されたフィールド名[fieldName]、cは何らかの値）の形式のTermクラスがあるかを判断する
     * 形式通りなら値を返し、そうでない場合はnullを返す
     * @return 値かnull
     */
    fun equateWithConstant(fieldName: String): Constant? {
        for (term in terms) {
            val constant = term.equatesWithConstant((fieldName))
            if (constant != null) {
                return constant
            }
        }
        return null
    }

    /**
     * クエリプランナーのための関数
     * 「F1 = F2」（F1は渡されたフィールド名、F2は別のフィールド）の形式のTermクラスがあるかを判断する
     * 形式通りならフィールド名を返し、そうでない場合はnullを返す
     * @return フィールド名かnull
     */
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
