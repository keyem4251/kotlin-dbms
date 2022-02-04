package simpledb.parse

/**
 * 条件式と値を扱う5種類のメソッドをもつクラス
 */
class PredicateParser(private val string: String) {
    private val lexer: Lexer = Lexer(string)

    /**
     * フィールド名を解析する
     */
    fun field() {
        lexer.eatId()
    }

    /**
     * フィールドの値を解析する
     * 検索した条件式の中の行の値
     */
    fun constant() {
        if (lexer.matchStringConstant()) {
            lexer.eatStringConstant()
        } else {
            lexer.eatIntConstant()
        }
    }

    /**
     * テーブルの列と値を解析する
     * 「ID = 1」の「ID」という列名なのか、「1」という値なのか
     */
    fun expression() {
        if (lexer.matchId()) {
            field()
        } else {
            constant()
        }
    }

    /**
     * 条件式の中のそれぞれの項を解析する
     * 「ID = 1」の「=」を除いた左右の値（ID、1）
     */
    fun term() {
        expression()
        lexer.eatDelimiter('=')
        expression()
    }

    /**
     * 条件式を解析する
     */
    fun predicate() {
        term()
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and")
            predicate()
        }
    }
}