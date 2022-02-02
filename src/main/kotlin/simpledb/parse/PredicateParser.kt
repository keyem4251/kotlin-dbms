package simpledb.parse

class PredicateParser(private val string: String) {
    private val lexer: Lexer = Lexer(string)

    fun field() {
        lexer.eatId()
    }

    fun constant() {
        if (lexer.matchStringConstant()) {
            lexer.eatStringConstant()
        } else {
            lexer.eatIntConstant()
        }
    }

    fun expression() {
        if (lexer.matchId()) {
            field()
        } else {
            constant()
        }
    }

    fun term() {
        expression()
        lexer.eatDelimiter('=')
        expression()
    }

    fun predicate() {
        term()
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and")
            predicate()
        }
    }
}