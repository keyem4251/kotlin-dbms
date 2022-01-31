package simpledb.parse

import java.io.IOException
import java.io.StreamTokenizer
import java.io.StringReader
import java.util.*

class Lexer {
    private lateinit var keywords: MutableCollection<String>
    private lateinit var tokenizer: StreamTokenizer

    constructor(string: String) {
        initKeywords()
        tokenizer = StreamTokenizer(StringReader(string))
        tokenizer.ordinaryChar(".".toInt())
        tokenizer.wordChars("_".toInt(), "_".toInt())
        tokenizer.lowerCaseMode(true)
        nextToken()
    }

    // Methods to check the status of the current token

    fun matchDelimiter(delimiter: Char): Boolean {
        return delimiter == tokenizer.ttype as Char
    }

    fun matchIntConstant(): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER
    }

    fun matchStringConstant(): Boolean {
        return '\'' == tokenizer.ttype as Char
    }

    fun matchKeyword(word: String): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && tokenizer.sval.equals(word)
    }

    fun matchId(): {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tokenizer.sval)
    }

    // Methods to "eat" the current token

    fun eatDelimiter(delimiter: Char) {
        if (!matchDelimiter(delimiter)) throw BadSyntaxException()
        nextToken()
    }

    fun eatIntConstant(): Int {
        if (!matchIntConstant()) throw BadSyntaxException()
        val i = tokenizer.nval as Int
        nextToken()
        return i
    }

    fun eatStringConstant(): String {
        if (!matchStringConstant()) throw BadSyntaxException()
        val s = tokenizer.sval
        nextToken()
        return s
    }

    fun eatKeyword(word: String) {
        if (!matchKeyword(word)) throw BadSyntaxException()
        nextToken()
    }

    fun eatId(): String {
        if (!matchId()) throw BadSyntaxException()
        val s = tokenizer.sval
        nextToken()
        return s
    }

    private fun nextToken() {
        try {
            tokenizer.nextToken()
        } catch (e: IOException) {
            throw BadSyntaxException()
        }
    }

    private fun initKeywords() {
        keywords = mutableListOf(
            "select", "from", "where", "and", "insert", "into", "values", "delete",
            "update", "set", "create", "table", "varchar", "int", "view", "as", "index", "on"
        )
    }
}