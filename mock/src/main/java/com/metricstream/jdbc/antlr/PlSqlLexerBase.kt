package com.metricstream.jdbc.antlr

import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.Lexer

abstract class PlSqlLexerBase(input: CharStream?) : Lexer(input) {
    @JvmField
    protected val self: PlSqlLexerBase = this

    protected fun IsNewlineAtPos(pos: Int): Boolean {
        val la = _input.LA(pos)
        return la == -1 || la == '\n'.code
    }
}
