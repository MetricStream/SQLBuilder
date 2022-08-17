package com.metricstream.jdbc.antlr

import org.antlr.v4.runtime.Parser
import org.antlr.v4.runtime.TokenStream

abstract class PlSqlParserBase(input: TokenStream?) : Parser(input) {
    @JvmField
    protected val self: PlSqlParserBase = this

    protected fun isVersion12() = true
    protected fun isVersion10() = true
}
