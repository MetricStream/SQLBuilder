package com.metricstream.jdbc.parser

import org.antlr.v4.runtime.BaseErrorListener
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.RecognitionException
import org.antlr.v4.runtime.Recognizer
import com.metricstream.jdbc.antlr.PlSqlLexer
import com.metricstream.jdbc.antlr.PlSqlParser

data class Issue(val line: Int, val positionInLine: Int, val message: String)

class SQLParser(val sql: String) {
    private val issues = mutableListOf<Issue>()

    fun parse(): Int {
        val errorListener = object : BaseErrorListener() {
            override fun syntaxError(
                recognizer: Recognizer<*, *>?,
                offendingSymbol: Any?,
                line: Int,
                charPositionInLine: Int,
                msg: String,
                e: RecognitionException?
            ) {
                issues += Issue(line, charPositionInLine, msg)
            }
        }

        issues.clear()

        val parser = PlSqlParser(CommonTokenStream(PlSqlLexer(CharStreams.fromString(sql))))
        parser.removeErrorListeners()
        parser.addErrorListener(errorListener)
        parser.sql_script()

        return issues.size
    }

    fun isValid() = parse() == 0

    fun isInvalid() = parse() != 0

    private fun String.truncate(max: Int) = if (length > max) take(max) + "..." else this

    fun showIssues(maxMessageLength: Int = 30, prefix: String = "") = buildString {
        append(prefix)
        sql.lineSequence().forEachIndexed { index, line ->
            appendLine(line)
            issues
                .filter { it.line == index + 1 }
                .sortedBy { it.positionInLine }
                .forEach {
                    append("^ ".padStart(it.positionInLine + 2, ' '))
                    appendLine(it.message.truncate(maxMessageLength))
                }
        }
    }
}
