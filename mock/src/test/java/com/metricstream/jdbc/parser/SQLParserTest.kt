package com.metricstream.jdbc.parser

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

import org.junit.jupiter.api.Test


internal class SQLParserTest {
    @Test
    fun `parse simple correct select`() {
        val sql = "select a, b from table"
        SQLParser(sql).parse() shouldBe 0
    }

    @Test
    fun `parse simple buggy select`() {
        val sql = "select a,, b from ? table"
        val parser = SQLParser(sql)
        parser.parse() shouldNotBe 0
        println(parser.showIssues())
    }

    @Test
    fun `parse multiline buggy select`() {
        val sql = """
            select a, b from table
              where a = 3
            order by
            """.trimIndent()
        val parser = SQLParser(sql)
        parser.parse() shouldNotBe 0
        println(parser.showIssues(50))
    }
}
