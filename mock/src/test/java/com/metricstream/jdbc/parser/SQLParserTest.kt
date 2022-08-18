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

    @Test
    fun `parse multiline TIMEZONE select`() {
        val sql = """
            SELECT COUNT(*) FROM(SELECT USER_ID, USER_NAME, FIRST_NAME, LAST_NAME, MIDDLE_INITIAL, NAME AS FULL_NAME,
             EMAIL_ADDRESS, PHONE_NUMBER, USER_STATUS, TIMEZONE, LOCALE, LOCATION, DEPARTMENT,
             SUPERVISOR, SUPERVISOR_FIRST_NAME, SUPERVISOR_MIDDLE_INITIAL, SUPERVISOR_LAST_NAME, CREATED_BY,
             INFOCENTER_COUNT, PREFERENCE_VALUE, REPORT_FORMAT, COMMENTS, to_date(END_DATE, 'MM-DD-YYYY:HH:MI:AM') as END_DATE,
             to_date(START_DATE, 'MM-DD-YYYY:HH:MI:AM') as START_DATE, INFOCENTER_FLAG, PACKAGE_ID, PROFILE_PICTURE_NAME,
             ISSUPERUSER, START_DT, END_DT, ACTIVATION_STATUS, USER_TYPE, USER_PROFILE_TYPE, USER_UUID, SCIM_USER_ID
             FROM SI_USER_ENT_V WHERE (UPPER(USER_NAME) LIKE UPPER(?) OR UPPER(FIRST_NAME) LIKE UPPER(?)
             OR UPPER(LAST_NAME) LIKE UPPER(?) OR UPPER(NAME) LIKE UPPER(?)) AND ACTIVATION_STATUS IN (?,?))
            """.trimIndent()
        val parser = SQLParser(sql)
        parser.parse() shouldBe 0
        println(parser.showIssues(50))
    }

    @Test
    fun `parse select with missing comma`() {
        val sql = """
            SELECT ENTITY_TYPE_ID, CLASS, UISEQUENCE, NVL(MULTISELECT,'N') AS MULTISELECT,
            NVL(HIERARCHIAL,'N') AS HIERARCHIAL, DEPENDENCIES AS PARENT  ICON
            FROM SI_MDOS_TUPLE_STRUCTURE_T WHERE TUPLE_TYPE = ?
            """.trimIndent()
        val parser = SQLParser(sql)
        parser.parse() shouldNotBe 0
        println(parser.showIssues(50))
    }


}
