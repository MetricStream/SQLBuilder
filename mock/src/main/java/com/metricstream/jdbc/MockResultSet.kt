/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc

import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.StringReader
import java.math.BigDecimal
import java.sql.Date
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicLong
import com.opencsv.CSVReader
import com.opencsv.exceptions.CsvException
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import org.slf4j.LoggerFactory

/**
 * Mocks a SQL ResultSet.
 * Initially copied from https://github.com/sharfah/java-utils/blob/master/src/test/java/com/sharfah/util/sql/MockResultSet.java
 * https://github.com/sharfah/java-utils/commit/0e930cbf74134e4d1cb71b0c4ca0602250e4f6fc
 */
class MockResultSet private constructor(tag: String, names: Array<String>?, private val data: Array<Array<Any?>>, usages: Int = 1) {
    private val tag: String
    private val columnIndices: Map<String, Int>
    private var rowIndex = -1
    private var wasNull = false
    private var generated = false
    private var remaining = usages - 1


    private fun index(columnName: String) = columnIndices[columnName.uppercase()] ?: Int.MAX_VALUE

    private fun outOfRange(columnIndex: Int) = generated || (rowIndex >= data.size || columnIndex >= data[rowIndex].size)

    private fun answerObject(columnIndex: Int): Any? = when {
        outOfRange(columnIndex) -> THE_ANSWER_TO_THE_ULTIMATE_QUESTION.toString()
        else -> data[rowIndex][columnIndex]
    }.also {
        wasNull = it == null
    }

    private fun answerDouble(columnIndex: Int) = when {
        outOfRange(columnIndex) -> THE_ANSWER_TO_THE_ULTIMATE_QUESTION.toDouble()
        else -> when (val value = data[rowIndex][columnIndex]) {
            is Double? -> value
            is String -> value.toDouble()
            else -> throw SQLException()
        }
    }.also {
        wasNull = it == null
    } ?: 0.0

    private fun answerTimestamp(columnIndex: Int) = when {
        outOfRange(columnIndex) -> Timestamp(THE_ANSWER_TO_THE_ULTIMATE_QUESTION.toLong())
        else -> when (val value = data[rowIndex][columnIndex]) {
            is Timestamp? -> value
            is String -> Timestamp.valueOf(value)
            is Long -> Timestamp(value)
            else -> throw SQLException()
        }
    }.also {
        wasNull = it == null
    }

    private fun answerDate(columnIndex: Int) = when {
        outOfRange(columnIndex) -> Date(THE_ANSWER_TO_THE_ULTIMATE_QUESTION.toLong())
        else -> when (val value = data[rowIndex][columnIndex]) {
            is Date? -> value
            is String -> Date.valueOf(value)
            is Long -> Date(value)
            else -> throw SQLException()
        }
    }.also {
        wasNull = it == null
    }

    private fun answerOffsetDateTime(columnIndex: Int) = when {
        outOfRange(columnIndex) -> OffsetDateTime.of(4242, 4, 2, 4, 2, 4, 2, ZoneOffset.UTC)
        else -> when (val value = data[rowIndex][columnIndex]) {
            is OffsetDateTime? -> value
            is String -> OffsetDateTime.parse(value)
            else -> throw SQLException()
        }
    }.also {
        wasNull = it == null
    }

    private fun answerLong(columnIndex: Int) = when {
        outOfRange(columnIndex) -> THE_ANSWER_TO_THE_ULTIMATE_QUESTION.toLong()
        else -> when (val value = data[rowIndex][columnIndex]) {
            is Long? -> value
            is String -> value.toLong()
            else -> throw SQLException()
        }
    }.also {
        wasNull = it == null
    } ?: 0L

    private fun answerInt(columnIndex: Int) = when {
        outOfRange(columnIndex) -> THE_ANSWER_TO_THE_ULTIMATE_QUESTION
        else -> when (val value = data[rowIndex][columnIndex]) {
            is Int? -> value
            is String -> value.toInt()
            else -> throw SQLException()
        }
    }.also {
        wasNull = it == null
    } ?: 0

    private fun answerString(columnIndex: Int) = when {
        outOfRange(columnIndex) -> THE_ANSWER_TO_THE_ULTIMATE_QUESTION.toString()
        else -> data[rowIndex][columnIndex] as String?
    }.also {
        wasNull = it == null
    }

    private fun answerBigDecimal(columnIndex: Int) = when {
        outOfRange(columnIndex) -> THE_ANSWER_TO_THE_ULTIMATE_QUESTION.toBigDecimal()
        else -> data[rowIndex][columnIndex] as BigDecimal?
    }.also {
        wasNull = it == null
    }

    @Throws(SQLException::class)
    private fun buildMock(): ResultSet {
        val rs: ResultSet = mockk()

        every { rs.next() } answers {
            if (remaining < -1) {
                throw SQLException("Forced exception")
            }
            rowIndex++
            if (rowIndex == data.size && remaining > 0) {
                rowIndex = 0
                remaining--
            }
            rowIndex < data.size
        }

        every { rs.wasNull() } answers { wasNull }

        every { rs.getString(any<String>()) } answers { answerString(index(firstArg())) }
        every { rs.getString(any<Int>()) } answers { answerString(firstArg<Int>() - 1) }

        every { rs.getInt(any<String>()) } answers { answerInt(index(firstArg())) }
        every { rs.getInt(any<Int>()) } answers { answerInt(firstArg<Int>() - 1) }

        every { rs.getLong(any<String>()) } answers { answerLong(index(firstArg())) }
        every { rs.getLong(any<Int>()) } answers { answerLong(firstArg<Int>() - 1) }

        every { rs.getDouble(any<String>()) } answers { answerDouble(index(firstArg())) }
        every { rs.getDouble(any<Int>()) } answers { answerDouble(firstArg<Int>() - 1) }

        every { rs.getDate(any<String>()) } answers { answerDate(index(firstArg())) }
        every { rs.getDate(any<Int>()) } answers { answerDate(firstArg<Int>() - 1) }

        every { rs.getTimestamp(any<String>()) } answers { answerTimestamp(index(firstArg())) }
        every { rs.getTimestamp(any<Int>()) } answers { answerTimestamp(firstArg<Int>() - 1) }

        every { rs.getBigDecimal(any<String>()) } answers { answerBigDecimal(index(firstArg())) }
        every { rs.getBigDecimal(any<Int>()) } answers { answerBigDecimal(firstArg<Int>() - 1) }

        every { rs.getObject(any<String>()) } answers { answerObject(index(firstArg())) }
        every { rs.getObject(any<Int>()) } answers { answerObject(firstArg<Int>() - 1) }

        every { rs.getObject(any<String>(), eq(OffsetDateTime::class.java, false)) } answers { answerOffsetDateTime(index(firstArg())) }
        every { rs.getObject(any<Int>(), eq(OffsetDateTime::class.java, false)) } answers { answerOffsetDateTime(firstArg<Int>() - 1) }

        val rsmd: ResultSetMetaData = mockk()

        every { rsmd.columnCount } answers { columnIndices.size }

        every { rs.findColumn(any()) } answers {
            val columnIndex = index(firstArg())
            if (columnIndex == Int.MAX_VALUE) throw SQLException("Invalid column name")
            columnIndex + 1
        }

        every { rsmd.getColumnName(any()) } answers {
            val columnIndex = firstArg<Int>() - 1
            columnIndices.filter { columnIndex == it.value }.keys.firstOrNull()
        }

        every { rs.toString() } answers { tag }

        every { rs.metaData } returns rsmd

        every { rs.type } returns ResultSet.TYPE_FORWARD_ONLY

        every { rs.close() } just runs

        return rs
    }

    companion object {
        private val logger = LoggerFactory.getLogger(MockResultSet::class.java)
        private val counter = AtomicLong(0)
        internal const val THE_ANSWER_TO_THE_ULTIMATE_QUESTION = 42

        /**
         * Creates the mock ResultSet.
         *
         * @param columnNames the names of the columns
         * @param data the data to be returned from the mocked ResultSet
         * @param usages the number of times this resultset is used, defaults to 1
         * @return a mocked ResultSet
         * @throws SQLException if building the mocked ResultSet fails
         */
        @JvmStatic
        @JvmOverloads
        @Throws(SQLException::class)
        fun create(tag: String, columnNames: Array<String>?, data: Array<Array<Any?>>, usages: Int = 1): ResultSet {
            return MockResultSet(tag, columnNames, data, usages).buildMock()
        }

        /**
         * Adds a mock ResultSet object to the queue.
         *
         * @param columnNames the names of the columns
         * @param data the data to be returned from the mocked ResultSet
         * @param usages the number of times this resultset is used, defaults to 1
         */
        @JvmStatic
        @JvmOverloads
        fun add(tag: String, columnNames: Array<String>?, data: Array<Array<Any?>>, usages: Int = 1) {
            MockSQLBuilderProvider.addResultSet(create(tag, columnNames, data, usages))
        }

        /**
         * Creates the mock ResultSet.
         *
         * @param data the data to be returned from the mocked ResultSet
         * @return a mocked ResultSet
         * @throws SQLException if building the mocked ResultSet fails
         */
        @JvmStatic
        @Throws(SQLException::class)
        fun create(tag: String, data: Array<Array<Any?>>): ResultSet {
            return MockResultSet(tag, null, data).buildMock()
        }

        /**
         * Adds a mock ResultSet object to the queue.
         *
         * @param data the data to be returned from the mocked ResultSet
         */
        @JvmStatic
        fun add(tag: String, data: Array<Array<Any?>>) {
            MockSQLBuilderProvider.addResultSet(create(tag, data))
        }

        /**
         * Creates the mock ResultSet.
         *
         * @param csv the data to be returned from the mocked ResultSet
         * @return a mocked ResultSet
         * @throws SQLException if building the mocked ResultSet fails
         */
        @JvmStatic
        @JvmOverloads
        @Throws(SQLException::class)
        fun create(tag: String, csv: String, withLabels: Boolean, generated: Boolean = false): ResultSet {
            try {
                CSVReader(StringReader(csv)).use { csvReader ->
                    val data: MutableList<Array<String?>> = csvReader.readAll()
                    val columnNames = if (withLabels) data.removeAt(0).map { it!! }.toTypedArray() else null
                    val mockResultSet = MockResultSet(tag, columnNames, data.toTypedArray() as Array<Array<Any?>>)
                    mockResultSet.generated = generated
                    return mockResultSet.buildMock()
                }
            } catch (ex: IOException) {
                logger.error("Cannot parse CSV {}", csv)
                throw SQLException("Invalid data")
            } catch (ex: CsvException) {
                logger.error("Cannot parse CSV {}", csv)
                throw SQLException("Invalid data")
            }
        }

        /**
         * Adds a mock ResultSet object to the queue.
         *
         * @param csv the data to be returned from the mocked ResultSet
         */
        @JvmStatic
        fun add(tag: String, csv: String, withLabels: Boolean) {
            MockSQLBuilderProvider.addResultSet(create(tag, csv, withLabels, false))
        }

        /**
         * Creates the mock ResultSet.
         *
         * @param csv A CSV file containing the data, optionally with a header line
         * @return a mocked ResultSet
         * @throws SQLException in case of errors
         */
        @JvmStatic
        @Throws(SQLException::class)
        fun create(tag: String, csv: InputStream, withLabels: Boolean): ResultSet {
            try {
                CSVReader(InputStreamReader(csv)).use { csvReader ->
                    val data = csvReader.readAll()
                    val columnNames = if (withLabels) data.removeAt(0) else null
                    return MockResultSet(tag, columnNames, data.toTypedArray() as Array<Array<Any?>>).buildMock()
                }
            } catch (ex: Exception) {
                logger.error("Cannot parse CSV {}", csv)
                throw SQLException("Invalid data")
            }
        }

        /**
         * Adds a mock ResultSet object to the queue.
         *
         * @param csv A CSV file containing the data, optionally with a header line
         */
        @JvmStatic
        @JvmOverloads
        fun add(tag: String, csv: InputStream, withLabels: Boolean = true) {
            MockSQLBuilderProvider.addResultSet(create(tag, csv, withLabels))
        }

        /**
         * Creates the mock ResultSet.
         *
         * @param csvs the data to be returned from the mocked ResultSet
         * @return a mocked ResultSet
         * @throws SQLException if building the mocked ResultSet fails
         */
        @JvmStatic
        @Throws(SQLException::class)
        fun create(tag: String, labels: String, vararg csvs: String): ResultSet {
            try {
                CSVReader(StringReader(labels)).use { csvReader1 ->
                    val columnNames = csvReader1.readNext()
                    val data = mutableListOf<Array<String>>()
                    for (csv in csvs) {
                        CSVReader(StringReader(csv)).use { csvReader2 -> data.addAll(csvReader2.readAll()) }
                    }
                    return MockResultSet(tag, columnNames, data.toTypedArray() as Array<Array<Any?>>).buildMock()
                }
            } catch (ex: IOException) {
                logger.error("Cannot parse CSV {}", listOf(*csvs))
                throw SQLException("Invalid data")
            } catch (ex: CsvException) {
                logger.error("Cannot parse CSV {}", listOf(*csvs))
                throw SQLException("Invalid data")
            }
        }

        /**
         * Adds a mock ResultSet object to the queue.
         *
         * @param csvs the data to be returned from the mocked ResultSet
         */
        @JvmStatic
        fun add(tag: String, labels: String, vararg csvs: String) {
            MockSQLBuilderProvider.addResultSet(create(tag, labels, *csvs))
        }

        /**
         * Creates an empty mock ResultSet.
         *
         * @return a mocked ResultSet
         * @throws SQLException if building the mocked ResultSet fails
         */
        @JvmStatic
        @Throws(SQLException::class)
        fun empty(tag: String): ResultSet {
            return MockResultSet(tag, arrayOf(), arrayOf(), 0).buildMock()
        }

        /**
         * Creates an empty mock ResultSet.
         *
         * @return a mocked ResultSet
         */
        @JvmStatic
        fun addEmpty(tag: String) {
            MockSQLBuilderProvider.addResultSet(empty(tag))
        }

        @JvmStatic
        @Throws(SQLException::class)
        fun broken(tag: String): ResultSet {
            return MockResultSet(tag, arrayOf(), arrayOf(), -1).buildMock()
        }

        @JvmStatic
        fun addBroken(tag: String) {
            MockSQLBuilderProvider.addResultSet(broken(tag))
        }
    }

    init {
        this.tag = tag.ifEmpty { "MockResultSet#${counter.incrementAndGet()}" }
        val columnNames: Array<String> = if (names.isNullOrEmpty()) {
            Array(data.getOrNull(0)?.size ?: 0) { i: Int -> "COLUMN${i + 1}" }
        } else {
            names
        }
        columnIndices = columnNames.mapIndexed { i, n -> n.uppercase() to i }.toMap()
    }
}
