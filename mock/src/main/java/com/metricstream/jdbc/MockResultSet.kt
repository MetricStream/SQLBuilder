/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc

import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.StringReader
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
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.lenient
import org.mockito.invocation.InvocationOnMock
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
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

    // We could use this method to unify the "by name" and "by number" mocks if we can somehow handle an union type of String and Int
    // Something like `whenever(rs).getLong(anyString() | anyInt())` or `whenever(rs).or(getLong(anyString()), getLong(anyInt()))`
    private fun InvocationOnMock.index() = when (val arg: Any = getArgument(0)) {
        is Int -> arg - 1
        is String -> columnIndices[arg.uppercase()] ?: Int.MAX_VALUE
        else -> Int.MAX_VALUE
    }

    private fun InvocationOnMock.indexByNumber(): Int = getArgument<Int>(0) - 1

    private fun InvocationOnMock.indexByName(): Int = columnIndices[getArgument<String>(0).uppercase()] ?: Int.MAX_VALUE

    private fun outOfRange(columnIndex: Int) = generated || (rowIndex >= data.size || columnIndex >= data[rowIndex].size)

    private fun answer(columnIndex: Int): Any? = when {
        outOfRange(columnIndex) -> THE_ANSWER_TO_THE_ULTIMATE_QUESTION.toString()
        else -> data[rowIndex][columnIndex]
    }.also {
        wasNull = it == null
    }

    @Throws(SQLException::class)
    private fun buildMock(): ResultSet {
        val rs: ResultSet = mock()

        // mock rs.next()
        lenient().doAnswer {
            if (remaining < -1) {
                throw SQLException("Forced exception")
            }
            rowIndex++
            if (rowIndex == data.size && remaining > 0) {
                rowIndex = 0
                remaining--
            }
            rowIndex < data.size
        }.whenever(rs).next()

        // mock rs.wasNull()
        lenient().doAnswer { wasNull }.whenever(rs).wasNull()

        // mock rs.getString(columnName)
        lenient().doAnswer { invocation ->
            answer(invocation.indexByName())
        }.whenever(rs).getString(anyString())

        // mock rs.getString(columnIndex)
        lenient().doAnswer { invocation ->
            answer(invocation.indexByNumber())
        }.whenever(rs).getString(anyInt())

        // mock rs.getInt(columnName)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByName())) {
                is String -> value.toInt()
                else -> value
            }
        }.whenever(rs).getInt(anyString())

        // mock rs.getInt(columnIndex)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByNumber())) {
                is String -> value.toInt()
                else -> value
            }
        }.whenever(rs).getInt(anyInt())

        // mock rs.getLong(columnName)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByName())) {
                is String -> value.toLong()
                else -> value
            }
        }.whenever(rs).getLong(anyString())

        // mock rs.getLong(columnIndex)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByNumber())) {
                is String -> value.toLong()
                else -> value
            }
        }.whenever(rs).getLong(anyInt())

        // mock rs.getDouble(columnName)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByName())) {
                is String -> value.toDouble()
                else -> value
            }
        }.whenever(rs).getDouble(anyString())

        // mock rs.getDouble(columnIndex)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByNumber())) {
                is String -> value.toDouble()
                else -> value
            }
        }.whenever(rs).getDouble(anyInt())

        // mock rs.getBigDecimal(columnName)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByName())) {
                is String -> value.toBigDecimal()
                else -> value
            }
        }.whenever(rs).getBigDecimal(anyString())

        // mock rs.getBigDecimal(columnIndex)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByNumber())) {
                is String -> value.toBigDecimal()
                else -> value
            }
        }.whenever(rs).getBigDecimal(anyInt())

        // mock rs.getTimestamp(columnIndex)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByNumber())) {
                is Timestamp -> value
                is String -> Timestamp.valueOf(value)
                is Long -> Timestamp(value)
                else -> value
            }
        }.whenever(rs).getTimestamp(anyInt())

        // mock rs.getTimestamp(columnName)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByName())) {
                is Timestamp -> value
                is String -> Timestamp.valueOf(value)
                is Long -> Timestamp(value)
                else -> value
            }
        }.whenever(rs).getTimestamp(anyString())

        // mock rs.getDate(columnIndex)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByNumber())) {
                is Date -> value
                is String -> Date.valueOf(value)
                is Long -> Date(value)
                else -> value
            }
        }.whenever(rs).getDate(anyInt())

        // mock rs.getDate(columnName)
        lenient().doAnswer { invocation ->
            when (val value = answer(invocation.indexByName())) {
                is Date -> value
                is String -> Date.valueOf(value)
                is Long -> Date(value)
                else -> value
            }
        }.whenever(rs).getDate(anyString())

        // mock rs.getObject(columnName)
        lenient().doAnswer { invocation ->
            answer(invocation.indexByName())
        }.whenever(rs).getObject(anyString())

        // mock rs.getObject(columnIndex)
        lenient().doAnswer { invocation ->
            answer(invocation.indexByNumber())
        }.whenever(rs).getObject(anyInt())

        // mock rs.getObject(columnName, OffsetDateTime.class)
        lenient().doAnswer { invocation ->
            val columnIndex = invocation.indexByName()
            when {
                outOfRange(columnIndex) -> OffsetDateTime.of(4242, 4, 2, 4, 2, 4, 2, ZoneOffset.UTC)
                else -> data[rowIndex][columnIndex] as OffsetDateTime?
            }.also {
                wasNull = it == null
            }
        }.whenever(rs).getObject(anyString(), ArgumentMatchers.eq(OffsetDateTime::class.java))

        // mock rs.getObject(columnIndex, OffsetDateTime.class)
        lenient().doAnswer { invocation ->
            val columnIndex = invocation.indexByNumber()
            when {
                outOfRange(columnIndex) -> OffsetDateTime.of(4242, 4, 2, 4, 2, 4, 2, ZoneOffset.UTC)
                else -> data[rowIndex][columnIndex] as OffsetDateTime?
            }.also {
                wasNull = it == null
            }
        }.whenever(rs).getObject(anyInt(), ArgumentMatchers.eq(OffsetDateTime::class.java))

        val rsmd: ResultSetMetaData = mock()

        // mock rsmd.getColumnCount()
        lenient().doReturn(columnIndices.size).whenever(rsmd).columnCount

        // mock rs.findColumn(String)
        lenient().doAnswer { invocation ->
            val columnIndex = invocation.indexByName()
            if (columnIndex == Int.MAX_VALUE) throw SQLException("Invalid column name")
            columnIndex + 1
        }.whenever(rs).findColumn(anyString())

        // mock rsmd.getColumnName(int)
        lenient().doAnswer { invocation ->
            val columnIndex = invocation.indexByNumber()
            columnIndices.filter { columnIndex == it.value }.keys.firstOrNull()
        }.whenever(rsmd).getColumnName(anyInt())

        // mock rs.toString()
        lenient().doReturn(tag).whenever(rs).toString()

        // mock rsmd.getMetaData()
        lenient().doReturn(rsmd).whenever(rs).metaData

        // mock rs.getType()
        lenient().doReturn(ResultSet.TYPE_FORWARD_ONLY).whenever(rs).type

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
