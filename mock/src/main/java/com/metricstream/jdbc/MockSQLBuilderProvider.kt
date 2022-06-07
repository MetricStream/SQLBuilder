/*
 * Copyright © 2020-2022, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Instant
import java.time.OffsetDateTime
import java.util.Optional
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiFunction
import java.util.function.Supplier
import com.metricstream.jdbc.MockResultSet.Companion.THE_ANSWER_TO_THE_ULTIMATE_QUESTION
import com.metricstream.jdbc.SQLBuilder.Companion.resetDelegate
import com.metricstream.jdbc.SQLBuilder.Companion.setDelegate
import com.metricstream.jdbc.parser.SQLParser

private val logger = mu.KotlinLogging.logger {}

class MockSQLBuilderProvider @JvmOverloads constructor(
    private val generateSingleRowResultSet: Boolean = true,
    private var enforceTags: Boolean = true,
    private var parseSql: Boolean = true,
) : SQLBuilderProvider {

    init {
        reset()
    }

    fun enableSqlParsing() {
        parseSql = true
    }

    fun disableSqlParsing() {
        parseSql = false
    }

    fun enableTagEnforcement() {
        enforceTags = true
    }

    fun disableTagEnforcement() {
        enforceTags = false
    }

    private fun validate(sqlBuilder: SQLBuilder) {
        if (parseSql) {
            val parser = SQLParser(sqlBuilder.toSQL())
            if (parser.isInvalid()) {
                val issues = parser.showIssues()
                throw SQLException("Invalid SQL:\n$issues")
            }
        }
        logger.debug { sqlBuilder }
    }

    override fun getResultSet(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        wrapConnection: Boolean
    ): ResultSet {
        invocations.getResultSet++
        validate(sqlBuilder)
        return getRs()
    }

    override fun getInt(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Int
    ): Int {
        invocations.getInt++
        validate(sqlBuilder)
        if (intByColumnIndex != null) {
            return intByColumnIndex!!.apply(columnNumber, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getInt(columnNumber) else defaultValue
    }

    @Throws(SQLException::class)
    override fun getInt(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Int
    ): Int {
        invocations.getInt++
        validate(sqlBuilder)
        if (intByColumnLabel != null) {
            return intByColumnLabel!!.apply(columnName, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getInt(columnName) else defaultValue
    }

    override fun getLong(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Long
    ): Long {
        invocations.getLong++
        validate(sqlBuilder)
        if (longByColumnIndex != null) {
            return longByColumnIndex!!.apply(columnNumber, defaultValue)
        }
        val rs = getRs()
        val next = rs.next()
        return if (next) rs.getLong(columnNumber) else defaultValue
    }

    override fun getLong(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Long
    ): Long {
        invocations.getLong++
        validate(sqlBuilder)
        if (longByColumnLabel != null) {
            return longByColumnLabel!!.apply(columnName, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getLong(columnName) else defaultValue
    }

    override fun getDouble(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Double
    ): Double {
        invocations.getDouble++
        validate(sqlBuilder)
        if (doubleByColumnIndex != null) {
            return doubleByColumnIndex!!.apply(columnNumber, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getDouble(columnNumber) else defaultValue
    }

    override fun getDouble(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Double
    ): Double {
        invocations.getDouble++
        validate(sqlBuilder)
        if (doubleByColumnLabel != null) {
            return doubleByColumnLabel!!.apply(columnName, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getDouble(columnName) else defaultValue
    }

    override fun getString(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: String?
    ): String? {
        invocations.getString++
        validate(sqlBuilder)
        if (stringByColumnIndex != null) {
            return stringByColumnIndex!!.apply(columnNumber, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getString(columnNumber) else defaultValue
    }

    override fun getString(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: String?
    ): String? {
        invocations.getString++
        validate(sqlBuilder)
        if (stringByColumnLabel != null) {
            return stringByColumnLabel!!.apply(columnName, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getString(columnName) else defaultValue
    }

    override fun getBigDecimal(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: BigDecimal?
    ): BigDecimal? {
        invocations.getBigDecimal++
        validate(sqlBuilder)
        if (bigDecimalByColumnIndex != null) {
            return bigDecimalByColumnIndex!!.apply(columnNumber, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getBigDecimal(columnNumber) else defaultValue
    }

    override fun getBigDecimal(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: BigDecimal?
    ): BigDecimal? {
        invocations.getBigDecimal++
        validate(sqlBuilder)
        if (bigDecimalByColumnLabel != null) {
            return bigDecimalByColumnLabel!!.apply(columnName, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getBigDecimal(columnName) else defaultValue
    }

    override fun getObject(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Any?
    ): Any? {
        invocations.getObject++
        validate(sqlBuilder)
        if (objectByColumnIndex != null) {
            return objectByColumnIndex!!.apply(columnNumber, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getObject(columnNumber) else defaultValue
    }

    override fun getObject(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Any?
    ): Any? {
        invocations.getObject++
        validate(sqlBuilder)
        if (objectByColumnLabel != null) {
            return objectByColumnLabel!!.apply(columnName, defaultValue)
        }
        val rs = getRs()
        return if (rs.next()) rs.getObject(columnName) else defaultValue
    }

    override fun getDateTime(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: OffsetDateTime?
    ): OffsetDateTime? {
        invocations.getDateTime++
        validate(sqlBuilder)
        val rs = getRs()
        return if (rs.next()) rs.getObject(columnNumber, OffsetDateTime::class.java) else defaultValue
    }

    override fun getDateTime(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: OffsetDateTime?
    ): OffsetDateTime? {
        invocations.getDateTime++
        validate(sqlBuilder)
        val rs = getRs()
        return if (rs.next()) rs.getObject(columnName, OffsetDateTime::class.java) else defaultValue
    }

    override fun getInstant(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Instant?
    ): Instant? {
        invocations.getInstant++
        validate(sqlBuilder)
        val rs = getRs()
        return if (rs.next()) rs.getObject(columnNumber, OffsetDateTime::class.java).toInstant() else defaultValue
    }

    override fun getInstant(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Instant?
    ): Instant? {
        invocations.getInstant++
        validate(sqlBuilder)
        val rs = getRs()
        return if (rs.next()) rs.getObject(columnName, OffsetDateTime::class.java).toInstant() else defaultValue
    }

    override fun getTimestamp(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Timestamp?
    ): Timestamp? {
        invocations.getTimestamp++
        validate(sqlBuilder)
        val rs = getRs()
        return if (rs.next()) rs.getTimestamp(columnNumber) else defaultValue
    }

    override fun getTimestamp(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Timestamp?
    ): Timestamp? {
        invocations.getTimestamp++
        validate(sqlBuilder)
        val rs = getRs()
        return if (rs.next()) rs.getTimestamp(columnName) else defaultValue
    }

    override fun getDate(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Date?
    ): Date? {
        invocations.getDate++
        validate(sqlBuilder)
        val rs = getRs()
        return if (rs.next()) rs.getDate(columnNumber) else defaultValue
    }

    override fun getDate(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Date?
    ): Date? {
        invocations.getDate++
        validate(sqlBuilder)
        val rs = getRs()
        return if (rs.next()) rs.getDate(columnName) else defaultValue
    }

    override fun execute(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        limit: Int?,
        batchSize: Int
    ): Int {
        invocations.execute++
        validate(sqlBuilder)
        checkTag(executeTag)
        return executeSupplier.get()
    }

    override fun execute(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        vararg keyColumns: String
    ): ResultSet {
        invocations.execute++
        validate(sqlBuilder)
        return getRs()
    }

    override fun <T> getList(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<T>
    ): List<T> {
        invocations.getList++
        validate(sqlBuilder)
        return getList(getRs(), rowMapper, false)
    }

    override fun <T> getListWithNull(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<T?>
    ): List<T?> {
        invocations.getList++
        validate(sqlBuilder)
        return getList(getRs(), rowMapper, true)
    }

    override fun <K, V> getMap(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<Map.Entry<K, V?>>,
        withNull: Boolean
    ): Map<K, V?> {
        invocations.getMap++
        validate(sqlBuilder)
        return getMap(getRs(), rowMapper, withNull)
    }

    override fun <T : Any> getSingle(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<T?>
    ): Optional<T> {
        invocations.getSingle++
        validate(sqlBuilder)
        val rs = getRs()
        return Optional.ofNullable(if (rs.next()) rowMapper.map(rs) else null)
    }

    override fun <T> getSingle(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<T?>,
        defaultValue: T?
    ): T? {
        invocations.getSingle++
        validate(sqlBuilder)
        val rs = getRs()
        return if (rs.next()) rowMapper.map(rs) else defaultValue
    }

    @Throws(SQLException::class)
    private fun getRs(): ResultSet {
        invocations.getRs++
        var rs = mockResultSets.poll()
        when {
            rs != null -> checkTag(rs.toString())
            generateSingleRowResultSet -> rs = MockResultSet.create(
                "",
                THE_ANSWER_TO_THE_ULTIMATE_QUESTION.toString(),
                withLabels = false,
                generated = true
            )
            else -> rs = MockResultSet.empty("")
        }
        logger.debug { "Using mock ResultSet $rs" }
        return rs
    }

    private fun candidate(stackTraceElement: StackTraceElement): String? {
        val declaringClass = stackTraceElement.className
        if (declaringClass in internalClasses) {
            return null
        }
        if (declaringClass.startsWith("org.junit.") ||
            declaringClass.startsWith("jdk.internal.") ||
            declaringClass.startsWith("java.lang.") ||
            declaringClass.startsWith("org.codehaus.groovy.")) {
            return null
        }

        var methodName = stackTraceElement.methodName.removeSuffix("\$mock")
        if (methodName == "doCall") {
            // undo Groovy 2.4 closure method name mangling
            groovyClosure.matchEntire(declaringClass)?.groupValues?.get(1)?.let { methodName = it }
        }
        if (methodName == "catchThrowable" ||
            methodName == "isThrownBy" ||
            methodName.startsWith("lambda$") ||
            methodName.contains(kotlinLambda)) {
            return null
        }

        return methodName
    }

    private fun checkTag(tag: String) {
        if (enforceTags && tag.isNotEmpty() && !tag.startsWith("MockResultSet#")) {
            val stackTrace = Throwable().stackTrace
            for (stackTraceElement in stackTrace) {
                val methodName = candidate(stackTraceElement)
                if (methodName != null) {
                    // We should accept all possible Java or Kotlin method names here, but this is tricky esp. for
                    // Kotlin which e.g. allows whitespace in identifiers if they are enclosed in ``. We thus simply
                    // use anything before the first : or #.
                    check(methodName == tag.split(":", "#").first()) {
                        "Trying to use mock data tagged with '$tag' in method '$methodName' of class ${stackTraceElement.className}"
                    }
                    break
                }
            }
        }
    }

    override val connectionProvider: ConnectionProvider = connectionProviderImpl

    companion object {
        private val mockResultSets: Queue<ResultSet> = ConcurrentLinkedQueue()
        private var intByColumnIndex: BiFunction<Int, Int, Int>? = null
        private var intByColumnLabel: BiFunction<String, Int, Int>? = null
        private var longByColumnIndex: BiFunction<Int, Long, Long>? = null
        private var longByColumnLabel: BiFunction<String, Long, Long>? = null
        private var doubleByColumnIndex: BiFunction<Int, Double, Double>? = null
        private var doubleByColumnLabel: BiFunction<String, Double, Double>? = null
        private var stringByColumnIndex: BiFunction<Int, String?, String?>? = null
        private var stringByColumnLabel: BiFunction<String, String?, String?>? = null
        private var bigDecimalByColumnIndex: BiFunction<Int, BigDecimal?, BigDecimal?>? = null
        private var bigDecimalByColumnLabel: BiFunction<String, BigDecimal?, BigDecimal?>? = null
        private var objectByColumnIndex: BiFunction<Int, Any?, Any?>? = null
        private var objectByColumnLabel: BiFunction<String, Any?, Any?>? = null
        private var executeSupplier: Supplier<Int> = Supplier { THE_ANSWER_TO_THE_ULTIMATE_QUESTION }
        private var executeTag: String = ""
        lateinit var invocations: Invocations

        private val groovyClosure = Regex(""".+\..+\${"$"}_(.+)_closure\d*""")
        private val kotlinLambda = Regex("""\${"$"}lambda-\d+$""")

        private val internalClasses = setOf(
            "com.metricstream.jdbc.JdbcSQLBuilderProvider",
            "com.metricstream.jdbc.LongString",
            "com.metricstream.jdbc.QueryParams",
            "com.metricstream.jdbc.SQLBuilder",
            "com.metricstream.jdbc.SQLBuilderProvider",
            "com.metricstream.jdbc.SQLBuilderProvider\$DefaultImpls",
            "com.metricstream.jdbc.Invocations",
            "com.metricstream.jdbc.MockResultSet",
            "com.metricstream.jdbc.MockResultSetMetaData",
            "com.metricstream.jdbc.MockSQLBuilderExtension",
            "com.metricstream.jdbc.MockSQLBuilderProvider",
        )

        @JvmStatic
        fun enable() {
            setDelegate(MockSQLBuilderProvider())
        }

        @JvmStatic
        fun disable() {
            resetDelegate()
        }

        @JvmStatic
        fun addResultSet(rs: ResultSet) {
            mockResultSets.add(rs)
        }

        @Throws(SQLException::class)
        @JvmStatic
        @Deprecated("Use MockResultSet.add", replaceWith = ReplaceWith("MockResultSet.add(tag, data)"))
        fun addResultSet(tag: String, data: Array<Array<Any?>>) {
            mockResultSets.add(MockResultSet.create(tag, data))
        }

        @Throws(SQLException::class)
        @JvmStatic
        @Deprecated("Use MockResultSet.add", replaceWith = ReplaceWith("MockResultSet.add(tag, labels, csvs)"))
        fun addResultSet(tag: String, labels: String, vararg csvs: String) {
            mockResultSets.add(MockResultSet.create(tag, labels, *csvs))
        }

        @Throws(SQLException::class)
        @JvmStatic
        @Deprecated("Use MockResultSet.add", replaceWith = ReplaceWith("MockResultSet.add(tag, csv, withLabels)"))
        fun addResultSet(tag: String, csv: String, withLabels: Boolean) {
            mockResultSets.add(MockResultSet.create(tag, csv, withLabels))
        }

        @Throws(SQLException::class)
        @JvmStatic
        @Deprecated("Use MockResultSet.add", replaceWith = ReplaceWith("MockResultSet.add(tag, csv, false)"))
        fun addResultSet(tag: String, csv: String) {
            mockResultSets.add(MockResultSet.create(tag, csv, false))
        }

        @Throws(SQLException::class)
        @JvmOverloads
        @JvmStatic
        @Deprecated("Use MockResultSet.add", replaceWith = ReplaceWith("MockResultSet.add(tag, csv, withLabels)"))
        fun addResultSet(tag: String, csv: InputStream, withLabels: Boolean = true) {
            mockResultSets.add(MockResultSet.create(tag, csv, withLabels))
        }

        @JvmStatic
        fun setIntByColumnIndex(intByColumnIndex: BiFunction<Int, Int, Int>?) {
            Companion.intByColumnIndex = intByColumnIndex
        }

        @JvmStatic
        fun setIntByColumnLabel(intByColumnLabel: BiFunction<String, Int, Int>?) {
            Companion.intByColumnLabel = intByColumnLabel
        }

        @JvmStatic
        fun setLongByColumnIndex(longByColumnIndex: BiFunction<Int, Long, Long>?) {
            Companion.longByColumnIndex = longByColumnIndex
        }

        @JvmStatic
        fun setLongByColumnLabel(longByColumnLabel: BiFunction<String, Long, Long>?) {
            Companion.longByColumnLabel = longByColumnLabel
        }

        @JvmStatic
        fun setStringByColumnIndex(stringByColumnIndex: BiFunction<Int, String?, String?>?) {
            Companion.stringByColumnIndex = stringByColumnIndex
        }

        @JvmStatic
        fun setStringByColumnLabel(stringByColumnLabel: BiFunction<String, String?, String?>?) {
            Companion.stringByColumnLabel = stringByColumnLabel
        }

        @JvmStatic
        fun setBigDecimalByColumnIndex(bigDecimalByColumnIndex: BiFunction<Int, BigDecimal?, BigDecimal?>?) {
            Companion.bigDecimalByColumnIndex = bigDecimalByColumnIndex
        }

        @JvmStatic
        fun setBigDecimalByColumnLabel(bigDecimalByColumnLabel: BiFunction<String, BigDecimal?, BigDecimal?>?) {
            Companion.bigDecimalByColumnLabel = bigDecimalByColumnLabel
        }

        @JvmStatic
        fun setObjectByColumnIndex(objectByColumnIndex: BiFunction<Int, Any?, Any?>?) {
            Companion.objectByColumnIndex = objectByColumnIndex
        }

        @JvmStatic
        fun setObjectByColumnLabel(objectByColumnLabel: BiFunction<String, Any?, Any?>?) {
            Companion.objectByColumnLabel = objectByColumnLabel
        }

        @JvmStatic
        fun setExecute(tag: String, supplier: Supplier<Int>) {
            executeTag = tag
            executeSupplier = supplier
        }

        @JvmStatic
        fun setExecute(tag: String, value: Int) {
            executeTag = tag
            executeSupplier = Supplier { value }
        }

        @JvmStatic
        fun setExecute(tag: String, vararg values: Int) {
            val count = AtomicInteger()
            executeTag = tag
            executeSupplier = Supplier {
                if (count.get() < values.size) {
                    values[count.getAndIncrement()]
                } else {
                    THE_ANSWER_TO_THE_ULTIMATE_QUESTION
                }
            }
        }

        @JvmStatic
        fun reset() {
            if (mockResultSets.isNotEmpty()) {
                logger.warn { "Unused mock ResultSet objects: ${mockResultSets.map { obj: ResultSet -> obj.toString() }}" }
                mockResultSets.clear()
            }
            intByColumnIndex = null
            intByColumnLabel = null
            longByColumnIndex = null
            longByColumnLabel = null
            stringByColumnIndex = null
            stringByColumnLabel = null
            bigDecimalByColumnIndex = null
            bigDecimalByColumnLabel = null
            objectByColumnIndex = null
            objectByColumnLabel = null
            setExecute("", THE_ANSWER_TO_THE_ULTIMATE_QUESTION)
            invocations = Invocations()
        }

        private val connectionImpl = MockConnection()

        private val connectionProviderImpl = object : ConnectionProvider {
            override fun getConnection(): Connection {
                return connectionImpl
            }
        }
    }
}
