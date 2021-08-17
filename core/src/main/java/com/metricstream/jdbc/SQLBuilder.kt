/*
 * Copyright © 2018-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc

import java.sql.ResultSet
import org.apache.commons.codec.digest.DigestUtils
import java.util.StringJoiner
import java.lang.StringBuffer
import kotlin.Throws
import java.sql.SQLException
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.time.Instant
import java.lang.IllegalStateException
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.util.AbstractMap.SimpleImmutableEntry
import kotlin.jvm.JvmOverloads
import java.lang.AutoCloseable
import java.lang.Exception
import java.lang.IllegalArgumentException
import java.lang.StringBuilder
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.Date
import java.sql.Statement
import java.sql.Timestamp
import java.util.Optional
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.slf4j.LoggerFactory

/**
 * Wrapper class around PreparedStatement
 *
 * This class helps to create parameterized SQL statements.  It allows to
 * aggregate the SQL statements in multiple steps.  Once the parameterized
 * statement is ready, it can be either converted into a ResultSet or it can
 * directly return values from the rows of the ResultSet.
 *
 * Parameters can be primitives (int, long, double), their object equivalents
 * (Integer, Long, Double), String, or collections.  When using a collection as
 * parameter, the corresponding placeholder (i.e. the ?) is automatically expanded
 * to have as many placeholders as the size of the collection.
 *
 * <pre>
 * `SQLBuilder sb = new SQLBuilder("select a from b");
 * List<String> dList = new ArrayList<>(); dList.add("a"); dList.add("b");
 * sb.append("where c=? or d in (?)", 42, dList);
 * int a = sb.getInt(connection, 1, -1);
 * </pre>
 */
class SQLBuilder {
    internal val statement = StringBuilder()
    internal val arguments: MutableList<Any?> = mutableListOf()
    private val names: MutableSet<String> = mutableSetOf()
    private val singleValuedNames: MutableMap<String, String> = mutableMapOf()
    private val multiValuedNames: MutableMap<String, List<String>> = mutableMapOf()
    private var delimiter = ""
    @JvmField
    var resultSetType = ResultSet.TYPE_FORWARD_ONLY
    @JvmField
    var fetchSize = -1
    @JvmField
    var maxRows = -1

    internal enum class Mode { APPLY_BINDINGS, EXPAND_AND_APPLY, EXPAND_AND_SQL, EXPAND_AND_STRING }

    /**
     * Simple wrapper class for "sensitive" data like full names, email addresses or SSNs.
     * When logging a SQLBuilder object, we will print the hash of sensitive values instead of the value itself
     * <pre>`String secret = "oops!";
     * String bad = new SQLBuilder("select name from user where secret=?", secret).toString();
     * // produces "select name from user where secret=?; args = [oops!]"
     * String good = new SQLBuilder("select name from user where secret=?", SQLBuilder.mask(secret)).toString();
     * // produces "select name from user where secret=?; args = [__masked__:982c0381c279d139fd221fce974916e7]"
     * `</pre>
     */
    class Masked(val data: Any?) {
        override fun toString(): String {
            return when (data) {
                null -> "null"
                "" -> ""
                else -> "__masked__:" + DigestUtils.md5Hex(data.toString())
            }
        }
    }

    /**
     * Creates a new SQBuilder object. The number of ? in the sql parameter
     * must be identical to the number of args
     * @param sql The initial SQL statement fragment
     * @param args The parameters for this fragment
     */
    constructor(sql: String, vararg args: Any?) {
        append(sql, *args)
        delimiter = " "
    }

    /**
     * Generates a shallow clone of a SQLBuilder object
     * @param sqlBuilder A SQLBuilder object
     */
    constructor(sqlBuilder: SQLBuilder) {
        resultSetType = sqlBuilder.resultSetType
        fetchSize = sqlBuilder.fetchSize
        maxRows = sqlBuilder.maxRows
        append(sqlBuilder)
        delimiter = " "
    }

    /**
     * Checks if this SQLBuilder object contains anything
     * @return true if it has no statement and no arguments, false otherwise
     */
    val isEmpty: Boolean
        get() = statement.isEmpty() && arguments.isEmpty()

    /**
     * Checks if this SQLBuilder object contains anything
     * @return true if it has q statement or arguments, false otherwise
     */
    val isNotEmpty: Boolean
        get() = statement.isNotEmpty() || arguments.isNotEmpty()

    /**
     * Adds a binding for a name
     * @param name the name used in the statement (e.g. ${view} or #{view})
     * @param value the value for this name.  This must be a valid table, view, or column name.  The value
     * will be quoted if necessary
     * @return the SQLBuilder object
     */
    fun bind(name: String, value: String): SQLBuilder {
        addName(name)
        singleValuedNames[name] = value
        return this
    }

    /**
     * Adds a binding for a name
     * @param name the name used in the statement (e.g. ${columns} or #{columns})
     * @param values a list of values for this name.  Each value must be a valid table, view, or column name.  The values
     * will be quoted if necessary
     * @return the SQLBuilder object
     */
    fun bind(name: String, values: List<String>): SQLBuilder {
        addName(name)
        multiValuedNames[name] = values
        return this
    }

    /**
     * Adds one or more single-valued binding.  This is a shorthand for calling bind for every entry of the map.
     * @param bindings a map of name->value pairs
     * @return the SQLBuilder object
     */
    fun bind(bindings: Map<String, String>): SQLBuilder {
        addNames(bindings.keys)
        singleValuedNames.putAll(bindings)
        return this
    }

    /**
     * This applies all the bindings to the statement.
     * @return the SQLBuilder object
     */
    fun applyBindings(): SQLBuilder {
        interpolate(Mode.APPLY_BINDINGS)
        return this
    }

    /**
     * Append a parameterized SQL fragment.  The original fragment and the new
     * fragment will be separated by a single space.
     * @param sql The SQL statement fragment
     * @param args The parameters for this fragment
     * @return The SQLBuilder object
     */
    fun append(sql: String, vararg args: Any?): SQLBuilder {
        statement.append(delimiter).append(sql)
        if (args.isNotEmpty()) {
            arguments.addAll(listOf(*args))
        }
        return this
    }

    /**
     * Appends another SQLBuilder object.  This appends both the fragment and shallow
     * copies of all parameters.  The original fragment and the new fragment
     * will be separated from the copies by a single space.
     * @param sqlBuilder The SQLBuilder object
     * @return The SQLBuilder object
     */
    fun append(sqlBuilder: SQLBuilder): SQLBuilder {
        addNames(sqlBuilder.names)
        singleValuedNames.putAll(sqlBuilder.singleValuedNames)
        multiValuedNames.putAll(sqlBuilder.multiValuedNames)
        arguments.addAll(sqlBuilder.arguments)
        statement.append(delimiter).append(sqlBuilder.statement)
        return this
    }

    /**
     * Wraps the SQL statement fragment into () and prepends before.  A typical
     * use case is <pre></pre>sb.wrap("select count(*) from");
     * @param before The string which will be prepended
     * @return the SQLBuilder object
     */
    fun wrap(before: String): SQLBuilder {
        return wrap("$before (", ")")
    }

    /**
     * Wraps the SQL statement fragment in before and after.  Unlike append,
     * there is no additional whitespace inserted between the original SQL
     * statement fragment and the arguments.
     * @param before The string which will be prepended
     * @param after The string which will be appended
     * @return the SQLBuilder object
     */
    fun wrap(before: String, after: String): SQLBuilder {
        statement.insert(0, before).append(after)
        return this
    }

    private fun addNames(names: Collection<String>) {
        for (n in names) {
            addName(n)
        }
    }

    private fun addName(name: String) {
        require(name.matches(Regex("""\w+"""))) { "The binding name \"$name\" must only consist of word characters [a-zA-Z_0-9]" }
        require(names.add(name)) { "The binding name \"$name\" must be unique" }
    }

    internal fun interpolate(mode: Mode, expanded: MutableList<Any?> = mutableListOf()): String {
        val expandedStatement = StringBuilder(statement)
        if (arguments.isNotEmpty() && mode != Mode.APPLY_BINDINGS) {
            // We must not expand in APPLY_BINDINGS mode because that can be called multiple times
            // and would thus expand the placeholders multiple times
            var sqlPos = 0
            for (arg in arguments) {
                sqlPos = expandedStatement.indexOf("?", sqlPos) + 1
                if (sqlPos == 0) {
                    // We ran out of placeholders (i.e. we have extra parameters).
                    // We do not consider that as a bug (though one could argue this
                    // should result in a warning).
                    break
                }
                if (arg is Collection<*>) {
                    val col = arg as Collection<Any?>
                    val length = col.size
                    if (length == 0) {
                        // TODO: This should throw an IllegalArgumentException, but we first have to check what that does to all the callers
                        throw SQLException("Collection parameters must contain at least one element")
                    }
                    // The statement already contains one "?", therefore we only add length - 1 additional placeholders
                    val additionalPlaceHolders = ",?".repeat(length - 1)
                    expandedStatement.insert(sqlPos, additionalPlaceHolders)
                    // move sqlPos beyond the inserted ",?"
                    sqlPos += additionalPlaceHolders.length
                    expanded.addAll(col)
                } else {
                    expanded.add(arg)
                }
            }
        }

        val quoted = mutableMapOf<String, String>()
        val regexp = StringJoiner("""|""", """[:$]\{(""", """)\}""")
        for ((key, value) in singleValuedNames) {
            quoted[key] = nameQuote(value)
            regexp.add(key)
        }
        for ((key, values) in multiValuedNames) {
            quoted[key] = values.joinToString(", ") { nameQuote(it) }
            regexp.add(key)
        }

        val sb = StringBuffer()
        if (quoted.isNotEmpty()) {
            val p = Pattern.compile(regexp.toString())
            val m = p.matcher(expandedStatement.toString())
            while (m.find()) {
                m.appendReplacement(sb, Matcher.quoteReplacement(quoted[m.group(1)]))
            }
            m.appendTail(sb)
        } else {
            sb.append(expandedStatement)
        }

        return when (mode) {
            Mode.APPLY_BINDINGS, Mode.EXPAND_AND_APPLY -> {
                names.clear()
                singleValuedNames.clear()
                multiValuedNames.clear()
                statement.setLength(0)
                statement.append(sb)
                sb.setLength(0)
                logger.debug("{}; args={}", statement, expanded)
                ""
            }
            Mode.EXPAND_AND_STRING -> {
                sb.append("; args=").append(expanded)
                sb.toString()
            }
            Mode.EXPAND_AND_SQL -> {
                sb.toString()
            }
        }
    }

    /**
     * This changes the ResultSet type from TYPE_FORWARD_ONLY to TYPE_SCROLL_INSENSITIVE
     * @return The SQLBuilder object
     */
    fun randomAccess(): SQLBuilder {
        resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE
        return this
    }

    /**
     * This changes the fetch size used for ResultSets.
     * @param fetchSize The new fetchSize. A value of -1 means that no fetch size is applied to the statement and that
     * the JDBC driver will use some default value
     * @return the SQLBuilder object
     */
    fun withFetchSize(fetchSize: Int): SQLBuilder {
        this.fetchSize = fetchSize
        return this
    }

    /**
     * This changes the fetch size used for ResultSets.
     * @param maxRows The maximum number of rows to be returned. A value of -1 means that no maximum is applied to the statement and that
     * the JDBC driver will return all rows.
     * @return the SQLBuilder object
     */
    fun withMaxRows(maxRows: Int): SQLBuilder {
        this.maxRows = maxRows
        return this
    }

    /**
     * Returns a ResultSet object created from a PreparedStatement object created using
     * the SQL statement and the parameters.  The PreparedStatement object
     * and the Connection object will be automatically closed when the ResultSet object is closed.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @return The ResultSet object
     * @throws SQLException the exception thrown when generating the ResultSet object
     */
    @Throws(SQLException::class)
    @JvmOverloads
    fun getResultSet(connection: Connection, wrapConnection: Boolean = false): ResultSet {
        return delegate.getResultSet(this, connection, wrapConnection)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getInt(connection: Connection, columnNumber: Int, defaultValue: Int): Int {
        return delegate.getInt(this, connection, columnNumber, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getInt(connection: Connection, columnName: String, defaultValue: Int): Int {
        return delegate.getInt(this, connection, columnName, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getLong(connection: Connection, columnNumber: Int, defaultValue: Long): Long {
        return delegate.getLong(this, connection, columnNumber, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getLong(connection: Connection, columnName: String, defaultValue: Long): Long {
        return delegate.getLong(this, connection, columnName, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getString(connection: Connection, columnNumber: Int, defaultValue: String?): String? {
        return delegate.getString(this, connection, columnNumber, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getString(connection: Connection, columnName: String, defaultValue: String?): String? {
        return delegate.getString(this, connection, columnName, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getBigDecimal(connection: Connection, columnNumber: Int, defaultValue: BigDecimal?): BigDecimal? {
        return delegate.getBigDecimal(this, connection, columnNumber, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getBigDecimal(connection: Connection, columnName: String, defaultValue: BigDecimal?): BigDecimal? {
        return delegate.getBigDecimal(this, connection, columnName, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getObject(connection: Connection, columnNumber: Int, defaultValue: Any?): Any? {
        return delegate.getObject(this, connection, columnNumber, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getObject(connection: Connection, columnName: String, defaultValue: Any?): Any? {
        return delegate.getObject(this, connection, columnName, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getDateTime(connection: Connection, columnNumber: Int, defaultValue: OffsetDateTime?): OffsetDateTime? {
        return delegate.getDateTime(this, connection, columnNumber, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getDateTime(connection: Connection, columnName: String, defaultValue: OffsetDateTime?): OffsetDateTime? {
        return delegate.getDateTime(this, connection, columnName, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getInstant(connection: Connection, columnNumber: Int, defaultValue: Instant?): Instant? {
        return delegate.getInstant(this, connection, columnNumber, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getInstant(connection: Connection, columnName: String, defaultValue: Instant?): Instant? {
        return delegate.getInstant(this, connection, columnName, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getTimestamp(connection: Connection, columnNumber: Int, defaultValue: Timestamp?): Timestamp? {
        return delegate.getTimestamp(this, connection, columnNumber, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getTimestamp(connection: Connection, columnName: String, defaultValue: Timestamp?): Timestamp? {
        return delegate.getTimestamp(this, connection, columnName, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getDate(connection: Connection, columnNumber: Int, defaultValue: Date?): Date? {
        return delegate.getDate(this, connection, columnNumber, defaultValue)
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun getDate(connection: Connection, columnName: String, defaultValue: Date?): Date? {
        return delegate.getDate(this, connection, columnName, defaultValue)
    }

    /**
     * Executes the SQL statement.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @return The result of executeUpdate of that statement
     * @throws SQLException the exception thrown when executing the query
     */
    @Throws(SQLException::class)
    fun execute(connection: Connection): Int {
        return delegate.execute(this, connection)
    }

    /**
     * Executes the SQL statement.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param keyColumns column names from the underlying table for which the inserted values will be returned.  Note that these names
     * not necessarily have to be part of the columns into which the builder explicitly inserts values.
     * @return The result of executeUpdate of that statement
     * @throws SQLException the exception thrown when executing the query
     */
    @Throws(SQLException::class)
    fun execute(connection: Connection, vararg keyColumns: String): ResultSet {
        return delegate.execute(this, connection, *keyColumns)
    }

    fun interface RowMapper<T> {
        @Throws(SQLException::class)
        fun map(rs: ResultSet): T?
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called per row to produce a matching list item.
     * @param withNull If false, null values returned from the lambda are ignored.  Otherwise they
     * are added to the returned list
     * @return The list of generated items
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    @JvmOverloads
    fun <T> getList(connection: Connection, rowMapper: RowMapper<T?>, withNull: Boolean = false): List<T?> {
        return delegate.getList(this, connection, rowMapper, withNull)
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called per row to produce a map entry. Null values returned from the mapper
     * lambda are ignored. Duplicate keys result in overwriting the previous value
     * @param withNull If false, null values returned from the lambda are ignored.  Otherwise they
     * are added to the returned map
     * @return The list of generated items
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class, IllegalStateException::class)
    @JvmOverloads
    fun <K, V> getMap(connection: Connection, rowMapper: RowMapper<Map.Entry<K, V?>>, withNull: Boolean = false): Map<K, V?> {
        return delegate.getMap(this, connection, rowMapper, withNull)
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called one the first row to produce a matching item.
     * @return the Optional containing the item returned from the mapping lambda, if any
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun <T> getSingle(connection: Connection, rowMapper: RowMapper<T?>): Optional<T> {
        return delegate.getSingle(this, connection, rowMapper)
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called one the first row to produce a matching item.
     * @return the item returned from the mapping lambda, or the defaultValue if no row was returned
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    @Throws(SQLException::class)
    fun <T> getSingle(connection: Connection, rowMapper: RowMapper<T?>, defaultValue: T?): T? {
        return delegate.getSingle(this, connection, rowMapper, defaultValue)
    }

    // We need to close the implicitly created Statement from the getResultSet
    // method below.  Instead of asking the caller to remember this, we wrap the
    // ResultSet and do that for the caller.  This also allow to use
    // getResultSet in try expressions
    private class WrappedResultSet(private var rs: ResultSet?, private val scope: Scope) : InvocationHandler {
        enum class Scope {
            ResultSet, Statement, Connection
        }

        @Throws(Throwable::class)
        override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
            // Warning: we have to go through the code below even is
            // rs.isClosed() is true because we still would need to close the
            // statement and connection.  Also, Oracle's ResultSet.next()
            // implicitly closes the ResultSet when it returns false (bypassing
            // this proxy method).
            if (rs == null) {
                return null
            }

            if (method.name == "close") {
                val stmt: Statement?
                var conn: Connection? = null
                when (scope) {
                    Scope.Connection -> {
                        stmt = rs!!.statement
                        if (stmt != null) {
                            conn = stmt.connection
                        }
                    }
                    Scope.Statement -> stmt = rs!!.statement
                    else -> stmt = null
                }
                close(rs, stmt, conn)
                rs = null
                return null
            }

            return try {
                if (args != null) {
                    method.invoke(rs, *args)
                } else {
                    method.invoke(rs)
                }
            } catch (e: InvocationTargetException) {
                throw e.cause ?: e
            }
        }
    }

    /**
     * Returns a String representation for logging purposes. This will contain
     * both the SQL statement fragment and the parameters.
     * @return A String representation
     */
    override fun toString(): String {
        return interpolate(Mode.EXPAND_AND_STRING)
    }

    /**
     * Returns a String representation. This will contain
     * only the SQL statement fragment and not the parameters.
     * @return A String representation
     */
    fun toSQL(): String {
        return interpolate(Mode.EXPAND_AND_SQL)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SQLBuilder::class.java)
        private val jdbcProvider: SQLBuilderProvider = JdbcSQLBuilderProvider()
        private var delegate = jdbcProvider
        @JvmStatic
        fun setDelegate(delegate: SQLBuilderProvider) {
            Companion.delegate = delegate
        }

        @JvmStatic
        fun resetDelegate() {
            delegate = jdbcProvider
        }

        @JvmStatic
        fun mask(data: Any?): Masked {
            return Masked(data)
        }

        @JvmStatic
        fun <K, V> entry(key: K, value: V): Map.Entry<K, V> {
            return SimpleImmutableEntry(key, value)
        }

        /**
         * @param sql A query template to be used with QueryParams
         * @param params the values for the query template
         * @return the SQLBuilder generated from evaluating the template
         */
        @Deprecated("use fromNumberedParameters", ReplaceWith("fromNumberedParameters(sql, params)"))
        fun fromNamedParams(sql: String, params: QueryParams): SQLBuilder {
            return fromNumberedParameters(sql, params)
        }

        /**
         * @param sql A query template to be used with QueryParams
         * @param params the values for the query template
         * @return the SQLBuilder generated from evaluating the template
         */
        @JvmStatic
        fun fromNumberedParameters(sql: String, params: QueryParams): SQLBuilder {
            val paramNames = params.paramNames
            if (paramNames.isEmpty()) {
                return SQLBuilder(sql)
            }
            val names = paramNames.joinToString("|") { """:\Q$it\E\b""" }
            val p = Pattern.compile(names)
            val args: MutableList<Any?> = mutableListOf()
            // To avoid replacement within quotes, split the string with quote
            // replace with alternative tokens and join after replacement logic from
            // NpiUtil.getTokens
            val tokens = sql.split("'")
            val queryBuilder = StringBuilder()
            for (i in tokens.indices) {
                if (i % 2 == 0) {
                    val m = p.matcher(tokens[i])
                    val sb = StringBuffer()
                    while (m.find()) {
                        // To check if parameter expects multiple values.  We cannot
                        // depend on parameter metadata since existing code depends
                        // on IN clause as part of the query and split the value
                        // considering comma as delimiter accordingly e.g. `select
                        // col1 from tab1 where col2 in(:1)`.  As part of parameter
                        // metadata update, parameter might not be chosen as multi,
                        // however it still works with existing code.
                        val subStr = tokens[i].substring(0, m.start())
                        val isMulti = subStr.matches(Regex("""(?is).*\bin\s*\(\s*"""))
                        val dateAsString = params.dateAsStringNeeded(subStr)
                        if (dateAsString) {
                            m.appendReplacement(sb, params.dateParameterAsString)
                        } else {
                            m.appendReplacement(sb, "?")
                        }
                        // parameter name is prefixed with ':', so get correct name with trimming ':';
                        args.add(params.getParameterValue(m.group().substring(1), isMulti, dateAsString))
                    }
                    m.appendTail(sb)
                    queryBuilder.append(sb)
                } else {
                    // alternative tokens are from with-in single quotes.
                    queryBuilder.append("'").append(tokens[i]).append("'")
                }
            }
            return SQLBuilder(queryBuilder.toString(), *args.toTypedArray())
        }

        /**
         * Quotes a system object name (e.g. table, column).
         *
         * @param name The table, view, or column name
         * @param noQuotes If true, don't quote.  Instead throw an exception if quoting would be necessary
         * @return string with "" around if necessary
         * @throws IllegalArgumentException if the name cannot be properly quoted or quoting would be required
         */
        @JvmOverloads
        @Throws(IllegalArgumentException::class)
        fun nameQuote(name: String?, noQuotes: Boolean = true): String {
            requireNotNull(name) { "Object name is null" }
            if (name.matches(Regex("""[A-Za-z][A-Za-z0-9_.]*|"[^"]+""""))) {
                return name
            }
            // allow table and column aliases.  Both real name and alias must be quoted
            val alias = name.split(Regex("""\s+(?i:as\s+)?"""), 2)
            if (alias.size == 2) {
                // found alias.  column aliases have an optional " as " prefix but table aliases does not allow this.
                // Therefore, we do not include the " as " so that we don't have to distinguish the 2 cases.
                return nameQuote(alias[0], noQuotes) + " " + nameQuote(alias[1], noQuotes)
            }
            require(!(noQuotes || name.indexOf('"') != -1)) { "Object name \"$name\" contains invalid characters" }
            return "\"" + name + "\""
        }

        /**
         * This function closes AutoCloseable objects.
         * @return true is all non-null resources could be closed, false otherwise
         */
        @JvmStatic
        fun close(vararg resources: AutoCloseable?): Boolean {
            var allClosed = true
            for (resource in resources) {
                if (resource != null) {
                    try {
                        resource.close()
                    } catch (e: Exception) {
                        allClosed = false
                        logger.error("Can't close {}", resource.javaClass.name, e)
                    }
                }
            }
            return allClosed
        }

        /**
         * Returns a ResultSet that automatically closes the statement it was created from
         * @param rs The original ResultSet
         * @return The wrapped ResultSet
         */
        @JvmStatic
        fun wrapStatement(rs: ResultSet): ResultSet {
            return Proxy.newProxyInstance(
                ResultSet::class.java.classLoader,
                arrayOf<Class<*>>(ResultSet::class.java),
                WrappedResultSet(rs, WrappedResultSet.Scope.Statement)
            ) as ResultSet
        }

        /**
         * Returns a ResultSet that automatically closes the statement and The Connection object it was created from
         * @param rs The original ResultSet
         * @return The wrapped ResultSet
         */
        @JvmStatic
        fun wrapConnection(rs: ResultSet): ResultSet {
            return Proxy.newProxyInstance(
                ResultSet::class.java.classLoader,
                arrayOf<Class<*>>(ResultSet::class.java),
                WrappedResultSet(rs, WrappedResultSet.Scope.Connection)
            ) as ResultSet
        }
    }
}
