/*
 * Copyright © 2020-2022, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc

import com.metricstream.jdbc.SQLBuilder.Companion.close
import com.metricstream.jdbc.SQLBuilder.Companion.wrapConnection
import com.metricstream.jdbc.SQLBuilder.Companion.wrapStatement
import kotlin.Throws
import java.sql.SQLException
import java.sql.PreparedStatement
import java.sql.ResultSet
import com.metricstream.jdbc.SQLBuilder.Masked
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.Instant
import java.util.Optional
import java.util.ServiceLoader

internal class JdbcSQLBuilderProvider : SQLBuilderProvider {

    @Throws(SQLException::class)
    private fun build(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        vararg columns: String
    ): PreparedStatement {
        val expanded: MutableList<Any?> = mutableListOf()
        sqlBuilder.interpolate(SQLBuilder.Mode.EXPAND_AND_APPLY, expanded)
        val ps: PreparedStatement = if (columns.isEmpty()) {
            connection.prepareStatement(
                sqlBuilder.statement.toString(),
                sqlBuilder.resultSetType,
                sqlBuilder.resultSetConcurrency
            )
        } else {
            connection.prepareStatement(
                sqlBuilder.statement.toString(),
                columns
            )
        }
        try {
            if (sqlBuilder.fetchSize > 0) {
                ps.fetchSize = sqlBuilder.fetchSize
            }
            if (sqlBuilder.maxRows >= 0) {
                ps.maxRows = sqlBuilder.maxRows
            }
            if (expanded.isNotEmpty()) {
                expanded.forEachIndexed { index, arg ->
                    when (arg) {
                        is LongString -> ps.setCharacterStream(index + 1, arg.reader)
                        is Masked -> ps.setObject(index + 1, arg.data)
                        else -> ps.setObject(index + 1, arg)
                    }
                }
            }
        } catch (ex: SQLException) {
            close(ps)
            throw ex
        }
        return ps
    }

    @Throws(SQLException::class)
    override fun getResultSet(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        wrapConnection: Boolean
    ): ResultSet {
        var preparedStatement: PreparedStatement? = null
        return try {
            preparedStatement = build(sqlBuilder, connection)
            preparedStatement.executeQuery()!!.let { rs ->
                if (wrapConnection) wrapConnection(rs) else wrapStatement(rs)
            }
        } catch (e: SQLException) {
            close(preparedStatement)
            throw e
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getInt(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Int
    ): Int {
        return get(
            sqlBuilder,
            connection,
            { it.getInt(columnNumber) },
            defaultValue
        )
    }

    private fun <T> get(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        transform: (ResultSet) -> T,
        defaultValue: T
    ): T {
        build(sqlBuilder, connection).use { ps ->
            ps.executeQuery().use { rs ->
                return if (rs.next()) transform(rs) else defaultValue
            }
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getInt(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Int
    ): Int {
        return get(
            sqlBuilder,
            connection,
            { it.getInt(columnName) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getLong(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Long
    ): Long {
        return get(
            sqlBuilder,
            connection,
            { it.getLong(columnNumber) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getLong(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Long
    ): Long {
        return get(
            sqlBuilder,
            connection,
            { it.getLong(columnName) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getDouble(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Double
    ): Double {
        return get(
            sqlBuilder,
            connection,
            { it.getDouble(columnNumber) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getDouble(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Double
    ): Double {
        return get(
            sqlBuilder,
            connection,
            { it.getDouble(columnName) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getString(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: String?
    ): String? {
        return get(
            sqlBuilder,
            connection,
            { it.getString(columnNumber) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getBigDecimal(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: BigDecimal?
    ): BigDecimal? {
        return get(
            sqlBuilder,
            connection,
            { it.getBigDecimal(columnName) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getBigDecimal(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: BigDecimal?
    ): BigDecimal? {
        return get(
            sqlBuilder,
            connection,
            { it.getBigDecimal(columnNumber) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getString(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: String?
    ): String? {
        return get(
            sqlBuilder,
            connection,
            { it.getString(columnName) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getObject(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Any?
    ): Any? {
        return get(
            sqlBuilder,
            connection,
            { it.getObject(columnNumber) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getObject(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Any?
    ): Any? {
        return get(
            sqlBuilder,
            connection,
            { it.getObject(columnName) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getDateTime(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: OffsetDateTime?
    ): OffsetDateTime? {
        return get(
            sqlBuilder,
            connection,
            { it.getObject(columnNumber, OffsetDateTime::class.java) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getDateTime(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: OffsetDateTime?
    ): OffsetDateTime? {
        return get(
            sqlBuilder,
            connection,
            { it.getObject(columnName, OffsetDateTime::class.java) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getInstant(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Instant?
    ): Instant? {
        return get(
            sqlBuilder,
            connection,
            { it.getObject(columnNumber, OffsetDateTime::class.java).toInstant() },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getInstant(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Instant?
    ): Instant? {
        return get(
            sqlBuilder,
            connection,
            { it.getObject(columnName, OffsetDateTime::class.java).toInstant() },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getTimestamp(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Timestamp?
    ): Timestamp? {
        return get(
            sqlBuilder,
            connection,
            { it.getTimestamp(columnName) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getTimestamp(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Timestamp?
    ): Timestamp? {
        return get(
            sqlBuilder,
            connection,
            { it.getTimestamp(columnNumber) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getDate(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnName: String,
        defaultValue: Date?
    ): Date? {
        return get(
            sqlBuilder,
            connection,
            { it.getDate(columnName) },
            defaultValue
        )
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Throws(SQLException::class)
    override fun getDate(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        columnNumber: Int,
        defaultValue: Date?
    ): Date? {
        return get(
            sqlBuilder,
            connection,
            { it.getDate(columnNumber) },
            defaultValue
        )
    }

    /**
     * Executes the SQL statement.
     * @param connection The Connection from which the PreparedStatement is created
     * @return The result of executeUpdate of that statement
     * @throws SQLException the exception thrown when executing the query
     */
    @Throws(SQLException::class)
    override fun execute(sqlBuilder: SQLBuilder, connection: Connection): Int {
        build(sqlBuilder, connection).use { ps -> return ps.executeUpdate() }
    }

    /**
     * Executes the SQL statement.
     * @param connection The Connection from which the PreparedStatement is created
     * @param keyColumns column names from the underlying table for which the inserted values will be returned.  Note that these names
     * not necessarily have to be part of the keyColumns into which the builder explicitly inserts values.
     * @return The result of getGeneratedKeys of that statement
     * @throws SQLException the exception thrown when executing the query
     */
    @Throws(SQLException::class)
    override fun execute(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        vararg keyColumns: String
    ): ResultSet {
        val ps = build(sqlBuilder, connection, *keyColumns)
        ps.executeUpdate()
        return wrapStatement(ps.generatedKeys)
    }

    @Throws(SQLException::class)
    override fun <T> getList(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<T>
    ): List<T> {
        build(sqlBuilder, connection).use { ps ->
            ps.executeQuery().use { rs ->
                return getList(rs, rowMapper, false)
            }
        }
    }

    @Throws(SQLException::class)
    override fun <T> getListWithNull(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<T?>,
    ): List<T?> {
        build(sqlBuilder, connection).use { ps ->
            ps.executeQuery().use { rs ->
                return getList(rs, rowMapper, true)
            }
        }
    }

    @Throws(SQLException::class)
    override fun <K, V> getMap(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<Map.Entry<K, V?>>,
        withNull: Boolean
    ): Map<K, V?> {
        build(sqlBuilder, connection).use { ps ->
            ps.executeQuery().use { rs ->
                return getMap(rs, rowMapper, withNull)
            }
        }
    }

    @Throws(SQLException::class)
    override fun <T : Any> getSingle(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<T?>
    ): Optional<T> {
        build(sqlBuilder, connection).use { ps ->
            ps.executeQuery().use { rs ->
                return Optional.ofNullable(if (rs.next()) rowMapper.map(rs) else null)
            }
        }
    }

    @Throws(SQLException::class)
    override fun <T> getSingle(
        sqlBuilder: SQLBuilder,
        connection: Connection,
        rowMapper: SQLBuilder.RowMapper<T?>,
        defaultValue: T?
    ): T? {
        return get(sqlBuilder, connection, { rowMapper.map(it) }, defaultValue)
    }

    // Lazy loading to avoid instantiating the connectionProviderImpl when a JdbcSQLBuilderProvider object is created.
    // This will change if and when a caller uses the connection-less access methods.
    override val connectionProvider: ConnectionProvider by lazy { connectionProviderImpl }

    companion object {
        // Defer searching for a provider implementation until the first usage of connectionProvider.
        private val connectionProviderImpl: ConnectionProvider by lazy {
            val service = ConnectionProvider::class.java
            ServiceLoader.load(service).firstOrNull() ?: throw IllegalStateException("Could not find an implementation of $service")
        }
    }
}
