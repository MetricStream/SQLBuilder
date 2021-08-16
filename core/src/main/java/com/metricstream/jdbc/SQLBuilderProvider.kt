/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc

import kotlin.Throws
import java.sql.SQLException
import java.sql.ResultSet
import java.math.BigDecimal
import java.time.OffsetDateTime
import java.time.Instant
import java.lang.IllegalStateException
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.util.Optional

interface SQLBuilderProvider {
    @Throws(SQLException::class)
    fun getResultSet(sqlBuilder: SQLBuilder, connection: Connection, wrapConnection: Boolean): ResultSet

    @Throws(SQLException::class)
    fun getInt(sqlBuilder: SQLBuilder, connection: Connection, columnNumber: Int, defaultValue: Int): Int

    @Throws(SQLException::class)
    fun getInt(sqlBuilder: SQLBuilder, connection: Connection, columnName: String, defaultValue: Int): Int

    @Throws(SQLException::class)
    fun getLong(sqlBuilder: SQLBuilder, connection: Connection, columnNumber: Int, defaultValue: Long): Long

    @Throws(SQLException::class)
    fun getLong(sqlBuilder: SQLBuilder, connection: Connection, columnName: String, defaultValue: Long): Long

    @Throws(SQLException::class)
    fun getString(sqlBuilder: SQLBuilder, connection: Connection, columnNumber: Int, defaultValue: String?): String?

    @Throws(SQLException::class)
    fun getString(sqlBuilder: SQLBuilder, connection: Connection, columnName: String, defaultValue: String?): String?

    @Throws(SQLException::class)
    fun getBigDecimal(sqlBuilder: SQLBuilder, connection: Connection, columnNumber: Int, defaultValue: BigDecimal?): BigDecimal?

    @Throws(SQLException::class)
    fun getBigDecimal(sqlBuilder: SQLBuilder, connection: Connection, columnName: String, defaultValue: BigDecimal?): BigDecimal?

    @Throws(SQLException::class)
    fun getObject(sqlBuilder: SQLBuilder, connection: Connection, columnNumber: Int, defaultValue: Any?): Any?

    @Throws(SQLException::class)
    fun getObject(sqlBuilder: SQLBuilder, connection: Connection, columnName: String, defaultValue: Any?): Any?

    @Throws(SQLException::class)
    fun getDateTime(sqlBuilder: SQLBuilder, connection: Connection, columnNumber: Int, defaultValue: OffsetDateTime?): OffsetDateTime?

    @Throws(SQLException::class)
    fun getDateTime(sqlBuilder: SQLBuilder, connection: Connection, columnName: String, defaultValue: OffsetDateTime?): OffsetDateTime?

    @Throws(SQLException::class)
    fun getTimestamp(sqlBuilder: SQLBuilder, connection: Connection, columnNumber: Int, defaultValue: Timestamp?): Timestamp?

    @Throws(SQLException::class)
    fun getTimestamp(sqlBuilder: SQLBuilder, connection: Connection, columnName: String, defaultValue: Timestamp?): Timestamp?

    @Throws(SQLException::class)
    fun getDate(sqlBuilder: SQLBuilder, connection: Connection, columnNumber: Int, defaultValue: Date?): Date?

    @Throws(SQLException::class)
    fun getDate(sqlBuilder: SQLBuilder, connection: Connection, columnName: String, defaultValue: Date?): Date?

    @Throws(SQLException::class)
    fun execute(sqlBuilder: SQLBuilder, connection: Connection): Int

    @Throws(SQLException::class)
    fun execute(sqlBuilder: SQLBuilder, connection: Connection, vararg keyColumns: String): ResultSet

    @Throws(SQLException::class)
    fun <T> getList(sqlBuilder: SQLBuilder, connection: Connection, rowMapper: SQLBuilder.RowMapper<T?>, withNull: Boolean = false): List<T?>

    @Throws(SQLException::class)
    fun <K, V> getMap(sqlBuilder: SQLBuilder, connection: Connection, rowMapper: SQLBuilder.RowMapper<Map.Entry<K, V?>>, withNull: Boolean = false): Map<K, V?>

    @Throws(SQLException::class)
    fun <T> getSingle(sqlBuilder: SQLBuilder, connection: Connection, rowMapper: SQLBuilder.RowMapper<T?>): Optional<T>

    @Throws(SQLException::class)
    fun <T> getSingle(sqlBuilder: SQLBuilder, connection: Connection, rowMapper: SQLBuilder.RowMapper<T?>, defaultValue: T?): T?

    @Throws(SQLException::class)
    fun getInstant(sqlBuilder: SQLBuilder, connection: Connection, columnNumber: Int, defaultValue: Instant?): Instant?

    @Throws(SQLException::class)
    fun getInstant(sqlBuilder: SQLBuilder, connection: Connection, columnName: String, defaultValue: Instant?): Instant?

    @Throws(SQLException::class)
    fun <T> getList(rs: ResultSet, rowMapper: SQLBuilder.RowMapper<T>, withNull: Boolean): List<T?> {
        val list = mutableListOf<T?>()
        while (rs.next()) {
            val item: T? = rowMapper.map(rs)
            if (withNull || item != null) {
                list.add(item)
            }
        }
        return list
    }

    @Throws(SQLException::class, IllegalStateException::class)
    fun <K, V> getMap(rs: ResultSet, rowMapper: SQLBuilder.RowMapper<Map.Entry<K, V?>>, withNull: Boolean): Map<K, V?> {
        val map = mutableMapOf<K, V?>()
        while (rs.next()) {
            val entry = rowMapper.map(rs)
            if (entry != null) {
                val key: K = checkNotNull(entry.key) { "Null as map key is unsupported" }
                val value: V? = entry.value
                if (withNull || value != null) {
                    check(map.put(key, value) == null) { "Duplicate map key '$key' is unsupported" }
                }
            }
        }
        return map
    }
}
