package com.metricstream.jdbc

import java.sql.ResultSetMetaData

class MockResultSetMetaData internal constructor(private val columnIndices: Map<String, Int>) : ResultSetMetaData {
    override fun <T : Any?> unwrap(p0: Class<T>?): T {
        TODO("Not yet implemented")
    }

    override fun isWrapperFor(p0: Class<*>?): Boolean {
        TODO("Not yet implemented")
    }

    override fun getColumnCount(): Int = columnIndices.size

    override fun isAutoIncrement(columnIndex: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isCaseSensitive(columnIndex: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isSearchable(columnIndex: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isCurrency(columnIndex: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isNullable(columnIndex: Int): Int {
        TODO("Not yet implemented")
    }

    override fun isSigned(columnIndex: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun getColumnDisplaySize(columnIndex: Int): Int {
        TODO("Not yet implemented")
    }

    override fun getColumnLabel(columnIndex: Int): String {
        TODO("Not yet implemented")
    }

    override fun getColumnName(columnIndex: Int): String? {
        return columnIndices.filter { columnIndex - 1 == it.value }.keys.firstOrNull()
    }

    override fun getSchemaName(columnIndex: Int): String {
        TODO("Not yet implemented")
    }

    override fun getPrecision(columnIndex: Int): Int {
        TODO("Not yet implemented")
    }

    override fun getScale(columnIndex: Int): Int {
        TODO("Not yet implemented")
    }

    override fun getTableName(columnIndex: Int): String {
        TODO("Not yet implemented")
    }

    override fun getCatalogName(columnIndex: Int): String {
        TODO("Not yet implemented")
    }

    override fun getColumnType(columnIndex: Int): Int {
        TODO("Not yet implemented")
    }

    override fun getColumnTypeName(columnIndex: Int): String {
        TODO("Not yet implemented")
    }

    override fun isReadOnly(columnIndex: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isWritable(columnIndex: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun isDefinitelyWritable(columnIndex: Int): Boolean {
        TODO("Not yet implemented")
    }

    override fun getColumnClassName(columnIndex: Int): String {
        TODO("Not yet implemented")
    }
}
