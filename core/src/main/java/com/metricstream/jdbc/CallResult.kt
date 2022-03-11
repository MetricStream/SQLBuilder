package com.metricstream.jdbc

interface CallResult : AutoCloseable {
    fun getString(columnNumber: Int): String?

    fun getInt(columnNumber: Int): Int

    fun getLong(columnNumber: Int): Long

    fun wasNull(): Boolean

    fun isClosed(): Boolean
}
