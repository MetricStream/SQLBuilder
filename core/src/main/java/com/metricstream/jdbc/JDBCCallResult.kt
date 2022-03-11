package com.metricstream.jdbc

import java.sql.CallableStatement

class JDBCCallResult(private val callableStatement: CallableStatement, private val mapIndex: Map<Int, Int>) : CallResult {
    override fun close() {
        callableStatement.close()
    }

    override fun getInt(columnNumber: Int): Int {
        return callableStatement.getInt(mapIndex[columnNumber] ?: throw IllegalArgumentException("Invalid column number $columnNumber"))
    }

    override fun getString(columnNumber: Int): String? {
        return callableStatement.getString(mapIndex[columnNumber] ?: throw IllegalArgumentException("Invalid column number $columnNumber"))
    }

    override fun getLong(columnNumber: Int): Long {
        return callableStatement.getLong(mapIndex[columnNumber] ?: throw IllegalArgumentException("Invalid column number $columnNumber"))
    }

    override fun wasNull(): Boolean {
        return callableStatement.wasNull()
    }

    override fun isClosed(): Boolean {
        return callableStatement.isClosed
    }
}
