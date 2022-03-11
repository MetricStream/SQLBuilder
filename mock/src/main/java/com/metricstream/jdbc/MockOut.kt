package com.metricstream.jdbc

import java.sql.CallableStatement
import java.sql.JDBCType

class MockOut<T>(type: JDBCType, private val data: T) : SQLBuilder.Out<T>(type) {
    override fun register(callableStatement: CallableStatement, index: Int) {
    }

    override fun get(): T {
        return data
    }
}
