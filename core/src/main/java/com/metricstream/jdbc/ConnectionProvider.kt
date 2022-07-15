package com.metricstream.jdbc

import java.sql.Connection

interface ConnectionProvider {
    fun getConnection(): Connection
}
