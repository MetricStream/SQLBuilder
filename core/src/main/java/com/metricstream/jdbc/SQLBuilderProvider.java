/*
 * Copyright © 2020, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;


public interface SQLBuilderProvider {
    ResultSet getResultSet(SQLBuilder sqlBuilder, Connection connection, boolean wrapConnection) throws SQLException;
    int getInt(SQLBuilder sqlBuilder, Connection connection, int columnNumber, int defaultValue) throws SQLException;
    int getInt(SQLBuilder sqlBuilder, Connection connection, String columnName, int defaultValue) throws SQLException;
    long getLong(SQLBuilder sqlBuilder, Connection connection, int columnNumber, long defaultValue) throws SQLException;
    long getLong(SQLBuilder sqlBuilder, Connection connection, String columnName, long defaultValue) throws SQLException;
    String getString(SQLBuilder sqlBuilder, Connection connection, int columnNumber, String defaultValue) throws SQLException;
    String getString(SQLBuilder sqlBuilder, Connection connection, String columnName, String defaultValue) throws SQLException;
    BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection connection, int columnNumber, BigDecimal defaultValue) throws SQLException;
    BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection connection, String columnName, BigDecimal defaultValue) throws SQLException;
    Object getObject(SQLBuilder sqlBuilder, Connection connection, int columnNumber, Object defaultValue) throws SQLException;
    Object getObject(SQLBuilder sqlBuilder, Connection connection, String columnName, Object defaultValue) throws SQLException;
    int execute(SQLBuilder sqlBuilder, Connection connection) throws SQLException;
    <T> List<T> getList(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper, boolean withNull) throws SQLException;
    <T> Optional<T> getSingle(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper) throws SQLException;
    <T> T getSingle(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper, T defaultValue) throws SQLException;
}
