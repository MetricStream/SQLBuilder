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
    ResultSet getResultSet(SQLBuilder sqlBuilder, Connection _con, boolean _wrapConnection) throws SQLException;
    int getInt(SQLBuilder sqlBuilder, Connection _con, int _numColumn, int _defValue) throws SQLException;
    int getInt(SQLBuilder sqlBuilder, Connection _con, String _columnName, int _defValue) throws SQLException;
    long getLong(SQLBuilder sqlBuilder, Connection _con, int _numColumn, long _defValue) throws SQLException;
    long getLong(SQLBuilder sqlBuilder, Connection _con, String _columnName, long _defValue) throws SQLException;
    String getString(SQLBuilder sqlBuilder, Connection _con, int _numColumn, String _defValue) throws SQLException;
    String getString(SQLBuilder sqlBuilder, Connection _con, String _columnName, String _defValue) throws SQLException;
    BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection _con, int _numColumn, BigDecimal _defValue) throws SQLException;
    BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection _con, String _columnName, BigDecimal _defValue) throws SQLException;
    Object getObject(SQLBuilder sqlBuilder, Connection _con, int _numColumn, Object _defValue) throws SQLException;
    Object getObject(SQLBuilder sqlBuilder, Connection _con, String _columnName, Object _defValue) throws SQLException;
    int execute(SQLBuilder sqlBuilder, Connection _con) throws SQLException;
    <T> List<T> getList(SQLBuilder sqlBuilder, Connection _con, SQLBuilder.RowMapper<T> _rm, boolean withNull) throws SQLException;
    <T> Optional<T> getSingle(SQLBuilder sqlBuilder, Connection _con, SQLBuilder.RowMapper<T> _rm) throws SQLException;
    <T> T getSingle(SQLBuilder sqlBuilder, Connection _con, SQLBuilder.RowMapper<T> _rm, T _default) throws SQLException;
}
