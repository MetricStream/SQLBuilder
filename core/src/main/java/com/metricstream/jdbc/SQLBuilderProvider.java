/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public interface SQLBuilderProvider {
    ResultSet getResultSet(SQLBuilder sqlBuilder, Connection connection, boolean wrapConnection) throws SQLException;
    int getInt(SQLBuilder sqlBuilder, Connection connection, int columnNumber, int defaultValue) throws SQLException;
    int getInt(SQLBuilder sqlBuilder, Connection connection, String columnName, int defaultValue) throws SQLException;
    long getLong(SQLBuilder sqlBuilder, Connection connection, int columnNumber, long defaultValue) throws SQLException;
    long getLong(SQLBuilder sqlBuilder, Connection connection, String columnName, long defaultValue) throws SQLException;
    double getDouble(SQLBuilder sqlBuilder, Connection connection, int columnNumber, double defaultValue) throws SQLException;
    double getDouble(SQLBuilder sqlBuilder, Connection connection, String columnName, double defaultValue) throws SQLException;
    String getString(SQLBuilder sqlBuilder, Connection connection, int columnNumber, String defaultValue) throws SQLException;
    String getString(SQLBuilder sqlBuilder, Connection connection, String columnName, String defaultValue) throws SQLException;
    BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection connection, int columnNumber, BigDecimal defaultValue) throws SQLException;
    BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection connection, String columnName, BigDecimal defaultValue) throws SQLException;
    Object getObject(SQLBuilder sqlBuilder, Connection connection, int columnNumber, Object defaultValue) throws SQLException;
    Object getObject(SQLBuilder sqlBuilder, Connection connection, String columnName, Object defaultValue) throws SQLException;
    OffsetDateTime getDateTime(SQLBuilder sqlBuilder, Connection connection, int columnNumber, OffsetDateTime defaultValue) throws SQLException;
    OffsetDateTime getDateTime(SQLBuilder sqlBuilder, Connection connection, String columnName, OffsetDateTime defaultValue) throws SQLException;
    Timestamp getTimestamp(SQLBuilder sqlBuilder, Connection connection, int columnNumber, Timestamp defaultValue) throws SQLException;
    Timestamp getTimestamp(SQLBuilder sqlBuilder, Connection connection, String columnName, Timestamp defaultValue) throws SQLException;
    int execute(SQLBuilder sqlBuilder, Connection connection) throws SQLException;
    ResultSet execute(SQLBuilder sqlBuilder, Connection connection, String... keyColumns) throws SQLException;
    <T> List<T> getList(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper, boolean withNull) throws SQLException;
    <K, V> Map<K, V> getMap(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<Map.Entry<K, V>> rowMapper, boolean withNull) throws SQLException;
    <T> Optional<T> getSingle(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper) throws SQLException;
    <T> T getSingle(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper, T defaultValue) throws SQLException;

    Instant getInstant(SQLBuilder sqlBuilder, Connection connection, int columnNumber, Instant defaultValue) throws SQLException;
    Instant getInstant(SQLBuilder sqlBuilder, Connection connection, String columnName, Instant defaultValue) throws SQLException;

    default <T> List<T> getList(ResultSet rs, SQLBuilder.RowMapper<T> rowMapper, boolean withNull) throws SQLException {
        final List<T> list = new ArrayList<>();
        while (rs.next()) {
            final T item = rowMapper.map(rs);
            if (withNull || item != null) {
                list.add(item);
            }
        }
        return list;
    }

    default <K, V> Map<K, V> getMap(ResultSet rs, SQLBuilder.RowMapper<Map.Entry<K, V>> rowMapper, boolean withNull) throws SQLException, IllegalStateException {
        final Map<K, V> map = new HashMap<>();
        while (rs.next()) {
            Map.Entry<K, V> entry = rowMapper.map(rs);
            if (entry != null) {
                final K key = entry.getKey();
                if (key == null) {
                    throw new IllegalStateException("null as key value is unsupported");
                }
                final V value = entry.getValue();
                if (withNull || value != null) {
                    if (map.put(key, value) != null) {
                        throw new IllegalStateException("Duplicate keys are unsupported");
                    }
                }
            }
        }
        return map;
    }
}
