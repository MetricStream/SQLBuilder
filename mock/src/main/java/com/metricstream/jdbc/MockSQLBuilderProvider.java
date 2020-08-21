/*
 * Copyright © 2020, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import java.math.BigDecimal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;


public final class MockSQLBuilderProvider implements SQLBuilderProvider {

    private static final Queue<ResultSet> mockResultSets = new ConcurrentLinkedQueue<>();

    private static BiFunction<Integer, Integer, Integer> intByColumnIndex;
    private static BiFunction<String, Integer, Integer> intByColumnLabel;
    private static BiFunction<Integer, Long, Long> longByColumnIndex;
    private static BiFunction<String, Long, Long> longByColumnLabel;
    private static BiFunction<Integer, String, String> stringByColumnIndex;
    private static BiFunction<String, String, String> stringByColumnLabel;
    private static BiFunction<Integer, BigDecimal, BigDecimal> bigDecimalByColumnIndex;
    private static BiFunction<String, BigDecimal, BigDecimal> bigDecimalByColumnLabel;
    private static BiFunction<Integer, Object, Object> objectByColumnIndex;
    private static BiFunction<String, Object, Object> objectByColumnLabel;
    private static Supplier<Integer> executeSupplier;
    private final boolean generateSingleRowResultSet;

    public MockSQLBuilderProvider() {
        this(true);
    }

    public MockSQLBuilderProvider(boolean generateSingleRowResultSet) {
        this.generateSingleRowResultSet = generateSingleRowResultSet;
        reset();
    }

    public static void addResultSet(ResultSet rs) {
        mockResultSets.add(rs);
    }

    public static void addResultSet(final String tag, final Object[][] data) throws SQLException {
        mockResultSets.add(MockResultSet.create(tag, data));
    }

    public static void addResultSet(final String tag, final String labels, final String... csvs) throws SQLException {
        mockResultSets.add(MockResultSet.create(tag, labels, csvs));
    }

    public static void addResultSet(final String tag, final String csv, boolean withLabels) throws SQLException {
        mockResultSets.add(MockResultSet.create(tag, csv, withLabels));
    }

    public static void addResultSet(final String tag, final String csv) throws SQLException {
        mockResultSets.add(MockResultSet.create(tag, csv, false));
    }

    public static void addResultSet(final String tag, final InputStream csv, boolean withLabels) throws SQLException {
        mockResultSets.add(MockResultSet.create(tag, csv, withLabels));
    }

    public static void addResultSet(final String tag, final InputStream csv) throws SQLException {
        mockResultSets.add(MockResultSet.create(tag, csv, true));
    }

    public static void setIntByColumnIndex(BiFunction<Integer, Integer, Integer> intByColumnIndex) {
        MockSQLBuilderProvider.intByColumnIndex = intByColumnIndex;
    }

    public static void setIntByColumnLabel(BiFunction<String, Integer, Integer> intByColumnLabel) {
        MockSQLBuilderProvider.intByColumnLabel = intByColumnLabel;
    }

    public static void setLongByColumnIndex(BiFunction<Integer, Long, Long> longByColumnIndex) {
        MockSQLBuilderProvider.longByColumnIndex = longByColumnIndex;
    }

    public static void setLongByColumnLabel(BiFunction<String, Long, Long> longByColumnLabel) {
        MockSQLBuilderProvider.longByColumnLabel = longByColumnLabel;
    }

    public static void setStringByColumnIndex(BiFunction<Integer, String, String> stringByColumnIndex) {
        MockSQLBuilderProvider.stringByColumnIndex = stringByColumnIndex;
    }

    public static void setStringByColumnLabel(BiFunction<String, String, String> stringByColumnLabel) {
        MockSQLBuilderProvider.stringByColumnLabel = stringByColumnLabel;
    }

    public static void setBigDecimalByColumnIndex(BiFunction<Integer, BigDecimal, BigDecimal> bigDecimalByColumnIndex) {
        MockSQLBuilderProvider.bigDecimalByColumnIndex = bigDecimalByColumnIndex;
    }

    public static void setBigDecimalByColumnLabel(BiFunction<String, BigDecimal, BigDecimal> bigDecimalByColumnLabel) {
        MockSQLBuilderProvider.bigDecimalByColumnLabel = bigDecimalByColumnLabel;
    }

    public static void setObjectByColumnIndex(BiFunction<Integer, Object, Object> objectByColumnIndex) {
        MockSQLBuilderProvider.objectByColumnIndex = objectByColumnIndex;
    }

    public static void setObjectByColumnLabel(BiFunction<String, Object, Object> objectByColumnLabel) {
        MockSQLBuilderProvider.objectByColumnLabel = objectByColumnLabel;
    }

    public static void setExecute(Supplier<Integer> supplier) {
        executeSupplier = supplier;
    }

    public static void setExecute(int value) {
        executeSupplier = () -> value;
    }

    public static void setExecute(int... values) {
        final AtomicInteger count = new AtomicInteger();
        executeSupplier = () -> count.get() < values.length ? values[count.getAndIncrement()] : 42;
    }

    public ResultSet getResultSet(SQLBuilder sqlBuilder, Connection connection, boolean wrapConnection) throws SQLException {
        return getRs();
    }

    @Override
    public int getInt(SQLBuilder sqlBuilder, Connection connection, int columnNumber, int defaultValue) throws SQLException {
        if (intByColumnIndex != null) {
            return intByColumnIndex.apply(columnNumber, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getInt(columnNumber) : defaultValue;
    }

    public int getInt(SQLBuilder sqlBuilder, Connection connection, String columnName, int defaultValue) throws SQLException {
        if (intByColumnLabel != null) {
            return intByColumnLabel.apply(columnName, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getInt(columnName) : defaultValue;
    }

    @Override
    public long getLong(SQLBuilder sqlBuilder, Connection connection, int columnNumber, long defaultValue) throws SQLException {
        if (longByColumnIndex != null) {
            return longByColumnIndex.apply(columnNumber, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getLong(columnNumber) : defaultValue;
    }

    @Override
    public long getLong(SQLBuilder sqlBuilder, Connection connection, String columnName, long defaultValue) throws SQLException {
        if (longByColumnLabel != null) {
            return longByColumnLabel.apply(columnName, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getLong(columnName) : defaultValue;
    }

    @Override
    public String getString(SQLBuilder sqlBuilder, Connection connection, int columnNumber, String defaultValue) throws SQLException {
        if (stringByColumnIndex != null) {
            return stringByColumnIndex.apply(columnNumber, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getString(columnNumber) : defaultValue;
    }

    @Override
    public String getString(SQLBuilder sqlBuilder, Connection connection, String columnName, String defaultValue) throws SQLException {
        if (stringByColumnLabel != null) {
            return stringByColumnLabel.apply(columnName, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getString(columnName) : defaultValue;
    }

    @Override
    public BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection connection, int columnNumber, BigDecimal defaultValue) throws SQLException {
        if (bigDecimalByColumnIndex != null) {
            return bigDecimalByColumnIndex.apply(columnNumber, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getBigDecimal(columnNumber) : defaultValue;
    }

    @Override
    public BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection connection, String columnName, BigDecimal defaultValue) throws SQLException {
        if (bigDecimalByColumnLabel != null) {
            return bigDecimalByColumnLabel.apply(columnName, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getBigDecimal(columnName) : defaultValue;
    }

    @Override
    public Object getObject(SQLBuilder sqlBuilder, Connection connection, int columnNumber, Object defaultValue) throws SQLException {
        if (objectByColumnIndex != null) {
            return objectByColumnIndex.apply(columnNumber, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getObject(columnNumber) : defaultValue;
    }

    @Override
    public Object getObject(SQLBuilder sqlBuilder, Connection connection, String columnName, Object defaultValue) throws SQLException {
        if (objectByColumnLabel != null) {
            return objectByColumnLabel.apply(columnName, defaultValue);
        }
        final ResultSet rs = getRs();
        return rs.next() ? rs.getObject(columnName) : defaultValue;
    }

    @Override
    public int execute(SQLBuilder sqlBuilder, Connection connection) {
        return executeSupplier.get();
    }

    @Override
    public <T> List<T> getList(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper, boolean withNull) throws SQLException {
        final ResultSet rs = getRs();
        final List<T> list = new ArrayList<>();
        while (rs.next()) {
            final T item = rowMapper.map(rs);
            if (withNull || item != null) {
                list.add(item);
            }
        }
        return list;
    }

    @Override
    public <K, V> Map<K, V> getMap(SQLBuilder sqlBuilder, Connection connection,
            SQLBuilder.RowMapper<Map.Entry<K, V>> rowMapper, boolean withNull) throws SQLException {
        final ResultSet rs = getRs();
        final Map<K, V> map = new HashMap<>();
        while (rs.next()) {
            Map.Entry<K, V> entry = rowMapper.map(rs);
            if (entry != null && entry.getKey() != null && (withNull || entry.getValue() != null)) {
                map.put(entry.getKey(), entry.getValue());
            }
        }
        return map;
    }

    @Override
    public <T> Optional<T> getSingle(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper) throws SQLException {
        final ResultSet rs = getRs();
        return Optional.ofNullable(rs.next() ? rowMapper.map(rs) : null);
    }

    @Override
    public <T> T getSingle(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper, T defaultValue) throws SQLException {
        final ResultSet rs = getRs();
        return rs.next() ? rowMapper.map(rs) : defaultValue;
    }

    private ResultSet getRs() throws SQLException {
        ResultSet rs = mockResultSets.poll();
        if (rs != null) {
            return rs;
        }
        return generateSingleRowResultSet ? MockResultSet.create("", "42", false) : MockResultSet.empty("");
    }

    public static void reset() {
        mockResultSets.clear();
        intByColumnIndex = null;
        intByColumnLabel = null;
        longByColumnIndex = null;
        longByColumnLabel = null;
        stringByColumnIndex = null;
        stringByColumnLabel = null;
        bigDecimalByColumnIndex = null;
        bigDecimalByColumnLabel = null;
        objectByColumnIndex = null;
        objectByColumnLabel = null;
        setExecute(42);
    }

}
