/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.math.BigDecimal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


final class JdbcSQLBuilderProvider implements SQLBuilderProvider {

    private PreparedStatement build(SQLBuilder sqlBuilder, Connection connection) throws SQLException {
        return build(sqlBuilder, connection, (String[]) null);
    }

    private PreparedStatement build(SQLBuilder sqlBuilder, Connection connection, String... columns) throws SQLException {
        ArrayList<Object> expanded = null;
        if (!sqlBuilder.arguments.isEmpty()) {
            expanded = new ArrayList<>(sqlBuilder.arguments.size());
            int sqlPos = 0;
            for (Object arg : sqlBuilder.arguments) {
                sqlPos = sqlBuilder.statement.indexOf("?", sqlPos) + 1;
                if (sqlPos == 0) {
                    // We ran out of placeholders (i.e. we have extra parameters).
                    // We do not consider that as a bug (though one could argue this
                    // should result in a warning).
                    break;
                }
                if (arg instanceof Collection) {
                    @SuppressWarnings("unchecked")
                    final Collection<Object> col = (Collection<Object>) arg;
                    final int length = col.size();
                    if (length == 0) {
                        throw new SQLException("Collection parameters must contain at least one element");
                    }
                    // The statement already contains one "?", therefore we start with 1 instead of 0
                    for (int k = 1; k < length; k++) {
                        sqlBuilder.statement.insert(sqlPos, ",?");
                    }
                    // move sqlPos beyond the inserted ",?"
                    sqlPos += 2 * (length - 1);
                    expanded.addAll(col);
                } else {
                    expanded.add(arg);
                }
            }
        }

        sqlBuilder.interpolate(true);
        final PreparedStatement ps;
        if (columns == null || columns.length == 0) {
            ps = connection.prepareStatement(sqlBuilder.statement.toString(), sqlBuilder.resultSetType, ResultSet.CONCUR_READ_ONLY);
        } else {
            ps = connection.prepareStatement(sqlBuilder.statement.toString(), columns);
        }
        try {
            if (sqlBuilder.fetchSize > 0) {
                ps.setFetchSize(sqlBuilder.fetchSize);
            }
            if (sqlBuilder.maxRows >= 0) {
                ps.setMaxRows(sqlBuilder.maxRows);
            }

            if (expanded != null && !expanded.isEmpty()) {
                int idx = 0;
                for (Object arg : expanded) {
                    if (arg instanceof LongString) {
                        ps.setCharacterStream(++idx, ((LongString) arg).getReader());
                    } else if (arg instanceof SQLBuilder.Masked) {
                        ps.setObject(++idx, ((SQLBuilder.Masked) arg).data);
                    } else {
                        ps.setObject(++idx, arg);
                    }
                }
            }
        } catch (SQLException ex) {
            SQLBuilder.close(ps);
            throw ex;
        }
        return ps;
    }

    @Override
    public ResultSet getResultSet(SQLBuilder sqlBuilder, Connection connection, boolean wrapConnection) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = build(sqlBuilder, connection);
            return wrapConnection
                   ? SQLBuilder.wrapConnection(preparedStatement.executeQuery())
                   : SQLBuilder.wrapStatement(preparedStatement.executeQuery());
        } catch (SQLException e) {
            SQLBuilder.close(preparedStatement);
            throw e;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public int getInt(SQLBuilder sqlBuilder, Connection connection, int columnNumber, int defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(columnNumber) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public int getInt(SQLBuilder sqlBuilder, Connection connection, String columnName, int defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(columnName) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public long getLong(SQLBuilder sqlBuilder, Connection connection, int columnNumber, long defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(columnNumber) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public long getLong(SQLBuilder sqlBuilder, Connection connection, String columnName, long defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(columnName) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public double getDouble(SQLBuilder sqlBuilder, Connection connection, int columnNumber, double defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getDouble(columnNumber) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public double getDouble(SQLBuilder sqlBuilder, Connection connection, String columnName, double defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getDouble(columnName) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public String getString(SQLBuilder sqlBuilder, Connection connection, int columnNumber, String defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getString(columnNumber) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection connection, String columnName, BigDecimal defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getBigDecimal(columnName) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public BigDecimal getBigDecimal(SQLBuilder sqlBuilder, Connection connection, int columnNumber, BigDecimal defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getBigDecimal(columnNumber) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public String getString(SQLBuilder sqlBuilder, Connection connection, String columnName, String defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getString(columnName) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public Object getObject(SQLBuilder sqlBuilder, Connection connection, int columnNumber, Object defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getObject(columnNumber) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public Object getObject(SQLBuilder sqlBuilder, Connection connection, String columnName, Object defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getObject(columnName) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public OffsetDateTime getDateTime(SQLBuilder sqlBuilder, Connection connection, int columnNumber, OffsetDateTime defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getObject(columnNumber, OffsetDateTime.class) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public OffsetDateTime getDateTime(SQLBuilder sqlBuilder, Connection connection, String columnName, OffsetDateTime defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getObject(columnName, OffsetDateTime.class) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public Instant getInstant(SQLBuilder sqlBuilder, Connection connection, int columnNumber, Instant defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getObject(columnNumber, OffsetDateTime.class).toInstant() : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public Instant getInstant(SQLBuilder sqlBuilder, Connection connection, String columnName, Instant defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getObject(columnName, OffsetDateTime.class).toInstant() : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public Timestamp getTimestamp(SQLBuilder sqlBuilder, Connection connection, String columnName, Timestamp defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getTimestamp(columnName) : defaultValue;
        }
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection from which the PreparedStatement is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet
     */
    @Override
    public Timestamp getTimestamp(SQLBuilder sqlBuilder, Connection connection, int columnNumber, Timestamp defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getTimestamp(columnNumber) : defaultValue;
        }
    }

    /**
     * Executes the SQL statement.
     * @param connection The Connection from which the PreparedStatement is created
     * @return The result of executeUpdate of that statement
     * @throws SQLException the exception thrown when executing the query
     */
    @Override
    public int execute(SQLBuilder sqlBuilder, Connection connection) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection)) {
            return ps.executeUpdate();
        }
    }

    /**
     * Executes the SQL statement.
     * @param connection The Connection from which the PreparedStatement is created
     * @param keyColumns column names from the underlying table for which the inserted values will be returned.  Note that these names
     *               not necessarily have to be part of the keyColumns into which the builder explicitly inserts values.
     * @return The result of getGeneratedKeys of that statement
     * @throws SQLException the exception thrown when executing the query
     */
    @Override
    public ResultSet execute(SQLBuilder sqlBuilder, Connection connection, String... keyColumns) throws SQLException {
        PreparedStatement ps = build(sqlBuilder, connection, keyColumns);
        ps.executeUpdate();
        return SQLBuilder.wrapStatement(ps.getGeneratedKeys());
    }

    @Override
    public <T> List<T> getList(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper, boolean withNull) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return getList(rs, rowMapper, withNull);
        }
    }

    @Override
    public <K, V> Map<K, V> getMap(SQLBuilder sqlBuilder, Connection connection,
            SQLBuilder.RowMapper<Map.Entry<K, V>> rowMapper, boolean withNull) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return getMap(rs, rowMapper, withNull);
        }
    }

    @Override
    public <T> Optional<T> getSingle(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return Optional.ofNullable(rs.next() ? rowMapper.map(rs) : null);
        }
    }

    @Override
    public <T> T getSingle(SQLBuilder sqlBuilder, Connection connection, SQLBuilder.RowMapper<T> rowMapper, T defaultValue) throws SQLException {
        try (PreparedStatement ps = build(sqlBuilder, connection); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rowMapper.map(rs) : defaultValue;
        }
    }

}
