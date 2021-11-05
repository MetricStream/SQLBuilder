/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mocks a SQL ResultSet.
 * Copied from https://github.com/sharfah/java-utils/blob/master/src/test/java/com/sharfah/util/sql/MockResultSet.java
 * https://github.com/sharfah/java-utils/commit/0e930cbf74134e4d1cb71b0c4ca0602250e4f6fc
 */
public class MockResultSet {

    private static final Logger logger = LoggerFactory.getLogger(MockResultSet.class);

    private static final AtomicLong counter = new AtomicLong(0);
    public final String tag;
    private final Map<String, Integer> columnIndices;
    private final Object[][] data;
    private int rowIndex = -1;
    private boolean wasNull = false;
    private boolean generateData = true;
    private boolean generated = false;

    private MockResultSet(final String tag, final String[] names, final Object[][] data) {
        if (tag == null || tag.isEmpty()) {
            this.tag = "MockResultSet#" + counter.incrementAndGet();
        } else {
            this.tag = tag;
        }
        final String[] columnNames;
        if (names == null || names.length == 0) {
            columnNames = new String[data == null || data.length == 0 || data[0] == null || data[0].length == 0 ? 0 : data[0].length];
            Arrays.setAll(columnNames, i -> "COLUMN" + (i + 1));
        } else {
            columnNames = names;
        }
        columnIndices = IntStream.range(0, columnNames.length)
                .boxed()
                .collect(Collectors.toMap(
                        k -> columnNames[k].toUpperCase(),
                        Function.identity(),
                        (a, b) -> {
                            throw new RuntimeException("Duplicate column " + a);
                        },
                        LinkedHashMap::new
                ));
        this.data = data;
    }

    private ResultSet buildMock() throws SQLException {
        final ResultSet rs = mock(ResultSet.class, withSettings().stubOnly());

        // mock rs.next()
        doAnswer(invocation -> {
            if (rowIndex == -2) {
                throw new SQLException("Forced exception");
            }
            rowIndex++;
            return rowIndex < data.length;
        }).when(rs).next();

        // mock rs.wasNull()
        doAnswer(invocation -> wasNull).when(rs).wasNull();

        // mock rs.getString(columnName)
        doAnswer(invocation -> {
            final String columnName = invocation.getArgumentAt(0, String.class).toUpperCase();
            final int columnIndex = columnIndices.getOrDefault(columnName, Integer.MAX_VALUE);
            if (generateData && (rowIndex >= data.length || columnIndex >= data[rowIndex].length)) {
                return "42";
            }
            final Object value = data[rowIndex][columnIndex];
            wasNull = value == null;
            return value;
        }).when(rs).getString(anyString());

        // mock rs.getString(columnIndex)
        doAnswer(invocation -> {
            final int columnIndex = invocation.getArgumentAt(0, Integer.class);
            if (generateData && (rowIndex >= data.length || columnIndex > data[rowIndex].length)) {
                return "42";
            }
            final Object value = data[rowIndex][columnIndex - 1];
            wasNull = value == null;
            return value;
        }).when(rs).getString(anyInt());

        // mock rs.getInt(columnName)
        doAnswer(invocation -> {
            final String columnName = invocation.getArgumentAt(0, String.class).toUpperCase();
            final int columnIndex = columnIndices.getOrDefault(columnName, Integer.MAX_VALUE);
            if (generateData && (rowIndex >= data.length || columnIndex >= data[rowIndex].length)) {
                return 42;
            }
            final Object value = data[rowIndex][columnIndex];
            wasNull = value == null;
            return value instanceof String ? Integer.valueOf((String) value) : value;
        }).when(rs).getInt(anyString());

        // mock rs.getInt(columnIndex)
        doAnswer(invocation -> {
            final int columnIndex = invocation.getArgumentAt(0, Integer.class);
            if (generateData && (rowIndex >= data.length || columnIndex > data[rowIndex].length)) {
                return 42;
            }
            final Object value = data[rowIndex][columnIndex - 1];
            wasNull = value == null;
            return value instanceof String ? Integer.valueOf((String) value) : value;
        }).when(rs).getInt(anyInt());

        // mock rs.getLong(columnName)
        doAnswer(invocation -> {
            final String columnName = invocation.getArgumentAt(0, String.class).toUpperCase();
            final int columnIndex = columnIndices.getOrDefault(columnName, Integer.MAX_VALUE);
            if (generateData && (rowIndex >= data.length || columnIndex >= data[rowIndex].length)) {
                return 42L;
            }
            final Object value = data[rowIndex][columnIndex];
            wasNull = value == null;
            return value instanceof String ? Long.valueOf((String) value) : value;
        }).when(rs).getLong(anyString());

        // mock rs.getLong(columnIndex)
        doAnswer(invocation -> {
            final int columnIndex = invocation.getArgumentAt(0, Integer.class);
            if (generateData && (rowIndex >= data.length || columnIndex > data[rowIndex].length)) {
                return 42L;
            }
            final Object value = data[rowIndex][columnIndex - 1];
            wasNull = value == null;
            return value instanceof String ? Long.valueOf((String) value) : value;
        }).when(rs).getLong(anyInt());

        // mock rs.getBigDecimal(columnName)
        doAnswer(invocation -> {
            final String columnName = invocation.getArgumentAt(0, String.class).toUpperCase();
            final int columnIndex = columnIndices.getOrDefault(columnName, Integer.MAX_VALUE);
            if (generateData && (rowIndex >= data.length || columnIndex >= data[rowIndex].length)) {
                return BigDecimal.valueOf(42);
            }
            final Object value = data[rowIndex][columnIndex];
            wasNull = value == null;
            return value instanceof String ? new BigDecimal((String) value) : value;
        }).when(rs).getBigDecimal(anyString());

        // mock rs.getBigDecimal(columnIndex)
        doAnswer(invocation -> {
            final int columnIndex = invocation.getArgumentAt(0, Integer.class);
            if (generateData && (rowIndex >= data.length || columnIndex > data[rowIndex].length)) {
                return BigDecimal.valueOf(42);
            }
            final Object value = data[rowIndex][columnIndex - 1];
            wasNull = value == null;
            return value instanceof String ? new BigDecimal((String) value) : value;
        }).when(rs).getBigDecimal(anyInt());

        // mock rs.getTimestamp(columnIndex)
        doAnswer(invocation -> {
            final int columnIndex = invocation.getArgumentAt(0, Integer.class);
            if (generateData && (rowIndex >= data.length || columnIndex > data[rowIndex].length)) {
                return new Timestamp(42);
            }
            final Object value = data[rowIndex][columnIndex - 1];
            wasNull = value == null;
            if (value instanceof Timestamp) return value;
            if (value instanceof String) return Timestamp.valueOf((String) value);
            if (value instanceof Long) return new Timestamp((long) value);
            return value;
        }).when(rs).getTimestamp(anyInt());

        // mock rs.getTimestamp(columnName)
        doAnswer(invocation -> {
            final String columnName = invocation.getArgumentAt(0, String.class).toUpperCase();
            final int columnIndex = columnIndices.getOrDefault(columnName, Integer.MAX_VALUE);
            if (generateData && (rowIndex >= data.length || columnIndex > data[rowIndex].length)) {
                return new Timestamp(42);
            }
            final Object value = data[rowIndex][columnIndex];
            wasNull = value == null;
            if (value instanceof Timestamp) return value;
            if (value instanceof String) return Timestamp.valueOf((String) value);
            if (value instanceof Long) return new Timestamp((long) value);
            return value;
        }).when(rs).getTimestamp(anyString());

        // mock rs.getObject(columnName)
        doAnswer(invocation -> {
            final String columnName = invocation.getArgumentAt(0, String.class).toUpperCase();
            final int columnIndex = columnIndices.getOrDefault(columnName, Integer.MAX_VALUE);
            if (generateData && (rowIndex >= data.length || columnIndex >= data[rowIndex].length)) {
                return "42";
            }
            final Object value = data[rowIndex][columnIndex];
            wasNull = value == null;
            return value;
        }).when(rs).getObject(anyString());

        // mock rs.getObject(columnIndex)
        doAnswer(invocation -> {
            final int columnIndex = invocation.getArgumentAt(0, Integer.class);
            if (generateData && (rowIndex >= data.length || columnIndex > data[rowIndex].length)) {
                return "42";
            }
            final Object value = data[rowIndex][columnIndex - 1];
            wasNull = value == null;
            return value;
        }).when(rs).getObject(anyInt());

        // mock rs.getObject(columnName, OffsetDateTime.class)
        doAnswer(invocation -> {
            final String columnName = invocation.getArgumentAt(0, String.class).toUpperCase();
            final int columnIndex = columnIndices.getOrDefault(columnName, Integer.MAX_VALUE);
            if (generated || generateData && (rowIndex >= data.length || columnIndex >= data[rowIndex].length)) {
                return OffsetDateTime.of(4242, 4, 2, 4, 2, 4, 2, ZoneOffset.UTC);
            }
            final OffsetDateTime value = (OffsetDateTime) data[rowIndex][columnIndex];
            wasNull = value == null;
            return value;
        }).when(rs).getObject(anyString(), eq(OffsetDateTime.class));

        // mock rs.getObject(columnIndex, OffsetDateTime.class)
        doAnswer(invocation -> {
            final int columnIndex = invocation.getArgumentAt(0, Integer.class);
            if (generated || generateData && (rowIndex >= data.length || columnIndex > data[rowIndex].length)) {
                return OffsetDateTime.of(4242, 4, 2, 4, 2, 4, 2, ZoneOffset.UTC);
            }
            final OffsetDateTime value = (OffsetDateTime) data[rowIndex][columnIndex - 1];
            wasNull = value == null;
            return value;
        }).when(rs).getObject(anyInt(), eq(OffsetDateTime.class));

        final ResultSetMetaData rsmd = mock(ResultSetMetaData.class, withSettings().stubOnly());

        // mock rsmd.getColumnCount()
        doReturn(columnIndices.size()).when(rsmd).getColumnCount();

        // mock rsmd.getColumnName(int)
        doAnswer(invocation -> {
            final int columnIndex = invocation.getArgumentAt(0, Integer.class);
            return columnIndices.keySet().stream().skip(columnIndex - 1).findFirst().orElse(null);
        }).when(rsmd).getColumnName(anyInt());

        // mock rs.findColumn(String)
        doAnswer((invocation -> {
            final String columnName = invocation.getArgumentAt(0, String.class).toUpperCase();
            final int columnIndex = columnIndices.getOrDefault(columnName, Integer.MAX_VALUE);
            if (columnIndex != Integer.MAX_VALUE) {
                return columnIndex + 1;
            }
            throw new SQLException("Invalid column name");
        })).when(rs).findColumn(anyString());

        // mock rs.toString()
        doReturn(tag).when(rs).toString();

        // mock rs.getMetaData()
        doReturn(rsmd).when(rs).getMetaData();

        // mock rs.getType()
        doReturn(ResultSet.TYPE_FORWARD_ONLY).when(rs).getType();

        return rs;
    }

    /**
     * Creates the mock ResultSet.
     *
     * @param columnNames the names of the columns
     * @param data the data to be returned from the mocked ResultSet
     * @return a mocked ResultSet
     * @throws SQLException if building the mocked ResultSet fails
     */
    public static ResultSet create(final String tag, final String[] columnNames, final Object[][] data) throws SQLException {
        return new MockResultSet(tag, columnNames, data).buildMock();
    }

    /**
     * Creates the mock ResultSet.
     *
     * @param data the data to be returned from the mocked ResultSet
     * @return a mocked ResultSet
     * @throws SQLException if building the mocked ResultSet fails
     */
    public static ResultSet create(String tag, final Object[][] data) throws SQLException {
        return new MockResultSet(tag, null, data).buildMock();
    }

    /**
     * Creates the mock ResultSet.
     *
     * @param csv the data to be returned from the mocked ResultSet
     * @return a mocked ResultSet
     * @throws SQLException if building the mocked ResultSet fails
     */
    public static ResultSet create(final String tag, final String csv, boolean withLabels) throws SQLException {
        return create(tag, csv, withLabels, false);
    }

    /**
     * Creates the mock ResultSet.
     *
     * @param csv the data to be returned from the mocked ResultSet
     * @return a mocked ResultSet
     * @throws SQLException if building the mocked ResultSet fails
     */
    public static ResultSet create(final String tag, final String csv, boolean withLabels, boolean generated) throws SQLException {
        try (CSVReader csvReader = new CSVReader(new StringReader(csv))) {
            List<String[]> data = csvReader.readAll();
            final String[] columnNames = withLabels ? data.remove(0) : null;
            final MockResultSet mockResultSet = new MockResultSet(tag, columnNames, data.toArray(new Object[0][0]));
            mockResultSet.generated = generated;
            return mockResultSet.buildMock();
        } catch (IOException | CsvException ex) {
            logger.error("Cannot parse CSV {}", csv);
            throw new SQLException("Invalid data");
        }
    }

    /**
     * Creates the mock ResultSet.
     *
     * @param csv A CSV file containing the data, optionally with a header line
     * @return a mocked ResultSet
     * @throws SQLException in case of errors
     */
    public static ResultSet create(final String tag, final InputStream csv, boolean withLabels) throws SQLException {
        try (CSVReader csvReader = new CSVReader(new InputStreamReader(csv))) {
            List<String[]> data = csvReader.readAll();
            final String[] columnNames = withLabels ? data.remove(0) : null;
            return new MockResultSet(tag, columnNames, data.toArray(new Object[0][0])).buildMock();
        } catch (Exception ex) {
            logger.error("Cannot parse CSV {}", csv);
            throw new SQLException("Invalid data");
        }
    }

    /**
     * Creates the mock ResultSet.
     *
     * @param csvs the data to be returned from the mocked ResultSet
     * @return a mocked ResultSet
     * @throws SQLException if building the mocked ResultSet fails
     */
    public static ResultSet create(final String tag, final String labels, final String... csvs) throws SQLException {
        try (CSVReader csvReader1 = new CSVReader(new StringReader(labels))) {
            final String[] columnNames = csvReader1.readNext();
            List<String[]> data = new ArrayList<>();
            for (String csv : csvs) {
                try (CSVReader csvReader2 = new CSVReader(new StringReader(csv))) {
                    data.addAll(csvReader2.readAll());
                }
            }
            return new MockResultSet(tag, columnNames, data.toArray(new Object[0][0])).buildMock();
        } catch (IOException | CsvException ex) {
            logger.error("Cannot parse CSV {}", Arrays.asList(csvs));
            throw new SQLException("Invalid data");
        }
    }

    /**
     * Creates an empty mock ResultSet.
     *
     * @return a mocked ResultSet
     * @throws SQLException if building the mocked ResultSet fails
     */
    public static ResultSet empty(final String tag) throws SQLException {
        return new MockResultSet(tag, new String[]{}, new Object[][]{}).buildMock();
    }

    public static ResultSet broken(final String tag) throws SQLException {
        final MockResultSet mockResultSet = new MockResultSet(tag, new String[]{}, new Object[][]{});
        mockResultSet.generateData = false;
        mockResultSet.rowIndex = -2;
        return mockResultSet.buildMock();
    }

}
