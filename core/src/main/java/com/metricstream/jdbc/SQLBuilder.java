/*
 * Copyright © 2018-2020, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.math.BigDecimal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.codec.digest.DigestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper class around PreparedStatement
 *
 * This class helps creating parameterized SQL statements.  It allows to
 * aggregate the SQL statements in multiple steps.  Once the parameterized
 * statement is ready, it can be either converted into a ResultSet or it can
 * directly return values from the rows of the ResultSet.
 *
 * Parameters can be primitives (int, long, double), their object equivalents
 * (Integer, Long, Double), String, or collections.  When using a collection as
 * parameter, the corresponding placeholder (i.e. the ?) is automatically expanded
 * to have as many placeholders as the size of the collection.
 *
 * <pre>
 * {@code
 * SQLBuilder sb = new SQLBuilder("select a from b");
 * List<String> dList = new ArrayList<>(); dList.add("a"); dList.add("b");
 * sb.append("where c=? or d in (?)", 42, dList);
 * int a = sb.getInt(connection, 1, -1);
 * }
 * </pre>
 */
public class SQLBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SQLBuilder.class);

    protected final StringBuilder statement = new StringBuilder();
    protected final List<Object> arguments = new ArrayList<>();
    protected final Set<String> names = new HashSet<>();
    protected final Map<String, String> singleValuedNames = new HashMap<>();
    protected final Map<String, List<String>> multiValuedNames = new HashMap<>();
    private String delimiter = "";
    protected int resultSetType = ResultSet.TYPE_FORWARD_ONLY;
    protected int fetchSize = -1;
    protected int maxRows = -1;
    private static SQLBuilderProvider delegate = new JdbcSQLBuilderProvider();

    public static void setDelegate(SQLBuilderProvider delegate) {
        SQLBuilder.delegate = delegate;
    }

    /**
     * Simple wrapper class for "sensitive" data like full names, email addresses or SSNs.
     * When logging a SQLBuilder object, we will print the hash of sensitive values instead of the value itself
     * <pre>{@code
     * String secret = "oops!";
     * String bad = new SQLBuilder("select name from user where secret=?", secret).toString();
     * // produces "select name from user where secret=?; args = [oops!]"
     * String good = new SQLBuilder("select name from user where secret=?", SQLBuilder.mask(secret)).toString();
     * // produces "select name from user where secret=?; args = [__masked__:982c0381c279d139fd221fce974916e7]"
     * }</pre>
     */
    protected static final class Masked {
        protected final Object data;

        private Masked(Object data) {
            this.data = data;
        }

        @Override
        public String toString() {
            if (data == null) {
                return "null";
            }
            if (data.equals("")) {
                return "";
            }
            return "__masked__:" + DigestUtils.md5Hex(data.toString());
        }
    }

    public static Masked mask(Object data) {
        return new Masked(data);
    }

    /**
     * Creates a new SQBuilder object. The number of ? in the sql parameter
     * must be identical to the number of args
     * @param sql The initial SQL statement fragment
     * @param args The parameters for this fragment
     */
    public SQLBuilder(String sql, Object... args) {
        append(sql, args);
        delimiter = " ";
    }

    /**
     * Generates a shallow clone of a SQLBuilder object
     * @param sqlBuilder A SQLBuilder object
     */
    public SQLBuilder(SQLBuilder sqlBuilder) {
        resultSetType = sqlBuilder.resultSetType;
        fetchSize = sqlBuilder.fetchSize;
        maxRows = sqlBuilder.maxRows;
        append(sqlBuilder);
        delimiter = " ";
    }

    /**
     * Checks if this SQLBuilder object contains anything
     * @return true if it has no statement and no arguments, false otherwise
     */
    public boolean isEmpty() {
        return statement.length() == 0 && arguments.isEmpty();
    }

    /**
     * Checks if this SQLBuilder object contains anything
     * @return true if it has q statement or arguments, false otherwise
     */
    public boolean isNotEmpty() {
        return statement.length() != 0 || !arguments.isEmpty();
    }

    /**
     * Adds a binding for a name
     * @param name the name used in the statement (e.g. ${view} or #{view})
     * @param value the value for this name.  This must be a valid table, view, or column name.  The value
     *             will be quoted if necessary
     * @return the SQLBuilder object
     */
    public SQLBuilder bind(String name, String value) {
        addName(name);
        singleValuedNames.put(name, value);
        return this;
    }

    /**
     * Adds a binding for a name
     * @param name the name used in the statement (e.g. ${columns} or #{columns})
     * @param values a list of values for this name.  Each value must be a valid table, view, or column name.  The values
     *             will be quoted if necessary
     * @return the SQLBuilder object
     */
    public SQLBuilder bind(String name, List<String> values) {
        addName(name);
        multiValuedNames.put(name, values);
        return this;
    }

    /**
     * Adds one or more single-valued binding.  This is a shorthand for calling bind for every entry of the map.
     * @param bindings a map of name->value pairs
     * @return the SQLBuilder object
     */
    public SQLBuilder bind(Map<String, String> bindings) {
        addNames(bindings.keySet());
        singleValuedNames.putAll(bindings);
        return this;
    }

    /**
     * This applies all the bindings to the statement.
     * @return the SQLBuilder object
     */
    public SQLBuilder applyBindings() {
        interpolate(true);
        return this;
    }

    /**
     * Append a parameterized SQL fragment.  The original fragment and the new
     * fragment will be separated by a single space.
     * @param sql The SQL statement fragment
     * @param args The parameters for this fragment
     * @return The SQLBuilder object
     */
    public SQLBuilder append(String sql, Object... args) {
        statement.append(delimiter).append(sql);
        if (args != null && args.length != 0) {
            arguments.addAll(Arrays.asList(args));
        }
        return this;
    }

    /**
     * Appends another SQLBuilder object.  This appends both the fragment and shallow
     * copies of all parameters.  The original fragment and the new fragment
     * will be separated from the copies by a single space.
     * @param sqlBuilder The SQLBuilder object
     * @return The SQLBuilder object
     */
    public SQLBuilder append(SQLBuilder sqlBuilder) {
        addNames(sqlBuilder.names);
        singleValuedNames.putAll(sqlBuilder.singleValuedNames);
        multiValuedNames.putAll(sqlBuilder.multiValuedNames);
        arguments.addAll(sqlBuilder.arguments);
        statement.append(delimiter).append(sqlBuilder.statement);
        return this;
    }

    /**
     * Wraps the SQL statement fragment into () and prepends before.  A typical
     * use case is <pre></pre>sb.wrap("select count(*) from");</pre>
     * @param before The string which will be prepended
     * @return the SQLBuilder object
     */
    public SQLBuilder wrap(String before) {
        return wrap(before + " (", ")");
    }

    /**
     * Wraps the SQL statement fragment in before and after.  Unlike append,
     * there is no additional whitespace inserted between the original SQL
     * statement fragment and the arguments.
     * @param before The string which will be prepended
     * @param after The string which will be appended
     * @return the SQLBuilder object
     */
    public SQLBuilder wrap(String before, String after) {
        statement.insert(0, before).append(after);
        return this;
    }

    private void addNames(Collection<String> names) {
        for (String n : names) {
            addName(n);
        }
    }

    private void addName(String name) {
        if (!name.matches("\\w+")) {
            throw new IllegalArgumentException("The binding name \"" + name + "\" must only consist of word characters [a-zA-Z_0-9]");
        }
        if (!names.add(name)) {
            throw new IllegalArgumentException("The binding name \"" + name + "\" must be unique");
        }
    }

    protected String interpolate(boolean apply) {
        final Map<String, String> quoted = new HashMap<>(names.size());
        final StringJoiner regexp = new StringJoiner("|", "[:$]\\{(", ")\\}");

        for (Map.Entry<String, String> placeholder : singleValuedNames.entrySet()) {
            final String key = placeholder.getKey();
            quoted.put(key, nameQuote(placeholder.getValue()));
            regexp.add(key);
        }

        for (Map.Entry<String, List<String>> placeholder : multiValuedNames.entrySet()) {
            List<String> values = placeholder.getValue();
            List<String> q = new ArrayList<>(values.size());
            for (String v : values) {
                q.add(nameQuote(v));
            }
            final String key = placeholder.getKey();
            quoted.put(key, String.join(", ", q));
            regexp.add(key);
        }

        final StringBuffer sb = new StringBuffer();
        if (!quoted.isEmpty()) {
            final Pattern p = Pattern.compile(regexp.toString());
            final Matcher m = p.matcher(statement.toString());
            while (m.find()) {
                m.appendReplacement(sb, Matcher.quoteReplacement(quoted.get(m.group(1))));
            }
            m.appendTail(sb);
        } else {
            sb.append(statement);
        }

        if (!apply) {
            return sb.append("; args=").append(arguments).toString();
        }

        names.clear();
        singleValuedNames.clear();
        multiValuedNames.clear();
        statement.setLength(0);
        statement.append(sb);
        return null;
    }

    /**
     * This changes the ResultSet type from TYPE_FORWARD_ONLY to TYPE_SCROLL_INSENSITIVE
     * @return The SQLBuilder object
     */
    public SQLBuilder randomAccess() {
        resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
        return this;
    }

    /**
     * This changes the fetch size used for ResultSets.
     * @param fetchSize The new fetchSize. A value of -1 means that no fetch size is applied to the statement and that
     *                 the JDBC driver will use some default value
     * @return the SQLBuilder object
     */
    public SQLBuilder withFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * This changes the fetch size used for ResultSets.
     * @param maxRows The maximum number of rows to be returned. A value of -1 means that no maximum is applied to the statement and that
     *                 the JDBC driver will return all rows.
     * @return the SQLBuilder object
     */
    public SQLBuilder withMaxRows(int maxRows) {
        this.maxRows = maxRows;
        return this;
    }

    /**
     * Returns a ResultSet object created from a PreparedStatement object created using
     * the SQL statement and the parameters.  The PreparedStatement
     * will be automatically closed when the ResultSet is closed.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @return The ResultSet object
     * @throws SQLException the exception thrown when generating the ResultSet object
     */
    public ResultSet getResultSet(Connection connection) throws SQLException {
        return getResultSet(connection, false);
    }

    /**
     * Returns a ResultSet object created from a PreparedStatement object created using
     * the SQL statement and the parameters.  The PreparedStatement object
     * and the Connection object will be automatically closed when the ResultSet object is closed.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @return The ResultSet object
     * @throws SQLException the exception thrown when generating the ResultSet object
     */
    public ResultSet getResultSet(Connection connection, boolean wrapConnection) throws SQLException {
        logger.debug("{}", this);
        return delegate.getResultSet(this, connection, wrapConnection);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public int getInt(Connection connection, int columnNumber, int defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getInt(this, connection, columnNumber, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public int getInt(Connection connection, String columnName, int defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getInt(this, connection, columnName, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public long getLong(Connection connection, int columnNumber, long defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getLong(this, connection, columnNumber, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public long getLong(Connection connection, String columnName, long defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getLong(this, connection, columnName, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public String getString(Connection connection, int columnNumber, String defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getString(this, connection, columnNumber, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public BigDecimal getBigDecimal(Connection connection, String columnName, BigDecimal defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getBigDecimal(this, connection, columnName, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public BigDecimal getBigDecimal(Connection connection, int columnNumber, BigDecimal defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getBigDecimal(this, connection, columnNumber, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public String getString(Connection connection, String columnName, String defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getString(this, connection, columnName, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public Object getObject(Connection connection, int columnNumber, Object defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getObject(this, connection, columnNumber, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public Object getObject(Connection connection, String columnName, Object defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getObject(this, connection, columnName, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param columnNumber The index of the column (starting with 1) from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public OffsetDateTime getDateTime(Connection connection, int columnNumber, OffsetDateTime defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getDateTime(this, connection, columnNumber, defaultValue);
    }

    public Instant getInstant(Connection connection, int columnNumber, Instant defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getInstant(this, connection, columnNumber, defaultValue);
    }

    public Instant getInstant(Connection connection, String columnName, Instant defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getInstant(this, connection, columnName, defaultValue);
    }

    /**
     * Returns a value from the first row returned when executing the query.
     * @param connection The Connection object from which the PreparedStatement is created
     * @param columnName The name of the column from which to return the value
     * @param defaultValue The default value that is returned if the query did not return any rows
     * @return the value from the query
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public OffsetDateTime getDateTime(Connection connection, String columnName, OffsetDateTime defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getDateTime(this, connection, columnName, defaultValue);
    }

    /**
     * Executes the SQL statement.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @return The result of executeUpdate of that statement
     * @throws SQLException the exception thrown when executing the query
     */
    public int execute(Connection connection) throws SQLException {
        logger.debug("{}", this);
        return delegate.execute(this, connection);
    }

    /**
     * Executes the SQL statement.
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param keyColumns column names from the underlying table for which the inserted values will be returned.  Note that these names
     *               not necessarily have to be part of the columns into which the builder explicitly inserts values.
     * @return The result of executeUpdate of that statement
     * @throws SQLException the exception thrown when executing the query
     */
    public ResultSet execute(Connection connection, String... keyColumns) throws SQLException {
        logger.debug("{}", this);
        return delegate.execute(this, connection, keyColumns);
    }

    public interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    public static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called per row to produce a matching list item. Null values returned from the
     *           lambda are ignored
     * @return The list of generated items
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public <T> List<T> getList(Connection connection, RowMapper<T> rowMapper) throws SQLException {
        return getList(connection, rowMapper, false);
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called per row to produce a matching list item.
     * @param withNull If false, null values returned from the lambda are ignored.  Otherwise they
     *                 are added to the returned list
     * @return The list of generated items
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public <T> List<T> getList(Connection connection, RowMapper<T> rowMapper, boolean withNull) throws SQLException {
        logger.debug("{}", this);
        return delegate.getList(this, connection, rowMapper, withNull);
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called per row to produce a map entry. Null values returned from the mapper
     *           lambda are ignored. Duplicate keys result in overwriting the previous value
     * @return The list of generated items
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public <K, V> Map<K, V> getMap(Connection connection, RowMapper<Map.Entry<K, V>> rowMapper) throws SQLException {
        logger.debug("{}", this);
        return delegate.getMap(this, connection, rowMapper, false);
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called per row to produce a map entry. Null values returned from the mapper
     *           lambda are ignored. Duplicate keys result in overwriting the previous value
     * @param withNull If false, null values returned from the lambda are ignored.  Otherwise they
     *                 are added to the returned map
     * @return The list of generated items
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public <K, V> Map<K, V> getMap(Connection connection, RowMapper<Map.Entry<K, V>> rowMapper, boolean withNull) throws SQLException {
        logger.debug("{}", this);
        return delegate.getMap(this, connection, rowMapper, withNull);
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called one the first row to produce a matching item.
     * @return the Optional containing the item returned from the mapping lambda, if any
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public <T> Optional<T> getSingle(Connection connection, RowMapper<T> rowMapper) throws SQLException {
        logger.debug("{}", this);
        return delegate.getSingle(this, connection, rowMapper);
    }

    /**
     * Returns a list of objects generated from the ResultSet
     * @param connection The Connection object from which the PreparedStatement object is created
     * @param rowMapper The lambda called one the first row to produce a matching item.
     * @return the item returned from the mapping lambda, or the defaultValue if no row was returned
     * @throws SQLException the exception thrown when generating or accessing the ResultSet object
     */
    public <T> T getSingle(Connection connection, RowMapper<T> rowMapper, T defaultValue) throws SQLException {
        logger.debug("{}", this);
        return delegate.getSingle(this, connection, rowMapper, defaultValue);
    }

    /**
     * @param sql A query template to be used with QueryParams
     * @param params the values for the query template
     * @return the SQLBuilder generated from evaluating the template
     */
    @Deprecated()
    public static SQLBuilder fromNamedParams(String sql, QueryParams params) {
        return fromNumberedParameters(sql, params);
    }
    /**
     * @param sql A query template to be used with QueryParams
     * @param params the values for the query template
     * @return the SQLBuilder generated from evaluating the template
     */
    public static SQLBuilder fromNumberedParameters(String sql, QueryParams params) {
        final List<String> paramNames = params.getParamNames();
        if (paramNames == null || paramNames.isEmpty()) {
            return new SQLBuilder(sql);
        }

        final StringJoiner names = new StringJoiner("|");
        for (String name : paramNames) {
            names.add(String.format(":\\Q%s\\E\\b", name));
        }

        final Pattern p = Pattern.compile(names.toString());

        final List<Object> args = new ArrayList<>();
        // To avoid replacement within quotes, split the string with quote
        // replace with alternative tokens and join after replacement logic from
        // NpiUtil.getTokens
        final String[] tokens = sql.split("'");
        final StringBuilder queryBuilder = new StringBuilder();
        for (int i = 0; i < tokens.length; i++) {
            if (i % 2 == 0) {
                final Matcher m = p.matcher(tokens[i]);
                final StringBuffer sb = new StringBuffer();
                while (m.find()) {
                    // To check if parameter expects multiple values.  We cannot
                    // depend on parameter metadata since existing code depends
                    // on IN clause as part of the query and split the value
                    // considering comma as delimiter accordingly e.g. `select
                    // col1 from tab1 where col2 in(:1)`.  As part of parameter
                    // metadata update, parameter might not be chosen as multi,
                    // however it still works with existing code.
                    final String subStr = tokens[i].substring(0, m.start());
                    final boolean isMulti = subStr.matches("(?is).*\\bin\\s*\\(\\s*");
                    final boolean dateAsString = params.dateAsStringNeeded(subStr);
                    if (dateAsString) {
                        m.appendReplacement(sb, params.getDateParameterAsString());
                    } else {
                        m.appendReplacement(sb, "?");
                    }
                    // parameter name is prefixed with ':', so get correct name with trimming ':';
                    args.add(params.getParameterValue(m.group().substring(1), isMulti, dateAsString));
                }
                m.appendTail(sb);
                queryBuilder.append(sb);
            } else {
                // alternative tokens are from with-in single quotes.
                queryBuilder.append("'").append(tokens[i]).append("'");
            }
        }
        return new SQLBuilder(queryBuilder.toString(), args.toArray(new Object[0]));
    }

    /**
     * Throws an exception if a system object (e.g. table or column) name needs quoting
     * @param name The table, view, or column name
     * @return the name if quoting is not required
     * @throws IllegalArgumentException if the name needs quoting
     */
    public static String nameQuote(String name) throws IllegalArgumentException {
        return nameQuote(name, true);
    }

    /**
     * Quotes a system object name (e.g. table, column).
     *
     * @param name The table, view, or column name
     * @param noQuotes If true, don't quote.  Instead throw an exception if quoting would be necessary
     * @return string with "" around if necessary
     * @throws IllegalArgumentException if the name cannot be properly quoted or quoting would be required
     */
    public static String nameQuote(String name, boolean noQuotes) throws IllegalArgumentException {
        if (name == null) {
            throw new IllegalArgumentException("Object name is null");
        }
        if (name.matches("[A-Za-z][A-Za-z0-9_.]*|\"[^\"]+\"")) {
            return name;
        }
        // allow table and column aliases.  Both real name and alias must be quoted
        final String[] alias = name.split("\\s+(?i:as\\s+)?", 2);
        if (alias.length == 2) {
            // found alias.  column aliases have an optional " as " prefix but table aliases does not allow this.
            // Therefore, we do not include the " as " so that we don't have to distinguish the 2 cases.
            return nameQuote(alias[0], noQuotes) + " " + nameQuote(alias[1], noQuotes);
        }
        if (noQuotes || name.indexOf('"') != -1) {
            throw new IllegalArgumentException("Object name \"" + name + "\" contains invalid characters");
        }
        return "\"" + name + "\"";
    }

    /**
     * This function closes AutoCloseable objects.
     * @return true is all non-null resources could be closed, false otherwise
     */
    public static boolean close(AutoCloseable... resources) {
        boolean allClosed = true;
        if (resources != null) {
            for (AutoCloseable resource : resources) {
                if (resource != null) {
                    try {
                        resource.close();
                    } catch (Exception e) {
                        allClosed = false;
                        logger.error("Can't close {}", resource.getClass().getName(), e);
                    }
                }
            }
        }
        return allClosed;
    }

    // We need to close the implicitly created Statement from the getResultSet
    // method below.  Instead of asking the caller to remember this, we wrap the
    // ResultSet and do that for the caller.  This also allow to use
    // getResultSet in try expressions
    private static class WrappedResultSet implements InvocationHandler {
        private ResultSet rs;
        private final Scope scope;

        public enum Scope { ResultSet, Statement, Connection }

        public WrappedResultSet(ResultSet rs, Scope scope) {
            this.rs = rs;
            this.scope = scope;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // Warning: we have to go though the code below even is
            // rs.isClosed() is true because we still would need to close the
            // statement and connection.  Also, Oracle's ResultSet.next()
            // implicitly closes the ResultSet when it returns false (bypassing
            // this proxy method).
            if (rs == null) {
                return null;
            }
            if ("close".equals(method.getName())) {
                Statement stmt = null;
                Connection conn = null;
                switch (scope) {
                case Connection:
                    stmt = rs.getStatement();
                    if (stmt != null) {
                        conn = stmt.getConnection();
                    }
                    break;
                case Statement:
                    stmt = rs.getStatement();
                    break;
                }
                close(rs, stmt, conn);
                rs = null;
                return null;
            }
            try {
                return method.invoke(rs, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }

    /**
     * Returns a ResultSet that automatically closes the statement it was created from
     * @param rs The original ResultSet
     * @return The wrapped ResultSet
     */
    public static ResultSet wrapStatement(ResultSet rs) {
        return rs != null ? (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
                new Class[] { ResultSet.class }, new WrappedResultSet(rs, WrappedResultSet.Scope.Statement)) : null;
    }

    /**
     * Returns a ResultSet that automatically closes the statement and The Connection object it was created from
     * @param rs The original ResultSet
     * @return The wrapped ResultSet
     */
    public static ResultSet wrapConnection(ResultSet rs) {
        return rs != null ? (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
                new Class[] { ResultSet.class }, new WrappedResultSet(rs, WrappedResultSet.Scope.Connection)) : null;
    }

    /**
     * Returns a String representation for logging purposes. This will contain
     * both the SQL statement fragment and the parameters.
     * @return A String representation
     */
    @Override
    public String toString() {
        return interpolate(false);
    }
}
