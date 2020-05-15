/*
 * Copyright © 2020, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.MatcherAssert.assertThat;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


class SQLBuilderTest {

    private static final Connection mockConnection = Mockito.mock(Connection.class);
    private final SQLBuilder sqlBuilder = new SQLBuilder("SELECT 42 FROM DUAL");

    @BeforeAll
    static void beforeAll() {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider());
    }

    @BeforeEach
    void setUp() {
        MockSQLBuilderProvider.reset();
    }

    @Test
    void testMock() throws SQLException {
        ResultSet mrs = MockResultSet.create("", new String[] { "name", "age" },
                new Object[][] {
                        { "Alice", 20 },
                        { "Bob", 35 },
                        { "Charles", 50 }
                });
        MockSQLBuilderProvider.addResultSet(mrs);
        MockSQLBuilderProvider.addResultSet(MockResultSet.create("", new String[] { "key", "value" }, new Object[][] {}));
        MockSQLBuilderProvider.addResultSet(MockResultSet.empty(""));
        MockSQLBuilderProvider.addResultSet(MockResultSet.create("", new Object[][] { { "a" }, { "b" }}));

        SQLBuilder sb1 = new SQLBuilder("select name, age from friends where age > 18");
        try (ResultSet rs = sb1.getResultSet(mockConnection)) {
            int total = 0;
            while (rs.next()) {
                total += rs.getInt(2);
            }
            assertEquals(105, total);
        }

        SQLBuilder sb2 = new SQLBuilder("select value from lookup where key=?", "first");
        try (ResultSet rs = sb2.getResultSet(mockConnection)) {
            String value = null;
            if (rs.next()) {
                value = rs.getString("value");
            }
            assertNull(value);
        }

        SQLBuilder sb3 = new SQLBuilder("select value from lookup where key=?", "second");
        try (ResultSet rs = sb3.getResultSet(mockConnection)) {
            String value = null;
            if (rs.next()) {
                value = rs.getString("value");
            }
            assertNull(value);
        }

        SQLBuilder sb4 = new SQLBuilder("select value from ${table}").bind("table", "VN");
        try (ResultSet rs = sb4.getResultSet(mockConnection)) {
            if (rs.next()) {
                assertEquals("a", rs.getString(1));
            }
            if (rs.next()) {
                assertEquals("b", rs.getString("COLUMN1"));
            }
            assertFalse(rs.next());
        }

        SQLBuilder sb5 = new SQLBuilder("select count(*) from lookup");
        MockSQLBuilderProvider.setIntByColumnIndex((idx, def) -> 10);
        assertEquals(10, sb5.getInt(mockConnection, 1, 0));

        SQLBuilder sb6 = new SQLBuilder("select count(*) from lookup");
        assertEquals(10, sb6.getInt(mockConnection, 1, 0));

        MockSQLBuilderProvider.addResultSet(MockResultSet.create("", new Object[][] { { "a" }, { "b" }}));
        SQLBuilder sb7 = new SQLBuilder("select value from lookup where key = ?", 42);
        assertEquals("a", sb7.getString(mockConnection, 1, "default"));

        MockSQLBuilderProvider.addResultSet("", "Alice,20\nBob,35\nCharles,50");
        SQLBuilder sb8 = new SQLBuilder("select name, age from friends where age > 18");
        try (ResultSet rs = sb8.getResultSet(mockConnection)) {
            int total = 0;
            while (rs.next()) {
                total += rs.getInt(2);
            }
            assertEquals(105, total);
        }

        MockSQLBuilderProvider.addResultSet("", "name,age", "Alice,20\nBob,35\nCharles,50");
        SQLBuilder sb9 = new SQLBuilder("select name, age from friends where age > 18");
        try (ResultSet rs = sb9.getResultSet(mockConnection)) {
            long total = 0;
            while (rs.next()) {
                total += rs.getLong("age");
            }
            assertEquals(105L, total);
        }

        MockSQLBuilderProvider.addResultSet("friends",
                "name,age",
                "Alice,20",
                "Bob,35",
                "Charles,50"
        );
        SQLBuilder sb10 = new SQLBuilder("select age, name from friends where age > 18");
        try (ResultSet rs = sb10.getResultSet(mockConnection)) {
            long total = 0;
            while (rs.next()) {
                total += rs.getLong("AGE");
            }
            assertEquals(105L, total);
        }

        MockSQLBuilderProvider.addResultSet("read from CSV file", getClass().getResourceAsStream("sb11.csv"));
        SQLBuilder sb11 = new SQLBuilder("select USER_ID, FIRST_NAME, LAST_NAME, DEPARTMENT from si_users_t");
        assertEquals("[100000, 100001, 100002, 100003]", sb11.getList(mockConnection, rs -> rs.getLong("USER_ID")).toString());

        // SI_USERS_T.csv was produced via SQLDeveloper using "Export as csv" from right-click on the table
        MockSQLBuilderProvider.addResultSet("read from sqldeveloper export file", getClass().getResourceAsStream("SI_USERS_T.csv"));
        SQLBuilder sb12 = new SQLBuilder("select USER_ID, FIRST_NAME, LAST_NAME, DEPARTMENT from si_users_t");
        assertEquals("[100000, 100001, 100002, 100003]", sb12.getList(mockConnection, rs -> rs.getLong("USER_ID")).toString());

        assertEquals(42, new SQLBuilder("update foo").execute(mockConnection));

        MockSQLBuilderProvider.setExecute(1);
        assertEquals(1, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(1, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(1, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(1, new SQLBuilder("update foo").execute(mockConnection));

        MockSQLBuilderProvider.setExecute(1, 0, 1);
        assertEquals(1, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(0, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(1, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(42, new SQLBuilder("update foo").execute(mockConnection));

        final AtomicInteger count = new AtomicInteger();
        MockSQLBuilderProvider.setExecute(() -> count.getAndIncrement() < 3 ? 1 : 2);
        assertEquals(1, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(1, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(1, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(2, new SQLBuilder("update foo").execute(mockConnection));
        assertEquals(2, new SQLBuilder("update foo").execute(mockConnection));

    }

    @Test
    void yesForever() throws SQLException {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider(true));

        SQLBuilder sb1 = new SQLBuilder("select count(*) from lookup");

        try (ResultSet rs = sb1.getResultSet(mockConnection)) {
            assertTrue(rs.next());
            assertFalse(rs.next());
        }

        try (ResultSet rs = sb1.getResultSet(mockConnection)) {
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }

    @Test
    void noForever() throws SQLException {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider(false));

        SQLBuilder sb1 = new SQLBuilder("select count(*) from lookup");

        try (ResultSet rs = sb1.getResultSet(mockConnection)) {
            assertFalse(rs.next());
        }

        try (ResultSet rs = sb1.getResultSet(mockConnection)) {
            assertFalse(rs.next());
        }
    }

    @Test
    void emptyForever() throws SQLException {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider());
        MockSQLBuilderProvider.addResultSet(MockResultSet.empty(""));

        SQLBuilder sb = new SQLBuilder("select count(*) from lookup");

        try (ResultSet rs = sb.getResultSet(mockConnection)) {
            assertFalse(rs.next());
        }

        try (ResultSet rs = sb.getResultSet(mockConnection)) {
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }

    @Test
    void getList_test1() throws Exception {
        // This is not so nice: `getList` only really works with resultsets and not with provider functions because these do not
        // participate in the `while (rs.next())` looping around the RowMapper that `getList` uses.  What could be nicer is a
        // projection (e.g. `addResultColumn(2, new int[] { 3, 1, 4})` or a limited provider function what affects `rs.next()`.
        // But then something like `sb.getList(conn, rs -> new Foo(rs.getLong(1), rs.getString(2)));` still would not work. So
        // perhaps this whole "provider functions" idea is bogus...

        // when query returns 3 rows
        // with 3 ints in column 2
        // then expect to get a list with 3 elements in the correct order
        MockSQLBuilderProvider.addResultSet("", "_,3\n_,1\n_,4");
        List<Integer> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getInt(2));
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(3, 1, 4)));
    }

    @Test
    void getList_test2() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null in column 2
        // then expect to get a list with 3 elements in the correct order
        MockSQLBuilderProvider.addResultSet("", new Object[][] {{"", 3}, {"", null}, {"", 4}});
        List<Integer> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getInt(2));
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(3, 0, 4)));
    }

    @Test
    void getList_test4() throws Exception {
        // when query returns 3 rows
        // with 2 Strings and a null in column 2
        // then expect to get a list with 2 elements in the correct order
        MockSQLBuilderProvider.addResultSet("", new Object[][] {{"", "first"}, {"", null}, {"", "third"}});
        List<String> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getString(2));
        assertThat(l.size(), is(2));
        assertThat(l, is(Arrays.asList("first", "third")));
    }

    @Test
    void getList_test5() throws Exception {
        // when query returns 3 rows
        // with 2 Strings and a null in column 2
        // then expect to get a list with 3 elements in the correct order
        MockSQLBuilderProvider.addResultSet("", new Object[][] {{"", "first"}, {"", null}, {"", "third"}});
        List<String> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getString(2), true);
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList("first", null, "third")));
    }

    @Test
    void getList_test6() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null (converted to 0 by getInt()) in column 2
        // then expect to get a list with 3 elements in the correct order
        MockSQLBuilderProvider.addResultSet("", new Object[][] {{"", 1}, {"", null}, {"", 3}});
        List<Integer> l = sqlBuilder.getList(mockConnection, (rs) -> { int i = rs.getInt(2); return rs.wasNull() ? -1 : i;});
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(1, -1, 3)));
    }

    @Test
    void getList_test7() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null (converted to 0 by getInt()) in column 2
        // then expect to get a list with 2 elements in the correct order.  We must avoid
        // calling `getInt` here because that automatically converts null to 0.
        MockSQLBuilderProvider.addResultSet("", new Object[][] {{"", 1}, {"", null}, {"", 3}});
        List<Integer> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getObject(2))
                .stream().map(i -> (Integer) i).collect(Collectors.toList());
        assertThat(l.size(), is(2));
        assertThat(l, is(Arrays.asList(1, 3)));
    }

    @Test
    void getList_test8() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null in column 2
        // then expect to get 3 elements with null mapped to null
        MockSQLBuilderProvider.addResultSet("", new Object[][] {{"", 1}, {"", null}, {"", 3}});
        List<Integer> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getObject(2), true)
                .stream().map(i -> (Integer) i).collect(Collectors.toList());
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(1, null, 3)));
    }

    @Test
    void getSingle_test1() throws SQLException {
        // when query returns 3 rows
        // with 3 longs in column 1
        // then expect to get the first element
        MockSQLBuilderProvider.addResultSet("", new Object[][] {{3}, {1}, {4}});
        Optional<Long> l = sqlBuilder.getSingle(mockConnection, (rs) -> rs.getLong(1));
        assertThat(l.isPresent(), is(true));
        assertThat(l.get(), is(3L));
    }

    @Test
    void getString_test1() throws SQLException {
        // when query returns 3 rows
        // with 3 Strings in column 1
        // then expect to get the first element
        MockSQLBuilderProvider.addResultSet("", "first\nsecond\nthird");
        String s = sqlBuilder.getString(mockConnection, 1, "default");
        assertThat(s, is("first"));
    }

    @Test
    void getString_test2() throws SQLException {
        // when query returns 1 row
        // with 3 Strings in column 1
        // then expect to get the first element
        MockSQLBuilderProvider.addResultSet("", "first");
        String s = sqlBuilder.getString(mockConnection, 1, "default");
        assertThat(s, is("first"));
    }

    @Test
    void getString_test3() throws SQLException {
        // when query returns no rows
        // then expect to get the default element
        MockSQLBuilderProvider.addResultSet(MockResultSet.empty(""));
        String s = sqlBuilder.getString(mockConnection, 1, "default");
        assertThat(s, is("default"));
    }

    @Test
    void getString_test4() throws SQLException {
        // when query returns no rows
        // then expect to get the default element even if that is null
        MockSQLBuilderProvider.addResultSet(MockResultSet.empty(""));
        String s = sqlBuilder.getString(mockConnection, 1, null);
        assertThat(s, nullValue());
    }

    @Test
    void getString_test5() throws SQLException {
        // when query returns 3 rows
        // with 3 Strings in column "LABEL"
        // then expect to get the first element
        MockSQLBuilderProvider.addResultSet("", "LABEL", "first\nsecond\nthird");
        String s = sqlBuilder.getString(mockConnection, "LABEL", "default");
        assertThat(s, is("first"));
    }


    @Test
    void getSingle_test2() throws SQLException {
        // when query returns no rows
        // then expect to get an empty optional
        MockSQLBuilderProvider.addResultSet(MockResultSet.empty(""));
        Optional<Long> l = sqlBuilder.getSingle(mockConnection, (rs) -> rs.getLong(1));
        assertThat(l.isPresent(), is(false));
    }

    @Test
    void getResultSet_test1() throws Exception {
        // when query returns 3 rows
        // with 3 ints in column 2
        // then expect to get a resultset that returns 3 rows in correct order
        MockSQLBuilderProvider.addResultSet("", "_,3\n_,1\n_,4");
        ResultSet rs = sqlBuilder.getResultSet(mockConnection);
            List<Integer> l = new ArrayList<>();
            while (rs.next()) {
                l.add(rs.getInt(2));
            }
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(3, 1, 4)));
    }

    @Test
    void getResultSet_test2() throws Exception {
        // when query returns 1 row
        // with 1 long in column 3
        // then expect to get a resultset that returns 1 row
        MockSQLBuilderProvider.addResultSet("", new Object[][] {{"", "", 3L}});
        ResultSet rs = sqlBuilder.getResultSet(mockConnection);
        List<Long> l = new ArrayList<>();
        while (rs.next()) {
            l.add(rs.getLong(3));
        }
        assertThat(l.size(), is(1));
        assertThat(l.get(0), is(3L));
    }

    @Test
    void getResultSet_test3() throws Exception {
        // when query returns no rows
        // then expect to get a resultset that returns no row
        MockSQLBuilderProvider.addResultSet(MockResultSet.empty(""));
        ResultSet rs = sqlBuilder.getResultSet(mockConnection);
        assertThat(rs.next(), is(false));
    }

    @Test
    void placeholder_dollar() {
        SQLBuilder sb = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        assertEquals("select a, ${b} from ${t} where x > ?; args=[5]", sb.toString());
        sb.bind("b", "BCOL").bind("t", "table1");
        assertEquals("select a, BCOL from table1 where x > ?; args=[5]", sb.toString());
    }

    @Test
    void placeholder_colon() {
        SQLBuilder sb = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        assertEquals("select a, :{b} from :{t} where x > ?; args=[5]", sb.toString());
        sb.bind("b", "BCOL").bind("t", "table1");
        assertEquals("select a, BCOL from table1 where x > ?; args=[5]", sb.toString());
    }

    @Test
    void multiPlaceholder_dollar() {
        SQLBuilder sb = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        assertEquals("select a, ${b} from ${t} where x > ?; args=[5]", sb.toString());
        sb.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb.toString());
    }

    @Test
    void multiPlaceholder_colon() {
        SQLBuilder sb = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        assertEquals("select a, :{b} from :{t} where x > ?; args=[5]", sb.toString());
        sb.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb.toString());
    }

    @Test
    void invalidPlaceholder_dollar() {
        SQLBuilder sb = new SQLBuilder("select a, ${b+} from ${t} where x > ?", 5);
        assertEquals("select a, ${b+} from ${t} where x > ?; args=[5]", sb.toString());
        assertThrows(IllegalArgumentException.class, () -> sb.bind("b+", "BCOL"));
    }

    @Test
    void invalidPlaceholder_colon() {
        SQLBuilder sb = new SQLBuilder("select a, :{b+} from :{t} where x > ?", 5);
        assertEquals("select a, :{b+} from :{t} where x > ?; args=[5]", sb.toString());
        assertThrows(IllegalArgumentException.class, () -> sb.bind("b+", "BCOL"));
    }

    @Test
    void repeatedPlaceholder() {
        assertThrows(IllegalArgumentException.class, () -> new SQLBuilder("").bind("a", "first").bind("a", "second"));
    }

    @Test
    void multiBuilder_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        SQLBuilder sb2 = new SQLBuilder(sb1);
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb2.toString());
    }

    @Test
    void multiBuilder_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        SQLBuilder sb2 = new SQLBuilder(sb1);
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb2.toString());
    }

    @Test
    void repeatedPlaceholder2() {
        SQLBuilder sb1 = new SQLBuilder("").bind("a", "first");
        SQLBuilder sb2 = new SQLBuilder("").bind("a", "first");
        assertThrows(IllegalArgumentException.class, () -> sb1.append(sb2));
    }

    @Test
    void repeatedPlaceholder3_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        SQLBuilder sb2 = new SQLBuilder("select ${b} from (").append(sb1).append(")");
        assertEquals("select BCOL, CCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]", sb2.toString());
    }

    @Test
    void repeatedPlaceholder3_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        SQLBuilder sb2 = new SQLBuilder("select :{b} from (").append(sb1).append(")");
        assertEquals("select BCOL, CCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]", sb2.toString());
    }

    @Test
    void repeatedPlaceholder4_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        SQLBuilder sb2 = new SQLBuilder("select ${b} from (").append(sb1).append(")");
        assertThrows(IllegalArgumentException.class, () -> sb2.bind("b", "BCOL"));
    }

    @Test
    void repeatedPlaceholder4_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        SQLBuilder sb2 = new SQLBuilder("select :{b} from (").append(sb1).append(")");
        assertThrows(IllegalArgumentException.class, () -> sb2.bind("b", "BCOL"));
    }

    @Test
    void repeatedPlaceholder5_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        SQLBuilder sb2 = new SQLBuilder("select ${b} from (").append(sb1.applyBindings()).append(")").bind("b", "BCOL");
        assertEquals("select BCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]", sb2.toString());
    }

    @Test
    void repeatedPlaceholder5_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        SQLBuilder sb2 = new SQLBuilder("select :{b} from (").append(sb1.applyBindings()).append(")").bind("b", "BCOL");
        assertEquals("select BCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]", sb2.toString());
    }

    @Test
    void repeatedPlaceholder6_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        String sb1s = sb1.toString();
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        sb1.applyBindings();
        assertEquals(sb1s, sb1.toString());
        SQLBuilder sb2 = new SQLBuilder("select ${b} from (").append(sb1).append(")").bind("b", "BCOL");
        assertEquals("select BCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]", sb2.toString());
    }

    @Test
    void repeatedPlaceholder6_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", Arrays.asList("BCOL", "CCOL")).bind("t", "table1");
        String sb1s = sb1.toString();
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", sb1.toString());
        sb1.applyBindings();
        assertEquals(sb1s, sb1.toString());
        SQLBuilder sb2 = new SQLBuilder("select :{b} from (").append(sb1).append(")").bind("b", "BCOL");
        assertEquals("select BCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]", sb2.toString());
    }

    @Test
    void testFromNumberedParams() {
        QueryParams params = new QueryParamsImpl();
        String query = "select BLUEPRINT_NAME, UPDATED_BY, UPDATION_DATE, METRIC_ID, BLUEPRINT_CODE,DEPLOYMENT_READY as dummy_code from table(ms_apps_utilities.GET_BLUEPRINT_LIST(:1))";
        SQLBuilder sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("select BLUEPRINT_NAME, UPDATED_BY, UPDATION_DATE, METRIC_ID, BLUEPRINT_CODE,DEPLOYMENT_READY as dummy_code from table(ms_apps_utilities.GET_BLUEPRINT_LIST(?)); args=[a]", sb.toString());

        query = "select DISPLAY_NAME, MODULE, UPDATED_BY, UPDATION_DATE, METRIC_ID, DB_TABLE_NAME from table(ms_apps_utilities.GET_DATA_OBJECT_LIST(:1, :2))";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("select DISPLAY_NAME, MODULE, UPDATED_BY, UPDATION_DATE, METRIC_ID, DB_TABLE_NAME from table(ms_apps_utilities.GET_DATA_OBJECT_LIST(?, ?)); args=[a, b]", sb.toString());

        query = "select DISPLAY_NAME, MODULE, UPDATED_BY, UPDATION_DATE, METRIC_ID, DB_TABLE_NAME from table(ms_apps_utilities.GET_DATA_OBJECT_LIST(:2, :1, :2))";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("select DISPLAY_NAME, MODULE, UPDATED_BY, UPDATION_DATE, METRIC_ID, DB_TABLE_NAME from table(ms_apps_utilities.GET_DATA_OBJECT_LIST(?, ?, ?)); args=[b, a, b]", sb.toString());

        query = "SELECT VE_TITLE, VE_MODULE, UPDATED_BY, UPDATION_DATE, METRIC_ID,VE_PUSHFORM_NAME, VE_DEPLOYED FROM TABLE(MS_APPS_UTILITIES.GET_FORM_LIST(:1, :2))";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("SELECT VE_TITLE, VE_MODULE, UPDATED_BY, UPDATION_DATE, METRIC_ID,VE_PUSHFORM_NAME, VE_DEPLOYED FROM TABLE(MS_APPS_UTILITIES.GET_FORM_LIST(?, ?)); args=[a, b]", sb.toString());

        query = "select count(1) from MS_APPS_MAM_SETUP where upper(profile_name) = upper(:1)";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("select count(1) from MS_APPS_MAM_SETUP where upper(profile_name) = upper(?); args=[a]", sb.toString());

        query = "select count(1), 'sdsd :3 ds' as dummy from MS_APPS_MAM_SETUP where upper(profile_name) = upper(:1) and dummy is not like 'jsjs :2 '";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("select count(1), 'sdsd :3 ds' as dummy from MS_APPS_MAM_SETUP where upper(profile_name) = upper(?) and dummy is not like 'jsjs :2 '; args=[a]", sb.toString());

        query = "select result_column_name||' ('||user_column_name||')' as display_column, column_name,result_column_name,user_column_name from si_metric_columns where metric_id = (select metric_id from si_metrics_t where metric_name = :1) and column_type = 1 order by result_column_name";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("select result_column_name||' ('||user_column_name||')' as display_column, column_name,result_column_name,user_column_name from si_metric_columns where metric_id = (select metric_id from si_metrics_t where metric_name = ?) and column_type = 1 order by result_column_name; args=[a]", sb.toString());

        query = "select (select ms_apps_utilities.GET_USER_FULL_NAME(column_value) from dual) as user_fullname, (select ms_apps_utilities.GET_USER_ID(column_value) from dual) as user_id, column_value as user_name from table(ms_apps_mam_engine_pkg.GET_new_USERS(:1,:2, :3)) t1";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("select (select ms_apps_utilities.GET_USER_FULL_NAME(column_value) from dual) as user_fullname, (select ms_apps_utilities.GET_USER_ID(column_value) from dual) as user_id, column_value as user_name from table(ms_apps_mam_engine_pkg.GET_new_USERS(?,?, ?)) t1; args=[a, b, c]", sb.toString());

        query = "SELECT a.installation_end_date as installedOn, decode(module_name, 'EGRCP', 'Platform', 'AppStudio','Platform', 'SMC', 'Platform', decode(a.parent_artifact, '0', 'App', 'Module')) as package_type, decode(a.ARTIFACT_TITLE,NULL, a.ARTIFACT_NAME,'null', a.ARTIFACT_NAME,a.ARTIFACT_TITLE) package_TITLE, a.VERSION, decode(a.EDITION, '', 'NA', a.EDITION) as edition, SMC_CONCAT(a.parent_artifact) as installedAsPartOf, a.type as installationType FROM SI_VERSION_INFO a where MODULE_NAME NOT IN (:1) AND (SHOW_ON_ABOUT_PAGE IS NULL OR SHOW_ON_ABOUT_PAGE != 'False' ) AND trunc(a.installation_end_date) >= trunc(:2) and trunc(a.installation_end_date) <= trunc(:3) order by a.installation_end_date desc";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("SELECT a.installation_end_date as installedOn, decode(module_name, 'EGRCP', 'Platform', 'AppStudio','Platform', 'SMC', 'Platform', decode(a.parent_artifact, '0', 'App', 'Module')) as package_type, decode(a.ARTIFACT_TITLE,NULL, a.ARTIFACT_NAME,'null', a.ARTIFACT_NAME,a.ARTIFACT_TITLE) package_TITLE, a.VERSION, decode(a.EDITION, '', 'NA', a.EDITION) as edition, SMC_CONCAT(a.parent_artifact) as installedAsPartOf, a.type as installationType FROM SI_VERSION_INFO a where MODULE_NAME NOT IN (?) AND (SHOW_ON_ABOUT_PAGE IS NULL OR SHOW_ON_ABOUT_PAGE != 'False' ) AND trunc(a.installation_end_date) >= trunc(?) and trunc(a.installation_end_date) <= trunc(?) order by a.installation_end_date desc; args=[[a, a, a, a], b, c]", sb.toString());

        query = "SELECT DISTINCT m.metric_title, M.METRIC_NAME, lookupmode.meaning run_mode,pack.package_title,m.package_id, M.METRIC_ID, Round((MAX(TO_CHAR(RS.RESULT_STORED, 'SSSSS.FF') - TO_CHAR(RS.REQUEST_TO_RUN, 'SSSSS.FF'))),2) MAX_RUN_TIME, Round((AVG(TO_CHAR(RS.RESULT_STORED, 'SSSSS.FF') - TO_CHAR(RS.REQUEST_TO_RUN, 'SSSSS.FF'))),2) AVG_RUN_TIME, COUNT(RS.INFOLET_ID) NO_OF_RUNS, MAX(RS.NUM_RECORDS) MAX_NUM_REC_FETCHED,Round((AVG(RS.NUM_RECORDS)),2) AVG_NUM_REC_FETCHED, INF_STAT.REQUEST_TO_RUN, DECODE(INF_STAT.SUCCESS_CODE, 0, 'Successful Run', -1,'Error Run', -2,'In Progress', -3, 'Skipped - No Data Change') RUN_STATUS, INF_STAT.NUM_RECORDS,RS.NODE_ID, :1 as numpastdays FROM SI_INFOLET_RUN_STATISTICS RS, SI_METRICS_T M, (SELECT MAX(INFOLET_RUN_ID) OVER (PARTITION BY INFOLET_ID ORDER BY REQUEST_TO_RUN DESC) AS MX_INFO_RUN_ID, INFOLET_ID,REQUEST_TO_RUN,RESULT_STORED, NUM_RECORDS, SUCCESS_CODE, INFOLET_RUN_ID AS INFO_RUN_ID FROM SI_INFOLET_RUN_STATISTICS) INF_STAT, si_register_application pack, si_lookups_t lookupmode WHERE INF_STAT.INFO_RUN_ID=INF_STAT.MX_INFO_RUN_ID AND RS.REQUEST_TO_RUN > sysdate - :1 and RS.INFOLET_ID = M.METRIC_ID AND RS.INFOLET_ID = INF_STAT.INFOLET_ID and M.METRIC_RUN_MODE in( :2) AND m.package_id = pack.module_id AND m.metric_run_mode = lookupmode.lookup_code AND lookupmode.lookup_type ='METRIC_RUN_MODE' GROUP BY RS.INFOLET_ID, M.METRIC_NAME, M.METRIC_ID, INF_STAT.REQUEST_TO_RUN, INF_STAT.SUCCESS_CODE,INF_STAT.NUM_RECORDS, RS.NODE_ID,m.metric_title,pack.package_title,m.package_id,lookupmode.meaning ORDER BY INF_STAT.REQUEST_TO_RUN DESC";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("SELECT DISTINCT m.metric_title, M.METRIC_NAME, lookupmode.meaning run_mode,pack.package_title,m.package_id, M.METRIC_ID, Round((MAX(TO_CHAR(RS.RESULT_STORED, 'SSSSS.FF') - TO_CHAR(RS.REQUEST_TO_RUN, 'SSSSS.FF'))),2) MAX_RUN_TIME, Round((AVG(TO_CHAR(RS.RESULT_STORED, 'SSSSS.FF') - TO_CHAR(RS.REQUEST_TO_RUN, 'SSSSS.FF'))),2) AVG_RUN_TIME, COUNT(RS.INFOLET_ID) NO_OF_RUNS, MAX(RS.NUM_RECORDS) MAX_NUM_REC_FETCHED,Round((AVG(RS.NUM_RECORDS)),2) AVG_NUM_REC_FETCHED, INF_STAT.REQUEST_TO_RUN, DECODE(INF_STAT.SUCCESS_CODE, 0, 'Successful Run', -1,'Error Run', -2,'In Progress', -3, 'Skipped - No Data Change') RUN_STATUS, INF_STAT.NUM_RECORDS,RS.NODE_ID, ? as numpastdays FROM SI_INFOLET_RUN_STATISTICS RS, SI_METRICS_T M, (SELECT MAX(INFOLET_RUN_ID) OVER (PARTITION BY INFOLET_ID ORDER BY REQUEST_TO_RUN DESC) AS MX_INFO_RUN_ID, INFOLET_ID,REQUEST_TO_RUN,RESULT_STORED, NUM_RECORDS, SUCCESS_CODE, INFOLET_RUN_ID AS INFO_RUN_ID FROM SI_INFOLET_RUN_STATISTICS) INF_STAT, si_register_application pack, si_lookups_t lookupmode WHERE INF_STAT.INFO_RUN_ID=INF_STAT.MX_INFO_RUN_ID AND RS.REQUEST_TO_RUN > sysdate - ? and RS.INFOLET_ID = M.METRIC_ID AND RS.INFOLET_ID = INF_STAT.INFOLET_ID and M.METRIC_RUN_MODE in( ?) AND m.package_id = pack.module_id AND m.metric_run_mode = lookupmode.lookup_code AND lookupmode.lookup_type ='METRIC_RUN_MODE' GROUP BY RS.INFOLET_ID, M.METRIC_NAME, M.METRIC_ID, INF_STAT.REQUEST_TO_RUN, INF_STAT.SUCCESS_CODE,INF_STAT.NUM_RECORDS, RS.NODE_ID,m.metric_title,pack.package_title,m.package_id,lookupmode.meaning ORDER BY INF_STAT.REQUEST_TO_RUN DESC; args=[a, a, [b, b, b, b]]", sb.toString());

        query = "SELECT col1, col2 from tab1 where col3 in (:1, :2)";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("SELECT col1, col2 from tab1 where col3 in (?, ?); args=[[a, a, a, a], b]", sb.toString());

        query = "SELECT col1, col2 from tab1 where col3 in (1, :2)";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("SELECT col1, col2 from tab1 where col3 in (1, ?); args=[b]", sb.toString());

        query = "SELECT col1, col2 from tab1 where col3 is not null";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals("SELECT col1, col2 from tab1 where col3 is not null; args=[]", sb.toString());

        query = "select A.CONTACT_NAME AS CONTACT_NAME, A.CONTACT_ID AS CONTACT_ID, A.REGULATOR AS REGULATOR from MS_REN_REGULATORY_CONTACT_DAT a, SI_LOCALES B, SI_USERS_T C\r\n"
                + "where a.LOCALE_ID = B.LOCALE_ID\r\n" + "and B.LOCALE_STRING = C.LOCALE\r\n"
                + "and C.USER_NAME =:1\r\n" + "and contact_id<>1\r\n" + "and a.regulator_id in (:2)";
        sb = SQLBuilder.fromNumberedParameters(query, params);
        assertEquals(
                "select A.CONTACT_NAME AS CONTACT_NAME, A.CONTACT_ID AS CONTACT_ID, A.REGULATOR AS REGULATOR from MS_REN_REGULATORY_CONTACT_DAT a, SI_LOCALES B, SI_USERS_T C\r\n"
                        + "where a.LOCALE_ID = B.LOCALE_ID\r\n" + "and B.LOCALE_STRING = C.LOCALE\r\n"
                        + "and C.USER_NAME =?\r\n" + "and contact_id<>1\r\n"
                        + "and a.regulator_id in (?); args=[a, [b, b, b, b]]",
                sb.toString());
    }

    @Test
    void maskData() {
        assertEquals("select name from user where secret=?; args=[__masked__:982c0381c279d139fd221fce974916e7]",
                new SQLBuilder("select name from user where secret=?", SQLBuilder.mask("oops!")).toString());
    }

    @Test
    void maskDataNull() {
        assertEquals("select name from user where secret=?; args=[null]",
                new SQLBuilder("select name from user where secret=?", SQLBuilder.mask(null)).toString());
    }

    @Test
    void maskDataEmpty() {
        assertEquals("select name from user where secret=?; args=[]",
                new SQLBuilder("select name from user where secret=?", SQLBuilder.mask("")).toString());
    }

    @Test
    void maskDataLong() {
        assertEquals("select name from user where secret=?; args=[__masked__:a1d0c6e83f027327d8461063f4ac58a6]",
                new SQLBuilder("select name from user where secret=?", SQLBuilder.mask(42L)).toString());
    }

    @Test
    void maskDataMixed() {
        assertEquals("select name from user where secret=? and public=?; args=[__masked__:982c0381c279d139fd221fce974916e7, ok]",
                new SQLBuilder("select name from user where secret=? and public=?", SQLBuilder.mask("oops!"), "ok").toString());
    }

    private String masked(Object value) {
        return new SQLBuilder("?", SQLBuilder.mask(value)).toString();
    }

    @Test
    void maskSame() {
        assertEquals(masked("hello"), masked("hello"));
        assertEquals(masked(42L), masked(Long.valueOf("42")));
        assertEquals(masked(42), masked(Long.valueOf("42")));
    }

    @Test
    void mockInt() throws SQLException {
        SQLBuilder sb = new SQLBuilder("select count(*) from foo");
        MockSQLBuilderProvider.setIntByColumnIndex((c, d) -> { switch (c) {case 1: return 3; default: return d;}});
        assertEquals(3, sb.getInt(null, 1, 4));
    }

    static class QueryParamsImpl implements QueryParams {

        // some arbitrary param values for testing
        final String[] values = {"a", "b", "c"};
        final List<String> names = IntStream.rangeClosed(1, values.length)
                .mapToObj(String::valueOf)
                .collect(Collectors.toList());

        @Override
        public Object getParameterValue(String name) {
            return getParameterValue(name, false );
        }

        @Override
        public Object getParameterValue(String name, boolean isMulti) {
            final String value = values[Integer.parseInt(name) - 1];
            if (isMulti) {
                String[] values = new String[4];
                Arrays.fill(values, value);
                return Arrays.asList(values);
            }
            return value;
        }

        @Override
        public Object getParameterValue(String name, boolean isMulti, boolean dateAsString) {
            return getParameterValue(name, isMulti);
        }

        @Override
        public List<String> getParamNames() {
            return names;
        }

        @Override
        public boolean dateAsStringNeeded(String subStr) {
            return false;
        }

        @Override
        public String getDateParameterAsString() {
            return null;
        }

    }
}
