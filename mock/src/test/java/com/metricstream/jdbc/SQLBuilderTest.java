/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import static com.metricstream.jdbc.MockSQLBuilderProvider.addResultSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


class SQLBuilderTest {

    private static final Connection mockConnection = Mockito.mock(Connection.class);
    private final SQLBuilder sqlBuilder = new SQLBuilder("SELECT 42 FROM DUAL");

    @BeforeAll
    static void beforeAll() {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider(true, true));
    }

    @AfterAll
    static void afterAll() {
        SQLBuilder.resetDelegate();
    }

    @AfterEach
    void afterEach() {
        MockSQLBuilderProvider.reset();
    }

    @Test
    void testMock() throws SQLException {
        ResultSet mrs = MockResultSet.create("testMock:sb1", new String[] { "name", "age" },
                new Object[][] {
                        { "Alice", 20 },
                        { "Bob", 35 },
                        { "Charles", 50 }
                }
        );
        addResultSet(mrs);
        addResultSet(MockResultSet.create("testMock:sb2", new String[] { "key", "value" }, new Object[][] {}));
        addResultSet(MockResultSet.empty("testMock:sb3"));
        addResultSet(MockResultSet.create("testMock:sb4", new Object[][] { { "a" }, { "b" } }));

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

        addResultSet(MockResultSet.create("testMock:sb7", new Object[][] { { "a" }, { "b" } }));
        SQLBuilder sb7 = new SQLBuilder("select value from lookup where key = ?", 42);
        assertEquals("a", sb7.getString(mockConnection, 1, "default"));

        addResultSet("testMock:sb8", "Alice,20\nBob,35\nCharles,50");
        SQLBuilder sb8 = new SQLBuilder("select name, age from friends where age > 18");
        try (ResultSet rs = sb8.getResultSet(mockConnection)) {
            int total = 0;
            while (rs.next()) {
                total += rs.getInt(2);
            }
            assertEquals(105, total);
        }

        addResultSet("testMock:sb9", "name,age", "Alice,20\nBob,35\nCharles,50");
        SQLBuilder sb9 = new SQLBuilder("select name, age from friends where age > 18");
        try (ResultSet rs = sb9.getResultSet(mockConnection)) {
            long total = 0;
            while (rs.next()) {
                total += rs.getLong("age");
            }
            assertEquals(105L, total);
        }

        addResultSet(
                "testMock:sb10",
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

        addResultSet("testMock:read from CSV file", getClass().getResourceAsStream("sb11.csv"));
        SQLBuilder sb11 = new SQLBuilder("select USER_ID, FIRST_NAME, LAST_NAME, DEPARTMENT from si_users_t");
        assertEquals("[100000, 100001, 100002, 100003]", sb11.getList(mockConnection, rs -> rs.getLong("USER_ID")).toString());

        // SI_USERS_T.csv was produced via SQLDeveloper using "Export as csv" from right-click on the table
        addResultSet("testMock:read from sqldeveloper export file", getClass().getResourceAsStream("SI_USERS_T.csv"));
        SQLBuilder sb12 = new SQLBuilder("select USER_ID, FIRST_NAME, LAST_NAME, DEPARTMENT from si_users_t");
        assertEquals("[100000, 100001, 100002, 100003]", sb12.getList(mockConnection, rs -> rs.getLong("USER_ID")).toString());

        Timestamp ts = Timestamp.from(Instant.now());
        addResultSet(MockResultSet.create("testMock:sb13", new Object[][] { { ts } }));
        SQLBuilder sb13 = new SQLBuilder("select value from lookup where key = ?", 42);
        assertEquals(ts, sb13.getTimestamp(mockConnection, 1, null));

        addResultSet(MockResultSet.create("testMock:sb14", new Object[][] { { ts } }));
        SQLBuilder sb14 = new SQLBuilder("select value from lookup where key = ?", 42);
        try (ResultSet rs14 = sb14.getResultSet(mockConnection)) {
            Timestamp ts14 = null;
            if (rs14.next()) {
                ts14 = rs14.getTimestamp(1);
            }
            assertEquals(ts, ts14);
        }

        addResultSet(MockResultSet.create("testMock:sb15", new String[] { "value" }, new Object[][] { { ts } }));
        SQLBuilder sb15 = new SQLBuilder("select value from lookup where key = ?", 42);
        try (ResultSet rs15 = sb15.getResultSet(mockConnection)) {
            Timestamp ts15 = null;
            if (rs15.next()) {
                ts15 = rs15.getTimestamp("value");
            }
            assertEquals(ts, ts15);
        }

        final SQLBuilder updateFoo = new SQLBuilder("update foo");
        assertEquals(42, updateFoo.execute(mockConnection));

        MockSQLBuilderProvider.setExecute(1);
        assertEquals(1, updateFoo.execute(mockConnection));
        assertEquals(1, updateFoo.execute(mockConnection));
        assertEquals(1, updateFoo.execute(mockConnection));
        assertEquals(1, updateFoo.execute(mockConnection));

        MockSQLBuilderProvider.setExecute(1, 0, 1);
        assertEquals(1, updateFoo.execute(mockConnection));
        assertEquals(0, updateFoo.execute(mockConnection));
        assertEquals(1, updateFoo.execute(mockConnection));
        assertEquals(42, updateFoo.execute(mockConnection));

        final AtomicInteger count = new AtomicInteger();
        MockSQLBuilderProvider.setExecute(() -> count.getAndIncrement() < 3 ? 1 : 2);
        assertEquals(1, updateFoo.execute(mockConnection));
        assertEquals(1, updateFoo.execute(mockConnection));
        assertEquals(1, updateFoo.execute(mockConnection));
        assertEquals(2, updateFoo.execute(mockConnection));
        assertEquals(2, updateFoo.execute(mockConnection));
    }

    @Test
    void reuseResultSetData1() throws SQLException {
        addResultSet(MockResultSet.create(
                "reuseResultSetData1",
                new String[] { "A", "B" },
                new Object[][] {
                        { 1, "hello" }
                },
                3
        ));

        assertThat(sqlBuilder.getList(mockConnection, rs -> rs.getInt(1)), is(List.of(1, 1, 1)));
    }

    @Test
    void reuseResultSetData2() throws SQLException {
        addResultSet(MockResultSet.create(
                "reuseResultSetData2",
                new String[] { "A", "B" },
                new Object[][] {
                        { 1, "hello" }
                },
                1
        ));

        assertThat(sqlBuilder.getList(mockConnection, rs -> rs.getInt(1)), is(List.of(1)));
    }

    @Test
    void reuseResultSetData3() throws SQLException {
        addResultSet(MockResultSet.create(
                "reuseResultSetData3",
                new String[] { "A", "B" },
                new Object[][] {
                        { 1, "hello" }
                }
        ));

        assertThat(sqlBuilder.getList(mockConnection, rs -> rs.getInt(1)), is(List.of(1)));
    }

    @Test
    void reuseResultSetData4() throws SQLException {
        addResultSet(MockResultSet.create(
                "reuseResultSetData4",
                new String[] { "A", "B" },
                new Object[][] {
                        { 1, "hello" },
                        { 2, "world" }
                },
                3
        ));

        assertThat(sqlBuilder.getList(mockConnection, rs -> rs.getInt(1)), is(List.of(1, 2, 1, 2, 1, 2)));
    }

    @Test
    void copyTest1() throws SQLException {
        // A resultset is consumed by a SQLBuilder `getResultSet` (or higher level callers like `getInt`). Therefore,
        // adding it once but trying to use it twice will not work.  Instead, the next usage will create a new
        // default mocked resultset
        ResultSet rs = MockResultSet.create("copyTest1", "A", "3");
        addResultSet(rs);
        assertEquals(3, sqlBuilder.getInt(mockConnection, 1, -1));
        assertEquals(42, sqlBuilder.getInt(mockConnection, 1, -1));
    }

    @Test
    void copyTest2() throws SQLException {
        // A resultset has an internal state which keeps track of the consumed rows.  Therefore, adding the same
        // resultset twice will not produce the same result.
        ResultSet rs = MockResultSet.create("copyTest2", "A", "3");
        addResultSet(rs);
        assertEquals(3, sqlBuilder.getInt(mockConnection, 1, -1));
        addResultSet(rs);
        assertEquals(-1, sqlBuilder.getInt(mockConnection, 1, -1));
    }

    @Test
    void brokenTest() throws SQLException {
        addResultSet(MockResultSet.broken("brokenTest"));
        assertThrows(SQLException.class, () -> new SQLBuilder("select A from T").getInt(mockConnection, 1, -1));
    }

    @Test
    void executeReturningTest() throws SQLException {
        addResultSet("executeReturningTest:id", "43");
        SQLBuilder sb = new SQLBuilder("insert into foo(foo_s.nextval, ?", "fooValue");
        ResultSet rs = sb.execute(mockConnection, "id");
        assertTrue(rs.next());
        assertEquals(43, rs.getInt(1));
    }

    @Test
    void unusedMockResultSet() throws SQLException {
        addResultSet("unusedMockResultSet:first", "1");
        addResultSet("unusedMockResultSet:second", "2");
        SQLBuilder sb1 = new SQLBuilder("select count(*) from foo");
        assertEquals(1, sb1.getInt(mockConnection, 1, 0));
    }

    @Test
    void testDateTime() throws SQLException {
        OffsetDateTime now = OffsetDateTime.now();
        addResultSet(MockResultSet.create("testDateTime", new Object[][] { { now } }));
        SQLBuilder sb1 = new SQLBuilder("select created from lookup where key = ?", 42);
        assertEquals(now, sb1.getDateTime(mockConnection, 1, null));

        SQLBuilder sb2 = new SQLBuilder("select created from lookup where key = ?", 42);
        final OffsetDateTime dt2 = sb2.getDateTime(mockConnection, 1, null);
        assertNotNull(dt2);
        // This will fail in about 3000 years.  Hopefully, mankind is still around by then but no longer uses JDBC...
        assertTrue(dt2.isAfter(now.plusYears(1000L)));

        SQLBuilder sb3 = new SQLBuilder("select created from lookup where key = ?", 42);
        final OffsetDateTime dt3 = sb3.getDateTime(mockConnection, "created", null);
        assertNotNull(dt3);
        // This will fail in about 3000 years.  Hopefully, mankind is still around by then but no longer uses JDBC...
        assertTrue(dt3.isAfter(now.plusYears(1000L)));
    }

    @Test
    void testInstant() throws SQLException {
        Instant now = Clock.systemUTC().instant();
        OffsetDateTime oNow = now.atOffset(ZoneOffset.UTC);
        addResultSet(MockResultSet.create("testInstant", new Object[][] { { oNow } }));
        SQLBuilder sb1 = new SQLBuilder("select created from lookup where key = ?", 42);
        assertEquals(now, sb1.getInstant(mockConnection, 1, null));

        SQLBuilder sb2 = new SQLBuilder("select created from lookup where key = ?", 42);
        final Instant dt2 = sb2.getInstant(mockConnection, 1, null);
        assertNotNull(dt2);
        // This will fail in about 1150 years.  Hopefully, mankind is still around by then but no longer uses JDBC...
        assertTrue(dt2.isAfter(now.plus(420_000L, ChronoUnit.DAYS)));

        SQLBuilder sb3 = new SQLBuilder("select created from lookup where key = ?", 42);
        final Instant dt3 = sb3.getInstant(mockConnection, "created", null);
        assertNotNull(dt3);
        // This will fail in about 1150 years.  Hopefully, mankind is still around by then but no longer uses JDBC...
        assertTrue(dt3.isAfter(now.plus(420_000L, ChronoUnit.DAYS)));
    }

    @Test
    void yesForever() throws SQLException {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider(true, true));

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
        try {
            SQLBuilder.setDelegate(new MockSQLBuilderProvider(false, true));

            SQLBuilder sb1 = new SQLBuilder("select count(*) from lookup");

            try (ResultSet rs = sb1.getResultSet(mockConnection)) {
                assertFalse(rs.next());
            }

            try (ResultSet rs = sb1.getResultSet(mockConnection)) {
                assertFalse(rs.next());
            }
        } finally {
            SQLBuilder.setDelegate(new MockSQLBuilderProvider(true, true));
        }
    }

    @Test
    void emptyForever() throws SQLException {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider());
        addResultSet(MockResultSet.empty(""));

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
    void getDouble1() throws SQLException {
        addResultSet(MockResultSet.create("getDouble1", "A", "123"));
        try (ResultSet rs = sqlBuilder.getResultSet(mockConnection)) {
            assertTrue(rs.next());
            assertEquals(123.0, rs.getDouble(1));
        }
    }

    @Test
    void getDouble2() throws SQLException {
        addResultSet(MockResultSet.create("getDouble2", "A", "123.456"));
        try (ResultSet rs = sqlBuilder.getResultSet(mockConnection)) {
            assertTrue(rs.next());
            assertEquals(123.456, rs.getDouble(1));
        }
    }

    @Test
    void getDouble3() throws SQLException {
        addResultSet(MockResultSet.create("getDouble3", new String[] { "A" }, new Object[][] { { 123.456 } }));
        try (ResultSet rs = sqlBuilder.getResultSet(mockConnection)) {
            assertTrue(rs.next());
            assertEquals(123.456, rs.getDouble(1));
        }
    }

    @Test
    void getDouble4() throws SQLException {
        addResultSet(MockResultSet.create("getDouble4", new String[] { "A" }, new Object[][] { { 123.456 } }));
        assertEquals(123.456, sqlBuilder.getDouble(mockConnection, 1, -1.0));
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
        addResultSet("", "_,3\n_,1\n_,4");
        List<Integer> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getInt(2));
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(3, 1, 4)));
    }

    @Test
    void getList_test2() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null in column 2
        // then expect to get a list with 3 elements in the correct order
        addResultSet("", new Object[][] { { "", 3 }, { "", null }, { "", 4 } });
        List<Integer> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getInt(2));
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(3, 0, 4)));
    }

    @Test
    void getList_test4() throws Exception {
        // when query returns 3 rows
        // with 2 Strings and a null in column 2
        // then expect to get a list with 2 elements in the correct order
        addResultSet("", new Object[][] { { "", "first" }, { "", null }, { "", "third" } });
        List<String> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getString(2));
        assertThat(l.size(), is(2));
        assertThat(l, is(Arrays.asList("first", "third")));
    }

    @Test
    void getList_test5() throws Exception {
        // when query returns 3 rows
        // with 2 Strings and a null in column 2
        // then expect to get a list with 3 elements in the correct order
        addResultSet("", new Object[][] { { "", "first" }, { "", null }, { "", "third" } });
        List<String> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getString(2), true);
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList("first", null, "third")));
    }

    @Test
    void getList_test6() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null (converted to 0 by getInt()) in column 2
        // then expect to get a list with 3 elements in the correct order
        addResultSet("", new Object[][] { { "", 1 }, { "", null }, { "", 3 } });
        List<Integer> l = sqlBuilder.getList(mockConnection, (rs) -> {
            int i = rs.getInt(2);
            return rs.wasNull() ? -1 : i;
        });
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(1, -1, 3)));
    }

    @Test
    void getList_test7() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null (converted to 0 by getInt()) in column 2
        // then expect to get a list with 2 elements in the correct order.  We must avoid
        // calling `getInt` here because that automatically converts null to 0.
        addResultSet("", new Object[][] { { "", 1 }, { "", null }, { "", 3 } });
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
        addResultSet("", new Object[][] { { "", 1 }, { "", null }, { "", 3 } });
        List<Integer> l = sqlBuilder.getList(mockConnection, (rs) -> rs.getObject(2), true)
                .stream().map(i -> (Integer) i).collect(Collectors.toList());
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(1, null, 3)));
    }

    @Test
    void getMap_test1() throws SQLException {
        // when query returns 3 rows
        // with 3 ints in column 2
        // then expect to get a list with 3 elements in the correct order
        addResultSet("", "3,Three\n1,One\n4,Four");
        Map<Integer, String> m = sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getInt(1), rs.getString(2)));
        assertThat(m.size(), is(3));
        assertThat(m.keySet(), containsInAnyOrder(3, 1, 4));
        assertTrue(m.containsValue("Three"));
    }

    @Test
    void getMap_testDuplicateKeys() throws SQLException {
        // when query returns 3 rows with duplicate keys
        // then expect to get an IllegalStateException
        addResultSet("", "3,Three\n1,One\n3,Four");
        assertThrows(IllegalStateException.class, () -> sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getInt(1), rs.getString(2))));
    }

    @Test
    void getMap_testNullKey() throws SQLException {
        // when query returns 3 rows with duplicate keys
        // then expect to get an IllegalStateException
        addResultSet("", new Object[][] { { 3, "Three" }, { null, "Zero" } });
        // Note: we cannot use `getInt` for the key here because that would automatically convert `null` to `0` and thus not throw the expected exception
        assertThrows(IllegalStateException.class, () -> sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getObject(1), rs.getString(2))));
    }

    @Test
    void getMap_test3() throws SQLException {
        // when query returns 3 rows
        // with 3 ints in column 2
        // then expect to get a list with 3 elements in the correct order
        addResultSet("", new Object[][] { { "1", 1 }, { "2", null }, { "3", 3 } });
        Map<String, Integer> m = sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getString(1), rs.getInt(2)));
        // size is 3 and not 2 although 2 is mapped to null because we use getInt which will automatically convert null to 0
        assertThat(m.size(), is(3));
        assertThat(m.keySet(), containsInAnyOrder("1", "2", "3"));
    }

    @Test
    void getMap_test4() throws SQLException {
        // when query returns 3 rows
        // with 3 ints in column 2
        // then expect to get a list with 3 elements in the correct order
        addResultSet("", new Object[][] { { "1", 1 }, { "2", null }, { "3", 3 } });
        Map<String, Integer> m = sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getString(1), rs.getInt(2)), false);
        assertThat(m.size(), is(3));
        assertThat(m.keySet(), containsInAnyOrder("1", "2", "3"));
    }

    @Test
    void getSingle_test1() throws SQLException {
        // when query returns 3 rows
        // with 3 longs in column 1
        // then expect to get the first element
        addResultSet("", new Object[][] { { 3L }, { 1L }, { 4L } });
        Optional<Long> l = sqlBuilder.getSingle(mockConnection, (rs) -> rs.getLong(1));
        assertThat(l.isPresent(), is(true));
        assertThat(l.get(), is(3L));
    }

    @Test
    void getString_test1() throws SQLException {
        // when query returns 3 rows
        // with 3 Strings in column 1
        // then expect to get the first element
        addResultSet("", "first\nsecond\nthird");
        String s = sqlBuilder.getString(mockConnection, 1, "default");
        assertThat(s, is("first"));
    }

    @Test
    void getString_test2() throws SQLException {
        // when query returns 1 row
        // with 3 Strings in column 1
        // then expect to get the first element
        addResultSet("", "first");
        String s = sqlBuilder.getString(mockConnection, 1, "default");
        assertThat(s, is("first"));
    }

    @Test
    void getString_test3() throws SQLException {
        // when query returns no rows
        // then expect to get the default element
        addResultSet(MockResultSet.empty(""));
        String s = sqlBuilder.getString(mockConnection, 1, "default");
        assertThat(s, is("default"));
    }

    @Test
    void getString_test4() throws SQLException {
        // when query returns no rows
        // then expect to get the default element even if that is null
        addResultSet(MockResultSet.empty(""));
        String s = sqlBuilder.getString(mockConnection, 1, null);
        assertThat(s, nullValue());
    }

    @Test
    void getString_test5() throws SQLException {
        // when query returns 3 rows
        // with 3 Strings in column "LABEL"
        // then expect to get the first element
        addResultSet("", "LABEL", "first\nsecond\nthird");
        String s = sqlBuilder.getString(mockConnection, "LABEL", "default");
        assertThat(s, is("first"));
    }


    @Test
    void getSingle_test2() throws SQLException {
        // when query returns no rows
        // then expect to get an empty optional
        addResultSet(MockResultSet.empty(""));
        Optional<Long> l = sqlBuilder.getSingle(mockConnection, (rs) -> rs.getLong(1));
        assertThat(l.isPresent(), is(false));
    }

    @Test
    void getResultSet_test1() throws Exception {
        // when query returns 3 rows
        // with 3 int values in column 2
        // then expect to get a ResultSet that returns 3 rows in correct order
        addResultSet("", "_,3\n_,1\n_,4");
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
        // with 1 long value in column 3
        // then expect to get a ResultSet that returns 1 row
        addResultSet("", new Object[][] { { "", "", 3L } });
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
        // then expect to get a ResultSet that returns no row
        addResultSet(MockResultSet.empty(""));
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
    void partialPlaceHolder() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).applyBindings();
        assertEquals("select a, BCOL, CCOL from table1 where x > ?; args=[5]", new SQLBuilder(sb1).bind("t", "table1").toString());
        assertEquals("select a, BCOL, CCOL from table2 where x > ?; args=[5]", new SQLBuilder(sb1).bind("t", "table2").toString());
    }

    @Test
    void testFromNumberedParams() {
        QueryParams params = new QueryParamsImpl();
        assertEquals("select n from t where i=?); args=[a]", SQLBuilder
                .fromNumberedParameters("select n from t where i=:1)", params).toString());
        assertEquals("select n from t where i=? or i=?); args=[a, b]", SQLBuilder
                .fromNumberedParameters("select n from t where i=:1 or i=:2)", params).toString());
        assertEquals("select n from t where i=? or i=?); args=[b, a]", SQLBuilder
                .fromNumberedParameters("select n from t where i=:2 or i=:1)", params).toString());
        assertEquals("select n from t where i=? or k=?); args=[b, b]", SQLBuilder
                .fromNumberedParameters("select n from t where i=:2 or k=:2)", params).toString());
        assertEquals("select n from t where i=? or k=':4'); args=[b]", SQLBuilder
                .fromNumberedParameters("select n from t where i=:2 or k=':4')", params).toString());
        assertEquals("select n from t where i=? or k=':2'); args=[b]", SQLBuilder
                .fromNumberedParameters("select n from t where i=:2 or k=':2')", params).toString());
    }

    @Test
    void maskData() {
        assertEquals(
                "select name from user where secret=?; args=[__masked__:982c0381c279d139fd221fce974916e7]",
                new SQLBuilder("select name from user where secret=?", SQLBuilder.mask("oops!")).toString()
        );
    }

    @Test
    void maskDataNull() {
        assertEquals(
                "select name from user where secret=?; args=[null]",
                new SQLBuilder("select name from user where secret=?", SQLBuilder.mask(null)).toString()
        );
    }

    @Test
    void maskDataEmpty() {
        assertEquals(
                "select name from user where secret=?; args=[]",
                new SQLBuilder("select name from user where secret=?", SQLBuilder.mask("")).toString()
        );
    }

    @Test
    void maskDataLong() {
        assertEquals(
                "select name from user where secret=?; args=[__masked__:a1d0c6e83f027327d8461063f4ac58a6]",
                new SQLBuilder("select name from user where secret=?", SQLBuilder.mask(42L)).toString()
        );
    }

    @Test
    void maskDataMixed() {
        assertEquals(
                "select name from user where secret=? and public=?; args=[__masked__:982c0381c279d139fd221fce974916e7, ok]",
                new SQLBuilder("select name from user where secret=? and public=?", SQLBuilder.mask("oops!"), "ok").toString()
        );
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
        MockSQLBuilderProvider.setIntByColumnIndex((c, d) -> c == 1 ? 3 : d);
        assertEquals(3, sb.getInt(null, 1, 4));
    }

    @Test
    void maxRows() throws SQLException {
        addResultSet("", "_,3\n_,1\n_,4");
        // TODO: this just tests that `withMaxRows` is accepted, but not the actual implementation.
        List<Integer> l = sqlBuilder.withMaxRows(1).getList(mockConnection, (rs) -> rs.getInt(2));
        assertThat(l.size(), is(3));
        assertThat(l, is(Arrays.asList(3, 1, 4)));
    }

    @Test
    void nameToIndexMapping() throws SQLException {
        addResultSet("", "columnA,columnB", "A,B");
        ResultSet rs = sqlBuilder.getResultSet(mockConnection);
        assertEquals(2, rs.findColumn("columnB"));
        assertEquals(2, rs.findColumn("COLUMNB"));
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("COLUMNB", rsmd.getColumnName(2));
    }

    @Test
    void nameQuote1() {
        assertEquals("columnA", SQLBuilder.nameQuote("columnA"));
        assertEquals("column_A", SQLBuilder.nameQuote("column_A"));
        assertEquals("COL_A", SQLBuilder.nameQuote("COL_A"));
        assertEquals("COL1", SQLBuilder.nameQuote("COL1"));
    }

    @Test
    void nameQuote2() {
        assertThrows(IllegalArgumentException.class, () -> SQLBuilder.nameQuote("column\"A"));
    }

    @Test
    void nameQuote3() {
        assertThrows(IllegalArgumentException.class, () -> SQLBuilder.nameQuote("column+A"));
        assertEquals("\"column+A\"", SQLBuilder.nameQuote("column+A", false));
        assertEquals("\"column;A\"", SQLBuilder.nameQuote("column;A", false));
    }

    @Test
    void nameQuote4() {
        assertEquals("columnA A", SQLBuilder.nameQuote("columnA A", false));
        assertEquals("\"column;A\" A", SQLBuilder.nameQuote("column;A A", false));
        assertEquals("\"column;A\" \"A+B\"", SQLBuilder.nameQuote("column;A A+B", false));
    }

    static class QueryParamsImpl implements QueryParams {

        // some arbitrary param values for testing
        final String[] values = { "a", "b", "c" };
        final List<String> names = IntStream.rangeClosed(1, values.length)
                .mapToObj(String::valueOf)
                .collect(Collectors.toList());

        @Override
        public Object getParameterValue(String name) {
            return getParameterValue(name, false);
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
