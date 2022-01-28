/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package com.metricstream.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.sql.Connection;
import java.sql.Date;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.ThrowableTypeAssert;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;


@ExtendWith(MockSQLBuilderExtension.class)
class SQLBuilderTestJava {

    private static final Connection mockConnection = Mockito.mock(Connection.class);
    private final SQLBuilder sqlBuilder = new SQLBuilder("SELECT 42 FROM DUAL");

    private static ThrowableTypeAssert<SQLException> assertThatSQLException() {
        return assertThatExceptionOfType(SQLException.class);
    }

    @Test
    void testMock() throws SQLException {
        MockResultSet.add(
                "testMock:sb1",
                new String[] { "name", "age" },
                new Object[][] {
                        { "Alice", 20 },
                        { "Bob", 35 },
                        { "Charles", 50 }
                }
        );
        MockResultSet.add("testMock:sb2", new String[] { "key", "value" }, new Object[][] {});
        MockResultSet.addEmpty("testMock:sb3");
        MockResultSet.add("testMock:sb4", new Object[][] { { "a" }, { "b" } });

        SQLBuilder sb1 = new SQLBuilder("select name, age from friends where age > 18");
        try (ResultSet rs = sb1.getResultSet(mockConnection)) {
            int total = 0;
            while (rs.next()) {
                total += rs.getInt(2);
            }
            assertThat(total).isEqualTo(105);
        }

        SQLBuilder sb2 = new SQLBuilder("select value from lookup where key=?", "first");
        try (ResultSet rs = sb2.getResultSet(mockConnection)) {
            String value = null;
            if (rs.next()) {
                value = rs.getString("value");
            }
            assertThat(value).isNull();
        }

        SQLBuilder sb3 = new SQLBuilder("select value from lookup where key=?", "second");
        try (ResultSet rs = sb3.getResultSet(mockConnection)) {
            String value = null;
            if (rs.next()) {
                value = rs.getString("value");
            }
            assertThat(value).isNull();
        }

        SQLBuilder sb4 = new SQLBuilder("select value from ${table}").bind("table", "VN");
        try (ResultSet rs = sb4.getResultSet(mockConnection)) {
            if (rs.next()) {
                assertThat(rs.getString(1)).isEqualTo("a");
            }
            if (rs.next()) {
                assertThat(rs.getString("COLUMN1")).isEqualTo("b");
            }
            assertThat(rs.next()).isFalse();
        }

        SQLBuilder sb5 = new SQLBuilder("select count(*) from lookup");
        MockSQLBuilderProvider.setIntByColumnIndex((idx, def) -> 10);
        assertThat(sb5.getInt(mockConnection, 1, 0)).isEqualTo(10);

        SQLBuilder sb6 = new SQLBuilder("select count(*) from lookup");
        assertThat(sb6.getInt(mockConnection, 1, 0)).isEqualTo(10);

        MockResultSet.add("testMock:sb7", new Object[][] { { "a" }, { "b" } });
        SQLBuilder sb7 = new SQLBuilder("select value from lookup where key = ?", 42);
        assertThat(sb7.getString(mockConnection, 1, "default")).isEqualTo("a");

        MockResultSet.add("testMock:sb8", "Alice,20\nBob,35\nCharles,50", false);
        SQLBuilder sb8 = new SQLBuilder("select name, age from friends where age > 18");
        try (ResultSet rs = sb8.getResultSet(mockConnection)) {
            int total = 0;
            while (rs.next()) {
                total += rs.getInt(2);
            }
            assertThat(total).isEqualTo(105);
        }

        MockResultSet.add("testMock:sb9", "name,age", "Alice,20\nBob,35\nCharles,50");
        SQLBuilder sb9 = new SQLBuilder("select name, age from friends where age > 18");
        try (ResultSet rs = sb9.getResultSet(mockConnection)) {
            long total = 0;
            while (rs.next()) {
                total += rs.getLong("age");
            }
            assertThat(total).isEqualTo(105L);
        }

        MockResultSet.add(
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
            assertThat(total).isEqualTo(105L);
        }

        MockResultSet.add("testMock:read from CSV file", getClass().getResourceAsStream("sb11.csv"));
        SQLBuilder sb11 = new SQLBuilder("select USER_ID, FIRST_NAME, LAST_NAME, DEPARTMENT from si_users_t");
        int rsCount1 = MockSQLBuilderProvider.invocations.next();
        assertThat(sb11.getList(mockConnection, rs -> rs.getLong("USER_ID"))
                .toString()).isEqualTo("[100000, 100001, 100002, 100003]");
        int rsCount2 = MockSQLBuilderProvider.invocations.next();
        assertThat(rsCount2 - rsCount1).isEqualTo(5);

        // SI_USERS_T.csv was produced via SQLDeveloper using "Export as csv" from right-click on the table
        MockResultSet.add("testMock:read from sqldeveloper export file", getClass().getResourceAsStream("SI_USERS_T.csv"));
        SQLBuilder sb12 = new SQLBuilder("select USER_ID, FIRST_NAME, LAST_NAME, DEPARTMENT from si_users_t");
        assertThat(sb12.getList(mockConnection, rs -> rs.getLong("USER_ID"))
                .toString()).isEqualTo("[100000, 100001, 100002, 100003]");

        Timestamp ts = Timestamp.from(Instant.now());
        MockResultSet.add("testMock:sb13", new Object[][] { { ts } });
        SQLBuilder sb13 = new SQLBuilder("select value from lookup where key = ?", 42);
        assertThat(sb13.getTimestamp(mockConnection, 1, null)).isEqualTo(ts);

        MockResultSet.add("testMock:sb14", new Object[][] { { ts } });
        SQLBuilder sb14 = new SQLBuilder("select value from lookup where key = ?", 42);
        try (ResultSet rs14 = sb14.getResultSet(mockConnection)) {
            Timestamp ts14 = null;
            if (rs14.next()) {
                ts14 = rs14.getTimestamp(1);
            }
            assertThat(ts14).isEqualTo(ts);
        }

        MockResultSet.add("testMock:sb15", new String[] { "value" }, new Object[][] { { ts } });
        SQLBuilder sb15 = new SQLBuilder("select value from lookup where key = ?", 42);
        try (ResultSet rs15 = sb15.getResultSet(mockConnection)) {
            Timestamp ts15 = null;
            if (rs15.next()) {
                ts15 = rs15.getTimestamp("value");
            }
            assertThat(ts15).isEqualTo(ts);
        }

        Date date = new Date(Instant.now().toEpochMilli());
        MockResultSet.add("testMock:sb16", new Object[][] { { date } });
        SQLBuilder sb16 = new SQLBuilder("select value from lookup where key = ?", 42);
        assertThat(sb16.getDate(mockConnection, 1, null)).isEqualTo(date);

        final SQLBuilder updateFoo = new SQLBuilder("update foo");
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(42);

        MockSQLBuilderProvider.setExecute("testMock", 1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(1);

        MockSQLBuilderProvider.setExecute("", 1, 0, 1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(0);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(42);

        final AtomicInteger count = new AtomicInteger();
        MockSQLBuilderProvider.setExecute("testMock", () -> count.getAndIncrement() < 3 ? 1 : 2);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(1);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(2);
        assertThat(updateFoo.execute(mockConnection)).isEqualTo(2);

        assertThatIllegalStateException().isThrownBy(() -> {
            MockSQLBuilderProvider.setExecute("abc", 1);
            updateFoo.execute(mockConnection);
        }).withMessage("Trying to use mock data tagged with 'abc' in method 'testMock' of class com.metricstream.jdbc.SQLBuilderTestJava");
    }

    @Test
    void expandTest() {
        assertThat(new SQLBuilder("select a from foo where a in (?)", 3).toSQL()).endsWith("a in (?)");
        assertThat(new SQLBuilder("select a from foo where a in (?)", Collections.singletonList(3)).toSQL()).endsWith("a in (?)");
        assertThat(new SQLBuilder("select a from foo where a in (?)", List.of(3, 1, 4)).toSQL()).endsWith("a in (?,?,?)");
        assertThat(new SQLBuilder("select a from foo where a in (?) and b in (?)", List.of(3, 1, 4), List.of(2, 1)).toSQL()).endsWith("a in (?,?,?) and b in (?,?)");
        assertThatSQLException()
                .isThrownBy(() -> new SQLBuilder("select a from foo where a in (?)", Collections.emptyList()).toSQL())
                .withMessage("Collection parameters must contain at least one element");
    }

    @Test
    void expandRepeatedTest() {
        List<Integer> args = new ArrayList<>(List.of(3, 1, 4));
        SQLBuilder sb = new SQLBuilder("select a from foo where a in (?)", args);
        assertThat(sb.toSQL()).endsWith("a in (?,?,?)");
        assertThat(sb.toString()).endsWith("a in (?,?,?); args=[3, 1, 4]");
        assertThat(sb.toSQL()).endsWith("a in (?,?,?)");
        assertThat(sb.toString()).endsWith("a in (?,?,?); args=[3, 1, 4]");
        args.add(1);
        assertThat(sb.toSQL()).endsWith("a in (?,?,?,?)");
        assertThat(sb.toString()).endsWith("a in (?,?,?,?); args=[3, 1, 4, 1]");
        assertThat(sb.toSQL()).endsWith("a in (?,?,?,?)");
        assertThat(sb.toString()).endsWith("a in (?,?,?,?); args=[3, 1, 4, 1]");
    }

    @Test
    void reuseResultSetData1() throws SQLException {
        MockResultSet.add(
                "reuseResultSetData1",
                new String[] { "A", "B" },
                new Object[][] {
                        { 1, "hello" }
                },
                3
        );

        assertThat(sqlBuilder.getList(mockConnection, rs -> rs.getInt(1))).isEqualTo(List.of(1, 1, 1));
        assertThat(MockSQLBuilderProvider.invocations.next()).isEqualTo(4);
    }

    @Test
    void reuseResultSetData2() throws SQLException {
        MockResultSet.add(
                "reuseResultSetData2",
                new String[] { "A", "B" },
                new Object[][] {
                        { 1, "hello" }
                },
                1
        );

        assertThat(sqlBuilder.getList(mockConnection, rs -> rs.getInt(1))).isEqualTo(List.of(1));
        assertThat(MockSQLBuilderProvider.invocations.next()).isEqualTo(2);
    }

    @Test
    void reuseResultSetData3() throws SQLException {
        MockResultSet.add(
                "reuseResultSetData3",
                new String[] { "A", "B" },
                new Object[][] {
                        { 1, "hello" }
                }
        );

        assertThat(sqlBuilder.getList(mockConnection, rs -> rs.getInt(1))).isEqualTo(List.of(1));
        assertThat(MockSQLBuilderProvider.invocations.next()).isEqualTo(2);
    }

    @Test
    void reuseResultSetData4() throws SQLException {
        MockResultSet.add(
                "reuseResultSetData4",
                new String[] { "A", "B" },
                new Object[][] {
                        { 1, "hello" },
                        { 2, "world" }
                },
                3
        );

        assertThat(sqlBuilder.getList(mockConnection, rs -> rs.getInt(1))).isEqualTo(List.of(1, 2, 1, 2, 1, 2));
        assertThat(MockSQLBuilderProvider.invocations.next()).isEqualTo(7);
    }

    @Test
    void copyTest1() throws SQLException {
        // A resultset is consumed by a SQLBuilder `getResultSet` (or higher level callers like `getInt`). Therefore,
        // adding it once but trying to use it twice will not work.  Instead, the next usage will create a new
        // default mocked resultset
        ResultSet rs = MockResultSet.create("copyTest1", "A", "3");
        MockSQLBuilderProvider.addResultSet(rs);
        assertThat(sqlBuilder.getInt(mockConnection, 1, -1)).isEqualTo(3);
        assertThat(sqlBuilder.getInt(mockConnection, 1, -1)).isEqualTo(42);
    }

    @Test
    void copyTest2() throws SQLException {
        // A resultset has an internal state which keeps track of the consumed rows.  Therefore, adding the same
        // resultset twice will not produce the same result.
        ResultSet rs = MockResultSet.create("copyTest2", "A", "3");
        MockSQLBuilderProvider.addResultSet(rs);
        assertThat(sqlBuilder.getInt(mockConnection, 1, -1)).isEqualTo(3);
        MockSQLBuilderProvider.addResultSet(rs);
        assertThat(sqlBuilder.getInt(mockConnection, 1, -1)).isEqualTo(-1);
    }

    @Test
    void brokenTest() {
        MockResultSet.addBroken("");
        assertThatSQLException().isThrownBy(() -> new SQLBuilder("select A from T").getInt(mockConnection, 1, -1));
    }

    @Test
    void executeReturningTest() throws SQLException {
        MockResultSet.add("executeReturningTest:id", "43", false);
        SQLBuilder sb = new SQLBuilder("insert into foo(foo_s.nextval, ?", "fooValue");
        ResultSet rs = sb.execute(mockConnection, "id");
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(43);
    }

    @Test
    void unusedMockResultSet() throws SQLException {
        MockResultSet.add("unusedMockResultSet:first", "1", false);
        MockResultSet.add("unusedMockResultSet:second", "2", false);
        SQLBuilder sb1 = new SQLBuilder("select count(*) from foo");
        assertThat(sb1.getInt(mockConnection, 1, 0)).isEqualTo(1);
    }

    @Test
    void testDateTime() throws SQLException {
        OffsetDateTime now = OffsetDateTime.now();
        MockResultSet.add("testDateTime", new Object[][] { { now } });
        SQLBuilder sb1 = new SQLBuilder("select created from lookup where key = ?", 42);
        assertThat(sb1.getDateTime(mockConnection, 1, null)).isEqualTo(now);

        SQLBuilder sb2 = new SQLBuilder("select created from lookup where key = ?", 42);
        final OffsetDateTime dt2 = sb2.getDateTime(mockConnection, 1, null);
        assertThat(dt2).isNotNull();
        // This will fail in about 3000 years.  Hopefully, mankind is still around by then but no longer uses JDBC...
        assertThat(dt2.isAfter(now.plusYears(1000L))).isTrue();

        SQLBuilder sb3 = new SQLBuilder("select created from lookup where key = ?", 42);
        final OffsetDateTime dt3 = sb3.getDateTime(mockConnection, "created", null);
        assertThat(dt3).isNotNull();
        // This will fail in about 3000 years.  Hopefully, mankind is still around by then but no longer uses JDBC...
        assertThat(dt3.isAfter(now.plusYears(1000L))).isTrue();
    }

    @Test
    void testInstant() throws SQLException {
        Instant now = Clock.systemUTC().instant();
        OffsetDateTime oNow = now.atOffset(ZoneOffset.UTC);
        MockResultSet.add("testInstant", new Object[][] { { oNow } });
        SQLBuilder sb1 = new SQLBuilder("select created from lookup where key = ?", 42);
        assertThat(sb1.getInstant(mockConnection, 1, null)).isEqualTo(now);

        SQLBuilder sb2 = new SQLBuilder("select created from lookup where key = ?", 42);
        final Instant dt2 = sb2.getInstant(mockConnection, 1, null);
        assertThat(dt2).isNotNull();
        // This will fail in about 1150 years.  Hopefully, mankind is still around by then but no longer uses JDBC...
        assertThat(dt2.isAfter(now.plus(420_000L, ChronoUnit.DAYS))).isTrue();

        SQLBuilder sb3 = new SQLBuilder("select created from lookup where key = ?", 42);
        final Instant dt3 = sb3.getInstant(mockConnection, "created", null);
        assertThat(dt3).isNotNull();
        // This will fail in about 1150 years.  Hopefully, mankind is still around by then but no longer uses JDBC...
        assertThat(dt3.isAfter(now.plus(420_000L, ChronoUnit.DAYS))).isTrue();
    }

    @Test
    void yesForever() throws SQLException {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider(true, true));

        SQLBuilder sb1 = new SQLBuilder("select count(*) from lookup");

        try (ResultSet rs = sb1.getResultSet(mockConnection)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.next()).isFalse();
        }

        try (ResultSet rs = sb1.getResultSet(mockConnection)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void noForever() throws SQLException {
        try {
            SQLBuilder.setDelegate(new MockSQLBuilderProvider(false, true));

            SQLBuilder sb1 = new SQLBuilder("select count(*) from lookup");

            try (ResultSet rs = sb1.getResultSet(mockConnection)) {
                assertThat(rs.next()).isFalse();
            }

            try (ResultSet rs = sb1.getResultSet(mockConnection)) {
                assertThat(rs.next()).isFalse();
            }
        } finally {
            SQLBuilder.setDelegate(new MockSQLBuilderProvider(true, true));
        }
    }

    @Test
    void emptyForever() throws SQLException {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider(true, true));
        MockResultSet.addEmpty("");

        SQLBuilder sb = new SQLBuilder("select count(*) from lookup");

        try (ResultSet rs = sb.getResultSet(mockConnection)) {
            assertThat(rs.next()).isFalse();
        }

        try (ResultSet rs = sb.getResultSet(mockConnection)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void getDouble1() throws SQLException {
        MockResultSet.add("getDouble1", "A", "123");
        try (ResultSet rs = sqlBuilder.getResultSet(mockConnection)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getDouble(1)).isEqualTo(123.0);
        }
    }

    @Test
    void getDouble2() throws SQLException {
        MockResultSet.add("getDouble2", "A", "123.456");
        try (ResultSet rs = sqlBuilder.getResultSet(mockConnection)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getDouble(1)).isEqualTo(123.456);
        }
    }

    @Test
    void getDouble3() throws SQLException {
        MockResultSet.add("getDouble3", new String[] { "A" }, new Object[][] { { 123.456 } });
        try (ResultSet rs = sqlBuilder.getResultSet(mockConnection)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getDouble(1)).isEqualTo(123.456);
        }
    }

    @Test
    void getDouble4() throws SQLException {
        MockResultSet.add("getDouble4", new String[] { "A" }, new Object[][] { { 123.456 } });
        assertThat(sqlBuilder.getDouble(mockConnection, 1, -1.0)).isEqualTo(123.456);
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
        MockResultSet.add("", "_,3\n_,1\n_,4", false);
        List<Integer> actual = sqlBuilder.getList(mockConnection, (rs) -> rs.getInt(2));
        assertThat(actual).containsExactly(3, 1, 4);
    }

    @Test
    void getList_test2() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null in column 2
        // then expect to get a list with 3 elements in the correct order
        MockResultSet.add("", new Object[][] { { "", 3 }, { "", null }, { "", 4 } });
        List<Integer> actual = sqlBuilder.getList(mockConnection, (rs) -> rs.getInt(2));
        assertThat(actual).containsExactly(3, 0, 4);
    }

    @Test
    void getList_test4() throws Exception {
        // when query returns 3 rows
        // with 2 Strings and a null in column 2
        // then expect to get a list with 2 elements in the correct order
        MockResultSet.add("", new Object[][] { { "", "first" }, { "", null }, { "", "third" } });
        List<String> actual = sqlBuilder.getList(mockConnection, (rs) -> rs.getString(2));
        assertThat(actual).containsExactly("first", "third");
    }

    @Test
    void getList_test5() throws Exception {
        // when query returns 3 rows
        // with 2 Strings and a null in column 2
        // then expect to get a list with 3 elements in the correct order
        MockResultSet.add("", new Object[][] { { "", "first" }, { "", null }, { "", "third" } });
        List<String> actual = sqlBuilder.getListWithNull(mockConnection, (rs) -> rs.getString(2));
        assertThat(actual).containsExactly("first", null, "third");
    }

    @Test
    void getList_test6() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null (converted to 0 by getInt()) in column 2
        // then expect to get a list with 3 elements in the correct order
        MockResultSet.add("", new Object[][] { { "", 1 }, { "", null }, { "", 3 } });
        List<Integer> actual = sqlBuilder.getList(mockConnection, (rs) -> {
            int i = rs.getInt(2);
            return rs.wasNull() ? -1 : i;
        });
        assertThat(actual).containsExactly(1, -1, 3);
    }

    @Test
    void getList_test7() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null (converted to 0 by getInt()) in column 2
        // then expect to get a list with 2 elements in the correct order.  We must avoid
        // calling `getInt` here because that automatically converts null to 0.
        MockResultSet.add("", new Object[][] { { "", 1 }, { "", null }, { "", 3 } });
        List<Integer> actual = sqlBuilder.getList(mockConnection, (rs) -> rs.getObject(2))
                .stream().map(i -> (Integer) i).collect(Collectors.toList());
        assertThat(actual).containsExactly(1, 3);
    }

    @Test
    void getList_test8() throws Exception {
        // when query returns 3 rows
        // with 2 ints and a null in column 2
        // then expect to get 3 elements with null mapped to null
        MockResultSet.add("", new Object[][] { { "", 1 }, { "", null }, { "", 3 } });
        List<Integer> actual = sqlBuilder.getListWithNull(mockConnection, (rs) -> rs.getObject(2))
                .stream().map(i -> (Integer) i).collect(Collectors.toList());
        assertThat(actual).containsExactly(1, null, 3);
    }

    @Test
    void getMap_test1() throws SQLException {
        // when query returns 3 rows
        // with 3 ints in column 2
        // then expect to get a list with 3 elements in the correct order
        MockResultSet.add("", "3,Three\n1,One\n4,Four", false);
        Map<Integer, String> m = sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getInt(1), rs.getString(2)));
        assertThat(m.keySet()).containsExactlyInAnyOrder(3, 1, 4);
        assertThat(m).containsValue("Three");
    }

    @Test
    void getMap_testDuplicateKeys() {
        // when query returns 3 rows with duplicate keys
        // then expect to get an IllegalStateException
        MockResultSet.add("", "3,Three\n1,One\n3,Four", false);
        assertThatIllegalStateException().isThrownBy(() -> sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getInt(1), rs.getString(2))));
    }

    @Test
    void getMap_testNullKey() {
        // when query returns 3 rows with duplicate keys
        // then expect to get an IllegalStateException
        MockResultSet.add("", new Object[][] { { 3, "Three" }, { null, "Zero" } });
        // Note: we cannot use `getInt` for the key here because that would automatically convert `null` to `0` and thus not throw the expected exception
        assertThatIllegalStateException().isThrownBy(() -> sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getObject(1), rs.getString(2))));
    }

    @Test
    void getMap_test3() throws SQLException {
        // when query returns 3 rows
        // with 3 ints in column 2
        // then expect to get a list with 3 elements in the correct order
        MockResultSet.add("", new Object[][] { { "1", 1 }, { "2", null }, { "3", 3 } });
        Map<String, Integer> m = sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getString(1), rs.getInt(2)));
        // size is 3 and not 2 although 2 is mapped to null because we use getInt which will automatically convert null to 0
        assertThat(m.keySet()).containsExactlyInAnyOrder("1", "2", "3");
    }

    @Test
    void getMap_test4() throws SQLException {
        // when query returns 3 rows
        // with 3 ints in column 2
        // then expect to get a list with 3 elements in the correct order
        MockResultSet.add("", new Object[][] { { "1", 1 }, { "2", null }, { "3", 3 } });
        Map<String, Integer> m = sqlBuilder.getMap(mockConnection, rs -> SQLBuilder.entry(rs.getString(1), rs.getInt(2)), false);
        assertThat(m.keySet()).containsExactlyInAnyOrder("1", "2", "3");
    }

    @Test
    void getSingle_test1() throws SQLException {
        // when query returns 3 rows
        // with 3 longs in column 1
        // then expect to get the first element
        MockResultSet.add("", new Object[][] { { 3L }, { 1L }, { 4L } });
        Optional<Long> actual = sqlBuilder.getSingle(mockConnection, (rs) -> rs.getLong(1));
        assertThat(actual).isPresent().hasValue(3L);
    }

    @Test
    void getString_test1() throws SQLException {
        // when query returns 3 rows
        // with 3 Strings in column 1
        // then expect to get the first element
        MockResultSet.add("", "first\nsecond\nthird", false);
        String s = sqlBuilder.getString(mockConnection, 1, "default");
        assertThat(s).isEqualTo("first");
    }

    @Test
    void getString_test2() throws SQLException {
        // when query returns 1 row
        // with 3 Strings in column 1
        // then expect to get the first element
        MockResultSet.add("", "first", false);
        String s = sqlBuilder.getString(mockConnection, 1, "default");
        assertThat(s).isEqualTo("first");
    }

    @Test
    void getString_test3() throws SQLException {
        // when query returns no rows
        // then expect to get the default element
        MockResultSet.addEmpty("");
        String s = sqlBuilder.getString(mockConnection, 1, "default");
        assertThat(s).isEqualTo("default");
    }

    @Test
    void getString_test4() throws SQLException {
        // when query returns no rows
        // then expect to get the default element even if that is null
        MockResultSet.addEmpty("");
        String s = sqlBuilder.getString(mockConnection, 1, null);
        assertThat(s).isNull();
    }

    @Test
    void getString_test5() throws SQLException {
        // when query returns 3 rows
        // with 3 Strings in column "LABEL"
        // then expect to get the first element
        MockResultSet.add("", "LABEL", "first\nsecond\nthird");
        String s = sqlBuilder.getString(mockConnection, "LABEL", "default");
        assertThat(s).isEqualTo("first");
        assertThat(MockSQLBuilderProvider.invocations.getString()).isEqualTo(1);
        assertThat(MockSQLBuilderProvider.invocations.getAnyColumn()).isEqualTo(1);
        assertThat(MockSQLBuilderProvider.invocations.getRs()).isEqualTo(1);
    }


    @Test
    void getSingle_test2() throws SQLException {
        // when query returns no rows
        // then expect to get an empty optional
        MockResultSet.addEmpty("");
        Optional<Long> actual = sqlBuilder.getSingle(mockConnection, (rs) -> rs.getLong(1));
        assertThat(actual).isNotPresent();
        assertThat(MockSQLBuilderProvider.invocations.getRs()).isEqualTo(1);
        assertThat(MockSQLBuilderProvider.invocations.getAnyColumn()).isEqualTo(0);
    }

    @Test
    void getResultSet_test1() throws Exception {
        // when query returns 3 rows
        // with 3 ints in column 2
        // then expect to get a resultset that returns 3 rows in correct order
        MockResultSet.add("", "_,3\n_,1\n_,4", false);
        try (ResultSet rs = sqlBuilder.getResultSet(mockConnection)) {
            List<Integer> actual = new ArrayList<>();
            while (rs.next()) {
                actual.add(rs.getInt(2));
            }
            assertThat(actual).containsExactly(3, 1, 4);
        }
    }

    @Test
    void getResultSet_test2() throws Exception {
        // when query returns 1 row
        // with 1 long in column 3
        // then expect to get a resultset that returns 1 row
        MockResultSet.add("", new Object[][] { { "", "", 3L } });
        try (ResultSet rs = sqlBuilder.getResultSet(mockConnection)) {
            List<Long> actual = new ArrayList<>();
            while (rs.next()) {
                actual.add(rs.getLong(3));
            }
            assertThat(actual).containsExactly(3L);
        }
    }

    @Test
    void getResultSet_test3() throws Exception {
        // when query returns no rows
        // then expect to get a resultset that returns no row
        MockResultSet.addEmpty("");
        try (ResultSet rs = sqlBuilder.getResultSet(mockConnection)) {
            assertThat(rs.next()).isFalse();
        }
        assertThat(MockSQLBuilderProvider.invocations.getResultSet()).isEqualTo(1);
    }

    @Test
    void invocation_test1() throws SQLException {
        SQLBuilder sb = new SQLBuilder("select a from b");

        sb.getLong(mockConnection, 1, 0L);
        assertThat(MockSQLBuilderProvider.invocations.getLong()).isEqualTo(1);
        assertThat(MockSQLBuilderProvider.invocations.getRsLong()).isEqualTo(1);
        assertThat(MockSQLBuilderProvider.invocations.getRs()).isEqualTo(1);
        assertThat(MockSQLBuilderProvider.invocations.next()).isEqualTo(1);

        sb.getLong(mockConnection, "a", 0L);
        assertThat(MockSQLBuilderProvider.invocations.getLong()).isEqualTo(2);
        assertThat(MockSQLBuilderProvider.invocations.getRsLong()).isEqualTo(2);
        assertThat(MockSQLBuilderProvider.invocations.getRs()).isEqualTo(2);
        assertThat(MockSQLBuilderProvider.invocations.next()).isEqualTo(2);

        // not calling SQLBuilder#getLong
        try (ResultSet rs = sb.getResultSet(mockConnection)) {
            if (rs.next()) {
                rs.getLong(1);
            }
        }
        assertThat(MockSQLBuilderProvider.invocations.getLong()).isEqualTo(2);
        assertThat(MockSQLBuilderProvider.invocations.getRsLong()).isEqualTo(3);
        assertThat(MockSQLBuilderProvider.invocations.getRs()).isEqualTo(3);
        assertThat(MockSQLBuilderProvider.invocations.next()).isEqualTo(3);

        // not calling SQLBuilder#getLong
        try (ResultSet rs = sb.getResultSet(mockConnection)) {
            if (rs.next()) {
                rs.getLong("a");
            }
        }
        assertThat(MockSQLBuilderProvider.invocations.getLong()).isEqualTo(2);
        assertThat(MockSQLBuilderProvider.invocations.getRsLong()).isEqualTo(4);
        assertThat(MockSQLBuilderProvider.invocations.getRs()).isEqualTo(4);
        assertThat(MockSQLBuilderProvider.invocations.next()).isEqualTo(4);

        // SQLBuilder#getList uses a "while" loop and thus calls ResultSet#next twice
        sb.getList(mockConnection, rs -> rs.getLong("a"));
        assertThat(MockSQLBuilderProvider.invocations.getLong()).isEqualTo(2);
        assertThat(MockSQLBuilderProvider.invocations.getRsLong()).isEqualTo(5);
        assertThat(MockSQLBuilderProvider.invocations.getRs()).isEqualTo(5);
        assertThat(MockSQLBuilderProvider.invocations.next()).isEqualTo(6);
    }

    @Test
    void placeholder_dollar() {
        SQLBuilder sb = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        assertThat(sb.toString()).isEqualTo("select a, ${b} from ${t} where x > ?; args=[5]");
        sb.bind("b", "BCOL").bind("t", "table1");
        assertThat(sb.toString()).isEqualTo("select a, BCOL from table1 where x > ?; args=[5]");
    }

    @Test
    void placeholder_colon() {
        SQLBuilder sb = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        assertThat(sb.toString()).isEqualTo("select a, :{b} from :{t} where x > ?; args=[5]");
        sb.bind("b", "BCOL").bind("t", "table1");
        assertThat(sb.toString()).isEqualTo("select a, BCOL from table1 where x > ?; args=[5]");
    }

    @Test
    void multiPlaceholder_dollar() {
        SQLBuilder sb = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        assertThat(sb.toString()).isEqualTo("select a, ${b} from ${t} where x > ?; args=[5]");
        sb.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        assertThat(sb.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
    }

    @Test
    void multiPlaceholder_colon() {
        SQLBuilder sb = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        assertThat(sb.toString()).isEqualTo("select a, :{b} from :{t} where x > ?; args=[5]");
        sb.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        assertThat(sb.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
    }

    @Test
    void invalidPlaceholder_dollar() {
        SQLBuilder sb = new SQLBuilder("select a, ${b+} from ${t} where x > ?", 5);
        assertThat(sb.toString()).isEqualTo("select a, ${b+} from ${t} where x > ?; args=[5]");
        assertThatIllegalArgumentException().isThrownBy(() -> sb.bind("b+", "BCOL"));
    }

    @Test
    void invalidPlaceholder_colon() {
        SQLBuilder sb = new SQLBuilder("select a, :{b+} from :{t} where x > ?", 5);
        assertThat(sb.toString()).isEqualTo("select a, :{b+} from :{t} where x > ?; args=[5]");
        assertThatIllegalArgumentException().isThrownBy(() -> sb.bind("b+", "BCOL"));
    }

    @Test
    void repeatedPlaceholder() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SQLBuilder("").bind("a", "first").bind("a", "second"));
    }

    @Test
    void multiBuilder_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        SQLBuilder sb2 = new SQLBuilder(sb1);
        assertThat(sb2.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
    }

    @Test
    void multiBuilder_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        SQLBuilder sb2 = new SQLBuilder(sb1);
        assertThat(sb2.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
    }

    @Test
    void repeatedPlaceholder2() {
        SQLBuilder sb1 = new SQLBuilder("").bind("a", "first");
        SQLBuilder sb2 = new SQLBuilder("").bind("a", "first");
        assertThatIllegalArgumentException().isThrownBy(() -> sb1.append(sb2));
    }

    @Test
    void repeatedPlaceholder3_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        SQLBuilder sb2 = new SQLBuilder("select ${b} from (").append(sb1).append(")");
        assertThat(sb2.toString()).isEqualTo("select BCOL, CCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]");
    }

    @Test
    void repeatedPlaceholder3_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        SQLBuilder sb2 = new SQLBuilder("select :{b} from (").append(sb1).append(")");
        assertThat(sb2.toString()).isEqualTo("select BCOL, CCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]");
    }

    @Test
    void repeatedPlaceholder4_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        SQLBuilder sb2 = new SQLBuilder("select ${b} from (").append(sb1).append(")");
        assertThatIllegalArgumentException().isThrownBy(() -> sb2.bind("b", "BCOL"));
    }

    @Test
    void repeatedPlaceholder4_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        SQLBuilder sb2 = new SQLBuilder("select :{b} from (").append(sb1).append(")");
        assertThatIllegalArgumentException().isThrownBy(() -> sb2.bind("b", "BCOL"));
    }

    @Test
    void repeatedPlaceholder5_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        SQLBuilder sb2 = new SQLBuilder("select ${b} from (").append(sb1.applyBindings()).append(")").bind("b", "BCOL");
        assertThat(sb2.toString()).isEqualTo("select BCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]");
    }

    @Test
    void repeatedPlaceholder5_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        SQLBuilder sb2 = new SQLBuilder("select :{b} from (").append(sb1.applyBindings()).append(")").bind("b", "BCOL");
        assertThat(sb2.toString()).isEqualTo("select BCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]");
    }

    @Test
    void repeatedPlaceholder6_dollar() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        String sb1s = sb1.toString();
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        sb1.applyBindings();
        assertThat(sb1.toString()).isEqualTo(sb1s);
        SQLBuilder sb2 = new SQLBuilder("select ${b} from (").append(sb1).append(")").bind("b", "BCOL");
        assertThat(sb2.toString()).isEqualTo("select BCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]");
    }

    @Test
    void repeatedPlaceholder6_colon() {
        SQLBuilder sb1 = new SQLBuilder("select a, :{b} from :{t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).bind("t", "table1");
        String sb1s = sb1.toString();
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        assertThat(sb1.toString()).isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        sb1.applyBindings();
        assertThat(sb1.toString()).isEqualTo(sb1s);
        SQLBuilder sb2 = new SQLBuilder("select :{b} from (").append(sb1).append(")").bind("b", "BCOL");
        assertThat(sb2.toString()).isEqualTo("select BCOL from ( select a, BCOL, CCOL from table1 where x > ? ); args=[5]");
    }

    @Test
    void partialPlaceHolder() {
        SQLBuilder sb1 = new SQLBuilder("select a, ${b} from ${t} where x > ?", 5);
        sb1.bind("b", List.of("BCOL", "CCOL")).applyBindings();
        assertThat(new SQLBuilder(sb1).bind("t", "table1").toString())
                .isEqualTo("select a, BCOL, CCOL from table1 where x > ?; args=[5]");
        assertThat(new SQLBuilder(sb1).bind("t", "table2").toString())
                .isEqualTo("select a, BCOL, CCOL from table2 where x > ?; args=[5]");
    }

    @Test
    void testFromNumberedParams() {
        QueryParams params = new QueryParamsImpl();
        assertThat(SQLBuilder.fromNumberedParameters("select n from t where i=:1)", params).toString())
                .isEqualTo("select n from t where i=?); args=[a]");
        assertThat(SQLBuilder.fromNumberedParameters("select n from t where i=:1 or i=:2)", params).toString())
                .isEqualTo("select n from t where i=? or i=?); args=[a, b]");
        assertThat(SQLBuilder.fromNumberedParameters("select n from t where i=:2 or i=:1)", params).toString())
                .isEqualTo("select n from t where i=? or i=?); args=[b, a]");
        assertThat(SQLBuilder.fromNumberedParameters("select n from t where i=:2 or k=:2)", params).toString())
                .isEqualTo("select n from t where i=? or k=?); args=[b, b]");
        assertThat(SQLBuilder.fromNumberedParameters("select n from t where i=:2 or k=':4')", params).toString())
                .isEqualTo("select n from t where i=? or k=':4'); args=[b]");
        assertThat(SQLBuilder.fromNumberedParameters("select n from t where i=:2 or k=':2')", params).toString())
                .isEqualTo("select n from t where i=? or k=':2'); args=[b]");
        assertThat(SQLBuilder.fromNumberedParameters("select n from t where i=:11 or i=:2)", params).toString())
                .isEqualTo("select n from t where i=:11 or i=?); args=[b]");
    }

    @Test
    void maskData() {
        assertThat(new SQLBuilder("select name from user where secret=?", SQLBuilder.mask("oops!")).toString())
                .isEqualTo("select name from user where secret=?; args=[__masked__:982c0381c279d139fd221fce974916e7]");
    }

    @Test
    void maskDataNull() {
        assertThat(new SQLBuilder("select name from user where secret=?", SQLBuilder.mask(null)).toString())
                .isEqualTo("select name from user where secret=?; args=[null]");
    }

    @Test
    void maskDataEmpty() {
        assertThat(new SQLBuilder("select name from user where secret=?", SQLBuilder.mask("")).toString())
                .isEqualTo("select name from user where secret=?; args=[]");
    }

    @Test
    void maskDataLong() {
        assertThat(new SQLBuilder("select name from user where secret=?", SQLBuilder.mask(42L)).toString())
                .isEqualTo("select name from user where secret=?; args=[__masked__:a1d0c6e83f027327d8461063f4ac58a6]");
    }

    @Test
    void maskDataMixed() {
        assertThat(new SQLBuilder("select name from user where secret=? and public=?", SQLBuilder.mask("oops!"), "ok").toString())
                .isEqualTo("select name from user where secret=? and public=?; args=[__masked__:982c0381c279d139fd221fce974916e7, ok]");
    }

    private String masked(Object value) {
        return new SQLBuilder("?", SQLBuilder.mask(value)).toString();
    }

    @Test
    void maskSame() {
        assertThat(masked("hello")).isEqualTo(masked("hello"));
        assertThat(masked(Long.valueOf("42"))).isEqualTo(masked(42L));
        assertThat(masked(Long.valueOf("42"))).isEqualTo(masked(42));
    }

    @Test
    void mockInt() throws SQLException {
        SQLBuilder sb = new SQLBuilder("select count(*) from foo");
        MockSQLBuilderProvider.setIntByColumnIndex((c, d) -> {
            switch (c) {
            case 1:
                return 3;
            default:
                return d;
            }
        });
        assertThat(sb.getInt(mockConnection, 1, 4)).isEqualTo(3);
    }

    @Test
    void maxRows() throws SQLException {
        MockResultSet.add("", "_,3\n_,1\n_,4", false);
        // TODO: this just tests that `withMaxRows` is accepted, but not the actual implementation.
        List<Integer> actual = sqlBuilder.withMaxRows(1).getList(mockConnection, (rs) -> rs.getInt(2));
        assertThat(actual).containsExactly(3, 1, 4);
    }

    @Test
    void nameToIndexMapping() throws SQLException {
        MockResultSet.add("", "columnA,columnB", "A,B");
        ResultSet rs = sqlBuilder.getResultSet(mockConnection);
        assertThat(rs.findColumn("columnB")).isEqualTo(2);
        assertThat(rs.findColumn("COLUMNB")).isEqualTo(2);
        ResultSetMetaData rsmd = rs.getMetaData();
        assertThat(rsmd.getColumnName(2)).isEqualTo("COLUMNB");
    }

    @Test
    void nameQuote1() {
        assertThat(SQLBuilder.nameQuote("columnA")).isEqualTo("columnA");
        assertThat(SQLBuilder.nameQuote("column_A")).isEqualTo("column_A");
        assertThat(SQLBuilder.nameQuote("COL1")).isEqualTo("COL1");
    }

    @Test
    void nameQuote2() {
        assertThatIllegalArgumentException().isThrownBy(() -> SQLBuilder.nameQuote("column\"A"));
    }

    @Test
    void nameQuote3() {
        assertThatIllegalArgumentException().isThrownBy(() -> SQLBuilder.nameQuote("column+A"));
        assertThat(SQLBuilder.nameQuote("column+A", false)).isEqualTo("\"column+A\"");
        assertThat(SQLBuilder.nameQuote("column;A", false)).isEqualTo("\"column;A\"");
    }

    @Test
    void nameQuote4() {
        assertThat(SQLBuilder.nameQuote("columnA A", false)).isEqualTo("columnA A");
        assertThat(SQLBuilder.nameQuote("column;A A", false)).isEqualTo("\"column;A\" A");
        assertThat(SQLBuilder.nameQuote("column;A A+B", false)).isEqualTo("\"column;A\" \"A+B\"");
    }

    static class QueryParamsImpl implements QueryParams {

        // some arbitrary param values for testing
        final String[] values = { "a", "b", "c" };
        final List<String> names = IntStream.rangeClosed(1, values.length)
                .mapToObj(String::valueOf)
                .collect(Collectors.toList());

        @Nullable
        @Override
        public Object getParameterValue(@NotNull String name) {
            return getParameterValue(name, false);
        }

        @Nullable
        @Override
        public Object getParameterValue(@NotNull String name, boolean isMulti) {
            final String value = values[Integer.parseInt(name) - 1];
            return isMulti ? List.of(value, value, value, value) : value;
        }

        @Nullable
        @Override
        public Object getParameterValue(@NotNull String name, boolean isMulti, boolean dateAsString) {
            return getParameterValue(name, isMulti);
        }

        @NotNull
        @Override
        public List<String> getParamNames() {
            return names;
        }

        @Override
        public boolean dateAsStringNeeded(@NotNull String subStr) {
            return false;
        }

        @NotNull
        @Override
        public String getDateParameterAsString() {
            return "";
        }

    }
}
