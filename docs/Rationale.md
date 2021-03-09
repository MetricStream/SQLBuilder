# Preface #
[![MetricStream](MetricStream.png)][MetricStream]

SQLBuilder is a JDBC-based Java library created by [MetricStream] for using SQL databases from within you Java
application.

# Introduction #

In an ideal world, no Java (or Kotlin) developer would ever have to directly interact with [JDBC]. Instead, we would all
use ORM tools like [Hibernate] or [iBATIS] or [JOOQ] to access our relational database. However, the "enterprise world"
still has lots of legacy code that was written using JDBC. Converting legacy code to use such an ORM is a very intrusive
and time-consuming process. Another alternative is to use libraries like Spring [JdbcTemplate] or [JDBI]. Using these
requires much less refactoring and is thus often the better alternative (especially if switching to an ORM framework is
not possible and thus the only other option is to do nothing).

SQLBuilder is a small library which is an alternative to [JDBI]. The one standout feature of [SQLBuilder] compared to
[JDBI] is that it allows to compose partial queries, pass them between methods, and then automatically combine them to
the final query. This makes it easier to convert legacy code which e.g. adds filters to queries using separate methods.

SQLBuilder is database agnostic and its only dependencies are [JDBC], [SLF4J], and [Commons Codec]. SQLBuilder also has
an optional test driver which in addition requires [Mockito] and [OpenCSV].

# Motivation #

We were faced with lots of legacy code using variants of the following (simplified code w/o error handling etc.):

```java
String filter(int age) {
    return age > 0 ? " and age >= ?" : "";
}

List<String> friends(int age) {
    String query = "select first_name from person where last_name = ?" + filter(age);
    try (PreparedStatement ps = con.prepareStatement(query)) {
        ps.setString(1, name);
        if (age > 0) {
            ps.setInt(2, age);
        }
        try (ResultSet rs = ps.getResultSet()) {
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                names.add(rs.getString(1));
            }
            return names;
        }
    }
}
```

What stands out here is that the SQL fragments and the query parameters are disconnected:

-  The place where the SQL is constructed and the place where the parameter values are provided are lines apart
-  conditions like the `age > 0` have to be repeated

Compare that code now with the same code using SQLBuilder:

```java
SQLBuilder filter(int age) {
    return age > 0 ? new SQLBuilder("and age > ?", age) : new SQLBuilder("");
}

List<String> friends(int age) {
    SQLBuilder query = new SQLBuilder("select first_name from person where last_name = ?", name).append(filter(age));
    try (ResultSet rs = query.getResultSet(con)) {
        List<String> names = new ArrayList<>();
        while (rs.next()) {
            names.add(rs.getString(1));
        }
        return names;
    }
}
```

Here, the `age` condition is only coded once. We also avoid the `PreparedStatement` and directly compute the `ResultSet`
from the `SQLBuilder` object. `SQLBuilder` also offers some more shortcuts for frequently used cases which are explained
later.


# SQLBuilder API #

## SQLBuilder Object Creation ##

`SQLBuilder` objects are a combination of a SQL fragment and a list of parameter values. When a `SQLBuilder` is used to
produce a `ResultSet`, these 2 parts are used to produce a `PreparedStatement` from which the `ResultSet` is then
returned. Because this effectively hides the `PreparedStatement`, the returned `ResultSet` is modified in a way that
closing the `ResultSet` also closes the `PreparedStatement` from which it was produced.

`SQLBuilder` objects are constructed from their 2 parts:

```java
SQLBuilder core1 = new SQLBuilder("select name, age from person");  
SQLBuilder core2 = new SQLBuilder("select name, age from person where age > ?", age);  
SQLBuilder core3 = new SQLBuilder("select name, age from person where age > ? and age < ?", age, age);
SQLBuilder core4 = new SQLBuilder(core3);
```

These objects can then be further modified using the `append` method which is also called with a SQL fragment and
optional parameters:

```java
core1.append("and name not null");
core2.append("and name not null").append("and id in (?)", ids);
```

The last example highlights another advantage of SQLBuilder over PreparedStatement: passing a list as a parameter value
automatically replaces the matching `?` with the correct number of `?`. Thus, if `ids` from above is a list containing 3
`Long` values, the resulting SQL query for `core2` would be `"select name, age from person where age > ? and age < ? and
name not null and id in (?,?,?)"` (and yes: SQLBuilder is smart enough to add a space before appending further
fragments).

Both the constructor and `append` also accept a `SQLBuilder` object. This is useful if we create partial queries in
other methods or need 2 queries with an identical core part: first create the core, and then create the 2 variants:

```java
SQLBuilder core = new SQLBuilder("select name from person");
SQLBuilder filter1 = new SQLBuilder("where age > 18");
SQLBuilder filter2 = new SQLBuilder("where first_name like '%A'");
SQLBuilder variant1 = new SQLBuilder(core).append(filter1);
SQLBuilder variant2 = new SQLBuilder(core).append(filter2);
```

In addition, SQLBuilder offers some constructor function like `wrap` which modifies a `SQLBuilder` by wrapping the SQL
fragment in the provided values:

```java
SQLBuilder core = new SQLBuilder(...);  
SQLBuilder count = core.wrap("select count(*) from (", ")");
```

Please note that this mutates the `core` object. If that is not desired (e.g. because you also need to execute the
unmodified `core` object), instead use

```java
SQLBuilder core = new SQLBuilder(...);  
SQLBuilder count = new SQLBuilder(core).wrap("select count(*) from (", ")");
```

## Using SQLBuilder Objects ##

There are 2 basic operations by which SQLBuilder objects can be used:

 1. create a ResultSet object using `getResultSet(Connection)`
 2. execute the query and return the result of that using `execute(Connection)`

In addition, SQLBuilder offers a set of convenience methods to optimize 2 common cases:

 1. return a single value from a query
 2. map the returned ResultSet objects into a list of application-level objects

For single values, SQLBuilder offers methods like

-  `int getInt(Connection connection, int columnIndex, int defaultValue)`
-  `int getInt(Connection connection, String columnName, int defaultValue)`

The same methods are available for `String`, `Long`, `BigDecimal` and `Object`. They all return a value from the first
returned row, or the default value if no row was returned. Examples for these methods are:

```java
int count = new SQLBuilder("select count(*) from person where age > ?", age).getInt(connection, 1, 0);
String name = new SQLBuilder("select last_name from person where id = ?", pid).getString(connection, "last_name", null);
```

As an additional feature, `SQLBuilder` also offers `getDateTime` as a single
value method.  This is implemented using `getObject(column,
OffsetDateTime.class)` and thus returns an `OffsetDateTime` object.  `getDateTime` can be
used for columns of type `TIMESTAMP WITH TIME ZONE`.  However, this is only
guaranteed to work for JDBC drivers implementing JDBC 4.2 or above because that
JDBC specification is the first revision which mandates support for `OffsetDateTime`.

For mapping rows from a ResultSet, SQLBuilder offers `getList(Connection connection, SQLBuilder.RowMapper<T> rowMapper)`
which allows to further simplify code from the [Motivation](#motivation) section:

```java
SQLBuilder filter(int age) {
    return age > 0 ? new SQLBuilder("and age > ?", age) : new SQLBuilder("");
}

SQLBuilder query = new SQLBuilder("select first_name from person where last_name = ?", name).append(filter(age));
return query.getList(connection, rs -> rs.getString(1));
```

## Name Binding ##

String concatenation is the root of all evil, or at least the root for SQL injections. `PreparedStatement` and
`SQLBuilder` avoid the need to concatenate parameter values, removing one large source of SQL injections. However, there
are still cases where String concatenation is required. One such case occurs when the table or column names are not
static: `int num = new SQLBuilder("select count(*) from " + tableName").getInt(con, 1, 0);` is one simple example.
SQLBuilder allows to avoid SQL injections here by using name bindings:

```java
String tableName = "person";
List<String> columns = List.of("first_name", "age");
...
int num = new SQLBuilder("select count(*) from ${table}").bind("table", tableName).getInt(con, 1, 0);
new SQLBuilder("select ${columns} from person").bind("columns", columns);
```

These "bindings" are resolved by quoting the values and then using string replacement before passing the SQL fragment on
to `PreparedStatement`. The values can be either a single string or a list of strings (e.g. allowing to return a
variable list of columns). Unlike positional parameters, these bindings could potentially clash when the SQLBuilder
object is further modified:

```java
SQLBuilder addFilter(SQLBuilder sqlBuilder, String columnName, int value) {
    return sqlBuilder.append("and ${col} > ?", value).bind("col", columnName);
}

List<String> getAdults(Connection connection, String tableName, String resultColumnName) {
    SQLBuilder query = new SQLBuilder("select ${col} from ${table}")
        .bind("table", tableName)
        .bind("col", resultColumnName);
    addFilter(query, "age", 18);
    return query.getList(connection, rs -> rs.getString(1));
}

List<String> adults = getAdults(connection, "person", "first_name");
```

Note how `"col"` is used twice with different values. SQLBuilder detects this conflict at runtime and throws an
exception. However, the recommended approach is to always force `SQLBuilder` to apply the current bindings before
passing `SQLBuilder` objects with bindings between methods:

```java
SQLBuilder addFilter(SQLBuilder sqlBuilder, String columnName, int value) {
    return sqlBuilder.append("and ${col} > ?", value).bind("col", columnName).applyBindings();
}

List<String> getAdults(Connection connection, String tableName, String resultColumnName) {
    SQLBuilder query = new SQLBuilder("select ${col} from ${table}")
        .bind("table", tableName)
        .bind("col", resultColumnName);
        .applyBindings();
    addFilter(query, "age", 18);
    return query.getList(connection, rs -> rs.getString(1));
}

List<String> adults = getAdults(connection, "person", "first_name");
```

The explicit call to `applyBindings` can also be necessary for `SQLBuilder` objects that are local to a method but
modified in a loop. Here you must either make sure to use unique binding names in every iteration or call
`applyBindings` repeatedly. One example is

```java
List<String> cols = List.of("first_name", "last_name");
String lookingFor = "Mary";
...
SQLBuilder sb = new SQLBuilder("select count(*) from person where 1=0");
list.forEach(col -> sb.append("or ${name}=?", lookingFor).bind("name", col).applyBindings()); 
```

Using unique binding names is possible but often results in less readable code:

```java
List<String> cols = List.of("first_name", "last_name");
String lookingFor = "Mary";
...
SQLBuilder sb = new SQLBuilder("select count(*) from person where 1=0");
for (int i = 0; i < cols.size(); i++) {
    String name = "name" + i;
    sb.append(String.format("or ${%s}=?", name), lookingFor).bind(name, col);
}
```

## Masked Values ##

`SQLBuilder` automatically logs all queries with their parameter values before executing them. This can lead to problems
if parameter values are sensitive (e.g. passport numbers). `SQLBuilder` therefore allows to mask such values by wrapping
them in `SQLBuilder.mask() `calls:

```java
String bad = new SQLBuilder("select name from passports where num=?", "DE#12-22").getString(con, 1, null);
String good = new SQLBuilder("select name from passports where num=?", SQLBuilder.mask("DE#12-22")).getString(con, 1, null);
```

The first call will log `select name from passports where num=?; args = DE#12-22` while the second call will log `select
name from passports where num=?; args = __masked__:a323g3f`. The `mask` method computes the logged value using a hash
function over the value. Thus, the same value will result in the same logged value which allows to trace usages across
multiple log messages.

# Unit Testing #

When it comes to unit testing database access code, then the best advise often is: "Don't!". In most cases it is much
better to contain all database code in a single "database access object" (aka DAO) layer and mock that layer for unit
tests of higher layers. However, not all code is written like that. In addition, for code that dynamically constructs
SQL queries based on user input, testing that we generate syntactically correct SQL queries is still very useful.

For the rest of the unit testing discussion we assume that it is not possible to run SQL queries against a real
database.

## SQLBuilder Mocking ##

The usual mocking approach (using e.g. [Mockito]) is often very painful to use for SQL code:

-  mocking of methods like `ResultSet#getString` is a global operation which is not aligned to logical boundaries in the
   code. This easily leads to "off-by-one" errors if some code that is called before the intended usage of this mock
   adds or removes a call to `ResultSet#getString`.
-  coding of test data is labour intensive, especially for queries that fill complex pojos.

`SQLBuilder` therefore offers an optional native mocking solution which avoids these problems. In this mode,
`SQLBuilder` internally delegates the calls to a mocking invoker instead of to the usual JDBC invoker. Unit tests should
instruct SQLBuilder to use the mocking invoker which then automatically mocks all database access code. This mocking
invoker can be customized (more on that later), but the default behavior is to return a single row of made-up data for
every requested ResultSet. As an example, consider a method like this (error handling omitted for brevity):

```java
List<Person> getPersons(Connection con, List<Long> ids) {
    SQLBuilder sb = new SQLBuilder("select name, age from person");
    if (!ids.isEmpty()) {
        sb.append("where id in (?)", ids);
    }
    try (ResultSet rs = sb.getResultSet(con)) {
        List<Person> result = new ArrayList<>();
        while (rs.next()) {
            result.add(new Person(rs.getString("name"), rs.getInt("age")));
        }
        return result;
    }
}
```

This method can be unit tested without any prepared test data or mocking:

```java
SQLBuilder.setDelegate(new MockSQLBuilderProvider());
assertFalse(dao.getPersons(con, List.of(1L, 2L)).isEmpty());
```

Now consider a developer improves this method to:

```java
List<Person> getPersons(Connection con, List<Long> ids) {
    SQLBuilder sb = new SQLBuilder("select name, age from person");
    if (!ids.isEmpty()) {
        sb.append("where id in (?)", ids);
    }
    return sb.getList(con, rs -> new Peson(rs.getString(1), rs.getInt(2)));
}
```

If we would have mocked `rs.getString("name")`, our unit test now would fail. However, the mocking `SQLBuilder` will
return the same result as before and thus our unit test will continue to work.

Internally, the mocking provider of `SQLBuilder` will by default return a ResultSet with a single row and for that row
will answer `42` for every `ResultSet#getXXX` call (42 because it is an unusual enough answer to understand that this is
generated test data and also because it is [the answer] to the ultimate question of life, the universe, and everything).
If that is not good enough (e.g. because the code uses these results for branching decisions), then you can also direct
`SQLBuilder` to return data that you provided.

## Test data for Mocking ##

The mocking provider for `SQLBuilder` internally maintains a queue of ResultSets and offers APIs to add to that queue.
If the queue is depleted and the code asks for another ResultSet, then it either returns a generated ResultSet as
described above or will return `null` which will usually lead to a `NullPointerException` in the calling code.

As a practical example, assume we want to unit test the following method:

```java
int countChildren(Connection con, int maxAge) {
    return getPersons(con, List.of()).stream().filter(p -> p.age <= maxAge).count();
}
```

This will out of the box always return `1` because as explained above `getPersons` called in a test using the mocking
invoker will always return a single-element list of a `Person` with name `"42"` and age `42`. To unit test the correct
handling of `maxAge`, we thus need to prepare test data:

```java
MockSQLBuilderProvider.addResultSet("getPersons", "name,age", "Peter,12", "Paul,11", "Mary,15");
assertEquals(2, countChildren(con, 14));
```

We will discuss various ways to add `ResultSet` objects later. What is important to understand here is that these
`ResultSet` objects are consumed. Thus, duplicating the `assertEquals` line will result in a test failure because the
second call to `countChildren` will return `1` as explained above. One way to test multiple conditions is:

```java
MockResultSet children = MockResultSet.create("getPersons", "name,age", "Peter,12", "Paul,11", "Mary,15");
MockSQLBuilderProvider.addResultSet(children);
assertEquals(2, countChildren(con, 14));
MockSQLBuilderProvider.addResultSet(children);
assertEquals(0, countChildren(con, 5));
MockSQLBuilderProvider.addResultSet(children);
assertEquals(3, countChildren(con, 18));
```

The first parameter for creating such `MockResultSet` objects is always a "tag". This tag is currently not used
operationally. However, it is included in the `MockResultSet#toString` return value. If you choose your tags carefully
(e.g. by using the name of the method in which you expect this `MockResultSet` object to be consumed), then you can
visually compare the tag with the method names in the stack trace in a debugger. The best way to accomplish this is to
set a breakpoint inside `MockSQLBuilderProvider#getRs`. This is of tremendous value while writing unit tests or adopting
them to changed code because it allows to easily spot these "off-by-one" errors where a test data set is consumed by the
wrong method.

There are numerous variants to create test data sets, see the `SQLBuilder` Javadoc and it's unit test code for the
complete list. Here we just highlight a few important ones:

-  `MockResultSet.empty(String tag)` creates a result set without any data (i.e. calls to `ResultSet#next` always return
   false).

-  `MockResultSet.broken(String tag)` creates a result set which throws a `SQLException` when the caller tries to return
   it from `SQLBuilder`.

-  `MockResultSet.create(String tag, String labels, String... rows)` creates a result set which uses the labels (split
   by `,`) as column names and creates a row from each of the `rows` values.

-  `MockResultSet.create(String tag, InputStream inputStream, boolean withLabels)` creates a result set from an
   InputStream. This is most useful if you extracted some test data from a database in CSV format and stored it in a
   file inside your test setup. This allows to create test data from such files using code like
   ```java
   MockResultSet csv = MockResultSet.create("getPersons", getClass().getResourceAsStream("persons.csv"), true);
   ```

- `MockResultSet.create(String tag, String[] labels, Object[][] data)` is the most low-level approach but unlike most
  other approaches allows to use correctly typed data instead of strings.

As explained above, these test data sets must be added to `MockSQLBuilderProvider` so that they can be consumed by your
code. Apart from the generic `MockSQLBuilderProvider.addResultSet(ResultSet resultSet)` which can be used to enqueue
such `MockResultSet` objects, `MockSQLBuilderProvider` also offers some shortcuts for commonly used calls as shown
above. Please look at the JavaDoc for the complete list.

So far we only looked at test data for `SQLBuilder` methods that return or consume `ResultSet` objects. However, there
is also `SQLBuilder#execute` which usually returns the result of calling `PreparedStatement#executeUpdate`. When using
the mocking provider, `SQLBuilder#execute` will by default return 42. If that is not acceptable (e.g. because the
calling code checks the return value), then the return value can be explicitly set by calling
`MockSQLBuilderProvider#setExecute`. The simplest case is to always return the same value using e.g.
`MockSQLBuilderProvider.setExecute(1)`. You can also pass multiple values in which case the results will be the values
provided, followed by the default `42`. The most flexible approach is to call `MockSQLBuilderProvider#setExecute` with a
`Supplier<Integer>` value which allows code like

```java
final AtomicInteger count = new AtomicInteger();
MockSQLBuilderProvider.setExecute(() -> count.getAndIncrement() < 3 ? 1 : 2);
```
## Other Ways To Provide Test Data ##

The final alternative for providing test data is to register supplier functions for `SQLBuilder#getInt` and related
methods. Similar to the supplier approach for `SQLBuilder#execute`, you can force `SQLBuilder#getInt` to always return
`10` for the first 5 columns and same value using e.g. `MockSQLBuilderProvider.setIntByColumnIndex((idx, def) -> idx <
10 ? 5 : def)`.

> It is unclear if these other ways are really adding anything or just complicating the approach. We might decide to
> remove them in a future version.

## Notes about Unit Testing ##

First of all: you must correctly prepare your unit test code to use the mocking provider. This is done using the
following steps (all examples are given for [Junit5], adapt for your test framework as required):

1. Change `SQLBuilder` to use the mocking provider using

   ```java
    @BeforeAll
    static void beforeAll() {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider());
    }
   ```

2. Reset MockSQLBuilderProvider to clear the test data queue and ensure that "SQLBuilder#execute" returns 42 executing
   tests to prevent spill-over from previous tests:

   ```java
    @BeforeEach
    void setUp() {
        MockSQLBuilderProvider.reset();
    }
   ```

3. Remove all other mocking for `SQLBuilder`:
   - remove all `SQLBuilder fields with their `@Mock` annotation
   - remove `SQLBuilder.class` from `@PrepareForTest`
   - remove all `mockStatic(SQLBuilder.class)`
   - remove all `SQLBuilder` variables, especially when initialized using `mock(SQLBuilder.class)`
   - remove all mocking for `SQLBuilder` constructors like `PowerMockito.whenNew(SQLBuilder.class)...`
   - remove all mocking for `SQLBuilder` methods like `when(sqlBuilder.getResultSet(conn)).thenReturn(rs);`
   - remove all mocking for `ResultSet` methods like `when(rs.next()).thenReturn(true);` or
     `when(rs.getLong("ID")).thenReturn(1L);`

# Release Notes #

- Version 2.0.0, released 2021-03-08
  - removed `com.metricstream.util.Check` class: this was only used internally in a few places but polluted the name
    space. Thus, this version inlines the usages and removes the class. Although no existing user called this class,
    removing it changes the public API in a backwards-incompatible way and thus mandates a major version increase.
  - added method `withMaxRows(int)` to limit the number of returned rows
  - added method `getMap` similar to `getList`, which -- together with the new `entry` method -- is a shortcut to create
    maps from `ResultSet` objects
  - added `execute` method variant which allows passing names of columns for which to return values from the inserted
    row(s)
  - added `getInstant` method which returns `Instant` for the provided column
  - added `getDateTime` method which returns `OffsetDateTime` for the provided column
  - upgraded external dependencies
    - `commons-codec:commons-codec` from `1.14` to `1.15`
    - `org.junit.jupiter:junit-jupiter-api` from `5.6.2` to `5.7.1`
    - `org.junit.jupiter:junit-jupiter-engine` from `5.6.2` to `5.7.1`
    - `org.mockito:mockito-core` from `3.6.0` to `3.8.0`

- Version 1.0.2, released 2020-05-17
  - initial public release



[JdbcTemplate]: https://spring.io/guides/gs/relational-data-access/
[JDBI]: https://jdbi.org
[SQLBuilder]: https://sqlbuilder.whereever
[JDBC]: https://www.oracle.com/technetwork/java/javase/tech/index-jsp-136101.html
[Mockito]: https://site.mockito.org/
[OpenCSV]: https://sourceforge.net/projects/opencsv/
[SLF4J]: http://www.slf4j.org/
[Commons Codec]:  https://commons.apache.org/proper/commons-codec/
[JOOQ]: https://www.jooq.org/
[Hibernate]: https://hibernate.org/
[iBATIS]: https://ibatis.apache.org/
[the answer]: https://en.wikipedia.org/wiki/Phrases_from_The_Hitchhiker%27s_Guide_to_the_Galaxy#Answer_to_the_Ultimate_Question_of_Life,_the_Universe,_and_Everything_(42)
[Junit5]: https://junit.org/junit5/
[MetricStream]: https://www.metricstream.com/
