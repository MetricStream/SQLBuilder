[![MetricStream](MetricStream_Logo.png)][MetricStream]

# Preface #

SQLBuilder is a JDBC-based Java library created by [MetricStream] for using SQL databases from within your Java
application.

# Introduction #

In an ideal world, no Java (or Kotlin) developer would ever have to directly interact with [JDBC]. Instead, we would all
use ORM tools like [Hibernate] or [iBATIS] or [JOOQ] to access our relational database. However, the "enterprise world"
still has lots of legacy code that was written using JDBC. Converting legacy code to use such an ORM is a very intrusive
and time-consuming process. Another alternative is to use libraries like Spring [JdbcTemplate] or [JDBI]. Using these
requires much less refactoring and is thus often the better alternative (especially if switching to an ORM framework is
not possible and thus the only other option is to do nothing).

SQLBuilder is a small library which is an alternative to [JDBI]. The one standout feature of [SQLBuilder] compared to
[JDBI] is that it allows composing partial queries, passing them between methods, and then automatically combining them
to the final query. This makes it easier to convert legacy code which e.g. adds filters to queries using separate
methods.

SQLBuilder is database agnostic and its only dependencies are [JDBC], [SLF4J], and [Commons Codec]. SQLBuilder also has
an optional test driver which in addition requires [Mockito] and [OpenCSV].

# Motivation #

We were faced with lots of legacy code using variants of the following (simplified code w/o error handling etc.):
- Java
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
- Kotlin
```kotlin
fun filter(age: Int): String {
    return if (age > 0) " and age >= ?" else ""
}

fun friends(age: Int): List<String> {
    val query = "select first_name from person where last_name = ?" + filter(age);
    con.prepareStatement(query).use { ps ->
        ps.setString(1, name)
        if (age > 0) {
            ps.setInt(2, age)
        }
        ps.getResultSet().use { rs ->
            val names = mutableListOf<String>()
            while (rs.next()) {
                names += rs.getString(1)
            }
            return names
        }
    }
}
```

What stands out here is that the SQL fragments and the query parameters are disconnected:
 - The place where the SQL is constructed and the place where the parameter values are provided are lines apart
 - Conditions like the `age > 0` have to be repeated

Compare that code with the equivalent SQLBuilder-based code:
- Java
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
- Kotlin
```kotlin
fun filter(age: Int): SQLBuilder {
    return if (age > 0) SQLBuilder("and age > ?", age) else SQLBuilder("")
}

fun friends(age: Int): List<String> {
    val query = SQLBuilder("select first_name from person where last_name = ?", name).append(filter(age))
    sb.getResultSet(con).use { rs ->
        val names = mutableListOf<String>()
        while (rs.next()) {
            names += rs.getString(1)
        }
        return names
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
- Java
```java
SQLBuilder core1 = new SQLBuilder("select name, age from person");
SQLBuilder core2 = new SQLBuilder("select name, age from person where age > ?", age);
SQLBuilder core3 = new SQLBuilder("select name, age from person where age > ? and age < ?", age, age);
SQLBuilder core4 = new SQLBuilder(core3);
```
- Kotlin
```kotlin
val core1 = SQLBuilder("select name, age from person")
val core2 = SQLBuilder("select name, age from person where age > ?", age)
val core3 = SQLBuilder("select name, age from person where age > ? and age < ?", age, age)
val core4 = SQLBuilder(core3)
```


These objects can then be further modified using the `append` method which is also called with a SQL fragment and
optional parameters:
- Java
```java
core1.append("and name not null");
core2.append("and name not null").append("and id in (?)", ids);
```
- Kotlin
```kotlin
core1.append("and name not null")
core2.append("and name not null").append("and id in (?)", ids)
```


The last example highlights another advantage of `SQLBuilder` over `PreparedStatement`: passing a list as a parameter
value automatically replaces the matching `?` with the correct number of `?`. Thus, if `ids` from above is a list
containing 3 `Long` values, the resulting SQL query for `core2` would be `"select name, age from person where age > ?
and age < ? and name not null and id in (?,?,?)"` (and yes: SQLBuilder is smart enough to add a space before appending
further fragments).

Both the constructor and `append` also accept a `SQLBuilder` object. This is useful if we create partial queries in
other methods or need 2 queries with an identical core part: first create the core, and then create the 2 variants:
- Java
```java
SQLBuilder core = new SQLBuilder("select name from person");
SQLBuilder filter1 = new SQLBuilder("where age > 18");
SQLBuilder filter2 = new SQLBuilder("where first_name like '%A'");
SQLBuilder variant1 = new SQLBuilder(core).append(filter1);
SQLBuilder variant2 = new SQLBuilder(core).append(filter2);
```
- Kotlin
```kotlin
val core = SQLBuilder("select name from person")
val filter1 = SQLBuilder("where age > 18")
val filter2 = SQLBuilder("where first_name like '%A'")
val variant1 = SQLBuilder(core).append(filter1)
val variant2 = SQLBuilder(core).append(filter2)
```


In addition, `SQLBuilder` offers some constructor function like `wrap` which modifies a `SQLBuilder` by wrapping the SQL
fragment in the provided values:
- Java
```java
SQLBuilder core = new SQLBuilder(...);  
SQLBuilder count = core.wrap("select count(*) from (", ")");
```
- Kotlin
```kotlin
val core = SQLBuilder(...)  
val count = core.wrap("select count(*) from (", ")")
```


Please note that this mutates the `core` object. If that is not desired (e.g. because you also need to execute the
unmodified `core` object), instead use
- Java
```java
SQLBuilder core = new SQLBuilder(...);  
SQLBuilder count = new SQLBuilder(core).wrap("select count(*) from (", ")");
```
- Kotlin
```kotlin
val core = SQLBuilder(...)  
val count = SQLBuilder(core).wrap("select count(*) from (", ")")
```


## Using SQLBuilder Objects ##

There are 2 basic operations by which `SQLBuilder` objects can be used:

 1. create a ResultSet object using `getResultSet(Connection)`
 2. execute the query and return the result of that using `execute(Connection)`

In addition, `SQLBuilder` offers a set of convenience methods to optimize 2 common cases:

 1. return a single value from a query
 2. map the returned ResultSet objects into a list or a map of application-level objects

For single values, SQLBuilder offers methods like

-  `int getInt(Connection connection, int columnIndex, int defaultValue)`
-  `int getInt(Connection connection, String columnName, int defaultValue)`

The same methods are available for `String`, `Long`, `BigDecimal` and `Object`. They all return a value from the first
returned row, or the default value if no row was returned. Examples for these methods are:
- Java
```java
int count = new SQLBuilder("select count(*) from person where age > ?", age).getInt(connection, 1, 0);
String name = new SQLBuilder("select last_name from person where id = ?", pid).getString(connection, "last_name", null);
```
- Kotlin
```kotlin
val count = SQLBuilder("select count(*) from person where age > ?", age).getInt(connection, 1, 0)
val name = SQLBuilder("select last_name from person where id = ?", pid).getString(connection, "last_name", null)
```


As an additional feature, `SQLBuilder` also offers `getDateTime` as a single
value method.  This is implemented using `getObject(column,
OffsetDateTime.class)` and thus returns an `OffsetDateTime` object.  `getDateTime` can be
used for columns of type `TIMESTAMP WITH TIME ZONE`.  However, this is only
guaranteed to work for JDBC drivers implementing JDBC 4.2 or above because that
JDBC specification is the first revision which mandates support for `OffsetDateTime`.

For mapping rows from a `ResultSet`, `SQLBuilder` offers `getList(Connection connection, SQLBuilder.RowMapper<T>
rowMapper)` which allows to further simplify code from the [Motivation](#motivation) section:
- Java
```java
SQLBuilder query = new SQLBuilder("select first_name, age from person where last_name = ?", name);
if (age > 0) {
    query.append("and age > ?", age);
}
List<String> firstNames = query.getList(connection, rs -> rs.getString(1));
Map<String, Integer> ages = query.getMap(connection, rs -> SQLBuilder.entry(rs.getString(1), rs.getInt(2)));
```
- Kotlin
```kotlin
val query = SQLBuilder("select first_name, age from person where last_name = ?", name)
if (age > 0) {
    query.append("and age > ?", age)
}
val firstNames = query.getList(connection, { rs -> rs.getString(1) })
val ages = query.getMap(connection, { rs -> SQLBuilder.entry(rs.getString(1), rs.getInt(2)) })
```


## Name Binding ##

String concatenation is the root of all evil, or at least the root for SQL injections. `PreparedStatement` and
`SQLBuilder` avoid the need to concatenate parameter values, removing one large source of SQL injections. However, there
are still cases where String concatenation is required. One such case occurs when the table or column names are not
static: `int num = new SQLBuilder("select count(*) from " + tableName").getInt(con, 1, 0);` is one simple example.
SQLBuilder allows avoiding SQL injections here by using name bindings:
- Java
```java
String tableName = "person";
List<String> columns = List.of("first_name", "age");
...
int num = new SQLBuilder("select count(*) from ${table}").bind("table", tableName).getInt(con, 1, 0);
new SQLBuilder("select ${columns} from person").bind("columns", columns);
```
- Kotlin
```kotlin
val tableName = "person"
val columns = listOf("first_name", "age")
...
val num = SQLBuilder("select count(*) from #{table}").bind("table", tableName).getInt(con, 1, 0)
SQLBuilder("select #{columns} from person").bind("columns", columns)
```


These "bindings" are resolved by quoting the values and then using string replacement before passing the SQL fragment on
to `PreparedStatement`. The values can be either a single string or a list of strings (e.g. allowing to return a
variable list of columns). Unlike positional parameters, these bindings could potentially clash when the SQLBuilder
object is further modified:
- Java
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
- Kotlin
```kotlin
fun addFilter(sqlBuilder: SQLBuilder, columnName: String, value: Int): SQLBuilder {
    return sqlBuilder.append("and #{col} > ?", value).bind("col", columnName)
}

fun getAdults(connection: Connection, tableName: String, resultColumnName: String): List<String> {
    val query = SQLBuilder("select #{col} from #{table}")
        .bind("table", tableName)
        .bind("col", resultColumnName)
    addFilter(query, "age", 18)
    return query.getList(connection, { rs -> rs.getString(1) })
}

val adults = getAdults(connection, "person", "first_name")
```


Note how `"col"` is used twice with different values. SQLBuilder detects this conflict at runtime and throws an
exception. However, the recommended approach is to always force `SQLBuilder` to apply the current bindings before
passing `SQLBuilder` objects with bindings between methods:
- Java
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
- Kotlin
```kotlin
fun addFilter(sqlBuilder: SQLBuilder, columnName: String, value: Int): SQLBuilder {
    return sqlBuilder.append("and #{col} > ?", value).bind("col", columnName).applyBindings()
}

fun getAdults(connection: Connection, tableName: String, resultColumnName: String): List<String> {
    val query = SQLBuilder("select #{col} from #{table}")
        .bind("table", tableName)
        .bind("col", resultColumnName)
    addFilter(query, "age", 18)
    return query.getList(connection, { rs -> rs.getString(1) })
}

val adults = getAdults(connection, "person", "first_name")
```


The explicit call to `applyBindings` can also be necessary for `SQLBuilder` objects that are local to a method but
modified in a loop. Here you must either make sure to use unique binding names in every iteration or call
`applyBindings` repeatedly. One example is:
- Java
```java
List<String> cols = List.of("first_name", "last_name");
String lookingFor = "Mary";
...
SQLBuilder sb = new SQLBuilder("select count(*) from person where 1=0");
list.forEach(col -> sb.append("or ${name}=?", lookingFor).bind("name", col).applyBindings());
```
- Kotlin
```kotlin
val cols = listOf("first_name", "last_name")
val lookingFor = "Mary"
...
val sb = SQLBuilder("select count(*) from person where 1=0")
list.forEach { col -> sb.append("or #{name}=?", lookingFor).bind("name", col).applyBindings() } 
```

Using unique binding names is possible but often results in less readable code:
- Java
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
- Kotlin
```kotlin
val cols = listOf("first_name", "last_name")
val lookingFor = "Mary"
...
val sb = SQLBuilder("select count(*) from person where 1=0")
cols.forEachIndexed { i, col -> "name$i".let { name -> sb.append("or #{$name}=?"), lookingFor).bind(name, col) } }
```


## Masked Values ##

`SQLBuilder` automatically logs all queries with their parameter values before executing them. This can lead to problems
if parameter values are sensitive (e.g. passport numbers). `SQLBuilder` therefore allows to mask such values by wrapping
them in `SQLBuilder.mask() `calls:
- Java
```java
String bad = new SQLBuilder("select name from passports where num=?", "DE#12-22").getString(con, 1, null);
String good = new SQLBuilder("select name from passports where num=?", SQLBuilder.mask("DE#12-22")).getString(con, 1, null);
```
- Kotlin
```kotlin
val bad = SQLBuilder("select name from passports where num=?", "DE#12-22").getString(con, 1, null)
val good = SQLBuilder("select name from passports where num=?", SQLBuilder.mask("DE#12-22")).getString(con, 1, null)
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

`SQLBuilder` therefore offers `MockSQLBuilderProvider` as an optional native mocking solution which avoids these
problems. After enabling `MockSQLBuilderProvider` by calling `MockSQLBuilderProvider.enable()`, `SQLBuilder` internally
delegates all calls to the mocking invoker instead of to the usual JDBC invoker. Unit tests should therefore instruct
SQLBuilder to use the mocking invoker which then automatically mocks all database access code. This mocking invoker can
be customized (more on that later), but the default behavior is to return a single row of made-up data for every
requested ResultSet. As an example, consider a method like this (error handling omitted for brevity):
- Java
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
- Kotlin
```kotlin
fun getPersons(con: Connection, ids: List<Long>): List<Person> {
    val sb = SQLBuilder("select name, age from person")
    if (ids.isNotEmpty()) {
        sb.append("where id in (?)", ids)
    }
    sb.getResultSet(con).use { rs ->
        return buildList {
            while (rs.next()) {
                add(Person(rs.getString("name"), rs.getInt("age")))
            }
        }
    }
}
```


This method can be unit tested without any prepared test data or mocking:
- Java
```java
assertFalse(dao.getPersons(con, List.of(1L, 2L)).isEmpty());
```
- Kotlin
```kotlin
assertFalse(dao.getPersons(con, listOf(1L, 2L)).isEmpty())
```

Because `MockSQLBuilderProvider` mocks all `SQLBuilder`-initiated calls, unit tests are generally easier to write and
more robust. As an example, consider a developer improves the `getPersons` method to:
- Java
```java
List<Person> getPersons(Connection con, List<Long> ids) {
    SQLBuilder sb = new SQLBuilder("select name, age from person");
    if (!ids.isEmpty()) {
        sb.append("where id in (?)", ids);
    }
    return sb.getList(con, rs -> new Person(rs.getString(1), rs.getInt(2)));
}
```
- Kotlin
```kotlin
fun getPersons(con: Connection, ids: List<Long>): List<Person> {
    val sb = SQLBuilder("select name, age from person")
    if (ids.isNotEmpty()) {
        sb.append("where id in (?)", ids)
    }
    return sb.getList(con, { rs -> Person(rs.getString(1), rs.getInt(2)) })
}
```

If we had mocked e.g. `rs.getString("name")`, our unit test would now fail. However, the mocking `SQLBuilder` will
return the same result as before and thus our unit test will continue to work.

By default, the `MockSQLBuilderProvider` will return a `ResultSet` object with a single row for every
`SQLBuilder#getResultSet` call and for that row will answer `42` for every `ResultSet#getXXX` call (42 because it is an
unusual enough answer to understand that this is generated test data and also because it is [the answer] to the ultimate
question of life, the universe, and everything). If that is not good enough (e.g. because the code uses these results
for branching decisions), then you can also direct `MockSQLBuilderProvider` to return data that you provided.

## Test Data for Mocking ##

`MockSQLBuilderProvider` internally maintains a queue of `ResultSet` objects and offers APIs to add to that queue. If
the queue is depleted and the code asks for another ResultSet, then it either returns a generated ResultSet as described
above or will return `null` which will usually lead to a `NullPointerException` in the calling code.

As a practical example, assume we want to unit test the following method:
- Java
```java
int countChildren(Connection con, int maxAge) {
    return getPersons(con, List.of()).stream().filter(p -> p.age <= maxAge).count();
}
```
- Kotlin
```kotlin
fun countChildren(con: Connection, maxAge: Int): Int {
    return getPersons(con, emptyList()).count { p -> p.age <= maxAge }
}
```


This will out of the box always return `1` because as explained above `getPersons` called in a test using the mocking
invoker will always return a single-element list of a `Person` with name `"42"` and age `42`. To unit test the correct
handling of `maxAge`, we thus need to prepare test data:
- Java
```java
MockResultSet.add("getPersons", "name,age", "Peter,12", "Paul,11", "Mary,15");
assertEquals(2, countChildren(con, 14));
```
- Kotlin
```kotlin
MockResultSet.add("getPersons", "name,age", "Peter,12", "Paul,11", "Mary,15")
assertEquals(2, countChildren(con, 14))
```


We will discuss various ways to add `ResultSet` objects later. What is important to understand here is that every
`ResultSet` object is only used for a single `SQLBuilder#getResultSet` call. Thus, duplicating the `assertEquals` line
will result in a test failure because the second call to `countChildren` will return `1` as explained above. One way to
test multiple conditions is:
- Java
```java
MockResultSet children = MockResultSet.create("getPersons", "name,age", "Peter,12", "Paul,11", "Mary,15");
MockSQLBuilderProvider.addResultSet(children);
assertEquals(2, countChildren(con, 14));
MockSQLBuilderProvider.addResultSet(children);
assertEquals(0, countChildren(con, 5));
MockSQLBuilderProvider.addResultSet(children);
assertEquals(3, countChildren(con, 18));
```
- Kotlin
```kotlin
val children = MockResultSet.create("getPersons", "name,age", "Peter,12", "Paul,11", "Mary,15")
MockSQLBuilderProvider.addResultSet(children)
assertEquals(2, countChildren(con, 14))
MockSQLBuilderProvider.addResultSet(children)
assertEquals(0, countChildren(con, 5))
MockSQLBuilderProvider.addResultSet(children)
assertEquals(3, countChildren(con, 18))
```

Another way is to instruct MockSQLBuilderProvider to use the data from a MockResultSet object multiple times by calling
`MockResultSet.add` with a `usage` value:
- Java
```java
MockResultSet.add(
        "getPersons",
        new String[] { "name", "age" },
        new Object[][] { { "Peter", 12 }, { "Paul", 11 }, { "Mary", 15 } },
        3
);
assertEquals(2, countChildren(con, 14));
assertEquals(0, countChildren(con, 5));
assertEquals(3, countChildren(con, 18));
```
- Kotlin
```kotlin
MockResultSet.add(
        "getPersons",
        arrayOf("name", "age"),
        arrayOf(arrayOf("Peter", 12), arrayOf("Paul", 11), arrayOf("Mary", 15)),
        3
)
assertEquals(2, countChildren(con, 14))
assertEquals(0, countChildren(con, 5))
assertEquals(3, countChildren(con, 18))
```

The first parameter for creating such `MockResultSet` objects is always a "tag". We will describe the reasons for these
tags later. For now, just assume it provides a means to identify a specific mocked object.

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
  other approaches allows using correctly typed data instead of strings.

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
- Java
```java
final AtomicInteger count = new AtomicInteger();
MockSQLBuilderProvider.setExecute("foo", () -> count.getAndIncrement() < 3 ? 1 : 2);
```
- Kotlin
```kotlin
val count = AtomicInteger()
MockSQLBuilderProvider.setExecute("foo", { count.getAndIncrement() < 3 ? 1 : 2 } )
```


## Tags and Tag Matching Enforcement ##

By default, tags of `MockResultSet` objects are not used operationally. However, they are included in the
`MockResultSet#toString` return value which can be observed in a debugger. The best way to accomplish this is to set a
breakpoint inside `MockSQLBuilderProvider#getRs`.

However, creating `MockSQLBuilderProvider` using `SQLBuilder.setDelegate(new MockSQLBuilderProvider(true, true))` will
turn on the `enforceTags` option (using the 2nd `true`; the first one is used to control whether to synthesize ResultSet
objects if none were explicitly added and is `true` by default). In this mode, the tag values of `MockResultSet` objects
are checked against the names of the methods in which they are consumed. More precisely: the method name must be equal
to the part of the tag name before the first `:` in the tag name. If this fails, an `IllegalStateException` is thrown.
The `:` rule allows adding more information about the tagged `MockResultSet` object and is especially useful if a method
contains multiple SQL queries.

As a concrete example, consider the following code:
- Java
```java
int getCount(Connection con) throws SQLException {
    int count = new SQLBuilder("select count(*) from persons").getInt(con, 1, -1);
    if (count <= 0) {
        count = new SQLBuilder("select count(*) from aliens").getInt(con, 1, -1);
    }
    return count;
}
```
- Kotlin
```kotlin
fun getCount(con: Connection): Int {
    var count = SQLBuilder("select count(*) from persons").getInt(con, 1, -1)
    if (count <= 0) {
        count = SQLBuilder("select count(*) from aliens").getInt(con, 1, -1)
    }
    return count
}
```


This method can be tested using:
- Java
```java
MockSQLBuilderProvider.addResultSet("getCount:persons", "10");
assertEquals(10, getCount(connection));
MockSQLBuilderProvider.addResultSet("getCount:persons", "0");
MockSQLBuilderProvider.addResultSet("getCount:aliens", "5");
assertEquals(5, getCount(connection));
```
- Kotlin
```kotlin
MockSQLBuilderProvider.addResultSet("getCount:persons", "10")
assertEquals(10, getCount(connection))
MockSQLBuilderProvider.addResultSet("getCount:persons", "0")
MockSQLBuilderProvider.addResultSet("getCount:aliens", "5")
assertEquals(5, getCount(connection))
```

Enforcing matching tag names helps to detect a very common problem with "classical" `ResultSet` mocking: if the tested
code was changed to add or remove a query somewhere, the mocked data gets out of sync. Such errors often manifest
themselves not where they occur but in a later step and are notoriously hard to trace. Enforcing matching tag names
instead directly stops the execution with a useful error message (containing the method name and the mismatched tag
name).

In summary, enforcing tag name matching is highly recommended. If it were not for backwards compatibility, this would be
enabled by default by now.

## Invocation counts ##

Sometimes. it is necessary to know how often specific mocked methods where called. Standard mocking frameworks like
Mockito or Mockk have "verify" methods which support such requirements. `MockSQLBuilder` has a similar though more
simple concept: it counts invocations of methods like `getResultSet` or `getLong` and allows to read these counters. A
concrete example would be (using [Assert4J] or [Kotest] assertions):
- Java
```java
@Test void checkGetTotal() {
    int total = getTotal();
    assertThat(total).isEqualTo(10);
    assertThat(MockSQLBuilderProvider.invocations.getNext()).isEqualTo(5);
}
```
- Kotlin
```kotlin
@Test fun `check getTotal`() {
    val total = getTotal()
    total shouldBe 10
    MockSQLBuilderProvider.invocations.getNext shouldBe 5
}
```

which ensures that `ResultSet#getNext` was called 5 times during the execution of `getTotal`. As described above, the
best practice is to call `MockSQLBuilder#reset` within `@AfterEach`, which resets all the invocation counts to `0`.


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

1. Change `SQLBuilder` to use the mocking provider with "autogenerate resultsets" and "enforce tags" enabled using

   ```java
    @BeforeAll
    static void beforeAll() {
        MockSQLBuilderProvider.enable();
    }
   ```

2. Make sure to restore the original `SQLBuilder` behavior once your test class is done

   ```java
    @AfterAll
    static void afterAll() {
        MockSQLBuilderProvider.disable();
    }
   ```

3. Reset MockSQLBuilderProvider to clear the test data queue and ensure that "SQLBuilder#execute" returns 42 executing
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
- Version 3.4.4, released 2022-01-06
  - ignore groovy runtime stack entries in tag matching code

- Version 3.4.3, released 2022-01-05
  - fixed bug in tag matching code
  - upgraded to Gradle 7.3.3
  - upgraded external dependencies
    - `ch.qos.logback:logback-classic` from `1.2.8` to `1.2.9`
    - `com.oracle.database.jdbc:ojdbc8` from `21.3.0.0` to `21.4.0.0.1` (only used for "examples" module)

- Version 3.4.2, released 2021-12-15
  - upgraded to Gradle 7.3.2

- Version 3.4.0, released 2021-12-14
  - upgraded to Kotlin 1.6.10
  - upgraded external dependencies
      - `ch.qos.logback:logback-classic` from `1.2.7` to `1.2.8`
    
- Version 3.3.0, released 2021-12-08
  - added `MockResultSet#add` methods which combine `create` and `MockResultSetProvider#addResultSet`
  - deprecated createXXX methods for `MockResultSet`
  - added `addGenerated` method for `MockResultSet`
  - reimplemented `MockResultSet` as standard derived class instead of as mocked `ResultSet`
  - added `MockResultSet#addGenerated` to add generated ResultSet objects
  - upgraded to Kotlin 1.6.0
  - upgraded to Gradle 7.3.1
  - upgraded external dependencies
    - `ch.qos.logback:logback-classic` from `1.2.6` to `1.2.7`
    - `org.slf4j:slf4j-api` from `1.7.31` to `1.7.32`
    - `org.junit.jupiter:junit-jupiter-api` from `5.8.1` to `5.8.2`
    - `org.junit.jupiter:junit-jupiter-engine` from `5.8.1` to `5.8.2`
  
- Version 3.2.2, released 2021-11-08
  - added `SQLBuilder.getDouble` and mocking support for `ResultSet.getDouble`

- Version 3.1.0, released 2021-10-15
  - allows to change ResultSet concurrency from default `CONCUR_READ_ONLY` to `CONCUR_UPDATABLE`
  - upgraded to Gradle 7.2
  - upgraded to Kotlin 1.5.31
  - upgraded to JVM 11
  - upgraded external dependencies
    - `ch.qos.logback:logback-classic` from `1.2.5` to `1.2.6`
    - `org.junit.jupiter:junit-jupiter-api` from `5.7.2` to `5.8.1`
    - `org.junit.jupiter:junit-jupiter-engine` from `5.7.2` to `5.8.1`
    - `org.mockito:mockito-core` from `3.11.2` to `4.0.0`
    - `org.mockito:mockito-junit-jupiter` from `3.11.2` to `4.0.0`
    - `com.opencsv:opencsv` from `5.4` to `5.5.2`
    - `org.postgresql:postgresql` from `42.2.19` to `42.2.24` (only used for "examples" module)
    - `com.oracle.database.jdbc:ojdbc8` from `21.1.0.0` to `21.3.0.0` (only used for "examples" module)
    - `org.jlleitschuh.gradle.ktlint` from `10.1.0` to `20.2.0` (only used for build)

- Version 3.0.2, released 2021-08-18
  - switched implementation language from Java to Kotlin (public API unchanged)
  - uses Kotest assertions instead of JUnit+Hamcrest assertions for Kotlin tests
  - uses AssertJ assertions instead of JUnit+Hamcrest for Java tests

- Version 3.0.1, released 2021-06-26
  - switched to Gradle 7.1

- Version 3.0.0, released 2021-06-26
  - switched to Mockito 3
  - upgraded external dependencies
    - `org.junit.jupiter:junit-jupiter-api` from `5.7.1` to `5.7.2`
    - `org.junit.jupiter:junit-jupiter-engine` from `5.7.1` to `5.7.2`
    - `org.mockito:mockito-core` from `1.10.19` to `3.11.2`
    - `org.slf4j:slf4j-api` from `1.7.30` to `1.7.31`
  - added `SQLBuilder.getDate` and mocking support for `ResultSet.getDate`
  - added `MockSQLBuilderProvider.enable` and `MockSQLBuilderProvider.disable`

- Version 2.2.0, released 2021-04-03
  - added `SQLBuilder.getTimestamp` and mocking support for `ResultSet.getTimestamp`

- Version 2.1.1, released 2021-04-02
  - reverted Mockito back to version `1.10.19` because of problems while upgrading client apps
  - added `SQLBuilder.resetDelegate`

- Version 2.0.2, released 2021-03-24
  - improved the documentation
  - updated copyright to include 2021

- Version 2.0.1, released 2021-03-18
  - switched to new MetricStream logo
  - use `wkhtmltopdf` to generate PDF version of the documentation
  - upgraded external dependencies
    - `gradle` from `6.4` to `6.8.3`
    - `com.opencsv:opencsv` from `5.3` to `5.4`
    - `org.postgresql:postgresql` from `42.2.18` to `42.2.19`

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
  - added `enforceTags` option to `mock` which requires that the tag names of `MockResultSet` objects match the method names
    in which the objects are consumed
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
[Assert4J]: https://joel-costigliola.github.io/assertj/
[Kotest]: https://kotest.io/
