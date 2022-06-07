# Use map instead of list for queuing resultsets

Right now, the only way to add resultsets is to queue them. This works normally nicely. However, sometimes we want to
provide just an explicit resultset for a specific query and want to use synthesized for the preceding queries. That is
currently not possible. I see 2 options:

   1. switch from a queue to a map from method name to queue
   2. add a `MockResultSet.synthetic()` method
   3. add a `MockResultSetBuilder.synthesize(n)` method

The 1rd option would require enforced tag name matching, but could then be used pretty flexible. One API idea is

```java
MockResultSetBuilder.add("getFoo", MockResultSet.create("col1", "33"));
```
Then, whenever we need a resultset, we look up the queue from the method name. However, this will not work if a method
contains more than 1 `SQLBuilder` query because we would not know which one to use.

The 3nd option looks pretty simple to implement and use: `MockResultSetBuilder.synthesize(4);` would enqueue 4
synthesized resultsets. Only downside is that we need counting. However, with enforced tag name matching we would at
least discover off-by-one errors as soon as we encounter a non-synthetic resultset.

The 2nd option does not seem to have any real advantage over the 3rd option.


# Improve or remove `setExecute`

The first problem with `setExecute` is that it does not support tags. That is just a miss which must be fixed. However,
another problem is that a code that does

```java
int stats() {
    int count = new SQLBuilder ("select count(*) from foo").getInt(con, 1, 0);
    int updated = new SQLBuilder("update counters set val=? where key=?", count, "foo").execute(con);
    return new SQLBuilder("select sum(val) from counters").getInt(con, 1, 0);
}
```

need mocking

```java
MockResultSetBuilder.add(MockResultSet.create("stats:count", "3"));
MockResultSetBuilder.setExecute(1);
MockResultSetBuilder.add(MockResultSet.create("stats:total", "6"));
```

But this `setExecute` is really different in that it does not enqueue a result. Moving this above of below the `add`
calls would not change the outcome. And if the method (or any method in the call graph) more `execute` calls, then we
quickly arrive back at the unmaintainable `.setExecute(3, 1, 4)` which loses the connection between the values and their
consumers. Therefore, perhaps instead of improving `setExecute`, we invent a `MockExecute` and then can use

```java
MockResultSetBuilder.add(MockResultSet.create("stats:count", "3"));
MockResultSetBuilder.add(MockExecute.create("stats:update", 1));
MockResultSetBuilder.add(MockResultSet.create("stats:total", "6"));
```

# Batch Execution

Batch execution is one of the last hold-outs of PreparedStatement, because SQLBuilder right now does not handle this efficiently.  While we could convert a batch update into a loop recreating SQLBuilder in every iteration, that would clearly defeat the purpose.

One idea to solve this is to mark some values as "batchable". Perhaps something like

```kotlin
data class BatchItem(val id: String, var value: Any?)

val sb = SQLBuilder("insert into foo (a, b, c) values (?,?,?)", va, BatchItem("b", vb[0]), BatchItem("c", vc)).addBatch()
// sb.addBatch() The first one would be done implicitly?!?
for (i in 1..num) {
    sb.set("b", vb[i]).set("c", vc + 1).addBatch()
}
sb.executeBatch()
```

```kotlin
sealed class BatchItem(sequence: Sequence<Any?>)
class Constant(a: Any?) : BatchItem(generateSequence(a) { it })
class Fixed(list: List<Any?>) : BatchItem(list.toSequence())
class Increment(i: Int) : BtachItem(generateSequence(i) { i + 1 })

SQLBuilder("insert into foo (a, b, c) values (?,?,?)", Constant(va), Fixed(vb), Increment(vc)).execute(con)
```

Another idea could be to pass a list/stream of grouped values.
```kotlin
val data = listOf(listOf("a", 1, 2L), listOf("b", 1, 3L), listOf("c", 2, 4L))
val sb = SQLBuilder("insert into foo (a, b, c) values (?,?,?)").executeBatch(data)
```

# SQL Validators

One big downside of mocking database operations is that this normally will not detect errors in the SQL statements:

- mis-spelled keywords or missing whitespace: "SELET a FROM foo" or "SELECT a FROMfoo" or "SELECT a FROM foo WHERE b - ?"
- mis-spelled table or column names
- wrong column types
- ...

We could use existing validators (e.g. ANTR-build Oracle SQL parser) to cover some of these areas.
