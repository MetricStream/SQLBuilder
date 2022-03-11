How to Represent "call procedure" in SQLBuilder
===============================================

**Author: Norbert Kiesel <nkiesel@metricstream.com>**

**Date: 2022-01-31**

---

# Introduction

`SQLBuilder` was initially designed to wrap `PreparedStatement` in a more high-level API.  The basic idea was:

- add methods like `getInt` or `getString` to simplify single-value queries
- support composition of queries
- be domain-agnostic (i.e. don't assume specific use cases or limitations)

Over time more APIs were added to cover common (and not so common) use cases:

 - `execute` to perform `delete` or `update` statements
 - `getList` and `getMap` to handle multi-row results
 - `getResultSet` with "return values"
 
One aspect that is commonly used in our code but not yet really supported it the "call PL/SQL procedure". While procedures w/o return values can be handled using `execute`, most of our PL/SQL procedures follow the practice to return errors using `out` parameters:

```java
try (var cs = connection.callableStatement("{call addFoo(?, ?, ?)}") {
    cs.setString(1, "bar");
    cs.setString(2, "baz");
    cs.registerOutParameter(3, Types.INTEGER);
    cs.execute();
    int error = cs.getInt(1);
    if (error == 1) {
    ...
 }
 ```	
 This is not yet supported by `SQLBuilder`, and this document explores different options on how to add this capability.
 
# Goals

The solution for `CallableStatement` should follow the principles of SQLBuilder. However, neither the "simplify" nor the "composition" goal stated for `PreparedStatement` does not really apply to `CallableStatement` because these are generally more limited in that they only consist of a method name and of a fixed set of parameters.

---

# Approaches

One general observation is that most `CallableStatement` calls only have a small number of `OUT` or `INOUT` parameters to return either an error status or a computed id. This suggests that adding some explicit handling for these should be acceptable.

## Explicit Parameter Types

One idea is to add explicit types to model these parameters.  The above example would then translate to
```
var error = new SQLBuilder.Out(Types.INTEGER);
var cs = new SQLBuilder("{call addFoo(?, ?, ?)}", "bar", "baz", error);
try (cs) {
    cs.call(connection);
    if (error.get() == 1) { 
    }
}
```

Some things are worth mentioning for this approach:

1. the `SQLBuilder#call` method does not return anything (though it still can throw exceptions)
2. the `CallableStatement` is never exposed in the API
2. all the `OUT` values access are done lazily against the (internal) `CallableSattement` object
3. the `SQLBuilder` object must still be closed (preferably using "try-with-expression" as shown above)
4. This could easily be extended to support `INOUT` parameters using `new SQLBuilder.InOut(42, Types.INTEGER)`

## Special Placeholders

This approach uses another placeholder literal for `OUT` parameters:
```
var cs = new SQLBuilder("{call addFoo(?, ?, !)}", "bar", "baz", JDBCType.INTEGER);
try (var result = cs.call(connection)) {
    if (result.getInt(3) == 1) { 
    }
}
```

Some points worth mentioning for this approach:

1. This looks more similar to the existing `var rs = query.getResultSet(connection)` API of `SQLBuilder`
2. the `SQLBuilder` constructor needs the JDBC type for the `OUT` parameter to correctly register it with the `CallableStatement`
3. Supporting `INOUT` parameters would require yet another placeholder literal so that `SQLBuilder` can perform the `set` operation with the correct index
4. The `CallableStatement` is never exposed in the API. Instead, the `call` method returns an object of a new `CallResult` type.

---

# INOUT Handling

We mentioned the `INOUT` problem before, so let's look at the possible implementations for the mentioned approaches.  The example is a method which takes 2 `IN` parameters, one `OUT` parameter, and one `INOUT` parameter:
```plsql
precedure upsertFoo(
     id INOUT INTEGER,
     name IN varchar2,
     error OUT varchar2
)
```

## Classical Approach

```java
try (var cs = connection.prepareCall("{ call upsertFoo(?, ?, ?) }") {
    if (isInsert) {
        cs.setInt(1, id);
    }
    cs.registerOutParameter(1, Types.INTEGER);
    cs.setString(2, "Bar");
    cs.registerOutParameter(3, Types.VARCHAR);
    cs.execute();
    var error = cs.getString(3);
    if (error != null) {
      logger.info("Inserted or updated Foo#{}", cs.getInt(1));
    }
}
```

## Explicit Parameter Types

```java
var idParam = new SQLBuilder.INOUT(id, Types.INTEGER);
var errorParam = new SQLBuilder.OUT(Types.VARCHAR);
var cs = new SQLBuilder("upsertFoo(?, ?, ?)", idParam, "Bar", errorParam);
try (cs) {
    cs.call(connection);
    var error = errorParam.get();
    if (error != null) {
      logger.info("Inserted or updated Foo#{}", idParam.get());
    }
}
```

## Special Placeholders
For INOUT parameters, we need 2 values: the actual value to pass in the call, and the type to register for the output. We can "guess" the correct type based on the type of the value.

```java
var cs = new SQLBuilder("upsertFoo(&, ?, !)", id, "Bar", JDBCType.VARCHAR);
try (var result = cs.call(connection)) {
    var error = result.getString(3);
    if (error != null) {
        logger.info("Inserted or updated Foo#{}", result.getInt(1));
    }
}
```

Note: These examples assume `SQLBuilder` it will automatically wrap the statement into `{ call ... }`.

## No Placeholders

As we have seen, we need to provide 1 or 2 values for every parameter: the actual input value and/or the output type. Therefore, we could also simplify the call by omitting the placeholders:
```java
var cs = new SQLBuilder("upsertFoo", InOut(id, JDBCType.Integer), "Bar", JDBCType.VARCHAR);
try (var result = cs.call(connection)) {
    var error = result.getString(3);
    if (error != null) {
        logger.info("Inserted or updated Foo#{}", result.getInt(1));
    }
}
```

This approach has two problems:

1. We need to decide if `JDBCType.VARCHAR` is the value for an IN parameter or the type for an OUT parameter. It is very unlikely that the former is the intention, and we thus would always treat `JDBC` values as indicators for OUT parameters.  If it really should be treated as IN parameter, `SQLBuilder` could offer an `SQLBuilder.IN` wrapper type: `SQLBuilder.IN(JDBCType.VARCHAR)` would always be handled as an IN parameter.
2. We need to decide if any value which is not of type `JDBCType` is an IN or an INOUT parameter.  Because IN parameters are much more common, the example above suggests that INOUT parameters must be explicitly wrapped in `InOut`.

---

# Mocking

One other "selling" point for `SQLBuilder` is the built-in mocking support for unit tests. Obviously, we must support the new "call" API for this as well. For this discussion, we assume that the above SQL code is wrapped into a
```java
public int upsertFoo(int id, String newName, boolean insert) throws SQLException;
```
which returns the created id in the "insert" case or the passed "id" otherwise.
## Explicit Parameter Types

```java
@Test
public void testUpsertFooUpdate() {
    MockCall.add("upsertFoo", new Object[][] { 12, null });
    var actual = upsertFoo(12, "Bar", false);
    assertEquals(12, actual);
}

@Test
public void testUpsertFooInsert() {
    MockCall.add("upsertFoo", new Object[] { 12, null });
    var actual = upsertFoo(0, "Bar", true);
    assertEquals(12, actual);
}

@Test
public void testUpsertFooError() {
    MockCall.add("upsertFoo", new Object[] { 12, "bug" });
    assertThrows(SQLException.class, upsertFoo(12, "Bar", false));
}
```
Here we pass a pair of values for every `OUT` or `INOUT` parameter consisting of the "in" value (ignored for `OUT` parameters) and the mocked "out" value.  This approach should work for both approaches.