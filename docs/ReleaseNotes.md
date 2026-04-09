[![MetricStream](MetricStream_Logo.png)][MetricStream] SQLBuilder Release Notes

- Version 4.0.0, released 2026-04-08
    - **Platform modernisation release — no new features or API changes**
    - This major version marks a deliberate technology shift: the public API is identical to 3.9.x,
      but the platform baseline has been raised to keep the library current, secure, and aligned with
      the modern JVM and Kotlin ecosystems. Consumers upgrading from 3.9.x should require no code changes.
    - **Java 21 is now the minimum required JVM version** (previously JVM 11)
    - upgraded to Kotlin `2.3.0`
    - migrated `kotlin-logging` from the deprecated `io.github.microutils:kotlin-logging-jvm` to
      `io.github.oshai:kotlin-logging-jvm` (package renamed from `mu` to `io.github.oshai.kotlinlogging`)
    - fixed tag matching compatibility with JUnit 6's updated internal method naming convention
    - upgraded build tooling
        - `io.gitlab.arturbosch.detekt` replaced by `dev.detekt` `2.0.0-alpha.2` (Kotlin 2.x compatible)
        - `org.jlleitschuh.gradle.ktlint` from `11.0.0` to `14.2.0`
        - migrated ktlint rule configuration from Gradle DSL to `.editorconfig`
        - `com.github.ben-manes.versions` from `0.42.0` to `0.53.0`
        - `nl.littlerobots.version-catalog-update` from `0.6.1` to `1.1.0`
    - upgraded external dependencies to latest secure versions
        - `org.antlr:antlr4` from `4.11.1` to `4.13.2`
        - `org.assertj:assertj-core` from `3.23.1` to `3.27.3`
        - `commons-codec:commons-codec` from `1.15` to `1.21.0`
        - `org.junit.jupiter` from `5.9.1` to `6.0.3`
        - `io.kotest:kotest-assertions-core` from `5.5.0` to `6.0.0.M4`
        - `ch.qos.logback:logback-classic` from `1.4.3` to `1.5.18`
        - `io.github.oshai:kotlin-logging-jvm` from `3.0.0` to `7.0.7`
        - `org.mockito:mockito-core` from `4.8.0` to `5.18.0`
        - `org.mockito:mockito-junit-jupiter` from `4.8.0` to `5.18.0`
        - `io.mockk:mockk-jvm` from `1.13.2` to `1.14.3`
        - `com.oracle.database.jdbc:ojdbc11` from `21.7.0.0` to `21.18.0.0`
        - `com.opencsv:opencsv` from `5.7.0` to `5.10`
        - `org.postgresql:postgresql` from `42.5.0` to `42.7.7`
        - `org.slf4j:slf4j-api` from `2.0.3` to `2.0.17`

- Version 3.9.1, released 2022-08-17
    - fixed release date for 3.9.0, no code changes

- Version 3.9.0, released 2022-08-17
    - added support for validating SQL statements when executing them using MockSQLBuilder
    - added methods to MockSQLBuilder for enabling/disabling tag enforcement and SQL parsing
    - upgraded external dependencies
        - `io.kotest:kotest-assertions-core` from `5.3.2` to `5.4.2`
        - `org.junit.jupiter:junit-jupiter-api` from `5.8.2` to `5.9.0`
        - `org.junit.jupiter:junit-jupiter-engine` from `5.8.2` to `5.9.0`
        - `org.mockito:mockito-core` from `4.6.1` to `4.7.0`
        - `org.mockito:mockito-junit-jupiter` from `4.6.1` to `4.7.0`
        - `io.mockk:mockk` from `1.12.4` to `1.12.5`
        - `org.postgresql:postgresql` from `42.4.0` to `42.4.1`
    - upgraded to Gradle `7.5.1`
 

- Version 3.8.1, released 2022-07-15
    - added SQLBuilder method variants which use the connection from an implicit ConnectionProvider

- Version 3.7.4, released 2022-07-14
    - upgraded to Kotlin `1.7.10`

- Version 3.7.1, released 2022-06-27
    - switch to TOML version of Gradle versions catalog
    - add versions-update plugin and dependent plugins
    - upgraded external dependencies
        - `io.kotest:kotest-assertions-core` from `5.3.0` to `5.3.2`
        - `com.oracle.database.jdbc:ojdbc11` from `21.5.0.0` to `21.6.0.0.1`
        - `org.postgresql:postgresql` from `42.3.6` to `42.4.0`

- Version 3.7.0, released 2022-06-10
    - fixes a regression around `getMap` introduced in version `3.1.0` which prevented to place
      the `rowMapper` lambda parameter for `getMap` outside the `()`
    - upgraded to Kotlin `1.7.0`

- Version 3.6.1, released 2022-06-06
    - improved the code to skip over non-application stack frames during tag matching
    - minor code cleanup to pass "detekt"
    - use gradle version catalog
    - upgraded external dependencies
        - `io.kotest:kotest-assertions-core` from `5.2.2` to `5.3.0`
        - `org.assertj:assertj-core` from `3.21.0` to `3.23.1`
        - `io.mockk:mockk` from `1.12.3` to `1.12.4`
        - `io.github.microutils:kotlin-logging-jvm` from `2.1.21` to `2.1.23`
        - `org.mockito:mockito-core` from `4.5.1` to `4.6.1`
        - `org.mockito:mockito-junit-jupiter` from `4.5.1` to `4.6.1`
        - `io.mockk:mockk` from `1.12.3` to `1.12.4`
        - `org.postgresql:postgresql` from `42.3.3` to `42.3.6`
        - `org.jlleitschuh.gradle.ktlint` from `10.2.0` to `10.3.0`
        - `io.gitlab.arturbosch.detekt` from `1.19.0` to `1.20.0`
    - use `ojdbc11` instead of `ojdbc8` for Oracle JDBC driver

- Version 3.5.0, released 2022-04-26
    - allow both `:` and `#` as start for `MockResultSet` tag suffixes
    - use `io.github.microutils:kotlin-logging-jvm` as wrapper around `SLF4J` for Kotlin code
    - added external dependencies
        - `io.github.microutils:kotlin-logging-jvm` version `2.1.21`
    - upgraded to Kotlin `1.6.21`
    - upgraded to Gradle `7.4.2`
    - upgraded external dependencies
        - `ch.qos.logback:logback-classic` from `1.2.9` to `1.2.11`
        - `org.slf4j:slf4j-api` from `1.7.32` to `1.7.36`
        - `org.mockito:mockito-core` from `4.0.0` to `4.5.1`
        - `org.mockito:mockito-junit-jupiter` from `4.0.0` to `4.5.1`
        - `com.opencsv:opencsv` from `5.5.2` to `5.6`
        - `io.mockk:mockk` from `1.12.1` to `1.12.3`
        - `io.kotest:kotest-assertions-core` from `5.0.1` to `5.2.2`
        - `org.assertj:assertj-core` from `3.21.0` to `3.22.0`
        - `com.oracle.database.jdbc:ojdbc8` from `21.4.0.0.1` to `21.5.0.0`
        - `org.postgresql:postgresql` from `42.3.1` to `42.3.3`

- Version 3.4.5, released 2022-01-28
    - extract method name from generated class name for Groovy 2.4 closures in tag matching code

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
        - `io.mockk:mockk` from `1.12.0` to `1.12.1`
        - `io.gitlab.arturbosch.detekt` from `1.18.1` to `1.19.0`

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
        - `org.jlleitschuh.gradle.ktlint` from `10.1.0` to `10.2.0` (only used for build)
    - added `io.gitlab.arturbosch.detekt` version `1.18.1` (only used for build)

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

[MetricStream]: https://www.metricstream.com/
