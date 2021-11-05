[![MetricStream](docs/MetricStream_Logo.png)][MetricStream]

SQLBuilder is a Java library which aims to be a better PreparedStatement. It was developed internally at MetricStream
and is in active use and maintained by MetricStream. We use lots of wonderful open source code in our projects and open
sourcing this library is an attempt to give a tiny bit back to the community. Hopefully someone will find it useful.
Feedback of any kind is definitely appreciated!

The [MavenCentral] coordinates are
[com.metricstream.jdbc:sqlbuilder-core:3.2,0](https://search.maven.org/artifact/com.metricstream.jdbc/sqlbuilder-core/3.2.0/jar)
for production usage and
[com.metricstream.jdbc:sqlbuilder-mock:3.2.0](https://search.maven.org/artifact/com.metricstream.jdbc/sqlbuilder-mock/3.2.0/jar)
for unit testing.

Note: Version 3 of sqlbuilder-mock depends on Mockito 3. If your project is still using Mockito 1.x, use version `2.6.0`
of `sqlbuilder-core` and `sqlbuilder-mock`.

Read the [documentation](docs/Rationale.md) for details on why and how to use SQLBuilder and the changes between releases.

[MetricStream]: https://www.metricstream.com/
[MavenCentral]: https://mvnrepository.com/
