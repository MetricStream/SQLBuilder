package com.metricstream.jdbc

import java.sql.Connection
import io.kotest.matchers.shouldBe
import io.mockk.spyk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(MockSQLBuilderExtension::class)
class ExtensionTest {

    private val connection = spyk<Connection>()

    @Test
    internal fun `with Extension`() {
        MockResultSet.add("", "sample", "5")

        val sqlBuilder = SQLBuilder("select sample from test")
        sqlBuilder.getInt(connection, "sample", 3) shouldBe 5
    }
}
