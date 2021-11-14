package com.metricstream.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith(MockSQLBuilderExtension.class)
public class ExtensionTestJava {

    private static final Connection connection = spy(Connection.class);

    @Test
    public void withExtension() throws SQLException {
        MockResultSet.add("", "sample", "5");

        var sqlBuilder = new SQLBuilder("select sample from test");
        assertThat(sqlBuilder.getInt(connection, "sample", 3)).isEqualTo(5);
    }

}
