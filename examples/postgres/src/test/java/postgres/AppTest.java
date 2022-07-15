/*
 * Copyright © 2020-2022, MetricStream, Inc. All rights reserved.
 */
package postgres;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.metricstream.jdbc.MockResultSet;
import com.metricstream.jdbc.MockSQLBuilderProvider;


@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AppTest {

    @InjectMocks
    App classUnderTest;

    @Mock
    DBConnection dbConnection;

    @Mock
    Connection connection;


    @BeforeAll void beforeAll() {
        MockSQLBuilderProvider.enable();
    }

    @AfterAll
    void afterAll() {
        MockSQLBuilderProvider.disable();
    }

    @BeforeEach void beforeEach() throws SQLException {
        MockSQLBuilderProvider.reset();
        doReturn(connection).when(dbConnection).getConnection();
    }

    @Test void isAnneInvited() throws SQLException {
        MockResultSet.add("invite", "firstname,age,sex", "Anne,33,F");
        List<String> actual = classUnderTest.invite(30, true);
        assertTrue(actual.contains("Anne"));
    }
}
