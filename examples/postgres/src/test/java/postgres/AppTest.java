/*
 * Copyright © 2020-2021, MetricStream, Inc. All rights reserved.
 */
package postgres;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.metricstream.jdbc.MockSQLBuilderProvider;
import com.metricstream.jdbc.SQLBuilder;


class AppTest {

    @InjectMocks
    App classUnderTest;

    // IDE shows this as unused but in reality this is required for mocking access to Postgres
    @Mock
    DBConnection dbConnection;


    @BeforeAll static void beforeAll() {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider());
    }

    @AfterAll
    static void afterAll() {
        SQLBuilder.resetDelegate();
    }

    @BeforeEach void beforeEach() {
        MockSQLBuilderProvider.reset();
        MockitoAnnotations.initMocks(this);
    }

    @Test void isAnneInvited() throws SQLException {
        MockSQLBuilderProvider.addResultSet("person", "firstname,age,sex", "Anne,33,F");
        List<String> actual = classUnderTest.invite(30, true);
        assertTrue(actual.contains("Anne"));
    }
}
