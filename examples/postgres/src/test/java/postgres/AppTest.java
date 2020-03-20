/*
 * Copyright © 2020, MetricStream, Inc. All rights reserved.
 */
package postgres;

import com.metricstream.jdbc.MockSQLBuilderProvider;
import com.metricstream.jdbc.SQLBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

class AppTest {

    @InjectMocks
    App classUnderTest;

    @Mock
    DBConnection dbConnection;

    @BeforeAll static void beforeAll() {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider());
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
