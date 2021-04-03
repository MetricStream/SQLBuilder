package oracle;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.metricstream.jdbc.MockResultSet;
import com.metricstream.jdbc.MockSQLBuilderProvider;
import com.metricstream.jdbc.SQLBuilder;


class TestTimestampTest {

    @Mock
    Connection connection;

    private final Timestamp time = Timestamp.from(Instant.now());
    private final Object[][] data = { { time } };
    private ResultSet rs;

    @BeforeAll
    public static void beforeAll() {
        SQLBuilder.setDelegate(new MockSQLBuilderProvider(true, true));
    }

    @AfterAll

    static void afterAll() {
        SQLBuilder.resetDelegate();
    }

    @BeforeEach
    public void beforeEach() throws SQLException {
        MockSQLBuilderProvider.reset();
        MockitoAnnotations.initMocks(this);
        rs = MockResultSet.create("getTimestamp", data);
    }

    @Test
    public void testGetTimeStamp() throws SQLException {
        MockSQLBuilderProvider.addResultSet(rs);
        assertEquals(time, new TestTimestamp().getTimestamp(connection));
    }

    @Test
    public void testGetTimeStamp_working() throws SQLException {
        doAnswer(invocation -> time).when(rs).getTimestamp(anyInt());
        MockSQLBuilderProvider.addResultSet(rs);
        assertEquals(time, new TestTimestamp().getTimestamp(connection));
    }
}
