package oracle;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.metricstream.jdbc.MockResultSet;
import com.metricstream.jdbc.MockSQLBuilderProvider;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestTimestampTest {

    @Mock
    Connection connection;

    private final Timestamp time = Timestamp.from(Instant.now());
    private final Object[][] data = { { time } };
    private ResultSet rs;

    @BeforeAll
    public static void beforeAll() {
        MockSQLBuilderProvider.enable();
    }

    @AfterAll

    static void afterAll() {
        MockSQLBuilderProvider.disable();
    }

    @BeforeEach
    public void beforeEach() throws SQLException {
        MockSQLBuilderProvider.reset();
        rs = MockResultSet.create("getTimestamp", data);
    }

    @Test
    public void testGetTimeStamp() throws SQLException {
        MockSQLBuilderProvider.addResultSet(rs);
        assertEquals(time, new TestTimestamp().getTimestamp(connection));
    }
}
