package oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

import com.metricstream.jdbc.SQLBuilder;


public class TestTimestamp {

    public Timestamp getTimestamp(Connection connection) throws SQLException {
        SQLBuilder sql = new SQLBuilder("select time from table");
        try (ResultSet rs = sql.getResultSet(connection)) {
            if (rs.next()) {
                return rs.getTimestamp(1);
            }
        }
        return Timestamp.from(Instant.now());
    }

}
