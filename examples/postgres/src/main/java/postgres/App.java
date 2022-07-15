/*
 * Copyright © 2020-2022, MetricStream, Inc. All rights reserved.
 */
package postgres;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.metricstream.jdbc.SQLBuilder;

public class App {

    private DBConnection dbConnection = new DBConnection();

    public String getGreeting() throws SQLException {
        return String.format("Lets party, %s!%n", invite(21, true));
    }

    public static void main(String[] args) throws SQLException {
        System.out.println(new App().getGreeting());
    }

    private Connection getConnection() throws SQLException {
        return dbConnection.getConnection();
    }

    public List<String> invite(int age, boolean onlyWomen) throws SQLException {
        try (Connection con = getConnection()) {
            SQLBuilder sb = new SQLBuilder("select firstname from Person where age >= ?", age);
            if (onlyWomen) {
                sb.append("and sex='F'");
            }
            return sb.getList(con, rs -> rs.getString(1));
        }
    }
}
