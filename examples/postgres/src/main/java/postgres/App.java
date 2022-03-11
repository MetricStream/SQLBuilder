/*
 * Copyright © 2020-2022, MetricStream, Inc. All rights reserved.
 */
package postgres;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.List;

import com.metricstream.jdbc.SQLBuilder;

public class App {

    private DBConnection dbConnection = new DBConnection();

    public String getGreeting() throws SQLException {
        return String.format("Lets party, %s!%n", invite(21, true));
    }

    public static void main(String[] args) throws SQLException {
        final App app = new App();
        System.out.println(app.getGreeting());
        app.addPerson("Mia", "Doe", 35, 'F');
        System.out.println(app.getGreeting());
    }

    private Connection getConnection() throws SQLException {
        return dbConnection.getConnection();
    }

    public List<String> invite(int age, boolean onlyWomen) throws SQLException {
        SQLBuilder sb = new SQLBuilder("select firstname from Person where age >= ?", age);
        if (onlyWomen) {
            sb.append("and sex='F'");
        }
        try (Connection con = getConnection()) {
            return sb.getList(con, rs -> rs.getString(1));
        }
    }

    public int addPerson(String firstName, String lastName, int age, char sex) throws SQLException {
        var count = new SQLBuilder.InOut<>(JDBCType.INTEGER, 2);
        SQLBuilder sb = new SQLBuilder("{ call addperson(?, ?, ?, ?, ?) }", firstName, lastName, age, sex, count);
        try (Connection con = getConnection()) {
            sb.call(con);
            return count.get();
        }
    }
}
