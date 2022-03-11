/*
 * Copyright © 2021-2022, MetricStream, Inc. All rights reserved.
 */
package oracle;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.metricstream.jdbc.SQLBuilder;


public class App {

    private final DBConnection dbConnection = new DBConnection();

    public String getGreeting() throws SQLException {
        return String.format("Lets party, %d users!%n", invite().size());
    }

    public static void main(String[] args) throws SQLException {
        final App app = new App();
        System.out.println(app.getGreeting());
        System.out.printf("Created user from insert has id %d%n", app.create());
        System.out.printf("Created user from call has id %d%n", app.call());
        System.out.println(app.getGreeting());
    }

    private Connection getConnection() throws SQLException {
        return dbConnection.getConnection();
    }

    public List<String> invite() throws SQLException {
        SQLBuilder sb = new SQLBuilder("select first_name from ${view} where last_name in (?)", Arrays.asList("Pan", "Stream")).bind("view", "si_users");
        try (Connection con = getConnection()) {
            return sb.getList(con, rs -> rs.getString(1));
        }
    }

    public int create() throws SQLException {
        SQLBuilder sb = new SQLBuilder("insert into si_users_t(user_id, first_name, last_name) values(SI_USERS_S.NEXTVAL, ?, ?)", "Peter", "Pan");
        try (Connection con = getConnection()) {
            System.out.printf("Oracle get gen keys: %s%n", getConnection().getMetaData().supportsGetGeneratedKeys());
            try (ResultSet rs = sb.execute(con, "USER_ID", "FIRST_NAME")) {
                if (rs.next()) {
                    System.out.printf("first name: %s%n", rs.getString(2));
                    return rs.getInt(1);
                } else {
                    return -1;
                }
            }
        }
    }

    public int call() throws SQLException {
        var firstName = "Paul";
        var userId = new SQLBuilder.OutInt();
        SQLBuilder sb = new SQLBuilder("{ call addUser(?, ?, ?) }", firstName, "Stream", userId);
        try (Connection con = getConnection()) {
            sb.call(con);
            System.out.printf("first name: %s%n", firstName);
            return userId.get();
        }
    }

}
