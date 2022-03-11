/*
 * Copyright © 2020-2022, MetricStream, Inc. All rights reserved.
 */
package postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost:5432/nkiesel", "nkiesel", "nk");
    }
}
