/*
 * Copyright © 2021, MetricStream, Inc. All rights reserved.
 */
package oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DBConnection {
    private static final Logger logger = LoggerFactory.getLogger(DBConnection.class);
    private static final String userName = System.getenv("DBUSER");
    private static final String password = System.getenv("DBPASS");

    public Connection getConnection() throws SQLException {
        try {
            return DriverManager.getConnection("jdbc:oracle:thin:@172.17.0.2:1521/pdb", userName, password);
        } catch (SQLException e) {
            logger.error("Could not create database connection for user {}", userName, e);
            throw e;
        }
    }
}
