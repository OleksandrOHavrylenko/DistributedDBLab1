package com.distributed.databases.lab1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Oleksandr Havrylenko
 **/
public class ConnectionUtils {
    public static Connection getConnection() throws SQLException {
        String user = "postgres";
        String password = "root";
        return DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", user, password);
    }
}
