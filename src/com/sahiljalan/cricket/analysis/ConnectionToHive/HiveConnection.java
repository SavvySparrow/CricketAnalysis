package com.sahiljalan.cricket.analysis.ConnectionToHive;

import com.sahiljalan.cricket.analysis.Constants.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by sahiljalan on 27/4/17.
 */
public class HiveConnection {

    private String DriverName = Constants.HiveDriver;
    private static Connection connection;

    public HiveConnection() throws SQLException , ClassNotFoundException {

        Class.forName(DriverName);
        start();

    }

    public static void start() throws SQLException{

        connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
                "sahiljalan","");

    }
    public static Connection getConnection(){
        return connection;
    }

    public static void close() throws SQLException {
        connection.close();
    }
}
