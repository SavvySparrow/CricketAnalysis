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

    public HiveConnection() throws SQLException {

        try {
            Class.forName(DriverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        start();
        System.out.println("\n\nConnection is Started Successfully\n\n");

    }

    public static void start(){

        try {
            connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
                    "sahiljalan","");
        } catch (SQLException e) {
            System.out.println(" <----- Connection is Refused -----> \n Restart Hive Server and Try again");
            System.exit(1);
        }


    }

    public static Connection getConnection(){
        return connection;
    }

    public static void close() throws SQLException {
        connection.close();
        System.out.println("\n\nConnection is Closed Successfully.....\n\n");
    }
}
