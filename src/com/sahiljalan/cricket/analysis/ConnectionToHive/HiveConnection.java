package com.sahiljalan.cricket.analysis.ConnectionToHive;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import org.apache.hive.service.cli.session.HiveSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by sahiljalan on 27/4/17.
 */
public class HiveConnection {

    private String DriverName = Constants.HiveDriver;
    private static Connection connection;

    public HiveConnection() throws SQLException , ClassNotFoundException {

        Class.forName(DriverName);
        setConnection();

    }

    public void setConnection() throws SQLException{

        connection = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
                "sahiljalan","");

    }
    public static Connection getConnection(){
        return connection;
    }

}
