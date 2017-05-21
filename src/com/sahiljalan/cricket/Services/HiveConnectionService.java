package com.sahiljalan.cricket.Services;

import com.sahiljalan.cricket.Constants.Constants;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.sql.*;
import java.util.concurrent.CountDownLatch;


/**
 * Created by sahiljalan on 27/4/17.
 */
public class HiveConnectionService extends BaseHealthChecker{

    private String DriverName = Constants.HiveDriver;
    private Connection con;
    private static Connection connection;
    private Boolean flag=false;

    public HiveConnectionService(CountDownLatch latch){
        super("Connection Service",latch);
    }

    @Override
    public void verifyService() throws Exception{

        System.out.println("Checking " + this.getServiceName());

        try {
            Class.forName(DriverName);
            con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default",
                    "sahiljalan","");
            connection = con;
        } catch (SQLException e) {
            System.out.println(this.getServiceName() + " is Down");
            flag=true;
        }
        if(!flag)
            System.out.println(this.getServiceName() + " is UP");
        else
            throw new Exception();
    }

    public static Connection getConnection(){
        return connection;
    }

    public static void close() throws SQLException {
        connection.close();
        System.out.println("\n\nConnection is Closed Successfully.....\n\n");
    }

}
