package com.sahiljalan.cricket.Services;

import com.sahiljalan.cricket.Databases.CreateDB;
import com.sahiljalan.cricket.Tables.Dictionary;
import com.sahiljalan.cricket.Tables.TimeZoneData;
import com.sahiljalan.cricket.TeamData.TeamCodes;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import static com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis.startCleaningOnly;

/**
 * Created by sahiljalan on 20/5/17.
 */
public class PreProcessingQueriesService extends BaseHealthChecker{

    private static Statement query;
    private static Connection connection;
    public static Boolean isRunningFirstTime;

    public PreProcessingQueriesService(CountDownLatch latch){
        super("Pre Processing Service",latch);
    }

    @Override
    public void verifyService() {


        try {
            Thread.sleep(2000);

            connection = HiveConnectionService.getConnection();
            query = connection.createStatement();
            query.execute("SET hive.support.sql11.reserved.keywords=false");
            query.execute("CREATE TEMPORARY FUNCTION NumberRows AS " +
                    "'com.sahil.jalan.UDFNumberRows'");
            new CreateDB("projectcricket");

            if(!startCleaningOnly){
                System.out.println(" <---- Pre Processing Queries Service Started---->\n\n");
                isEmpty();

                //Loading TimeZone ,Dictionary And TeamData
                new TimeZoneData();
                new Dictionary();
                new TeamCodes();

                System.out.println("\n\n <---- Pre Processing Queries Service Stopped---->\n");
                System.out.println("Generating Flume Configuration File\n");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public static Boolean isEmpty() throws SQLException {
        query.execute("use projectcricket");
        ResultSet res = query.executeQuery("show tables");
        while(res.next()){}
        if(res.getRow()==0){
            isRunningFirstTime = true;
            return true;
        }
        isRunningFirstTime = false;
        return false;
    }

    public static Statement getStatement() {
        return query;
    }
}

