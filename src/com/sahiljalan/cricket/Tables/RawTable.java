package com.sahiljalan.cricket.Tables;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import com.sahiljalan.cricket.Services.CleanTraces.Records;
import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;

import static com.sahiljalan.cricket.Services.CleanTraces.Records.isRunningFirstTime;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class RawTable {

    private Statement query = CricketAnalysis.getStatement();
    private static Timestamp UTC_TimeStampCopy;

    public RawTable(String TableName) throws SQLException, InterruptedException {

        System.out.println("Running : " + getClass());

        if(Records.isEmpty()){
            System.out.println("Executing : Records EMPTY? -----> " + isRunningFirstTime);
            Constants.setTableName(TableName);
            System.out.println("Executing : Creating Raw Table ----> "+Constants.TableName);
            CreateTB.mainTable();
        }else{
            System.out.println("Executing : Records EMPTY? -----> " + isRunningFirstTime);
            new CricketAnalysis().startCleaningService("MAIN");
            UTC_TimeStampCopy = CreateTB.getStartingUTCTimeStamp();
            Constants.setTableName("rawtable_temp");
            System.out.println("\nExecuting : Creating Temporary Raw Table");
            CreateTB.mainTable();
            Constants.setTableName(TableName);
            createTableView();
        }

        System.out.println();
    }

    public void createTableView() throws SQLException {

        System.out.println("Executing : Generating New TimeStamp Format For Each Row");
        query.execute("create view rawtableview as select (cast(from_unixtime(unix_timestamp(" +
                "concat(substring(created_at,27,4),substring(created_at,4,16))," +
                "'yyyy MMM dd hh:mm:ss')) as timestamp)) ts,* from rawtable_temp");


        System.out.println("Executing : Filtering RealTime Data And Generate Working Table ----> "+Constants.TableName);
        query.execute("CREATE table "+Constants.TableName+" as select * from rawtableview where ts > '"+ UTC_TimeStampCopy +"'");

    }
}
