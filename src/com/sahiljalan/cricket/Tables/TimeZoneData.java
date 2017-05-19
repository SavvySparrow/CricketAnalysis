package com.sahiljalan.cricket.Tables;

import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.Services.CleanTraces.Records;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class TimeZoneData extends Dictionary{

    private Statement query = CricketAnalysis.getStatement();

    public TimeZoneData() throws SQLException {
        //First it calls the default constructor of Dictionary than create TimeZoneTable
        System.out.println("Running : "+ getClass());
        createTimeZoneTable();
        System.out.println();
    }

    private void createTimeZoneTable() throws SQLException {

        System.out.println("Executing : Creating TimeZone Table IF NOT EXISTS");
        query.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "+ Constants.TimeZoneTable + "(" +
                "zone string," +
                "country string" +
                ")" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ");

        if(Records.isRunningFirstTime){
            System.out.println("Executing : Loading TimeZone Data");
            query.execute("LOAD DATA LOCAL INPATH '"+Constants.TimeZoneLocation+"' OVERWRITE INTO TABLE " +
                    Constants.TimeZoneTable);
        }

    }
}
