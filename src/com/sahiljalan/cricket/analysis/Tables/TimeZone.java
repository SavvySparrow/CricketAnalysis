package com.sahiljalan.cricket.analysis.Tables;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Main;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class TimeZone extends Dictionary{

    private Statement query = CricketAnalysis.getStatement();

    public TimeZone() throws SQLException {
        //First it calls the default constructor of Dictionary than create TimeZoneTable
        System.out.println("Running : TimeZone class");
        createTimeZoneTable();
    }

    private void createTimeZoneTable() throws SQLException {

        System.out.println("Running : creating TimeZone Table() IF NOT EXISTS");
        query.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "+ Constants.TimeZoneTable + "(" +
                "zone string," +
                "country string" +
                ")" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ");
        query.execute("LOAD DATA LOCAL INPATH '"+Constants.TimeZoneLocation+"' OVERWRITE INTO TABLE " +
                Constants.TimeZoneTable);
    }
}
