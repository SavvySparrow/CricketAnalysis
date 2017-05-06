package com.sahiljalan.cricket.analysis.Tables;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import com.sahiljalan.cricket.analysis.ClearTraces.Records;
import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class RawTable {

    private Statement query = CricketAnalysis.getStatement();
    private Records clear = new Records();
    private static Timestamp ISTtimestamp,UTCtimestamp, UTCtimestampCopy;

    //Create Default TableName
    /*public RawTable() throws SQLException{
        System.out.println("Running : RawTable class");
        createTable();
    }*/

    //Create User-Defined TableName
    public RawTable(String TableName) throws SQLException{

        if(Records.isEmpty()){
            System.out.println("Running : Records EMPTY? -----> " + Records.isRunningFirstTime);
            Constants.setTableName(TableName);
            System.out.println("Running : creating Raw Table");
            createTable();
        }else{
            System.out.println("Running : Records EMPTY? -----> " + Records.isRunningFirstTime);
            clear.mainTable();
            Constants.setTableName("rawtable_temp");
            UTCtimestampCopy = UTCtimestamp;
            System.out.println("Running : creating Raw Table Temp");
            createTable();
            Constants.setTableName(TableName);
            createTableView();
        }
    }

    public void createTableView() throws SQLException {

        System.out.println("Running : creating Raw Table View");
        query.execute("create view rawtableview as select (cast(from_unixtime(unix_timestamp(" +
                "concat(substring(created_at,27,4),substring(created_at,4,16))," +
                "'yyyy MMM dd hh:mm:ss')) as timestamp)) ts,* from rawtable_temp");


        System.out.println("Running : creating Raw Table View matchbuzz");
        query.execute("CREATE table "+Constants.TableName+" as select * from rawtableview where ts > '"+ UTCtimestampCopy +"'");

    }


    public void createTable() throws SQLException{

        query.execute("create external table if NOT EXISTS " + Constants.TableName + " (id BIGINT," +
                "   created_at STRING," +
                "   retweet_count INT," +
                "   retweeted_status STRUCT<" +
                "      text:STRING," +
                "      user:STRUCT<screen_name:STRING,name:STRING>>," +
                "   entities STRUCT<" +
                "      urls:ARRAY<STRUCT<expanded_url:STRING>>," +
                "      user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>," +
                "      hashtags:ARRAY<STRUCT<text:STRING>>>," +
                "   text STRING," +
                "   user STRUCT<" +
                "      screen_name:STRING," +
                "      name:STRING," +
                "      friends_count:INT," +
                "      followers_count:INT," +
                "      statuses_count:INT," +
                "      verified:BOOLEAN," +
                "      utc_offset:INT," +
                "      time_zone:STRING>) " +
                "ROW FORMAT SERDE '" + Constants.SerDeDriver + "' " +
                "Location '" + Constants.Prefix_Location + Constants.Postfix_Location +"'");

        System.out.println("Running : creating Timestamp");
        ResultSet res = query.executeQuery("select to_utc_timestamp(from_unixtime(unix_timestamp" +
                "(current_timestamp(),'yyyy MMM dd hh:mm:ss')),'IST')");
        while (res.next()){
            UTCtimestamp = res.getTimestamp(1);
        }

        //TODO Creating time in IST may not be correct
        System.out.println("Running : creating Timestamp");
        res = query.executeQuery("select from_unixtime(unix_timestamp" +
                "(current_timestamp(),'yyyy MMM dd hh:mm:ss'))");
        while (res.next()){
            ISTtimestamp = res.getTimestamp(1);
        }
    }


    public static Timestamp getStartingUTCTimeStamp() throws SQLException {

        return UTCtimestamp;
    }
    public static Timestamp getStartingISTTimeStamp() throws SQLException {

        return ISTtimestamp;
    }
}
