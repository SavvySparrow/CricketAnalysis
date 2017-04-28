package com.sahiljalan.cricket.analysis.tables;

import java.sql.SQLException;
import java.sql.Statement;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.MainCricket;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class RawTable extends TimeZone{

    private Statement query = MainCricket.getStatement();

    //Create Default TableName
    public RawTable() throws SQLException{
        //First it calls the default constructor of TimeZone than create RawTable
        System.out.println("Running : RawTable class");
        createTable();
    }

    //Create User-Defined TableName
    public RawTable(String TableName) throws SQLException{
        Constants.setTableName(TableName);
        createTable();
    }


    void createTable() throws SQLException{

        query.execute("SET hive.support.sql11.reserved.keywords=false");

        System.out.println("Running : droping Raw Table");
        query.execute("drop table if exists " + Constants.TableName);
        System.out.println("Running : creating Raw Table");
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
    }
}
