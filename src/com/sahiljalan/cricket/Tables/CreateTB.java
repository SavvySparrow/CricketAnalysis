package com.sahiljalan.cricket.Tables;

import com.sahiljalan.cricket.Configuration.UserSpecific;
import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.Services.PreProcessingQueriesService;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 * Created by sahiljalan on 19/5/17.
 */
public class CreateTB {

    private static Statement query = PreProcessingQueriesService.getStatement();
    private static Timestamp UserDefinedTimeStamp, UTC_TimeStamp;
    private static int totalMatchTweets;

    public static void mainTable() throws SQLException {

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
                "Location '" + Constants.HDFS_SERVER_LOCATION + UserSpecific.getCricketType() + Constants.HDFS_POSTFIX_LOCATION +"'");

        System.out.print("Executing : Starting UTC TimeStamp ----> ");
        ResultSet res = query.executeQuery("select to_utc_timestamp(from_unixtime(unix_timestamp" +
                "(current_timestamp(),'yyyy MMM dd hh:mm:ss')),'"+Constants.USER_DEFINED_TIMEZONE +"')");
        while (res.next()){
            UTC_TimeStamp = res.getTimestamp(1);
        }
        System.out.println(UTC_TimeStamp);

        System.out.print("Executing : Starting "+Constants.USER_DEFINED_TIMEZONE +" Timestamp ----> ");
        UserDefinedTimeStamp = CricketAnalysis.getCurrentUserDefinedTimeStamp();
        System.out.println(UserDefinedTimeStamp);

        res = query.executeQuery("select count(user.screen_name) from " + Constants.TableName);
        while (res.next()) {
            totalMatchTweets = Integer.parseInt(res.getString(1));
        }
        System.out.print("Total Match Tweets ----> ");
        System.out.println(totalMatchTweets);

    }

    public static void storageTemporaryTable() throws SQLException{

        query.execute("create table if not EXISTS ipl_cricketHour(" +
                "Code BIGINT," +
                "Team1 STRING," +
                "Team2 STRING," +
                "startingTimeStamp TIMESTAMP," +
                "endingTimeStamp TIMESTAMP," +
                "Team1_Count BIGINT," +
                "Team2_Count BIGINT," +
                "Match_Total_Tweets BIGINT," +
                "Team1_SBF_Name String," +
                "Team1_SBF_UserName String," +
                "Team1_SBF_Max_Count BIGINT," +
                "Team1_SBF_Verified_Check Boolean," +
                "Team1_SBF_Country String," +
                "Team2_SBF_Name String," +
                "Team2_SBF_UserName String," +
                "Team2_SBF_Max_Count BIGINT," +
                "Team2_SBF_Verified_Check Boolean," +
                "Team2_SBF_Country String)");

    }

    public static void storageFinalTable() throws SQLException{

        query.execute("create table if NOT EXISTS ipl_cricket(" +
                "Code BIGINT," +
                "Team1 STRING," +
                "Team2 STRING," +
                "Day_Starting_TimeStamp TIMESTAMP," +
                "Team1_Count BIGINT," +
                "Team2_Count BIGINT," +
                "Match_Total_Day_Tweets BIGINT)");

    }

    public static Timestamp getStartingUTCTimeStamp() throws SQLException {

        return UTC_TimeStamp;
    }
    public static Timestamp getStartingISTTimeStamp() throws SQLException {

        return UserDefinedTimeStamp;
    }
}
