package com.sahiljalan.cricket.analysis.Storage;

import com.sahiljalan.cricket.analysis.ConnectionToHive.HiveConnection;
import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Tables.RawTable;
import com.sahiljalan.cricket.analysis.TeamData.TeamHASHMEN;
import com.sahiljalan.cricket.analysis.TeamData.TeamHASHMENData;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

/**
 * Created by sahiljalan on 3/5/17.
 */
public class Storage {

    private static Statement query = CricketAnalysis.getStatement();
    private ResultSet res;
    private long t1c,t2c,totalMatchTweets;
    private static long code,team1DayCount,team2DayCount,totalMatchDayTweets;
    private static String team1hash,team2hash;
    private Timestamp startingStandardTimeStamp,startingUtcTimeStamp;
    private static Timestamp dayLastTimeStamp;
    private Map<String,TeamHASHMEN> map = TeamHASHMENData.getMAP();
    Connection con = HiveConnection.getConnection();
    private PreparedStatement preparedStatement;

    public Storage() throws SQLException {
        team1hash = map.get(TeamName.TEAM1).getHashtag();
        team2hash = map.get(TeamName.TEAM2).getHashtag();
        System.out.println("\nStorage Processing Started.");
        storageTable();
        System.out.println("\nStorage Processing Completed...");
    }

    public void storageTable() {

        try {

            startingUtcTimeStamp = RawTable.getStartingUTCTimeStamp();
            startingStandardTimeStamp = RawTable.getStartingISTTimeStamp();

            res = query.executeQuery("select code from " + Constants.TeamCodeTable +
                    " where teambattle = '" + team1hash + "VS" + team2hash + "'");
            while (res.next()) {
                code = Integer.parseInt(res.getString(1));

            }
            res = query.executeQuery("select count(rowid) from " + Constants.PosHype1);
            while (res.next()) {
                t1c = Integer.parseInt(res.getString(1));

            }
            res = query.executeQuery("select count(rowid) from " + Constants.PosHype2);
            while (res.next()) {
                t2c = Integer.parseInt(res.getString(1));

            }
            res = query.executeQuery("select count(id) from " + Constants.TableName);
            while (res.next()) {
                totalMatchTweets = Integer.parseInt(res.getString(1));

            }

            query.execute("use " + Constants.DataBaseAnalaysedResults);

            if (CricketAnalysis.getHour() == Constants.setStopHour) {

            }

            query.execute("create table if not EXISTS ipl_cricketHour(" +
                    "Code BIGINT," +
                    "Team1 STRING," +
                    "Team2 STRING," +
                    "startingTimeStamp TIMESTAMP," +
                    "endingTimeStamp TIMESTAMP," +
                    "Team1_Count BIGINT," +
                    "Team2_Count BIGINT," +
                    "Match_Total_Tweets BIGINT)");

            //query.execute("drop table if Exists ipl_cricket");
            query.execute("create table if not EXISTS ipl_cricket(" +
                    "Code BIGINT," +
                    "Team1 STRING," +
                    "Team2 STRING," +
                    "Day_Starting_TimeStamp TIMESTAMP," +
                    "Team1_Count BIGINT," +
                    "Team2_Count BIGINT," +
                    "Match_Total_Day_Tweets BIGINT)");


            preparedStatement = (PreparedStatement) con.prepareStatement("" +
                    "insert into table IPL_CricketHour values(?,?,?,?,?,?,?,?)");
            preparedStatement.setLong(1, code);
            preparedStatement.setString(2, team1hash);
            preparedStatement.setString(3, team2hash);
            preparedStatement.setTimestamp(4, startingUtcTimeStamp);
            preparedStatement.setTimestamp(5, startingStandardTimeStamp);
            preparedStatement.setLong(6, t1c);
            preparedStatement.setLong(7, t2c);
            preparedStatement.setLong(8, totalMatchTweets);
            System.out.println("\nInserting Resulted Records into Database..");
            int i = preparedStatement.executeUpdate() + 1;
            System.out.println("\n" + i + " Below Record Inserted Successfully...");
            System.out.println("" + code + "  " + team1hash + "  " + team2hash + "  " + startingUtcTimeStamp +
                    "  " + startingStandardTimeStamp + "  " + t1c + "  " + t2c + "  " + totalMatchTweets);


        }catch (SQLException e){
            System.out.println("\nSQLException : \n"+e+"\n");
            try {
                Constants.setDBName("projectcricket");
                new CricketAnalysis().startCleaningService();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            System.exit(1);
        }catch (NullPointerException e){
            System.out.println("\nNullPointerException : \n"+e+"\n");
            try {
                Constants.setDBName("projectcricket");
                new CricketAnalysis().startCleaningService();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            System.exit(1);
        }

    }
    public static void AfterAnalysisStorage(){

        try {
            ResultSet res = query.executeQuery("select from_unixtime(unix_timestamp" +
                    "(current_timestamp(),'yyyy MMM dd hh:mm:ss'))");
            while (res.next()){
                dayLastTimeStamp = res.getTimestamp(1);
            }
            res = query.executeQuery("select sum(team1_count) from ipl_crickethour");
            while (res.next()){
                team1DayCount = Integer.parseInt(res.getString(1));
            }
            res = query.executeQuery("select sum(team2_count) from ipl_crickethour");
            while (res.next()){
                team2DayCount = Integer.parseInt(res.getString(1));
            }
            res = query.executeQuery("select sum(Match_Total_Tweets) from ipl_crickethour");
            while (res.next()){
                totalMatchDayTweets = Integer.parseInt(res.getString(1));
            }
            query.execute("drop table if Exists ipl_cricketHour");
            PreparedStatement preparedStatement = (PreparedStatement) HiveConnection.getConnection().prepareStatement("" +
                    "insert into table IPL_Cricket values(?,?,?,?,?,?,?)");
            preparedStatement.setLong(1, code);
            preparedStatement.setString(2, team1hash);
            preparedStatement.setString(3, team2hash);
            preparedStatement.setTimestamp(4, dayLastTimeStamp);
            preparedStatement.setLong(5, team1DayCount);
            preparedStatement.setLong(6, team2DayCount);
            preparedStatement.setLong(7, totalMatchDayTweets);
            System.out.println("\nInserting End Day Resulted Records into Database..");
            int j = preparedStatement.executeUpdate() + 1;
            System.out.println(j+"Record Inserted into Database Successfully...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

