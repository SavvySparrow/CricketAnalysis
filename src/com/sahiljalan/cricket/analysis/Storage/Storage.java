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
    private static int StatementNumber = 1;
    private long t1c,t2c,totalMatchTweets;
    private static long code,team1DayCount,team2DayCount,totalMatchDayTweets;
    private static String team1hash,team2hash;
    private Timestamp startingStandardTimeStamp,startingUtcTimeStamp;
    private static Timestamp dayLastTimeStamp;
    private Map<String,TeamHASHMEN> map = TeamHASHMENData.getMAP();
    Connection con = HiveConnection.getConnection();
    private PreparedStatement preparedStatement;
    private static String SBFT1Name,SBFT2Name;
    private static boolean SBFT1VerifiedCheck=false,SBFT2VerifiedCheck=false;
    private static String SBFT1CountryCheck,SBFT2CountryCheck;


    public Storage() throws SQLException {
        team1hash = map.get(TeamName.TEAM1).getHashtag();
        team2hash = map.get(TeamName.TEAM2).getHashtag();
        System.out.println("\nStorage Processing Started.");
        storageTable();
        System.out.println("\nStorage Processing Completed...");
    }

    public void storageTable() {

        System.out.print("\nPerforming Pre-Storage Queries ----> ");

        try {

            startingUtcTimeStamp = RawTable.getStartingUTCTimeStamp();
            startingStandardTimeStamp = RawTable.getStartingISTTimeStamp();

            //Statement 1
            System.out.print(StatementNumber+" ");
            res = query.executeQuery("select code from " + Constants.TeamCodeTable +
                    " where teambattle = '" + team1hash + "VS" + team2hash + "'");
            while (res.next()) {
                code = Integer.parseInt(res.getString(1));

            }StatementNumber++;

            //Statement 2
            System.out.print(StatementNumber+" ");
            res = query.executeQuery("select count(rowid) from " + Constants.PosHype1);
            while (res.next()) {
                t1c = Integer.parseInt(res.getString(1));

            }StatementNumber++;

            //Statement 3
            System.out.print(StatementNumber+" ");
            res = query.executeQuery("select count(rowid) from " + Constants.PosHype2);
            while (res.next()) {
                t2c = Integer.parseInt(res.getString(1));

            }StatementNumber++;

            //Statement 4
            System.out.print(StatementNumber+" ");
            res = query.executeQuery("select count(id) from " + Constants.TableName);
            while (res.next()) {
                totalMatchTweets = Integer.parseInt(res.getString(1));

            }StatementNumber++;


            //Sabse Bada Fan Queries

            //Statement 5
            System.out.print(StatementNumber+" ");
            res = query.executeQuery("select Distinct user.name from " + Constants.TableName+" " +
                    "where user.screen_name = '"+Constants.SBF_MAX_COUNT_USERNAME1+"'");
            if(res.next()) {
                SBFT1Name = res.getString(1);
            }else{
                SBFT1Name = "N/A";
            }StatementNumber++;

            //Statement 6
            System.out.print(StatementNumber+" ");
            res = query.executeQuery("select Distinct user.name from " + Constants.TableName+" " +
                    "where user.screen_name = '"+Constants.SBF_MAX_COUNT_USERNAME2+"'");
            if (res.next()) {
                SBFT2Name = res.getString(1);
            }else{
                SBFT2Name = "N/A";
            }StatementNumber++;

            //Statement 7
            System.out.print(StatementNumber+" ");
            res = query.executeQuery("select verified,country from " + Constants.SABSE_BADA_FAN_TEAM_1+" " +
                    "where screen_name = '"+Constants.SBF_MAX_COUNT_USERNAME1+"' limit 1");
            if (res.next()) {
                SBFT1VerifiedCheck = Boolean.parseBoolean(res.getString(1));
                SBFT1CountryCheck = res.getString(2);
            }else{
                SBFT1VerifiedCheck = false;
                SBFT1CountryCheck = "N/A";
            }StatementNumber++;

            //Statement 8
            System.out.print(StatementNumber+" ");
            res = query.executeQuery("select verified,country from " + Constants.SABSE_BADA_FAN_TEAM_2+" " +
                    "where screen_name = '"+Constants.SBF_MAX_COUNT_USERNAME2+"' limit 1");
            if (res.next()) {
                SBFT2CountryCheck = res.getString(2);
                SBFT2VerifiedCheck = Boolean.parseBoolean(res.getString(1));
            }else{
                SBFT2CountryCheck = "N/A";
                SBFT2VerifiedCheck = false;
            }StatementNumber++;

            System.out.print("\nPerforming Storage Queries ----> ");
            query.execute("use " + Constants.DataBaseAnalaysedResults);

            if (CricketAnalysis.getHour() == Constants.setStopHour) {

            }

            //Statement 9
            System.out.print(StatementNumber+" ");
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
            StatementNumber++;
            //query.execute("drop table if Exists ipl_cricket");

            //Statement 10
            System.out.print(StatementNumber+" ");
            query.execute("create table if not EXISTS ipl_cricket(" +
                    "Code BIGINT," +
                    "Team1 STRING," +
                    "Team2 STRING," +
                    "Day_Starting_TimeStamp TIMESTAMP," +
                    "Team1_Count BIGINT," +
                    "Team2_Count BIGINT," +
                    "Match_Total_Day_Tweets BIGINT)");

            StatementNumber++;
            //Statement 11
            System.out.print("\nPerforming Insertion Queries ----> ");
            System.out.println(StatementNumber+" \n");
            preparedStatement = (PreparedStatement) con.prepareStatement("" +
                    "insert into table IPL_CricketHour values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            preparedStatement.setLong(1, code);
            preparedStatement.setString(2, team1hash);
            preparedStatement.setString(3, team2hash);
            preparedStatement.setTimestamp(4, startingUtcTimeStamp);
            preparedStatement.setTimestamp(5, startingStandardTimeStamp);
            preparedStatement.setLong(6, t1c);
            preparedStatement.setLong(7, t2c);
            preparedStatement.setLong(8, totalMatchTweets);
            preparedStatement.setString(9, SBFT1Name);
            preparedStatement.setString(10, Constants.SBF_MAX_COUNT_USERNAME1);
            preparedStatement.setInt(11, Constants.SBF_MAX_COUNT1);
            preparedStatement.setBoolean(12, SBFT1VerifiedCheck);
            preparedStatement.setString(13, SBFT1CountryCheck);
            preparedStatement.setString(14, SBFT2Name);
            preparedStatement.setString(15, Constants.SBF_MAX_COUNT_USERNAME2);
            preparedStatement.setInt(16, Constants.SBF_MAX_COUNT2);
            preparedStatement.setBoolean(17, SBFT2VerifiedCheck);
            preparedStatement.setString(18, SBFT2CountryCheck);
            System.out.println("\nInserting Resulted Records into Database..");
            int i = preparedStatement.executeUpdate() + 1;
            StatementNumber++;
            System.out.println("\n" + i + " Below Record Inserted Successfully...");
            System.out.println("" + code + "  " + team1hash + "  " + team2hash + "  " + startingUtcTimeStamp +
                    "  " + startingStandardTimeStamp + "  " + t1c + "  " + t2c + "  " + totalMatchTweets);
            StatementNumber++;

        }catch (SQLException e){
            System.out.println("\nStatement Number : "+StatementNumber);
            System.out.println("SQLException : \n"+e+"\n");
            try {
                Constants.setDBName("projectcricket");
                new CricketAnalysis().startCleaningService();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            System.exit(1);
        }catch (NullPointerException e){
            System.out.println("\nStatement Number : "+StatementNumber);
            System.out.println("NullPointerException : \n"+e+"\n");
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
            query.execute("use " + Constants.DataBaseAnalaysedResults);
            ResultSet res = query.executeQuery("select from_unixtime(unix_timestamp" +
                    "(current_timestamp(),'yyyy MMM dd hh:mm:ss'))");
            while (res.next()){
                dayLastTimeStamp = res.getTimestamp(1);
            }
            res = query.executeQuery("select sum(team1_count) from ipl_cricketHour");
            while (res.next()){
                team1DayCount = Integer.parseInt(res.getString(1));
            }
            res = query.executeQuery("select sum(team2_count) from ipl_cricketHour");
            while (res.next()){
                team2DayCount = Integer.parseInt(res.getString(1));
            }
            res = query.executeQuery("select sum(Match_Total_Tweets) from ipl_cricketHour");
            while (res.next()){
                totalMatchDayTweets = Integer.parseInt(res.getString(1));
            }
            PreparedStatement preparedStatement = (PreparedStatement) HiveConnection.getConnection().prepareStatement("" +
                    "insert into table ipl_cricket values(?,?,?,?,?,?,?)");
            preparedStatement.setLong(1, code);
            preparedStatement.setString(2, team1hash);
            preparedStatement.setString(3, team2hash);
            preparedStatement.setTimestamp(4, dayLastTimeStamp);
            preparedStatement.setLong(5, team1DayCount);
            preparedStatement.setLong(6, team2DayCount);
            preparedStatement.setLong(7, totalMatchDayTweets);
            System.out.println("\nInserting End Day Resulted Records into Database..");
            int j = preparedStatement.executeUpdate() + 1;
            System.out.println("Closing After Analysis Storage...\n");
        } catch (SQLException e) {
            System.out.println("HiveSQLException : "+e);
        }
    }
}

