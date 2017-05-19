package com.sahiljalan.cricket.Storage;

import com.sahiljalan.cricket.Analaysis.SabseBadaFan;
import com.sahiljalan.cricket.Configuration.DefaultConf;
import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.Services.CleanTraces.Records;
import com.sahiljalan.cricket.Tables.CreateTB;
import com.sahiljalan.cricket.ConnectionToHive.HiveConnection;

import java.sql.*;

/**
 * Created by sahiljalan on 3/5/17.
 */
public class Storage {

    Connection con = HiveConnection.getConnection();
    private static Statement query = CricketAnalysis.getStatement();
    private ResultSet res;
    private static int StatementNumber = 1;
    private long t1c,t2c,totalMatchTweets;
    private static long team1DayCount,team2DayCount,totalMatchDayTweets;
    private Timestamp startingStandardTimeStamp,startingUtcTimeStamp;
    private static Timestamp dayLastTimeStamp;
    private PreparedStatement preparedStatement;
    private static String SBFT1Name,SBFT2Name;
    private static boolean SBFT1VerifiedCheck=false,SBFT2VerifiedCheck=false;
    private static String SBFT1CountryCheck="N/A",SBFT2CountryCheck="N/A";
    private static final String team1hash = DefaultConf.getTeamHash();
    private static final String team2hash = DefaultConf.getTeamHash();
    private static final long code = DefaultConf.getBattleCode();
    private String SBFT1UserName = Constants.SBF_MAX_COUNT_USERNAME1;
    private String SBFT2UserName = Constants.SBF_MAX_COUNT_USERNAME2;


    public Storage() throws SQLException {
        System.out.println("\nStorage Processing Started.");
        storageTable();
        System.out.println("\nStorage Processing Completed...");
    }

    public void storageTable() {

        System.out.println("\n <---- Performing Pre-Storage Queries ----> \n");

        try {

            startingUtcTimeStamp = CreateTB.getStartingUTCTimeStamp();
            startingStandardTimeStamp = CreateTB.getStartingISTTimeStamp();

            //Statement 1
            System.out.println(StatementNumber+": Fetching Positive Hype Of Team 1");
            res = query.executeQuery("select count(rowid) from " + Constants.PosHype1);
            while (res.next()) {
                t1c = Integer.parseInt(res.getString(1));

            }StatementNumber++;

            //Statement 2
            System.out.println(StatementNumber+": Fetching Positive Hype Of Team 2");
            res = query.executeQuery("select count(rowid) from " + Constants.PosHype2);
            while (res.next()) {
                t2c = Integer.parseInt(res.getString(1));

            }StatementNumber++;

            //Statement 3
            System.out.println(StatementNumber+": Fetching Total Tweets At This Moment");
            res = query.executeQuery("select count(id) from " + Constants.TableName);
            while (res.next()) {
                totalMatchTweets = Integer.parseInt(res.getString(1));

            }StatementNumber++;


            //Sabse Bada Fan Queries

            //Statement 4
            System.out.println(StatementNumber+": Fetching SabseBadaFan Name Of Team 1");
            res = query.executeQuery("select Distinct user.name from " + Constants.TableName+" " +
                    "where user.screen_name = '"+SBFT1UserName+"'");
            if(res.next()) {
                SBFT1Name = res.getString(1);
            }else{
                if(!SabseBadaFan.isTie(1))
                    SBFT1Name = "N/A";
                else
                    SBFT1Name = "Tie";
            }StatementNumber++;

            //Statement 5
            System.out.println(StatementNumber+": Fetching SabseBadaFan Name Of Team 2");
            res = query.executeQuery("select Distinct user.name from " + Constants.TableName+" " +
                    "where user.screen_name = '"+SBFT2UserName+"'");
            if (res.next()) {
                SBFT2Name = res.getString(1);
            }else{
                if(SabseBadaFan.isTie(2))
                    SBFT2Name = "Tie";
                else
                    SBFT2Name = "N/A";
            }StatementNumber++;

            //Statement 6
            System.out.println(StatementNumber+": Checking SabseBadaFan Verification And Country Of Team 1");
            res = query.executeQuery("select verified,country from " + Constants.SABSE_BADA_FAN_TEAM_1+" " +
                    "where screen_name = '"+SBFT1UserName+"' limit 1");
            System.out.println("\nBefore Check ----> Verified User : "+SBFT1VerifiedCheck+ "" +
                    " | Country : "+SBFT1CountryCheck);

            if (res.next()) {
                SBFT1VerifiedCheck = Boolean.parseBoolean(res.getString(1));
                try{
                    if(res.getString(2).equalsIgnoreCase("null")){
                        //Null Pointer Exception
                    }else{
                        SBFT1CountryCheck = res.getString(2);
                        System.out.println("After Check ----> Verified User : "+SBFT1VerifiedCheck+ "" +
                                " | Country : "+SBFT1CountryCheck+"\n");
                    }
                }catch (NullPointerException e){
                    setCountryNullToNA(1);
                }
            }else{
                SBFT1VerifiedCheck = false;
                SBFT1CountryCheck = "N/A";
                if(SabseBadaFan.isTie(1))
                    System.out.println("Tie Found!");
                System.out.println("After Check ----> Verified User : "+SBFT1VerifiedCheck+ "" +
                        " | Country : "+SBFT1CountryCheck+"\n");
            }
            StatementNumber++;

            //Statement 7
            System.out.println(StatementNumber+": Checking SabseBadaFan Verification And Country Of Team 2");
            res = query.executeQuery("select verified,country from " + Constants.SABSE_BADA_FAN_TEAM_2+" " +
                    "where screen_name = '"+SBFT2UserName+"' limit 1");
            System.out.println("\nBefore Check ----> Verified User : "+SBFT2VerifiedCheck+ "" +
                    " | Country : "+SBFT2CountryCheck);
            if (res.next()) {
                try{
                    if(res.getString(2).equalsIgnoreCase("null")){
                        //Null Pointer Exception
                    }else{
                        SBFT2CountryCheck = res.getString(2);
                        System.out.println("After Check ----> Verified User : "+SBFT2VerifiedCheck+ "" +
                                " | Country : "+SBFT2CountryCheck+"\n");
                    }
                }catch (NullPointerException e){
                    setCountryNullToNA(2);
                }
                SBFT2VerifiedCheck = Boolean.parseBoolean(res.getString(1));
            }else{
                SBFT2CountryCheck = "N/A";
                SBFT2VerifiedCheck = false;
                if(SabseBadaFan.isTie(2))
                    System.out.println("Tie Found!");
                System.out.println("After Check ----> Verified User : "+SBFT2VerifiedCheck+ "" +
                        " | Country : "+SBFT2CountryCheck+"\n");
            }
            StatementNumber++;


            System.out.println(" <---- Performing Storage Queries ----> \n");

            //Statement 8
            System.out.println(StatementNumber+": Setting Storage DataBase");
            query.execute("use " + Constants.DataBaseAnalaysedResults);
            StatementNumber++;

            //Statement 9
            System.out.println(StatementNumber+": Creating Storage Table IF NOT EXISTS For RealTime Data");
            if(Records.isRunningFirstTime){
                CreateTB.storageTemporaryTable();
            }
            StatementNumber++;

            /** Statement 10 */
            System.out.println("\n <---- Performing Insertion Queries ----> \n");
            System.out.println(StatementNumber+": Pre-Insertion Queries \n");
            preparedStatement = (PreparedStatement) con.prepareStatement("" +
                    "insert into table IPL_CricketHour values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            System.out.print("TeamCode ");
            preparedStatement.setLong(1, code);
            StatementNumber++;
            //Statement 11
            System.out.print("Team1Name ");
            preparedStatement.setString(2, team1hash);
            StatementNumber++;
            //Statement 12
            System.out.print("Team2Name ");
            preparedStatement.setString(3, team2hash);
            StatementNumber++;
            //Statement 13
            System.out.print("Starting UTC Time ");
            preparedStatement.setTimestamp(4, startingUtcTimeStamp);
            StatementNumber++;
            //Statement 14
            System.out.print("Starting "+Constants.USER_DEFINED_TIMEZONE +" Time ");
            preparedStatement.setTimestamp(5, startingStandardTimeStamp);
            StatementNumber++;
            //Statement 15
            System.out.print("Team1Count ");
            preparedStatement.setLong(6, t1c);
            StatementNumber++;
            //Statement 16
            System.out.print("Team2Count ");
            preparedStatement.setLong(7, t2c);
            StatementNumber++;
            //Statement 17
            System.out.print("MatchTotalCount ");
            preparedStatement.setLong(8, totalMatchTweets);
            StatementNumber++;
            //Statement 18
            System.out.print("SBFT1Name ");
            preparedStatement.setString(9, SBFT1Name);
            StatementNumber++;
            //Statement 19
            System.out.print("SBFT1UserName ");
            preparedStatement.setString(10, Constants.SBF_MAX_COUNT_USERNAME1);
            StatementNumber++;
            //Statement 20
            System.out.print("SBFT1Count ");
            preparedStatement.setInt(11, Constants.SBF_MAX_COUNT1);
            StatementNumber++;
            //Statement 21
            System.out.print("SBFT1VerifiedCheck ");
            preparedStatement.setBoolean(12, SBFT1VerifiedCheck);
            StatementNumber++;
            //Statement 22
            System.out.print("SBFT1CountryCheck ");
            preparedStatement.setString(13, SBFT1CountryCheck);
            StatementNumber++;
            //Statement 23
            System.out.print("SBFT2Name ");
            preparedStatement.setString(14, SBFT2Name);
            StatementNumber++;
            //Statement 24
            System.out.print("SBFT2UserName ");
            preparedStatement.setString(15, Constants.SBF_MAX_COUNT_USERNAME2);
            StatementNumber++;
            //Statement 25
            System.out.print("SBFT2Count ");
            preparedStatement.setInt(16, Constants.SBF_MAX_COUNT2);
            StatementNumber++;
            //Statement 26
            System.out.print("SBFT2VerifiedCheck ");
            preparedStatement.setBoolean(17, SBFT2VerifiedCheck);
            StatementNumber++;
            //Statement 27
            System.out.print("SBFT2CountryCheck\n");
            preparedStatement.setString(18, SBFT2CountryCheck);
            System.out.println("\nInserting Resulted Records into Database..");
            int i = preparedStatement.executeUpdate() + 1;
            System.out.println("\n" + i + " Below Record Inserted Successfully...");
            System.out.println("" + code + "  " + team1hash + "  " + team2hash + "  " + startingUtcTimeStamp +
                    "  " + startingStandardTimeStamp + "  " + t1c + "  " + t2c + "  " + totalMatchTweets+"" +
                    "  " + SBFT1Name + "  " + SBFT1UserName + "  " + SBFT1VerifiedCheck + "  " + SBFT1CountryCheck+"" +
                    "  " + SBFT2Name + "  " + SBFT2UserName + "  " + SBFT2VerifiedCheck + "  " + SBFT2CountryCheck+"" +
                    "");
            StatementNumber=1;
        }catch (SQLException e){
            System.out.println("\nStatement Number : "+StatementNumber);
            System.out.println("SQLException : \n"+e+"\n");
            try {
                Constants.setDBName("projectcricket");
                new CricketAnalysis().startCleaningService("ALL");
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            System.exit(1);
        }catch (NullPointerException e){
            System.out.println("\nStatement Number : "+StatementNumber);
            System.out.println("SQLException : \n"+e+"\n");
            try {
                Constants.setDBName("projectcricket");
                new CricketAnalysis().startCleaningService("ALL");
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            System.exit(1);
        }

    }

    private void setCountryNullToNA(int i) {
        System.out.println("After Check ---> Null Pointer Exception is Detected at "+StatementNumber+" " +
                "Country Check ----> Setting Country to N/A\n");
        if(i==1)
            SBFT1CountryCheck = "N/A";
        else
            SBFT2CountryCheck = "N/A";

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

