package com.sahiljalan.cricket.CricketAnalysis;

import com.sahiljalan.cricket.Analaysis.SabseBadaFan;
import com.sahiljalan.cricket.Configuration.DefaultConf;
import com.sahiljalan.cricket.Configuration.UserSpecific;
import com.sahiljalan.cricket.Services.*;
import com.sahiljalan.cricket.Tables.Dictionary;
import com.sahiljalan.cricket.Tables.TimeZoneData;
import com.sahiljalan.cricket.Views.RawViews;
import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.Constants.TeamName;
import com.sahiljalan.cricket.Databases.CreateDB;
import com.sahiljalan.cricket.Storage.Storage;
import com.sahiljalan.cricket.Tables.RawTable;
import com.sahiljalan.cricket.TeamData.TeamHASHMENData;
import com.sahiljalan.cricket.Analaysis.SentimentsView;
import com.sahiljalan.cricket.Views.Team;

import java.sql.Statement;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;

import static com.sahiljalan.cricket.Services.CleanTraces.Records.isRunningFirstTime;

/**
 * Created by sahiljalan on 29/4/17.
 */
public class CricketAnalysis implements CricketAnalysisInterface {

    private static TimeZone flumeLocalTimeZone = java.util.TimeZone.getTimeZone(Constants.FLUME_LOCAL_TIMEZONE);
    private static TimeZone userDefinedTimeZone = java.util.TimeZone.getTimeZone(Constants.USER_DEFINED_TIMEZONE);
    private static Statement query = PreProcessingQueriesService.getStatement();
    private static int count = 1;
    public static int totalRecords;
    private static CountDownLatch latch;
    private static Thread startAnalysis;
    private static Thread clearRecords;
    private static boolean isFlumeConfigurationSet;
    public static boolean startCleaningOnly;

    public static void closeConnection() throws SQLException {
        HiveConnectionService.close();
    }

    public static void generateFlumeConfigurationFile() {
        new UserSpecific();
        System.out.println("Configuration is Set\n\n" +
                "Agent Name ----> " +
                ""+ DefaultConf.getBattleCode()+"\n" +
                "Team 1 ----> " +
                ""+ DefaultConf.getTeam1Hash()+"\n" +
                "Team 2 ----> " +
                ""+ DefaultConf.getTeam2Hash()+"\n\n");

        isFlumeConfigurationSet = true;
    }


    public static int getYear() {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY");
        return Integer.parseInt(sdf.format(Calendar.getInstance(flumeLocalTimeZone).getTime()));
    }

    public static String getMonth() {
        SimpleDateFormat sdf = new SimpleDateFormat("MM");
        return sdf.format(Calendar.getInstance(flumeLocalTimeZone).getTime());
    }

    public static String getDay() {
        SimpleDateFormat sdf = new SimpleDateFormat("dd");
        return sdf.format(Calendar.getInstance(flumeLocalTimeZone).getTime());
    }

    public static int getHour() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH");
        return Integer.parseInt(sdf.format(Calendar.getInstance(flumeLocalTimeZone).getTime()));
    }

    public static int getMinuets() {
        SimpleDateFormat sdf = new SimpleDateFormat("mm");
        return Integer.parseInt(sdf.format(Calendar.getInstance(flumeLocalTimeZone).getTime()));
    }

    public static int getSeconds() {
        SimpleDateFormat sdf = new SimpleDateFormat("ss");
        return Integer.parseInt(sdf.format(Calendar.getInstance(flumeLocalTimeZone).getTime()));
    }

    public static Timestamp getCurrentUserDefinedTimeStamp(){
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return Timestamp.valueOf(sdf.format(Calendar.getInstance(userDefinedTimeZone).getTime()));
    }

    public static Boolean checkFlumeConfiguration(){
        return isFlumeConfigurationSet;
    }

    public static void startCleaningOnly(Boolean cleaningOnly){
        startCleaningOnly = cleaningOnly;
    }

    @Override
    public void setCricketType(String cricketType) {
        UserSpecific.setCricketType(cricketType);
    }

    @Override
    public void SetTeams(String t1, String t2) {
        TeamName.setTeams(t1,t2);
        new TeamHASHMENData();
    }

    @Override
    public void setTwitterKeys(String consumerKey, String consumerSecret, String accessTokken, String accessTokkenSecret) {
        UserSpecific.setConsumerKey(consumerKey);
        UserSpecific.setConsumerSecret(consumerSecret);
        UserSpecific.setAccessTokken(accessTokken);
        UserSpecific.setAccessTokkenSecret(accessTokkenSecret);
    }



    @Override
    public void selectDB() throws SQLException {
        new CreateDB();
    }

    @Override
    public void selectDB(String DBName) throws SQLException {
        new CreateDB(DBName);
    }

    @Override
    public void createRawTable() throws SQLException {
    }

    @Override
    public void createRawTable(String TableName) throws SQLException, InterruptedException {

        //Generating Raw Table
        new RawTable(TableName);

        //Generate Raw Views
        //Four Raw Views : Two For Each TeamView
        //one Hashtag and one Mention View for each TeamView
        createRawViews();

        //Generate Teams View using RawViews where we actual start queries
        //These Views are generated from RawViews
        createTeamViews();
    }

    @Override
    public void createRawViews() throws SQLException {
        new RawViews();
    }

    @Override
    public void createTeamViews() throws SQLException {
        new Team();
    }

    @Override
    public void createSentimentsViews(String Team1View,String Team2View) throws SQLException {
        new SentimentsView(Team1View,Team2View);
    }

    @Override
    public void calSabseBadaFan() {
        new SabseBadaFan();
    }

    @Override
    public void storeResults() throws SQLException {
        new Storage();
    }

    @Override
    public void setLocation(int battleCode,int year, String month, String day) {
        Constants.setLocationWithOutHour(battleCode,year,month,day);
        System.out.println("Selected Working Location : "+Constants.HDFS_POSTFIX_LOCATION);
    }

    @Override
    public void setLocation(int battleCode,int year, String month, String day,int hour) {
        Constants.setLocation(battleCode,year,month,day,hour);
        System.out.println("Selected Working Location : "+Constants.HDFS_POSTFIX_LOCATION);
    }

    @Override
    public void startAnalysisService(){

        if(!startCleaningOnly){
            if(Constants.setStopHour == getHour()){
                System.out.println("Stop Hour must not be equal to current hour \nCheck/Try Again....");
                System.exit(1);
            }else{
                System.out.println("\n\n\nAnalysis Application is Started\n\n\n");
                while(getHour()!=Constants.setStopHour){

                    printStartingPattern();
                    latch = new CountDownLatch(1);
                    startAnalysis = new Thread(new MainService(latch));
                    startAnalysis.start();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        ApplicationStartupUtil.shutdownStartupServices();
                        System.exit(1);
                    }
                    printEndingPattern();

                }

                System.out.println("\n\n\nAnalysis Application is Stopped\n\n\n");
                System.out.println("\nStarting After Analysis Storage.");
                Storage.AfterAnalysisStorage();
                System.out.println("Record Inserted into Database Successfully....");
                try {
                    query.execute("drop table if Exists ipl_cricketHour");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private void printEndingPattern() {

        System.out.println();
        for(int j=0;j<200;j++){
            if(j%2==0)
                System.out.print("* ");
        }System.out.println();
        for(int i=0;i<200;i+=2){
            if(i==10)
                System.out.print(" ENDED");
            else if(i==20)
                System.out.print("");
            else if(i==42)
                System.out.print(" Total Record Inserted Today : "+(++totalRecords));
            else
                System.out.print(" *");
        }System.out.println();
        for(int j=0;j<200;j++){
            if(j%2==0)
                System.out.print("* ");
        }System.out.println();
    }

    private void printStartingPattern() {

        for(int j=0;j<200;j++){
            if(j%2==0)
                System.out.print(" *");
        }
        System.out.println();
        for(int i=0;i<200;i+=2){
            if(i==10)
                System.out.print("STARTED ");
            else if(i==20)
                System.out.print("");
            else if(i==42)
                System.out.print("Performing Analysis : "+(count++)+" ");
            else
                System.out.print("* ");
        }System.out.println();
        for(int j=0;j<200;j++){
            if(j%2==0)
                System.out.print(" *");
        }
        System.out.println("\n");
    }

    @Override
    public void startCleaningService(String cleanType) throws InterruptedException {

        if(!Constants.KeepTableAndViews){
            System.out.println("\n <---- Cleaning Service Started ---->\n");
            latch = new CountDownLatch(1);
            clearRecords = new Thread(new CleanService(latch,cleanType));
            clearRecords.start();
            latch.await();
            System.out.println("\n <---- Cleaning Service Completed ---->");
        }

    }

    @Override
    public void specifyFlumeConfigurationFileLocation(String flumeConfigLocation) {
        Constants.FLUME_CONFIG_FILE_LOCATION = flumeConfigLocation;
    }

    @Override
    public void setHDFSServerLocation() {
        Constants.HDFS_SERVER_LOCATION = "hdfs://localhost:9000/";
    }

    @Override
    public void setHDFSServerLocation(String hdfsServerLocationion) {
        Constants.HDFS_SERVER_LOCATION = hdfsServerLocationion;
    }

    @Override
    public void specifyFlumeLocalTimeZone(String timezone) {
        Constants.FLUME_LOCAL_TIMEZONE = timezone;
    }

    @Override
    public void setResultTimeZone(String timezone) {
        Constants.USER_DEFINED_TIMEZONE = timezone;
    }

    @Override
    public void keepTablesAndViews() {
        Constants.KeepTableAndViews  = false;
    }

    @Override
    public void keepTablesAndViews(Boolean KeepTablesAndViews) {
        Constants.KeepTableAndViews  = KeepTablesAndViews;
    }

    @Override
    public void setStopHour() {
        System.out.println("Stop Hour cannot be null\nPlease Specify Stop Hour using setStopHour()");
        System.exit(1);
    }

    @Override
    public void setStopHour(int stopHour){
        Constants.setStopHour = stopHour;
    }


}
