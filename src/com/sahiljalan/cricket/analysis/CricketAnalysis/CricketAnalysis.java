package com.sahiljalan.cricket.analysis.CricketAnalysis;

import com.sahiljalan.cricket.analysis.ConnectionToHive.HiveConnection;
import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.Databases.CreateDB;
import com.sahiljalan.cricket.analysis.Services.CleanService;
import com.sahiljalan.cricket.analysis.Services.MainService;
import com.sahiljalan.cricket.analysis.Storage.Storage;
import com.sahiljalan.cricket.analysis.Tables.RawTable;
import com.sahiljalan.cricket.analysis.Tables.TimeZone;
import com.sahiljalan.cricket.analysis.TeamData.TeamHASHMENData;
import com.sahiljalan.cricket.analysis.Views.RawViews;
import com.sahiljalan.cricket.analysis.Views.SentimentsView.SentimentsView;
import com.sahiljalan.cricket.analysis.Views.Team;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.CountDownLatch;

/**
 * Created by sahiljalan on 29/4/17.
 */
public class CricketAnalysis implements CricketAnalysisInterface {

    private static Statement query;
    private static int count = 1;
    public static int totalRecords;
    private static CountDownLatch latch;
    private static Thread startAnalysis;
    private static Thread clearRecords;

    protected static void startConnection() throws SQLException, ClassNotFoundException {
        HiveConnection.start();
        query = HiveConnection.getConnection().createStatement();
        query.execute("SET hive.support.sql11.reserved.keywords=false");
        query.execute("CREATE TEMPORARY FUNCTION NumberRows AS " +
                "'com.sahil.jalan.UDFNumberRows'");
    }

    protected static void closeConnection() throws SQLException {
        HiveConnection.close();
    }
    public static Statement getStatement() {
        return query;
    }


    public static int getYear() {
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY");
        return Integer.parseInt(sdf.format(Calendar.getInstance().getTime()));
    }

    public static String getMonth() {
        SimpleDateFormat sdf = new SimpleDateFormat("MM");
        return sdf.format(Calendar.getInstance().getTime());
    }

    public static String getDay() {
        SimpleDateFormat sdf = new SimpleDateFormat("dd");
        return sdf.format(Calendar.getInstance().getTime());
    }

    public static int getHour() {
        SimpleDateFormat sdf = new SimpleDateFormat("HH");
        return Integer.parseInt(sdf.format(Calendar.getInstance().getTime()));
    }

    public static int getMinuets() {
        SimpleDateFormat sdf = new SimpleDateFormat("mm");
        return Integer.parseInt(sdf.format(Calendar.getInstance().getTime()));
    }

    @Override
    public void SetTeams(String t1, String t2) {
        TeamName.setTeams(t1,t2);
        new TeamHASHMENData();
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
    public void createRawTable(String TableName) throws SQLException {
        new RawTable(TableName);
        new TimeZone();

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
    public void storeResults() throws SQLException {
        new Storage();
    }

    @Override
    public void setLocation(String team,int year, String month, String day) {
        Constants.setLocationWithOutHour(team,year,month,day);
        System.out.println("Selected Working Location : "+Constants.Postfix_Location);
    }

    @Override
    public void setLocation(String team,int year, String month, String day,int hour) {
        Constants.setLocation(team,year,month,day,hour);
        System.out.println("Selected Working Location : "+Constants.Postfix_Location);
    }

    @Override
    public void startAnalysisService(){

        if(Constants.setStopHour == getHour()){
            System.out.println("Stop Hour must not be equal to current hour \nCheck/Try Again....");
            System.exit(1);
        }else{
            System.out.println("\n\n\nAnalysis Application is Started\n\n\n");
            while(getHour()!=Constants.setStopHour){

                System.out.println("\nPerforming Analysis : "+(count++)+"\n");
                latch = new CountDownLatch(1);
                startAnalysis = new Thread(new MainService(latch));
                startAnalysis.start();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                System.out.println("\nTotal Record Inserted Today : "+(++totalRecords));

            }
            Storage.AfterAnalysisStorage();
            System.out.println("\n\n\nAnalysis Application is Stopped\n\n\n");
        }


    }

    @Override
    public void startCleaningService() throws InterruptedException {

        if(!Constants.KeepTableAndViews){
            System.out.println("Cleaning Service Started \nPlease Wait........\n");
            latch = new CountDownLatch(1);
            clearRecords = new Thread(new CleanService(latch));
            clearRecords.start();
            latch.await();
            System.out.println("\nCleaning Service Completed.");
        }

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
