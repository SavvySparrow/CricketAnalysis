package com.sahiljalan.cricket.analysis.CricketAnalysis;

import com.sahiljalan.cricket.analysis.ConnectionToHive.HiveConnection;
import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.Databases.CreateDB;
import com.sahiljalan.cricket.analysis.Storage.Storage;
import com.sahiljalan.cricket.analysis.Tables.RawTable;
import com.sahiljalan.cricket.analysis.TeamData.TeamHASHMENData;
import com.sahiljalan.cricket.analysis.Views.RawViews;
import com.sahiljalan.cricket.analysis.Views.SentimentsView.SentimentsView;
import com.sahiljalan.cricket.analysis.Views.Team;

import java.sql.Statement;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by sahiljalan on 29/4/17.
 */
public class CricketAnalysis implements CricketAnalysisInterface {

    private static Statement query;
    Calendar cal;
    private int year,hour,min;
    private String month,day;

    protected static void startConnection() throws SQLException, ClassNotFoundException {
        HiveConnection startHive = new HiveConnection();
        query = startHive.getConnection().createStatement();
        query.execute("CREATE TEMPORARY FUNCTION NumberRows AS " +
                "'com.sahil.jalan.UDFNumberRows'");
    }
    public static Statement getStatement() {
        return query;
    }


    @Override
    public int getYear() {
        cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY");
        year = Integer.parseInt(sdf.format(cal.getTime()));
        return year;
    }

    @Override
    public String getMonth() {
        cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("MM");
        month = sdf.format(cal.getTime());
        return month;
    }

    @Override
    public String getDay() {
        cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd");
        day = sdf.format(cal.getTime());
        return day;
    }

    @Override
    public int getHour() {
        cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("HH");
        hour = Integer.parseInt(sdf.format(cal.getTime()));
        return hour;
    }

    @Override
    public int getMinuets() {
        cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("mm");
        min = Integer.parseInt(sdf.format(cal.getTime()));
        return min;
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
        new RawTable();

        //Generate Raw Views
        //Four Raw Views : Two For Each TeamView
        //one Hashtag and one Mention View for each TeamView
        createRawViews();

        //Generate Teams View using RawViews where we actual start queries
        //These Views are generated from RawViews
        createTeamViews();
    }

    @Override
    public void createRawTable(String TableName) throws SQLException {
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
    public void storeResults() throws SQLException {
        new Storage();
    }

    @Override
    public void setLocation(String team,int year, String month, String day) {
        Constants.setLocationWithOutHour(team,year,month,day);
    }

    @Override
    public void setLocation(String team,int year, String month, String day,int hour) {
        Constants.setLocation(team,year,month,day,hour);
    }

}
