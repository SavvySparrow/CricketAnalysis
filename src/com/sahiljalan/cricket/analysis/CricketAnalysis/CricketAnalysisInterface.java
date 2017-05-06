package com.sahiljalan.cricket.analysis.CricketAnalysis;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by sahiljalan on 29/4/17.
 */
public interface CricketAnalysisInterface {


    void SetTeams(String t1, String t2);

    void selectDB() throws SQLException;
    void selectDB(String DBName) throws SQLException;

    void createRawTable() throws SQLException;
    void createRawTable(String TableName) throws SQLException;

    void createRawViews() throws SQLException;
    void createTeamViews() throws SQLException;

    void createSentimentsViews(String t1,String t2) throws SQLException;

    void storeResults() throws SQLException;

    void setLocation(String t,int y,String m,String d);

    void setLocation(String t,int y,String m,String d,int hour);

    void keepTablesAndViews();
    void keepTablesAndViews(Boolean KTV);

    void setStopHour(int stopHour);

    void startAnalysisService() throws InterruptedException;
    void startCleaningService() throws InterruptedException;
}

