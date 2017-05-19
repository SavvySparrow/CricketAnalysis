package com.sahiljalan.cricket.CricketAnalysis;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by sahiljalan on 29/4/17.
 */
public interface CricketAnalysisInterface {

    void setCricketType(String CK);
    void SetTeams(String t1, String t2);
    void setTwitterKeys(String s, String s1, String s2, String s3);
    void generateFlumeConfigurationFile();

    void selectDB() throws SQLException;
    void selectDB(String DBName) throws SQLException;

    void createRawTable() throws SQLException;
    void createRawTable(String TableName) throws SQLException, InterruptedException;

    void createRawViews() throws SQLException;
    void createTeamViews() throws SQLException;

    void createSentimentsViews(String t1,String t2) throws SQLException;
    void calSabseBadaFan();

    void storeResults() throws SQLException;

    void setLocation(int t,int y,String m,String d);

    void setLocation(int t,int y,String m,String d,int hour);

    void keepTablesAndViews();
    void keepTablesAndViews(Boolean KTV);

    void setStopHour();
    void setStopHour(int stopHour);

    void startAnalysisService() throws InterruptedException;
    void startCleaningService(String cleanType) throws InterruptedException;

    void specifyFlumeConfigurationFileLocation(String flumeConfigLocation);
    void setHDFSServerLocation();
    void setHDFSServerLocation(String hdfsLocation);
    void specifyFlumeLocalTimeZone(String timezone);
    void setResultTimeZone(String timezone);


}

