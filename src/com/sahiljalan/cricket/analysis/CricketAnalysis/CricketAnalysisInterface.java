package com.sahiljalan.cricket.analysis.CricketAnalysis;

import java.sql.SQLException;

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

    void createSentimentsViews() throws SQLException;
}

