package com.sahiljalan.cricket.analysis.CricketAnalysis;

import com.sahiljalan.cricket.analysis.ConnectionToHive.HiveConnection;
import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.Databases.CreateDB;
import com.sahiljalan.cricket.analysis.Tables.RawTable;
import com.sahiljalan.cricket.analysis.TeamData.TeamData;
import com.sahiljalan.cricket.analysis.Views.RawViews;
import com.sahiljalan.cricket.analysis.Views.SentimentsView.SentimentsView;
import com.sahiljalan.cricket.analysis.Views.Team;

import java.sql.Statement;

import java.sql.SQLException;

/**
 * Created by sahiljalan on 29/4/17.
 */
public class CricketAnalysis implements CricketAnalysisInterface {

    private static Statement query;

    protected static void startConnection() throws SQLException, ClassNotFoundException {
        HiveConnection startHive = new HiveConnection();
        query = startHive.getConnection().createStatement();
    }
    public static Statement getStatement() {
        return query;
    }

    @Override
    public void SetTeams(String t1, String t2) {
        TeamName.setTeams(t1,t2);
        new TeamData();
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
    public void createSentimentsViews() throws SQLException {
        new SentimentsView();
    }
}
