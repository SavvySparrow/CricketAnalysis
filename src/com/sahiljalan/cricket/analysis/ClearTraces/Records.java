package com.sahiljalan.cricket.analysis.ClearTraces;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 5/5/17.
 */
public class Records implements Tables , Views {

    public static Boolean isRunningFirstTime = true;

    private static Statement query = CricketAnalysis.getStatement();

    public Records() throws SQLException {
        //Constants.setDBName("projectcricket");Constants.setTableName("matchbuzz");
        query.execute("use "+ Constants.DataBaseName);
    }

    public static Boolean isEmpty() throws SQLException {
        query.execute("use projectcricket");
        ResultSet res = query.executeQuery("show tables");
        while(res.next()){}
        if(res.getRow()==0){
            return true;
        }
        isRunningFirstTime=false;
        return false;
    }

    public void clearAllRecords() throws SQLException{
        System.out.println("Running : Cleaning ALl Views from Records IF EXISTS");
        clearAllViews();
        System.out.println("Running : Cleaning ALl Tables from Records IF EXISTS");
        clearAllTables();
    }

    @Override
    public void clearAllViews() throws SQLException {
        clearRawViews();
        clearTeamViews();
        clearSentimentsView();
    }

    @Override
    public void clearRawViews() throws SQLException {
        System.out.println("Running : Cleaning Raw Views from Records IF EXISTS");
        query.execute("drop view if exists " + Constants.Team1Hashtags);
        query.execute("drop view if exists " + Constants.Team1Mentions);
        query.execute("drop view if exists " + Constants.Team2Hashtags);
        query.execute("drop view if exists " + Constants.Team2Mentions);
        query.execute("drop view if exists rawtableview");
    }

    @Override
    public void clearTeamViews() throws SQLException {
        System.out.println("Running : Cleaning Team Views from Records IF EXISTS");
        query.execute("drop view if EXISTS " + Constants.Team1_Temp);
        query.execute("drop view if EXISTS " + Constants.Team2_Temp);
        query.execute("drop view if EXISTS " + Constants.TEAM1_VIEW);
        query.execute("drop view if EXISTS " + Constants.TEAM2_VIEW);
    }

    @Override
    public void clearSentimentsView() throws SQLException {
        System.out.println("Running : Cleaning Sentiments Views from Records IF EXISTS");
        query.execute("drop view if EXISTS " + Constants.SentiViewT1V1);
        query.execute("drop view if EXISTS " + Constants.SentiViewT1V2);
        query.execute("drop view if EXISTS " + Constants.SentiViewT1V3);
        query.execute("drop table if EXISTS " + Constants.Team1Sentiments);
        query.execute("drop view if EXISTS " + Constants.PosHype1);

        query.execute("drop view if EXISTS " + Constants.SentiViewT2V1);
        query.execute("drop view if EXISTS " + Constants.SentiViewT2V2);
        query.execute("drop view if EXISTS " + Constants.SentiViewT2V3);
        query.execute("drop table if EXISTS " + Constants.Team2Sentiments);
        query.execute("drop view if EXISTS " + Constants.PosHype2);
    }


    @Override
    public void clearAllTables() throws SQLException {
        mainTable();
        teamData();
        dictionaryTable();
        timeZoneTable();
    }

    @Override
    public void mainTable() throws SQLException {
        System.out.println("Running : Cleaning Raw Table from Records IF EXISTS");
        query.execute("drop table if exists " + Constants.TableName);
        query.execute("drop table if exists rawtable_temp");
    }

    @Override
    public void dictionaryTable() throws SQLException {
        System.out.println("Running : Cleaning Dictionary Table from Records IF EXISTS");
        query.execute("drop table if exists " + Constants.DictionaryTable);
    }

    @Override
    public void timeZoneTable() throws SQLException {
        System.out.println("Running : Cleaning TimeZone Table from Records IF EXISTS");
        query.execute("drop table if exists " + Constants.TimeZoneTable);
    }

    @Override
    public void teamData() throws SQLException {
        System.out.println("Running : Cleaning TeamData Table from Records IF EXISTS");
        query.execute("drop table if exists " + Constants.TeamCodeTable);
    }
}
