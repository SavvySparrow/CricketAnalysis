package com.sahiljalan.cricket.analysis;

import com.sahiljalan.cricket.analysis.ConnectionToHive.HiveConnection;
import com.sahiljalan.cricket.analysis.tables.RawTable;
import com.sahiljalan.cricket.analysis.views.RawViews;
import com.sahiljalan.cricket.analysis.Databases.CreateDB;
import com.sahiljalan.cricket.analysis.views.Team1;
import com.sahiljalan.cricket.analysis.views.Team2;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class MainCricket {

    private static Statement query;

    public static void main(String arg[]) throws SQLException, ClassNotFoundException {

        //Create Connection to Hive by calling the Default
        //Constructor of HiveConnection class
        HiveConnection startHive = new HiveConnection();
        query = startHive.getConnection().createStatement();

        //Select Database if exists,
        //it will create new DataBase if Not Exits
        //For Default Database pass empty parameter : selectDB();
        selectDB("ProjectCricket");

        //Generate Raw Table Based On JsonFormat
        //For Default TableName pass empty parameter : createRawTable();
        createRawTable("MatchBuzz");

        //Generate Raw Views
        //Four Raw Views : Two For Each Team
        //one Hashtag and one Mention View for each Team
        createRawViews();

        //Generate Teams View where we actual start queries using RawViews
        createTeam1Views();
        createTeam2Views();

    }

    private static void selectDB() throws SQLException {
        new CreateDB();
    }
    private static void selectDB(String DBName) throws SQLException {
        new CreateDB(DBName);
    }

    private static void createRawTable() throws SQLException {
        new RawTable();
    }
    private static void createRawTable(String TableName) throws SQLException {
        new RawTable(TableName);
    }

    private static void createRawViews() throws SQLException {
        new RawViews();
    }
    public static Statement getStatement(){
        return query;
    }

    private static void createTeam1Views() throws SQLException {
        new Team1();
    }
    private static void createTeam2Views() throws SQLException {
        new Team2();
    }


}
