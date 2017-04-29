package com.sahiljalan.cricket.analysis;

import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Views.SentimentsView.SentimentsView;

import java.sql.SQLException;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Main extends CricketAnalysis {

    public static void main(String arg[]) throws SQLException, ClassNotFoundException {

        //Create Object of this class to access Non-Static methods of superClass
        CricketAnalysis ca = new Main();

        //Create Connection to Hive
        startConnection();

        //Set Team1 AND Team2
        //Initialize TeamData
        ca.SetTeams(TeamName.PUNJAB,TeamName.MUMBAI);

        //Select Database if exists,it will create new DataBase if Not Exits
        //For Default Database pass empty parameter : selectDB();
        ca.selectDB("ProjectCricket");

        //Create , Clean And Filter Data
        //Generate Raw Table Based On JsonFormat
        //For Default TableName pass empty parameter : createRawTable();
        ca.createRawTable("MatchBuzz");

        //Analyse the Sentiments of Fans of Both The Teams
        ca.createSentimentsViews();

    }
}
