package com.sahiljalan.cricket.analysis;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Services.CleanService;
import com.sahiljalan.cricket.analysis.Services.MainService;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Main extends CricketAnalysis{

    public static void main(String arg[]) throws SQLException, ClassNotFoundException, InterruptedException {


        //Create Object of this class to access Non-Static methods of superClass
        CricketAnalysis ca = new CricketAnalysis();

        //Set Location for ReadingData
        ca.setLocation("RCBvsKXIP",ca.getYear(),ca.getMonth(),ca.getDay(),ca.getHour());
        System.out.println("Selected Working Location : "+Constants.Postfix_Location);

        //Create Connection to Hive
        startConnection();

        //Set Team1 AND Team2
        //Initialize TeamHASHMENData
        ca.SetTeams(TeamName.BANGALURU,TeamName.PUNJAB);

        //Keep Tables & Views after analysis (For Debugging Purpose)
        //Default value False (Empty Parameter = Default Value)
        ca.keepTablesAndViews();

        //Start Analysis on RealTime Data
        ca.startAnalysisService();

        //Start Cleaning After Analysis
        //Dependencies : KeepTableAndViews(Boolean) : set True to start this service
        ca.startCleaningService();

        closeConnection();
        System.out.println("Connection is Successfully Closed");
    }
}
