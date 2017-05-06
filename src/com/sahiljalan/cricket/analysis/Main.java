package com.sahiljalan.cricket.analysis;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Services.CleanService;
import com.sahiljalan.cricket.analysis.Services.MainService;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Main extends CricketAnalysis{

    public static void main(String arg[]) throws SQLException, ClassNotFoundException, InterruptedException {


        //Create Object of this class to access Non-Static methods of superClass
        CricketAnalysis ca = new CricketAnalysis();

        //HDFS Location : Folder Partition Format (Team1vsTeam2/year/month/day/hour)
        ca.setLocation("DDvsMI",getYear(),getMonth(),getDay(),getHour());
        System.out.println("Selected Working Location : "+Constants.Postfix_Location);

        //Create Connection to Hive
        startConnection();
        System.out.println("\n\nConnection is Started Successfully\n\n");

        //Set Team1 AND Team2
        //Initialize TeamHASHMENData
        ca.SetTeams(TeamName.DELHI,TeamName.MUMBAI);

        //Set how many hours you want to do Realtime Processing
        //After set hours application will stop processing and close the connection
        //Max 24 Hours
        ca.setStopHour(1);

        //Keep Tables & Views after analysis (For Debugging Purpose)
        //Default value is False (Empty Parameter = Default Value)
        ca.keepTablesAndViews();

        //Start Analysis on RealTime Data
        ca.startAnalysisService();

        //Start Cleaning After Analysis
        //Dependencies : KeepTablesAndViews(Boolean) : set True to start this service
        ca.startCleaningService();

        closeConnection();
        System.out.println("Connection is Closed Successfully");
    }
}
