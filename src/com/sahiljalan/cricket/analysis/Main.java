package com.sahiljalan.cricket.analysis;

import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;

import java.sql.SQLException;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Main extends CricketAnalysis{

    public static void main(String arg[]) throws SQLException, ClassNotFoundException, InterruptedException {


        //Create Object of this class to access Non-Static methods of superClass
        CricketAnalysis ca = new CricketAnalysis();

        //HDFS Location : Folder Partition Format (Team1vsTeam2/year/month/day/hour)
        ca.setLocation("DDvsMI",getYear(),getMonth(),getDay());

        //Create Connection to Hive
        startConnection();

        //Set Team1 AND Team2
        //Initialize TeamHASHMENData
        ca.SetTeams(TeamName.DELHI,TeamName.MUMBAI);

        //Set how Maximum hours you want to do Realtime Processing
        //After set hours application will stop processing and close the connection
        //Maximum Duration of Running 23 Hours , Must Be Specify
        //Example : If setStopHour(16) , means it will stop at 4 Pm IST
        ca.setStopHour(5);

        //Keep Tables & Views after analysis (For Debugging Purpose)
        //Default value is False (Empty Parameter = Default Value)
        ca.keepTablesAndViews();

        //Start Analysis on RealTime Data
        //Dependencies : setStopHour(),must not be equal to current hour
        ca.startAnalysisService();

        //Start Cleaning After Analysis
        //Dependencies : KeepTablesAndViews(Boolean) : set False to start this service
        ca.startCleaningService();

        closeConnection();
    }
}
