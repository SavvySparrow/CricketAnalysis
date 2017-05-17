package com.sahiljalan.cricket.analysis;

import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;

import java.sql.SQLException;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Main extends CricketAnalysis{

    //Todo Future Updates Realtime Sentiment Analysis to Specific Player in the world cricket
    public static void main(String arg[]) throws SQLException, ClassNotFoundException, InterruptedException {

        //Create Object of this class to access Non-Static methods of superClass
        CricketAnalysis ca = new CricketAnalysis();

        //HDFS Location : Folder Partition Format (Team1vsTeam2/year/month/day/hour)
        ca.setLocation("SRHvsMI",getYear(),getMonth(),"08");

        //Create Connection to Hive
        startConnection();

        //Set Team1 AND Team2
        //Initialize TeamHASHMENData
        ca.SetTeams(TeamName.HYDERABAD,TeamName.MUMBAI);

        //set your current TimeZone ----> timeStamp will store in results
        //Enter Code
        ca.setCurrentTimeZone("IST");

        //Todo calc Sabse Bada Fan and if it is a valid user or not

        //Set Maximum hours(SEE EXMAPLE BELOW) you want to do Processing on RealTime Data
        //After set hours application will stop processing and close the connection
        //Maximum Duration of Running 23 Hours , Must Be Specify
        //Example : If setStopHour(16) , means it will stop at 4 Pm IST
        ca.setStopHour(14);

        //Keep Tables & Views after analysis (For Debugging Purpose)
        //Default value is False (Empty Parameter = Default Value)
        ca.keepTablesAndViews();

        //Start Analysis on RealTime Data
        //Dependencies : setStopHour(),must not be equal to current hour
        ca.startAnalysisService();

        //Start Cleaning After Analysis
        //Dependencies : KeepTablesAndViews(Boolean) : set False to start this service
        //TODO Control Cleaning Exception
        ca.startCleaningService();

        closeConnection();
    }
}
