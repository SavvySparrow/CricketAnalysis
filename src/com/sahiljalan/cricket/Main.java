package com.sahiljalan.cricket;

import com.sahiljalan.cricket.Constants.TeamName;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;

import java.sql.SQLException;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Main extends CricketAnalysis{

    //Todo Future Updates Realtime Sentiment Analysis to Specific Player in the world cricket
    public static void main(String arg[]) throws SQLException, ClassNotFoundException, InterruptedException {

        /** Create Object Of This Class To Access Non-Static Methods Of SuperClass */
        CricketAnalysis ca = new CricketAnalysis();

        /** Create Connection to Hive */
        startConnection();

        /**
         * Start Execution To Clean Only (Cleaning Previous Tables & Views)
         * It Will Skip Analysing Application
        */
        startCleaningOnly(false);

        /** Set Cricket Type */
        ca.setCricketType("IPL");

        /**
        * Set Team1 AND Team2
        * Initialize TeamHASHMENData
        */
        ca.SetTeams(TeamName.MUMBAI,TeamName.KOLKATA);

        /** Twitter Keys */
        ca.setTwitterKeys(
                "xUoyQfCgTDmhLdbAYW25h3Rrv",
                "gurryXvVypbBoAANft0kj5VWMxrGzstmZTbtGoqdqiFYn8fw9P",
                "124173649-9jvQ8ULHPfXMJQ1CTqLrdwigXsFtoKPATj4kwzIY" ,
                "20XtCTZjJTwqQ3k9Wt47cpgZeV0UysUguv5qltMQOCybT");

        /**
        * HDFS Location Format : "hdfs://localhost:9000/"
        * Default Location = "hdfs://localhost:9000/"
        * (Empty Parameter = Default Location)
        */
        ca.setHDFSServerLocation();

        /** Flume Conf Location Format : "/CompleteFlumeInstallationPath/flume/conf/" */
        ca.specifyFlumeConfigurationFileLocation("/home/sahiljalan/flume/conf/");

        /** Generate Flume Configuration File Using Above Properties */
        //ca.generateFlumeConfigurationFile();

        /**
        * Data Stored in HDFS Acc. to Flume Installation Server TimeZone
        * Specify Flume Server Local TimeZone
        * For Fetching Data From HDFS
        */
        ca.specifyFlumeLocalTimeZone("IST");

        /**
        * Specify Any TimeZone
        * According to this TimeZone, Current Local Time Will Store In Results
        */
        //TODO Check TimeZone String
        ca.setResultTimeZone("IST");

        /**
        * Set Maximum hours(SEE EXMAPLE BELOW) you want to do Processing on RealTime Data
        * After set hours application will stop processing and close the connection
        * Maximum Duration of Running 23 Hours , Must Be Specify
        * Example : If setStopHour(16) , means it will stop at 4 Pm USER_DEFINED_TIMEZONE
        */
        ca.setStopHour(2);

        /**
        * Keep Tables & Views after analysis (For Debugging Purpose)
        * DefaultConf value is False (Empty Parameter = DefaultConf Value)
        */
        ca.keepTablesAndViews();

        /**
        * Start Analysis on RealTime Data
        * Dependencies : setStopHour(),must not be equal to current hour
        */
        ca.startAnalysisService();

        /**
        * Start Cleaning After Analysis
        * Dependencies : KeepTablesAndViews(Boolean) : set False to start this service
        */
        ca.startCleaningService("ALL");

        closeConnection();
    }
}
