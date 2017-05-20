package com.sahiljalan.cricket;

import com.sahiljalan.cricket.Constants.TeamName;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;

import com.sahiljalan.cricket.Services.ApplicationStartupUtil;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Main extends CricketAnalysis {

    private static boolean result = false;

    //Todo Future Updates Realtime Sentiment Analysis to Specific Player in the world cricket
    public static void main(String arg[]) throws InterruptedException {

        /** Create Object Of This Class To Access Non-Static Methods Of SuperClass */
        CricketAnalysis ca = new CricketAnalysis();

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
        ca.SetTeams(TeamName.MUMBAI, TeamName.PUNE);

        /**
         * Twitter Keys ---> Order Of Keys
         * --> CONSUMER KEY         <--
         * --> CONSUMER SECRET      <--
         * --> ACCESS TOKKEN        <--
         * --> ACCESS TOKKEN SECRET <--
         */
        ca.setTwitterKeys(
                "xUoyQfCgTDmhLdbAYW25h3Rrv",
                "gurryXvVypbBoAANft0kj5VWMxrGzstmZTbtGoqdqiFYn8fw9P",
                "124173649-9jvQ8ULHPfXMJQ1CTqLrdwigXsFtoKPATj4kwzIY",
                "20XtCTZjJTwqQ3k9Wt47cpgZeV0UysUguv5qltMQOCybT");

        /**
         * HDFS Location Format : "hdfs://localhost:9000/"
         * Default Location = "hdfs://localhost:9000/"
         * (Empty Parameter = Default Location)
         */
        ca.setHDFSServerLocation();

        /** Flume Conf Location Format : "/CompleteFlumeInstallationPath/flume/conf/" */
        ca.specifyFlumeConfigurationFileLocation("/home/sahiljalan/flume/conf/");

        try {
            result = ApplicationStartupUtil.startStartupServices();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Startup Services Validation Completed !! Result was :: " + result);

        if (result) {

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
            ca.setStopHour(3);

            /**
             * Keep Tables & Views after analysis (For Debugging Purpose)
             * DefaultConf value is False (Empty Parameter = DefaultConf Value)
             */
            ca.keepTablesAndViews();

            /**
             * Start Analysis on RealTime Data
             * Dependencies :
             * setStopHour(),must not be equal to current hour
             * startCleaningOnly(),If it is true,this Service will not initiated
             */
            ca.startAnalysisService();

            /**
             * Start Cleaning After Analysis
             * Dependencies : KeepTablesAndViews(Boolean) : set False to start this service
             */
            ca.startCleaningService("ALL");


        }
        ApplicationStartupUtil.shutdownStartupServices();
    }
}
