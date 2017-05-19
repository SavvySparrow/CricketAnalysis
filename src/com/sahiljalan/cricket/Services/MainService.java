package com.sahiljalan.cricket.Services;

import com.sahiljalan.cricket.Configuration.DefaultConf;
import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

/**
 * Created by sahiljalan on 5/5/17.
 */
public class MainService extends CricketAnalysis implements Runnable {

    private final CountDownLatch latch;

    public MainService(CountDownLatch latch){
        this.latch = latch;
    }

    @Override
    public void run() {

        CricketAnalysis ca = new CricketAnalysis();

            try {

                //HDFS Location : Folder Partition Format (TeamCode/year/month/day/hour)
                ca.setLocation(DefaultConf.getBattleCode(),getYear(),getMonth(),getDay(),getHour());

                //Select Database if exists,it will create new DataBase if Not Exists
                //For DefaultConf Database pass empty parameter : selectDB();
                ca.selectDB("ProjectCricket");
                //Select Database
                //query.execute("use "+Constants.DataBaseName);

                //Create , Clean And Filter Data
                //Generate Raw Table Based On JsonFormat
                //For DefaultConf TableName pass empty parameter : createRawTable();
                ca.createRawTable("MatchBuzz");

                //Analyse the Sentiments of Fans of Both The Teams
                System.out.println("\nSentiment Analysis Application is Started.\n");
                ca.createSentimentsViews(Constants.TEAM1_VIEW,Constants.TEAM2_VIEW);
                System.out.println("\nSentiment Analysis Application is Stopped.\n");

                //Sabse Bada Fan
                System.out.println("\nSabseBadaFan Application is Started.\n");
                ca.calSabseBadaFan();
                System.out.println("\nSabseBadaFan Application is Stopped.\n");

                //Store Results in DataBase
                ca.storeResults();

                ca.startCleaningService("VIEWS");

                latch.countDown();

            } catch (SQLException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

    }
}
