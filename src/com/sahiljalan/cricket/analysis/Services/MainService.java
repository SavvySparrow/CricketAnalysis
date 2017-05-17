package com.sahiljalan.cricket.analysis.Services;

import com.sahiljalan.cricket.analysis.ClearTraces.Records;
import com.sahiljalan.cricket.analysis.ConnectionToHive.HiveConnection;
import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

/**
 * Created by sahiljalan on 5/5/17.
 */
public class MainService extends CricketAnalysis implements Runnable {

    private final CountDownLatch latch;
    private Statement query = CricketAnalysis.getStatement();

    public MainService(CountDownLatch latch){
        this.latch = latch;
    }

    @Override
    public void run() {

        CricketAnalysis ca = new CricketAnalysis();

            try {

                    //Select Database if exists,it will create new DataBase if Not Exists
                    //For Default Database pass empty parameter : selectDB();
                    ca.selectDB("ProjectCricket");
                    //Select Database
                    //query.execute("use "+Constants.DataBaseName);

                    //Create , Clean And Filter Data
                    //Generate Raw Table Based On JsonFormat
                    //For Default TableName pass empty parameter : createRawTable();
                    ca.createRawTable("MatchBuzz");

                    //Analyse the Sentiments of Fans of Both The Teams
                    ca.createSentimentsViews(Constants.TEAM1_VIEW,Constants.TEAM2_VIEW);

                    //Sabse Bada Fan
                    ca.calSabseBadaFan();

                    //Store Results in DataBase
                    ca.storeResults();

                    new Records().clearAllViews();

                    latch.countDown();

                } catch (SQLException e) {
                    e.printStackTrace();
            }

    }
}
