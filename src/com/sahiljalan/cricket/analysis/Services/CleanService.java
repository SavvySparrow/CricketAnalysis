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
public class CleanService implements Runnable{

    private final CountDownLatch latch;

    public CleanService(CountDownLatch latch){
        this.latch = latch;
    }

    @Override
    public void run() {

        try {
            new Records().clearAllRecords();
            latch.countDown();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
