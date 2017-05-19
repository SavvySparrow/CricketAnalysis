package com.sahiljalan.cricket.Services;

import com.sahiljalan.cricket.Services.CleanTraces.Records;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by sahiljalan on 5/5/17.
 */
public class CleanService implements Runnable{

    private final CountDownLatch latch;
    private String CleanType;

    public CleanService(CountDownLatch latch,String cleanType){
        CleanType = cleanType;
        this.latch = latch;
    }

    @Override
    public void run() {

        try {

            if(CleanType.contentEquals("all")||CleanType.contentEquals("ALL")){
                new Records().clearAllRecords();
                latch.countDown();
            }

            else if(CleanType.contentEquals("tables")||CleanType.contentEquals("TABLES")){
                new Records().clearAllRecords();
                latch.countDown();
            }

            else if(CleanType.contentEquals("views")||CleanType.contentEquals("VIEWS")){
                new Records().clearAllViews();
                latch.countDown();
            }

            else if(CleanType.contentEquals("main")||CleanType.contentEquals("MAIN")){
                new Records().clearMainTable();
                latch.countDown();
            }

            else {
                System.out.println("<----Enter Correct Clean Type---->");
                latch.countDown();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
