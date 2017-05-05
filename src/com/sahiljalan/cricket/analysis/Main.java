package com.sahiljalan.cricket.analysis;

import com.sahiljalan.cricket.analysis.ConnectionToHive.HiveConnection;
import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Service.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.CountDownLatch;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Main extends CricketAnalysis{

    private static int count = 1;
    public static int totalRecords;
    private static CountDownLatch latch;
    private static Thread startAnalysis,startAnalysis2;

    public static void main(String arg[]) throws SQLException, ClassNotFoundException {


        //Create Object of this class to access Non-Static methods of superClass
        CricketAnalysis ca = new CricketAnalysis();
        ca.setLocation("RCBvsKXIP",ca.getYear(),ca.getMonth(),ca.getDay(),ca.getHour());


        System.out.println("Selected Working Data : "+Constants.Postfix_Location);

        //Create Connection to Hive
        startConnection();

        //Set Team1 AND Team2
        //Initialize TeamHASHMENData
        ca.SetTeams(TeamName.BANGALURU,TeamName.PUNJAB);

        //Select Database if exists,it will create new DataBase if Not Exits
        //For Default Database pass empty parameter : selectDB();
        ca.selectDB("ProjectCricket");


        while(ca.getHour()!=13){

            System.out.println("Performing Analysis : "+count++);
            latch = new CountDownLatch(1);
            startAnalysis = new Thread(new Service(latch));
            startAnalysis.start();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Total Record Inserted Today : "+(++totalRecords));

        }


        System.out.println("Analysis is Complete Now....");
        Connection con = HiveConnection.getConnection();
        con.close();
    }
}
