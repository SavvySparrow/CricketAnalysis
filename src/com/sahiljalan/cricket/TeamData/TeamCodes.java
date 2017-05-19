package com.sahiljalan.cricket.TeamData;

import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.Services.CleanTraces.Records;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 4/5/17.
 */
public class TeamCodes {
    private Statement query = CricketAnalysis.getStatement();

    public TeamCodes() throws SQLException {
        System.out.println("Running : "+ getClass());
        createTeamCodesTable();
        System.out.println();
    }

    private void createTeamCodesTable() throws SQLException {

        System.out.println("Executing : Creating TeamCodes Table IF NOT EXISTS");
        query.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "+ Constants.TeamCodeTable + "(" +
                "code BIGINT," +
                "teambattle string," +
                "team1 string," +
                "team2 string)" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ");

        if(Records.isRunningFirstTime){
            System.out.println("Executing : Loading TeamCodes Data");
            query.execute("LOAD DATA LOCAL INPATH '"+Constants.TeamCodeLocation+"' OVERWRITE INTO TABLE " +
                    Constants.TeamCodeTable);
        }

    }

}
