package com.sahiljalan.cricket.analysis.TeamData;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Main;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 4/5/17.
 */
public class TeamCodes {
    private Statement query = CricketAnalysis.getStatement();

    public TeamCodes() throws SQLException {
        System.out.println("Running : TeamCodes class");
        createTeamCodesTable();
    }

    private void createTeamCodesTable() throws SQLException {

        System.out.println("Running : creating TeamCodes Table() IF NOT EXISTS");
        query.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "+ Constants.TeamCodeTable + "(" +
                "code BIGINT," +
                "teambattle string," +
                "team1 string," +
                "team2 string)" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ");

        query.execute("LOAD DATA LOCAL INPATH '"+Constants.TeamCodeLocation+"' OVERWRITE INTO TABLE " +
                Constants.TeamCodeTable);
    }

}
