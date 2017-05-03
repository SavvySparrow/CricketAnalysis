package com.sahiljalan.cricket.analysis.Storage;

import com.sahiljalan.cricket.analysis.Main;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 3/5/17.
 */
public class Storage {

    private Statement query = Main.getStatement();

    //TODO create DB for Storage
    //TODO create Table for each type of cricket EX:IPL,Normal Matches etc
    //TODO create array of structure for team1 and team2
    //TODO store value in it
    //TODO List Goes on...

    public Storage(){

    }

    public void storageTable() throws SQLException {

        query.execute("create external table if not EXISTS IPL_Cricket(" +
                "Code BIGINT," +
                "Team1 STRING," +
                "Team2 STRING," +
                "Team1_TimeStamp TIMESTAMP," +
                "Team1_Count BIGINT," +
                "Team2_TimeStamp TIMESTAMP," +
                "Team2_Count BIGINT)");

    }
}
