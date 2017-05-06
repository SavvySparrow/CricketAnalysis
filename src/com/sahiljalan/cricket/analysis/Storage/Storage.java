package com.sahiljalan.cricket.analysis.Storage;

import com.sahiljalan.cricket.analysis.ConnectionToHive.HiveConnection;
import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Tables.RawTable;
import com.sahiljalan.cricket.analysis.TeamData.TeamHASHMEN;
import com.sahiljalan.cricket.analysis.TeamData.TeamHASHMENData;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

/**
 * Created by sahiljalan on 3/5/17.
 */
public class Storage {

    private Statement query = CricketAnalysis.getStatement();
    private ResultSet res;
    private static long code,t1c,t2c;
    private String team1hash,team2hash;
    private Timestamp startingStandardTimeStamp,startingUtcTimeStamp;
    private Map<String,TeamHASHMEN> map = TeamHASHMENData.getMAP();
    Connection con = HiveConnection.getConnection();
    private PreparedStatement preparedStatement;
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("HH");

    public Storage() throws SQLException {
        storageTable();
    }

    public void storageTable() throws SQLException {

        startingUtcTimeStamp = RawTable.getStartingTimeStamp();
        team1hash = map.get(TeamName.TEAM1).getHashtag();
        team2hash = map.get(TeamName.TEAM2).getHashtag();

        res = query.executeQuery("select code from "+Constants.TeamCodeTable+
                " where teambattle = '"+team1hash+"VS"+team2hash+"'");
        while (res.next()) {
            code = Integer.parseInt(res.getString(1));

        }
        res = query.executeQuery("select from_utc_timestamp(from_unixtime(unix_timestamp" +
                "(RawTable.getStartingTimeStamp(),'yyyy MMM dd hh:mm:ss')),'IST')");
        while (res.next()){
            startingStandardTimeStamp = res.getTimestamp(1);

        }
        res = query.executeQuery("select count(rowid) from "+ Constants.PosHype1);
        while (res.next()) {
            t1c = Integer.parseInt(res.getString(1));

        }
        res = query.executeQuery("select count(rowid) from "+ Constants.PosHype2);
        while (res.next()){
            t2c  = Integer.parseInt(res.getString(1));

        }

        query.execute("use "+Constants.DataBaseAnalaysedResults);

        if(new CricketAnalysis().getHour()==23){
            query.execute("drop table if Exists ipl_cricketHour");
        }

        query.execute("create table if not EXISTS ipl_cricketHour(" +
                "Code BIGINT," +
                "Team1 STRING," +
                "Team2 STRING," +
                "startingTimeStamp TIMESTAMP," +
                "endingTimeStamp TIMESTAMP," +
                "Team1_Count BIGINT," +
                "Team2_Count BIGINT)");

        query.execute("drop table if Exists ipl_cricket");
        query.execute("create table if not EXISTS ipl_cricket(" +
                "Code BIGINT," +
                "Team1 STRING," +
                "Team2 STRING," +
                "startingTimeStamp TIMESTAMP," +
                "endingTimeStamp TIMESTAMP," +
                "Team1_Count BIGINT," +
                "Team2_Count BIGINT)");


            preparedStatement = (PreparedStatement) con.prepareStatement("" +
                    "insert into table IPL_CricketHour values(?,?,?,?,?,?,?)");
            preparedStatement.setLong(1,code);
            preparedStatement.setString(2,team1hash);
            preparedStatement.setString(3,team2hash);
            preparedStatement.setTimestamp(4,startingUtcTimeStamp);
            preparedStatement.setTimestamp(5, startingStandardTimeStamp);
            preparedStatement.setLong(6,t1c);
            preparedStatement.setLong(7,t2c);
            System.out.println("\n\nInserting Resulted Records into Database........");
            int i=preparedStatement.executeUpdate()+1;
            System.out.println("\n"+i+" Below Record Inserted Successfully.");
            System.out.println("\n"+code+"  "+team1hash+"  "+team2hash+"  "+startingUtcTimeStamp +
                    "  "+startingStandardTimeStamp+"  "+t1c+"  "+t2c);








        }
    }

