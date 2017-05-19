package com.sahiljalan.cricket.Configuration;

import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.Constants.TeamName;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.TeamData.TeamHASHMEN;
import com.sahiljalan.cricket.TeamData.TeamHASHMENData;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.sahiljalan.cricket.Services.CleanTraces.Records.isRunningFirstTime;

/**
 * Created by sahiljalan on 18/5/17.
 */
public class DefaultConf {

    private static final String hash = "#";
    private static final String atTheRate = "@";
    private static final String vs = "vs";
    private static final String comma = ",";
    private static Statement query = CricketAnalysis.getStatement();
    private static int teamHash=1,teamMen=1;
    private static FlumeSortedProperties flumeDefault = UserSpecific.getFlumeConf();
    private static Map<String,TeamHASHMEN> map = TeamHASHMENData.getMAP();
    private static int code;
    private static final String team1hash = map.get(TeamName.TEAM1).getHashtag();
    private static final String team2hash = map.get(TeamName.TEAM2).getHashtag();
    private static final String team1men = map.get(TeamName.TEAM1).getMention();
    private static final String team2men = map.get(TeamName.TEAM2).getMention();
    private static ResultSet res;
    private static String keywords = hash + team1hash + vs + team2hash + comma +
            hash + team2hash + vs + team1hash + comma +
            hash + team1hash + comma +
            hash + team2hash + comma +
            atTheRate + team1men + comma +
            atTheRate + team2men;


    public static void setDefaultFlumeConf(String sourceName,String channelName,String sinkName,String sinkType) {


        flumeDefault.setProperty(""+code+".sources",sourceName);
        flumeDefault.setProperty(""+code+".channels",channelName);
        flumeDefault.setProperty(""+code+".sinks",sinkName);

        flumeDefault.setProperty(""+code+".sources."+ sourceName +".type","sahil.jalan.flume.twitter.source.TwitterSource");
        flumeDefault.setProperty(""+code+".sources."+ sourceName +".channels","MemChannel");
        flumeDefault.setProperty(""+code+".sources."+ sourceName +".keywords", keywords);

        flumeDefault.setProperty(""+code+".sinks."+ sinkName +".channel",channelName);
        flumeDefault.setProperty(""+code+".sinks."+ sinkName +".type",sinkType);
        flumeDefault.setProperty(""+code+".sinks."+ sinkName +"."+ sinkType +".fileType","DataStream");
        flumeDefault.setProperty(""+code+".sinks."+ sinkName +"."+ sinkType +".writeFormat","Text");
        flumeDefault.setProperty(""+code+".sinks."+ sinkName +"."+ sinkType +".batchSize","1000");
        flumeDefault.setProperty(""+code+".sinks."+ sinkName +"."+ sinkType +".rollSize","0");
        flumeDefault.setProperty(""+code+".sinks."+ sinkName +"."+ sinkType +".rollCount","10000");

        flumeDefault.setProperty(""+code+".channels."+ channelName +".type","memory");
        flumeDefault.setProperty(""+code+".channels."+ channelName +".capacity","10000");
        flumeDefault.setProperty(""+code+".channels."+ channelName +".transactionCapacity","10000");


    }

    private static void setBattleCode() {

        try {
            res = query.executeQuery("select code from " + Constants.TeamCodeTable +
                    " where teambattle = '" + team1hash + "VS" + team2hash + "'");

            while (res.next()) {
                code = Integer.parseInt(res.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static String getTeamHash(){
        if(teamHash == 1){
            teamHash++;
            return team1hash;
        }
        else{
            teamHash--;
            return team2hash;
        }
    }

    public static String getTeamMen(){
        if(teamMen == 1){
            teamMen+=1;
            return team1men;
        }
        else{
            teamMen-=1;
            return team2men;
        }
    }


    public static final String getTeam1Hash(){
        return team1hash;
    }

    public static final String getTeam2Hash(){
        return team2hash;
    }

    public static final String getTeam1Men(){
        return team1men;
    }

    public static final String getTeam2Men(){
        return team2men;
    }

    public static final int getBattleCode(){
        if(isRunningFirstTime){
            setBattleCode();
        }
        return code;
    }
}
