package com.sahiljalan.cricket.Views;

import com.sahiljalan.cricket.Configuration.DefaultConf;
import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.Constants.TeamName;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.TeamData.TeamHASHMEN;
import com.sahiljalan.cricket.TeamData.TeamHASHMENData;

import java.sql.Statement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class RawViews {

    private Statement query = CricketAnalysis.getStatement();

    public RawViews() throws SQLException{

        System.out.println("Running : "+ getClass());

        System.out.println("Executing : Filtering Team 1 Data");
        setRawViews("Team1");
        System.out.println("Executing : Filtering Team 2 Data");
        setRawViews("Team2");

        System.out.println();

    }

    private void setRawViews(String Team) throws SQLException {

        if(Team.contentEquals("Team1")){
            Constants.setTeamHashtags(Constants.Team1Hashtags);
            Constants.setTeamMentions(Constants.Team1Mentions);
        }else{
            Constants.setTeamHashtags(Constants.Team2Hashtags);
            Constants.setTeamMentions(Constants.Team2Mentions);
        }
        createRawViews();
    }

    private void createRawViews() throws SQLException {

        System.out.println("Filtering HashTags Data..");
        query.execute("create view if NOT exists " + Constants.TeamHashtags + " " +
                "as select " +
                "user.screen_name,user.verified,created_at,user.time_zone,text " +
                "from " + Constants.TableName + " " +
                "lateral view explode(entities.hashtags.text) team1 as hashtags " +
                "where hashtags like '"+DefaultConf.getTeamHash()+"'");

        System.out.println("Filtering Mentions Data..");
        query.execute("create view if NOT exists " + Constants.TeamMentions + " " +
                "as select " +
                "user.screen_name,user.verified,created_at,user.time_zone,text " +
                "from " + Constants.TableName + " " +
                "lateral view explode(entities.user_mentions.screen_name) team1 as mentions " +
                "where mentions like '"+DefaultConf.getTeamMen()+"'");

    }
}
