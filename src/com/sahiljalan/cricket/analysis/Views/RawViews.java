package com.sahiljalan.cricket.analysis.Views;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Constants.TeamName;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.TeamData.TeamHASHMEN;
import com.sahiljalan.cricket.analysis.TeamData.TeamHASHMENData;

import java.sql.Statement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class RawViews {

    private int count=1;
    private TeamHASHMEN value;
    private Statement query = CricketAnalysis.getStatement();
    private Map<String,TeamHASHMEN> map = TeamHASHMENData.getMAP();

    public RawViews() throws SQLException{
        System.out.println("Running : RawViews class");

        Constants.setTeamHashtags(Constants.Team1Hashtags);
        Constants.setTeamMentions(Constants.Team1Mentions);
        value = map.get(TeamName.TEAM1);
        createRawViews(value);

        Constants.setTeamHashtags(Constants.Team2Hashtags);
        Constants.setTeamMentions(Constants.Team2Mentions);
        value = map.get(TeamName.TEAM2);
        createRawViews(value);
    }

    private void createRawViews(TeamHASHMEN value) throws SQLException {

        query.execute("create view if NOT exists " + Constants.TeamHashtags + " " +
                "as select cast ( from_unixtime( unix_timestamp" +
                "(concat(substring(created_at,27,4),substring(created_at,4,15))," +
                "'yyyy MMM dd hh:mm:ss')) as timestamp) ts," +
                "user.screen_name,user.verified,created_at,user.time_zone,text " +
                "from " + Constants.TableName + " " +
                "lateral view explode(entities.hashtags.text) team1 as hashtags " +
                "where hashtags like '"+value.getHashtag()+"'");

        query.execute("create view if NOT exists " + Constants.TeamMentions + " " +
                "as select cast ( from_unixtime( unix_timestamp" +
                "(concat(substring(created_at,27,4),substring(created_at,4,15))," +
                "'yyyy MMM dd hh:mm:ss')) as timestamp) ts," +
                "user.screen_name,user.verified,created_at,user.time_zone,text " +
                "from " + Constants.TableName + " " +
                "lateral view explode(entities.user_mentions.screen_name) team1 as mentions " +
                "where mentions like '"+value.getMention()+"'");

    }
}
