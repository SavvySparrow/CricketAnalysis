package com.sahiljalan.cricket.analysis.views;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.MainCricket;

import java.sql.Statement;
import java.sql.SQLException;
/**
 * Created by sahiljalan on 28/4/17.
 */
public class RawViews {

    private Statement query = MainCricket.getStatement();

    public RawViews() throws SQLException{
        System.out.println("Running : RawViews class");
        createRawViews();
    }

    private void createRawViews() throws SQLException {

        System.out.println("Running : creating Raw Views");
        query.execute("drop view if exists " + Constants.Team1Hashtags);
        query.execute("drop view if exists " + Constants.Team2Hashtags);
        query.execute("drop view if exists " + Constants.Team1Mentions);
        query.execute("drop view if exists " + Constants.Team2Mentions);
        query.execute("drop view if exists " + Constants.Team1);
        query.execute("drop view if exists " + Constants.Team2);

        query.execute("create view if NOT exists " + Constants.Team1Hashtags + " " +
                "as select cast ( from_unixtime( unix_timestamp" +
                "(concat(substring(created_at,27,4),substring(created_at,4,15))," +
                "'yyyy MMM dd hh:mm:ss')) as timestamp) ts," +
                "user.screen_name,user.verified,created_at,user.time_zone,text " +
                "from " + Constants.TableName + " " +
                "lateral view explode(entities.hashtags.text) team1 as hashtags " +
                "where hashtags like 'KXIP'");

        query.execute("create view if NOT exists " + Constants.Team1Mentions + " " +
                "as select cast ( from_unixtime( unix_timestamp" +
                "(concat(substring(created_at,27,4),substring(created_at,4,15))," +
                "'yyyy MMM dd hh:mm:ss')) as timestamp) ts," +
                "user.screen_name,user.verified,created_at,user.time_zone,text " +
                "from " + Constants.TableName + " " +
                "lateral view explode(entities.user_mentions.screen_name) team1 as mentions " +
                "where mentions like 'lionsdenkxip'");

        query.execute("create view if NOT exists " + Constants.Team2Hashtags + " " +
                "as select cast ( from_unixtime( unix_timestamp" +
                "(concat(substring(created_at,27,4),substring(created_at,4,15))," +
                "'yyyy MMM dd hh:mm:ss')) as timestamp) ts," +
                "user.screen_name,user.verified,created_at,user.time_zone,text " +
                "from " + Constants.TableName + " " +
                "lateral view explode(entities.hashtags.text) team2 as hashtags " +
                "where hashtags like 'MI'");

        query.execute("create view if NOT exists " + Constants.Team2Mentions + " " +
                "as select cast ( from_unixtime( unix_timestamp" +
                "(concat(substring(created_at,27,4),substring(created_at,4,15))," +
                "'yyyy MMM dd hh:mm:ss')) as timestamp) ts," +
                "user.screen_name,user.verified,created_at,user.time_zone,text " +
                "from " + Constants.TableName + " " +
                "lateral view explode(entities.user_mentions.screen_name) team2 as mentions " +
                "where mentions like 'mipaltan'");

    }
}
