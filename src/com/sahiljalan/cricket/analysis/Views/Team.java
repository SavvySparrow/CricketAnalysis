package com.sahiljalan.cricket.analysis.Views;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Main;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Team {

    private Statement query = Main.getStatement();

    public Team() throws SQLException {

        Constants.setTeamHashtags(Constants.Team1Hashtags);
        Constants.setTeamMentions(Constants.Team1Mentions);
        Constants.setTeamView(Constants.TEAM1_VIEW);
        Constants.setTeamViewTemp(Constants.Team1_Temp);
        createTeamView();
        Constants.setTeamHashtags(Constants.Team2Hashtags);
        Constants.setTeamMentions(Constants.Team2Mentions);
        Constants.setTeamView(Constants.TEAM2_VIEW);
        Constants.setTeamViewTemp(Constants.Team2_Temp);
        createTeamView();

    }

    private void createTeamView() throws SQLException {

        System.out.println("Running : droping " + Constants.TeamViewTemp + " View IF EXISTS");
        query.execute("drop view if EXISTS " + Constants.TeamViewTemp);

        System.out.println("Running : creating " + Constants.TeamViewTemp + " View");
        query.execute("create view if NOT EXISTS " + Constants.TeamViewTemp + " as " +
                "select m.* from " + Constants.TeamMentions +
                " m left join " + Constants.TeamHashtags + " h on (m.text = h.text) where h.text is NULL " +
                "union all " +
                "select h.* from " + Constants.TeamHashtags +
                " h left join " + Constants.TeamMentions + " m on (h.text = m.text) where m.text is Null " +
                "union all " +
                "select h.* from " + Constants.TeamHashtags +
                " h inner join " + Constants.TeamMentions + " m on (h.text = m.text) " +
                "where ((h.created_at = m.created_at) AND (h.screen_name = m.screen_name))");

        System.out.println("Running : droping " + Constants.TeamView + " View IF EXISTS");
        query.execute("drop view if EXISTS " + Constants.TeamView);

        System.out.println("Running : creating " + Constants.TeamView + " View");
        query.execute("create view if not EXISTS "+Constants.TeamView +" as " +
                "select a.ts,a.created_at,a.screen_name,a.verified,a.text,b.country from "+Constants.TeamViewTemp +
                " a left join "+Constants.TimeZoneTable+" b on (a.time_zone=b.zone)");


    }
}
