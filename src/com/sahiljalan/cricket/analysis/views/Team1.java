package com.sahiljalan.cricket.analysis.views;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.MainCricket;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Team1 {

    private Statement query = MainCricket.getStatement();

    public Team1() throws SQLException {

        createTeam1View();

    }

    private void createTeam1View() throws SQLException {

        System.out.println("Running : creating " + Constants.Team1_Temp + " View");
        query.execute("create view if NOT EXISTS " + Constants.Team1_Temp + " as " +
                "select m.* from " + Constants.Team1Mentions +
                " m left join " + Constants.Team1Hashtags + " h on (m.text = h.text) where h.text is NULL " +
                "union all " +
                "select h.* from " + Constants.Team1Hashtags +
                " h left join " + Constants.Team1Mentions + " m on (h.text = m.text) where m.text is Null " +
                "union all " +
                "select h.* from " + Constants.Team1Hashtags +
                " h inner join " + Constants.Team1Mentions + " m on (h.text = m.text) " +
                "where ((h.created_at = m.created_at) AND (h.screen_name = m.screen_name))");

        System.out.println("Running : droping " + Constants.Team1 + " View IF EXISTS");
        query.execute("drop view if EXISTS " + Constants.Team1);

        System.out.println("Running : creating " + Constants.Team1 + " View");
        query.execute("create view if not EXISTS "+Constants.Team1+" as " +
                "select a.ts,a.screen_name,a.verified,a.text,b.country from "+Constants.Team1_Temp+
                " a left join "+Constants.TimeZoneTable+" b on (a.time_zone=b.zone)");

        System.out.println("Running : droping " + Constants.Team1_Temp + " View IF EXISTS");
        query.execute("drop view if EXISTS " + Constants.Team1_Temp);
    }
}
