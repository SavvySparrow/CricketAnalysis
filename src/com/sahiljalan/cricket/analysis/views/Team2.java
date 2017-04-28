package com.sahiljalan.cricket.analysis.views;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.MainCricket;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Team2 {

    private Statement query = MainCricket.getStatement();

    public Team2() throws SQLException {

        createTeam2View();

    }

    private void createTeam2View() throws SQLException {

        System.out.println("Running : creating " + Constants.Team2_Temp + " View");
        query.execute("create view if not EXISTS " + Constants.Team2_Temp + " as " +
                "select m.* from " + Constants.Team2Mentions +
                " m left join " + Constants.Team2Hashtags + " h on (m.text = h.text) where h.text is NULL " +
                "union all " +
                "select h.* from " + Constants.Team2Hashtags +
                " h left join " + Constants.Team2Mentions + " m on (h.text = m.text) where m.text is Null " +
                "union all " +
                "select h.* from " + Constants.Team2Hashtags +
                " h inner join " + Constants.Team2Mentions + " m on (h.text = m.text) " +
                "where ((h.created_at = m.created_at) AND (h.screen_name = m.screen_name))");

        System.out.println("Running : droping " + Constants.Team2 + " View IF EXISTS");
        query.execute("drop view if EXISTS " + Constants.Team2);

        System.out.println("Running : creating " + Constants.Team2 + " View");
        query.execute("create view if not EXISTS "+Constants.Team2+" as " +
                "select a.ts,a.screen_name,a.verified,a.text,b.country from "+Constants.Team2_Temp+
                " a left join "+Constants.TimeZoneTable+" b on (a.time_zone=b.zone)");

        System.out.println("Running : droping " + Constants.Team2_Temp + " View IF EXISTS");
        query.execute("drop view if EXISTS " + Constants.Team2_Temp);
    }
}
