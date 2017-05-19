package com.sahiljalan.cricket.Views;

import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Team {

    private Statement query = CricketAnalysis.getStatement();

    public Team() throws SQLException {

        System.out.println("Running : "+ getClass());

        System.out.println("Executing : Team 1 Queries");
        Constants.setTeamHashtags(Constants.Team1Hashtags);
        Constants.setTeamMentions(Constants.Team1Mentions);
        Constants.setTeamView(Constants.TEAM1_VIEW);
        Constants.setTeamViewTable(Constants.TEAM1_VIEWTABLE);
        Constants.setTeamViewTemp(Constants.Team1_Temp);
        createTeamView();

        System.out.println("Executing : Team 2 Queries");
        Constants.setTeamHashtags(Constants.Team2Hashtags);
        Constants.setTeamMentions(Constants.Team2Mentions);
        Constants.setTeamView(Constants.TEAM2_VIEW);
        Constants.setTeamViewTable(Constants.TEAM2_VIEWTABLE);
        Constants.setTeamViewTemp(Constants.Team2_Temp);
        createTeamView();

        System.out.println();

    }

    private void createTeamView() {

        try{
            System.out.println("Executing : Merging Unique HashMen Data");
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

            System.out.println("Executing : Generating Country According to Specific TimeZone");
            query.execute("create view if NOT EXISTS "+Constants.TeamView +" as " +
                    "select cast(NumberRows(a.screen_name) as bigint) rowid,a.created_at,a.screen_name,a.verified,a.text,b.country from "+Constants.TeamViewTemp +
                    " a left join "+Constants.TimeZoneTable+" b on (a.time_zone=b.zone)");

            System.out.println("Executing : Creating Temporary Table" + Constants.TeamViewTable);
            query.execute("create table "+Constants.TeamViewTable+" as " +
                    "select * from "+Constants.TeamView);
        }catch (SQLException e){
            System.out.print("\nSQLException : "+e+"\n");
            try {
                new CricketAnalysis().startCleaningService("all");
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            System.exit(1);
        }


    }
}
