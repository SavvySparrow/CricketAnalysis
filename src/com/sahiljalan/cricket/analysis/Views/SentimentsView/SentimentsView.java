package com.sahiljalan.cricket.analysis.Views.SentimentsView;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Main;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 29/4/17.
 */
public class SentimentsView {

    private Statement query = Main.getStatement();

    public SentimentsView() throws SQLException {

        Constants.setsentimentView1(Constants.SentiViewT1V1);
        Constants.setsentimentView2(Constants.SentiViewT1V2);
        Constants.setsentimentView3(Constants.SentiViewT1V3);
        Constants.TeamSentiments(Constants.Team1Sentiments);
        Constants.setTeamView(Constants.TEAM1_VIEW);
        createSentimentsView();

        Constants.setsentimentView1(Constants.SentiViewT2V1);
        Constants.setsentimentView2(Constants.SentiViewT2V2);
        Constants.setsentimentView3(Constants.SentiViewT2V3);
        Constants.TeamSentiments(Constants.Team2Sentiments);
        Constants.setTeamView(Constants.TEAM2_VIEW);
        createSentimentsView();

    }

    private void createSentimentsView() throws SQLException {

        query.execute("drop view if EXISTS " + Constants.SentimentView1);
        query.execute("drop view if EXISTS " + Constants.SentimentView2);
        query.execute("drop view if EXISTS " + Constants.SentimentView3);


        query.execute("create view "+Constants.SentimentView1+" as select text,verified,created_at,screen_name, words from " +
                Constants.TeamView +" lateral view explode(sentences(lower(text))) dummy as words");

        query.execute("create view "+Constants.SentimentView2+" as select text,verified,created_at," +
                "screen_name, word from "+Constants.SentimentView1+" lateral " +
                "view explode(words) dummy as word");

        query.execute("create view "+Constants.SentimentView3+" as select " +
                "text,created_at,screen_name,verified," +
                "c.word, " +
                "case d.polarity " +
                "   when 'negative' then -1" +
                "   when 'positive' then 1 " +
                "else 0 end as polarity " +
                "from "+Constants.SentimentView2+" c left outer join dictionary d on c.word = d.word");


        query.execute("drop table if EXISTS " + Constants.TeamSentiments);


        /* TODO Grouping is not accurate right now , need to find out another way of grouping */
        query.execute("create table "+Constants.TeamSentiments+" as select " +
                "screen_name, " +
                "case " +
                "  when sum( polarity ) > 0 then 'positive' " +
                "  when sum( polarity ) < 0 then 'negative'  " +
                "else 'neutral' end as sentiment " +
                "from "+Constants.SentimentView3+" group by created_at,screen_name,text");
    }
}
