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
        Constants.setPosHype(Constants.PosHype1);
        createSentimentsView();

        Constants.setsentimentView1(Constants.SentiViewT2V1);
        Constants.setsentimentView2(Constants.SentiViewT2V2);
        Constants.setsentimentView3(Constants.SentiViewT2V3);
        Constants.TeamSentiments(Constants.Team2Sentiments);
        Constants.setTeamView(Constants.TEAM2_VIEW);
        Constants.setPosHype(Constants.PosHype2);
        createSentimentsView();

    }

    private void createSentimentsView() throws SQLException {

        query.execute("drop view if EXISTS " + Constants.SentimentView1);
        query.execute("drop view if EXISTS " + Constants.SentimentView2);
        query.execute("drop view if EXISTS " + Constants.SentimentView3);


        query.execute("create view "+Constants.SentimentView1+" as select rowid," +
                " words from " +Constants.TeamView +" lateral " +
                "view explode(sentences(lower(text))) dummy as words");

        query.execute("create view "+Constants.SentimentView2+" as select rowid," +
                " word from "+Constants.SentimentView1+" lateral " +
                "view explode(words) dummy as word");

        query.execute("create view "+Constants.SentimentView3+" as select rowid," +
                "c.word, " +
                "case d.polarity " +
                "   when 'negative' then -1" +
                "   when 'positive' then 1 " +
                "else 0 end as polarity " +
                "from "+Constants.SentimentView2+" c left outer join dictionary d on c.word = d.word");


        query.execute("drop table if EXISTS " + Constants.TeamSentiments);

        
        query.execute("create table "+Constants.TeamSentiments+" as select " +
                "rowid, " +
                "case " +
                "  when sum( polarity ) > 0 then 'positive' " +
                "  when sum( polarity ) < 0 then 'negative'  " +
                "else 'neutral' end as sentiment " +
                "from "+Constants.SentimentView3+" group by rowid");

        query.execute("drop view if EXISTS " + Constants.PosHype);

        query.execute("create view "+Constants.PosHype+" as select rowid,sentiment from "+Constants.TeamSentiments+
        " where not sentiment = 'negative'");


    }
}
