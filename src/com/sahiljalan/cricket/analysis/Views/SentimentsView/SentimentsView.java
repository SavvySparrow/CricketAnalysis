package com.sahiljalan.cricket.analysis.Views.SentimentsView;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Main;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 29/4/17.
 */
public class SentimentsView {

    private Statement query = CricketAnalysis.getStatement();

    public SentimentsView(String TeamView1,String TeamView2) throws SQLException {

        System.out.println("\nSentiment Analysis Application is Started.");
        Constants.setsentimentView1(Constants.SentiViewT1V1);
        Constants.setsentimentView2(Constants.SentiViewT1V2);
        Constants.setsentimentView3(Constants.SentiViewT1V3);
        Constants.TeamSentiments(Constants.Team1Sentiments);
        Constants.setTeamView(TeamView1);
        Constants.setPosHype(Constants.PosHype1);
        createSentimentsView();
        System.out.println("Team 1 Analysis Completed..");

        Constants.setsentimentView1(Constants.SentiViewT2V1);
        Constants.setsentimentView2(Constants.SentiViewT2V2);
        Constants.setsentimentView3(Constants.SentiViewT2V3);
        Constants.TeamSentiments(Constants.Team2Sentiments);
        Constants.setTeamView(TeamView2);
        Constants.setPosHype(Constants.PosHype2);
        createSentimentsView();
        System.out.println("Team 2 Analysis Completed...");
    }

    private void createSentimentsView() throws SQLException {

        query.execute("create view "+Constants.SentimentView1+" as select rowid,text, words from " +
                Constants.TeamView +" lateral view explode(sentences(lower(text))) dummy as words");

        query.execute("create view "+Constants.SentimentView2+" as select rowid,text," +
                " word from "+Constants.SentimentView1+" lateral " +
                "view explode(words) dummy as word");

        query.execute("create view "+Constants.SentimentView3+" as select " +
                "rowid," +
                "c.word, " +
                "case d.polarity " +
                "   when 'negative' then -1" +
                "   when 'positive' then 1 " +
                "else 0 end as polarity " +
                "from "+Constants.SentimentView2+" c left outer join dictionary d on c.word = d.word");


        query.execute("create table "+Constants.TeamSentiments+" as select " +
                "rowid, " +
                "case " +
                "  when sum( polarity ) > 0 then 'positive' " +
                "  when sum( polarity ) < 0 then 'negative'  " +
                "else 'neutral' end as sentiment " +
                "from "+Constants.SentimentView3+" group by rowid");

        query.execute("create view "+Constants.PosHype+" as select rowid,sentiment from "+Constants.TeamSentiments+
        " where not sentiment = 'negative'");
    }
}
