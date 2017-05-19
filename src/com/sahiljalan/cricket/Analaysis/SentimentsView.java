package com.sahiljalan.cricket.Analaysis;

import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 29/4/17.
 */
public class SentimentsView {

    private Statement query = CricketAnalysis.getStatement();

    public SentimentsView(String TeamView1,String TeamView2) throws SQLException {

        System.out.println("Running : "+ getClass());

        System.out.println("Team 1 Analysis Started..");
        Constants.setsentimentView1(Constants.SentiViewT1V1);
        Constants.setsentimentView2(Constants.SentiViewT1V2);
        Constants.setsentimentView3(Constants.SentiViewT1V3);
        Constants.TeamSentiments(Constants.Team1Sentiments);
        Constants.setTeamView(TeamView1);
        Constants.setPosHype(Constants.PosHype1);
        createSentimentsView();
        System.out.println("Team 1 Analysis Completed..");

        System.out.println("Team 2 Analysis Started..");
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

        System.out.println("Executing : Converting Tweets---->Sentences");
        query.execute("create view "+Constants.SentimentView1+" as select rowid,text, words from " +
                Constants.TeamView +" lateral view explode(sentences(lower(text))) dummy as words");

        System.out.println("Executing : Converting Sentences---->Words");
        query.execute("create view "+Constants.SentimentView2+" as select rowid,text," +
                " word from "+Constants.SentimentView1+" lateral " +
                "view explode(words) dummy as word");

        System.out.println("Executing : Checking Positivity And Negativity Of Each Word");
        query.execute("create view "+Constants.SentimentView3+" as select " +
                "rowid," +
                "c.word, " +
                "case d.polarity " +
                "   when 'negative' then -1" +
                "   when 'positive' then 1 " +
                "else 0 end as polarity " +
                "from "+Constants.SentimentView2+" c left outer join dictionary d on c.word = d.word");

        System.out.println("Executing : Converting words---->tweets");
        query.execute("create table "+Constants.TeamSentiments+" as select " +
                "rowid, " +
                "case " +
                "  when sum( polarity ) > 0 then 'positive' " +
                "  when sum( polarity ) < 0 then 'negative'  " +
                "else 'neutral' end as sentiment " +
                "from "+Constants.SentimentView3+" group by rowid");

        System.out.println("Executing : Filtering Negative Tweets");
        query.execute("create view "+Constants.PosHype+" as select rowid,sentiment from "+Constants.TeamSentiments+
        " where not sentiment = 'negative'");
    }
}
