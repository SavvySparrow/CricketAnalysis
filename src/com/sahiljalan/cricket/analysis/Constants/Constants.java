package com.sahiljalan.cricket.analysis.Constants;

/**
 * Created by sahiljalan on 27/4/17.
 */
public class Constants {

    public static final String SerDeDriver = "org.openx.data.jsonserde.JsonSerDe";
    public static final String HiveDriver = "org.apache.hive.jdbc.HiveDriver";
    public static final String Team1Hashtags = "Team1_hashtagsView";
    public static final String Team2Hashtags = "Team2_hashtagsView";
    public static final String Team1Mentions = "Team1_mentionsView";
    public static final String Team2Mentions = "Team2_mentionsView";
    public static final String Team1_Temp = "Team1_Temp";
    public static final String TEAM1_VIEW = "Team1View";
    public static final String Team2_Temp = "Team2_Temp";
    public static final String TEAM2_VIEW = "Team2View";

    public static final String Prefix_Location = "hdfs://localhost:9000/IPL";
    public static String Postfix_Location = "/KXIPvsMI/year=2017/month=04/day=20/";

    public static String TableName = "Match_Buzz_Default";
    public static String DictionaryTable = "dictionary";
    public static String TimeZoneTable = "timezone";
    public static String DataBaseName = "Project_Cricket_Defualt";

    public static String TeamHashtags = "TeamHashtags_Sample";
    public static String TeamMentions = "TeamMentions_Sample";
    public static String TeamView = "Team_Sample";
    public static String TeamViewTemp = "Team_Temp_Sample";

    public static String Team1Sentiments = "Team1_Sentiments";
    public static String Team2Sentiments = "Team2_Sentiments";
    public static String TeamSentiments = "TeamSentiments_Sample";

    public static String SentiViewT1V1 = "Sentiment_View_T1_V1";
    public static String SentiViewT1V2 = "Sentiment_View_T1_V2";
    public static String SentiViewT1V3 = "Sentiment_View_T1_V3";
    public static String SentiViewT2V1 = "Sentiment_View_T2_V1";
    public static String SentiViewT2V2 = "Sentiment_View_T2_V2";
    public static String SentiViewT2V3 = "Sentiment_View_T2_V3";

    public static String SentimentView1 = "SentimentView1_Sample";
    public static String SentimentView2 = "SentimentView2_Sample";
    public static String SentimentView3 = "SentimentView3_Sample";

    public static String DictionaryLocation = "/home/sahiljalan/IdeaProjects/CricketAnalysis/data/dictionary.tsv";
    public static String TimeZoneLocation = "/home/sahiljalan/IdeaProjects/CricketAnalysis/data/timezone.tsv";


    public static void setTableName(String TBName){
        TableName = TBName;
    }

    public static void setDBName(String DBName){
        DataBaseName = DBName;
    }

    public static void setTeamHashtags(String THName){
        TeamHashtags = THName;
    }

    public static void setTeamMentions(String TMName){
        TeamMentions = TMName;
    }

    public static void setTeamView(String TName){
        TeamView = TName;
    }

    public static void setTeamViewTemp(String TTame){
        TeamViewTemp = TTame;
    }


    public static void setLocation(String Teams, String year,String month,String day){
        Postfix_Location = "/"+Teams+"/year="+year+"/month="+month+"/day="+day;
    }

    public static void setsentimentView1(String SVName) {
        SentimentView1 = SVName;
    }

    public static void setsentimentView2(String SVName) {
        SentimentView2 = SVName;
    }

    public static void setsentimentView3(String SVName) {
        SentimentView3 = SVName;
    }

    public static void TeamSentiments(String TSName) {
        TeamSentiments = TSName;
    }
}