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
    public static final String Team1 = "Team1View";
    public static final String Team2_Temp = "Team2_Temp";
    public static final String Team2 = "Team2View";

    public static final String Prefix_Location = "hdfs://localhost:9000/IPL";
    public static String Postfix_Location = "/KXIPvsMI/year=2017/month=04/day=20/";

    public static String TableName = "Match_Buzz";
    public static String DictionaryTable = "dictionary";
    public static String TimeZoneTable = "timezone";
    public static String DataBaseName = "ProjectCricket";

    public static String DictionaryLocation = "/home/sahiljalan/IdeaProjects/CricketAnalysis/data/dictionary.tsv";
    public static String TimeZoneLocation = "/home/sahiljalan/IdeaProjects/CricketAnalysis/data/timezone.tsv";


    public static void setTableName(String TBName){
        TableName = TBName;
    }

    public static void setDBName(String DBName){
        DataBaseName = DBName;
    }

    public static void setLocation(String Teams, String year,String month,String day){
        Postfix_Location = "/"+Teams+"/year="+year+"/month="+month+"/day="+day;
    }
}