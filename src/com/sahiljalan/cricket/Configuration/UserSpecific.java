package com.sahiljalan.cricket.Configuration;

import com.sahiljalan.cricket.Constants.Constants;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Vector;

/**
 * Created by sahiljalan on 18/5/17.
 */
public class UserSpecific {

    private static String sourceName = "Twitter";
    private static String channelName = "MemChannel";
    private static String sinkName = "HDFS";
    private static String sinkType = "hdfs";
    private static String CricketType;
    private int code = DefaultConf.getBattleCode();
    private static FlumeSortedProperties flume = new FlumeSortedProperties();
    OutputStream flumeConf = null;

    public UserSpecific(){
        setFlumeConfiguration();
    }

    private void setFlumeConfiguration() {

        try {

            flumeConf = new FileOutputStream(Constants.FLUME_CONFIG_FILE_LOCATION+"flume.conf");

            //set Properties values
            DefaultConf.setDefaultFlumeConf(sourceName, channelName, sinkName,sinkType);

            flume.setProperty(""+code+".sources."+ sourceName +".consumerKey",Constants.CONSUMER_KEY);
            flume.setProperty(""+code+".sources."+ sourceName +".consumerSecret",Constants.CONSUMER_SECRET);
            flume.setProperty(""+code+".sources."+ sourceName +".accessToken",Constants.ACCESS_TOKKEN);
            flume.setProperty(""+code+".sources."+ sourceName +".accessTokenSecret",Constants.ACCESS_TOKKEN_SECRET);


            flume.setProperty(""+code+".sinks."+ sinkName +"."+ sinkType +".path","" +
                    Constants.HDFS_SERVER_LOCATION+CricketType+"/"+code+"/year=%Y/month=%m/day=%d/hour=%H");

            //Save Properties
            flume.store(flumeConf,null);

        } catch (IOException e) {
            System.out.print("IOException :\n"+e);
        }
    }

    public static FlumeSortedProperties getFlumeConf(){
        return flume;
    }

    public static void setCricketType(String cType){
        CricketType = cType;
    }

    public static String getCricketType(){
        return CricketType;
    }

    public static void setConsumerKey(String consumerKey){
        Constants.CONSUMER_KEY = consumerKey;
    }

    public static void setConsumerSecret(String consumerSecret){
        Constants.CONSUMER_SECRET = consumerSecret;
    }

    public static void setAccessTokken(String accessTokken){
        Constants.ACCESS_TOKKEN = accessTokken;
    }

    public static void setAccessTokkenSecret(String accessTokkenSecret){
        Constants.ACCESS_TOKKEN_SECRET = accessTokkenSecret;
    }
}

class FlumeSortedProperties extends com.sahiljalan.cricket.Configuration.Properties {
    public Enumeration keys() {
        Enumeration keysEnum = super.keys();
        Vector<String> keyList = new Vector<String>();
        while (keysEnum.hasMoreElements()) {
            keyList.add((String) keysEnum.nextElement());
        }
        Collections.sort(keyList);
        return keyList.elements();
    }
}
