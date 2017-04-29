/*package com.sahiljalan.cricket.analysis;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import java.sql.Statement;


/**
 * Created by sahiljalan on 14/4/17.
 */
/*
public class Create {


    private static String serde = Constants.SerDeDriver;
    private static Statement query;
    private int max = 0;
    private String max_user_name, sql;
    private ResultSet res;
    String tableName = Constants.TableName;
    String team1_hashtags = Constants.Team1Hashtags;
    String team2_hashtags = Constants.Team2Hashtags;
    String team1_mentions = Constants.Team1Mentions;
    String team2_mentions = Constants.Team2Mentions;
    String team1 = Constants.TEAM1_VIEW;
    String team2 = Constants.TEAM2_VIEW;


    public static void main(String[] arg) throws SQLException, ClassNotFoundException {

        HiveConnection startHive = new HiveConnection();
        Create obj = new Create();

        query = startHive.getConnection().createStatement();
        query.execute("SET hive.support.sql11.reserved.keywords=false");
        //query.execute("SET hive.input.dir.recursive=true");
        //query.execute("SET hive.mapred.supports.subdirectories=true");
        //query.execute("SET hive.supports.subdirectories=true");
        //query.execute("SET mapred.input.dir.recursive=true");


        //TO GET CURRENT TIME
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("HH");
        int time = Integer.parseInt(sdf.format(cal.getTime())) - 1;
        System.out.println("Getting data for " + time + " to " + (time + 1) + " hours");



        obj.sql = "select Screen_name, count(Screen_name) from " + team1 +
                " where verified=FALSE" +
                " group by Screen_name" +
                " having count(Screen_name)>1";

        String result = obj.SabseBadaFan(tableName,obj.sql,team1);

        obj.sql = "select Screen_name, count(Screen_name) from " + team2 +
                " where verified=FALSE" +
                " group by Screen_name" +
                " having count(Screen_name)>1";


    }

    public String SabseBadaFan(String TableName, String sql, String TeamNo) throws SQLException {

        System.out.println("\nRunning: " + sql);
        res = query.executeQuery(sql);
        while (res.next()) {
            //System.out.println(res.getString(1) + "\t" + res.getString(2));

            if (Integer.parseInt(res.getString(2)) > max) {
                max = Integer.parseInt(res.getString(2));
                max_user_name = res.getString(1);
            }
        }
        sql = "Select Distinct user.name from " + TableName +
                " where user.Screen_name = '" + max_user_name + "'";
        System.out.println("\nRunning: " + sql);
        res = query.executeQuery(sql);
        while (res.next()) {
            String User_Name = res.getString(1);
            System.out.println("\n\n\t\t********Sabse Bada Fan " + TeamNo + " ********\n\n" +
                    User_Name + "\t\t" + max + " Tweets");
            /*sql = "Select text from " + tableName +
                    " where user.name = '"+ res.getString(1) +"'";
            res = query.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }*/
/*            return max_user_name + "\t" + User_Name + "\t" + max;
        }
        return null;
    }
*/
