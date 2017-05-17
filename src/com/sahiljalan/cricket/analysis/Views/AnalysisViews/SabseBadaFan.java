package com.sahiljalan.cricket.analysis.Views.AnalysisViews;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.analysis.Services.CleanService;
import com.sun.org.apache.regexp.internal.RE;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 7/5/17.
 */
public class SabseBadaFan {

    private Statement query = CricketAnalysis.getStatement();
    private static int max = 0;
    private static String max_user_name = "";

    public SabseBadaFan(){

        System.out.println("\nTeam1 Fan Analaysing Started.");
        Constants.setSabseBadaFanTable(Constants.SABSE_BADA_FAN_TEAM_1);
        Constants.setTeamViewTable(Constants.TEAM1_VIEWTABLE);
        Constants.setPosHype(Constants.PosHype1);
        sabseBadaFan();
        Constants.setSBFMaxCount1(max);
        Constants.setSBFMaxCountUserName1(max_user_name);
        System.out.println("Team1 Fan Analaysing Completed...\n");

        System.out.println("Team2 Fan Analaysing Started.");
        Constants.setSabseBadaFanTable(Constants.SABSE_BADA_FAN_TEAM_2);
        Constants.setTeamViewTable(Constants.TEAM2_VIEWTABLE);
        Constants.setPosHype(Constants.PosHype2);
        sabseBadaFan();
        Constants.setSBFMaxCount2(max);
        Constants.setSBFMaxCountUserName2(max_user_name);
        System.out.println("Team2 Fan Analaysing Completed...\n");
    }

    private void sabseBadaFan() {

        try{
            query.execute("create table "+Constants.SabseBadaFan+" as " +
                    "select a.rowid,a.screen_name,a.verified,a.country,a.text from "+Constants.TeamViewTable +
                    " a inner join "+Constants.PosHype+" b on (a.rowid=b.rowid)");
            ResultSet res = query.executeQuery("select Screen_name, count(Screen_name) from " + Constants.SabseBadaFan +
                    " group by Screen_name" +
                    " having count(Screen_name)>1");

            if(res.next()){
                do {
                    if (Integer.parseInt(res.getString(2)) > max) {
                        max = Integer.parseInt(res.getString(2));
                        max_user_name = res.getString(1);
                    }
                }while (res.next());
            }else {
                max=0;
                max_user_name="N/A";
            }
            System.out.println("SBFMaxCountUserName : " +max_user_name+ "\t" + "SBFMaxCount : "+max);

        }catch (SQLException e){
            System.out.println("SQLException : "+e);
            try {
                new CricketAnalysis().startCleaningService();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            System.exit(1);
        }


    }
}
