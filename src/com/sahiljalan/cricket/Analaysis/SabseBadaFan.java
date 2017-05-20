package com.sahiljalan.cricket.Analaysis;

import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.Services.HiveConnectionService;
import com.sahiljalan.cricket.Services.PreProcessingQueriesService;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 7/5/17.
 */
public class SabseBadaFan {

    private Statement query = PreProcessingQueriesService.getStatement();
    private static int max = 0;
    private int Team = 1;
    private static String max_user_name = "";
    private static boolean isTieTeam1,isTieTeam2;

    public SabseBadaFan(){

        System.out.println("Running : "+ getClass());

        System.out.println("Team 1 Fan Analaysing Started..");
        Constants.setSabseBadaFanTable(Constants.SABSE_BADA_FAN_TEAM_1);
        Constants.setTeamViewTable(Constants.TEAM1_VIEWTABLE);
        Constants.setPosHype(Constants.PosHype1);
        sabseBadaFan();
        Constants.setSBFMaxCount1(max);resetMax();
        Constants.setSBFMaxCountUserName1(max_user_name);
        System.out.println("Team 1 Fan Analaysing Completed..");

        System.out.println("Team 2 Fan Analaysing Started...");
        Constants.setSabseBadaFanTable(Constants.SABSE_BADA_FAN_TEAM_2);
        Constants.setTeamViewTable(Constants.TEAM2_VIEWTABLE);
        Constants.setPosHype(Constants.PosHype2);
        sabseBadaFan();
        Constants.setSBFMaxCount2(max);resetMax();
        Constants.setSBFMaxCountUserName2(max_user_name);
        System.out.println("Team 2 Fan Analaysing Completed...");
    }

    private void resetMax() {
        max=0;
    }

    private void sabseBadaFan() {

        try{
            System.out.println("Executing : Fetching Specific Needed Columns");
            query.execute("create table "+Constants.SabseBadaFan+" as " +
                    "select a.rowid,a.screen_name,a.verified,a.country,a.text from "+Constants.TeamViewTable +
                    " a inner join "+Constants.PosHype+" b on (a.rowid=b.rowid)");

            System.out.println("Executing : Searching Sabse Bada Fan (Highest Positive Tweets Fan)");
            ResultSet res = query.executeQuery("select Screen_name, count(Screen_name) from " + Constants.SabseBadaFan +
                    " group by Screen_name" +
                    " having count(Screen_name)>1");

            if(res.next()){
                do {
                    if(Integer.parseInt(res.getString(2)) != max){
                        if (Integer.parseInt(res.getString(2)) > max) {
                            max = Integer.parseInt(res.getString(2));
                            max_user_name = res.getString(1);
                            setTie(false);
                        }
                    }else{
                        max_user_name = "Tie";
                        setTie(true);
                    }

                }while (res.next());
            }else {
                max=0;
                max_user_name="N/A";
                setTie(false);
            }
            System.out.println("SBFMaxCountUserName : " +max_user_name+ "\t" + "SBFMaxCount : "+max);

        }catch (SQLException e){
            System.out.println("SQLException : "+e);
            try {
                new CricketAnalysis().startCleaningService("all");
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            System.exit(1);
        }finally {
            Team++;
        }


    }

    public static Boolean isTie(int team){

        if(team==1)
            return isTieTeam1;
        else
            return isTieTeam2;

    }

    public void setTie(boolean tie) {

        if(Team==1)
            isTieTeam1 = tie;
        else
            isTieTeam2 = tie;

    }
}
