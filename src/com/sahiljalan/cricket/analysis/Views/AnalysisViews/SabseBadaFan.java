package com.sahiljalan.cricket.analysis.Views.AnalysisViews;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.CricketAnalysis.CricketAnalysis;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 7/5/17.
 */
public class SabseBadaFan {

    private Statement query = CricketAnalysis.getStatement();

    public SabseBadaFan(){

        sabseBadaFan();
    }

    private void sabseBadaFan() {



        String sql = "select Screen_name, count(Screen_name) from " + Constants.PosHype1 +
                " where verified=FALSE" +
                " group by Screen_name" +
                " having count(Screen_name)>1";
        try{
            query.execute("select view sabseBadaFanView as " +
                    "select a.rowid,a.screen_name,a.verified,a.country from "+Constants.Team1_Temp+" a" +
                    "left join "+Constants.PosHype1+" b on a.rowid=b.rowid");
            query.execute(sql);
        }catch (SQLException e){
            System.out.println("SQLException : "+e);
        }


    }
}
