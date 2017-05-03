package com.sahiljalan.cricket.analysis.Tables;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.Main;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 2/5/17.
 */
public class CompareTeamPosHype {

    private Statement query = Main.getStatement();

    public CompareTeamPosHype() throws SQLException {

        query.execute("drop table if EXISTS poshype");

        query.execute("create table poshype as " +
                "select count(a.rowid) Team1,count(b.rowid) Team2 from "+Constants.PosHype1+" a " +
                "full outer join " +Constants.PosHype2+" b");
    }
}
