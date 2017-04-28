package com.sahiljalan.cricket.analysis.Databases;

import com.sahiljalan.cricket.analysis.Constants.Constants;
import com.sahiljalan.cricket.analysis.MainCricket;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class CreateDB {

    private Statement query = MainCricket.getStatement();

    public CreateDB() throws SQLException {
        query.execute("create database if Not EXISTS "+ Constants.DataBaseName);
        userDB();
    }

    public CreateDB(String DBName) throws SQLException {
        Constants.setDBName(DBName);
        query.execute("create database if Not EXISTS "+Constants.DataBaseName);
        userDB();
    }

    private void userDB() throws SQLException {
        query.execute("use "+Constants.DataBaseName);
    }
}
