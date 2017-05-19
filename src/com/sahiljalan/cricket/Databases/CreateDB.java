package com.sahiljalan.cricket.Databases;

import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class CreateDB {

    private Statement query = CricketAnalysis.getStatement();

    public CreateDB() throws SQLException {
        query.execute("create database if Not EXISTS "+ Constants.DataBaseName);
        query.execute("create database if not EXISTS "+ Constants.DataBaseAnalaysedResults);
        useDB();
    }

    public CreateDB(String DBName) throws SQLException {
        Constants.setDBName(DBName);
        query.execute("create database if Not EXISTS "+Constants.DataBaseName);
        query.execute("create database if not EXISTS "+ Constants.DataBaseAnalaysedResults);
        useDB();
    }

    private void useDB() throws SQLException {
        query.execute("use "+Constants.DataBaseName);
    }
}
