package com.sahiljalan.cricket.analysis.ClearTraces;

import java.sql.SQLException;

/**
 * Created by sahiljalan on 5/5/17.
 */
public interface Tables {

    void clearAllTables() throws SQLException;
    void mainTable() throws SQLException;
    void dictionaryTable() throws SQLException;
    void timeZoneTable() throws SQLException;
    void teamData() throws SQLException;
}
