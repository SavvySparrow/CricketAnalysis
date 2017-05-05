package com.sahiljalan.cricket.analysis.ClearTraces;

import com.sahiljalan.cricket.analysis.Constants.Constants;

import java.sql.SQLException;

/**
 * Created by sahiljalan on 5/5/17.
 */
public interface Views {

    void clearAllViews() throws SQLException;
    void clearRawViews() throws SQLException;
    void clearTeamViews() throws SQLException;
    void clearSentimentsView() throws SQLException;
}
