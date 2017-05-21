package com.sahiljalan.cricket.Tables;

import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;
import com.sahiljalan.cricket.Constants.Constants;
import com.sahiljalan.cricket.Services.CleanTraces.Records;
import com.sahiljalan.cricket.Services.HiveConnectionService;
import com.sahiljalan.cricket.Services.PreProcessingQueriesService;
import com.sahiljalan.cricket.TeamData.TeamCodes;

import java.sql.SQLException;
import java.sql.Statement;

import static com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis.getCurrentTimeStamp;

/**
 * Created by sahiljalan on 28/4/17.
 */
public class Dictionary{

    private Statement query = PreProcessingQueriesService.getStatement();

    public Dictionary() throws SQLException {
        //First it calls the default constructor of TeamCodes than create Dictionary
        System.out.println(getCurrentTimeStamp()+"Running : "+ getClass());
        createDictionaryTable();
        System.out.println();
    }

    private void createDictionaryTable() throws SQLException {

        System.out.println("Executing : Creating Dictionary Table IF NOT EXISTS");
        query.execute("CREATE EXTERNAL TABLE IF NOT EXISTS "+ Constants.DictionaryTable + "(" +
                "type string," +
                "length int," +
                "word string," +
                "pos string," +
                "stemmed string," +
                "polarity string" +
                ")" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ");

            System.out.println("Executing : Loading Dictionary Data");
            query.execute("LOAD DATA LOCAL INPATH '"+Constants.DictionaryLocation+"' OVERWRITE INTO TABLE " +
                    Constants.DictionaryTable);

    }
}
