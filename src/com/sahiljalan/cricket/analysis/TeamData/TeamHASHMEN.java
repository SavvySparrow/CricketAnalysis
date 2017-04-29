package com.sahiljalan.cricket.analysis.TeamData;

/**
 * Created by sahiljalan on 29/4/17.
 */
public class TeamHASHMEN {

    private int id;
    private String hashtag;
    private String mention;

    public TeamHASHMEN(){

    }
    public TeamHASHMEN(int id,String hashtag,String mention){

        this.id = id;
        this.hashtag = hashtag;
        this.mention = mention;
    }

    public int getId(){
        return id;
    }

    public String getHashtag() {
        return hashtag;
    }

    public String getMention(){
        return mention;
    }
}
