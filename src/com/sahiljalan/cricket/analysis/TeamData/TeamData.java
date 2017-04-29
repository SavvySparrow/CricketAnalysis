package com.sahiljalan.cricket.analysis.TeamData;

import com.sahiljalan.cricket.analysis.Constants.TeamName;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sahiljalan on 29/4/17.
 */
public class TeamData {

    private static Map<String,TeamHASHMEN> map= new HashMap<>();

    public TeamData(){
        initializeTeamData();
    }

    private void initializeTeamData() {

        TeamHASHMEN punjabHM = new TeamHASHMEN(1,"KXIP","lionsdenkxip");
        TeamHASHMEN kolkataHM = new TeamHASHMEN(2,"KKR","KKRiders");
        TeamHASHMEN gujratHM = new TeamHASHMEN(3,"GL","TheGujaratLions");
        TeamHASHMEN puneHM = new TeamHASHMEN(4,"RPS","RPSupergiants");
        TeamHASHMEN mumbaiHM = new TeamHASHMEN(5,"MI","mipaltan");
        TeamHASHMEN hyderabadHM = new TeamHASHMEN(6,"SRH","SunRisers");
        TeamHASHMEN bangaluruHM = new TeamHASHMEN(7,"RCB","RCBTweets");
        TeamHASHMEN delhiHM = new TeamHASHMEN(8,"DD","DelhiDaredevils");

        map.put(TeamName.PUNJAB,punjabHM);
        map.put(TeamName.KOLKATA,kolkataHM);
        map.put(TeamName.GUJRAT,gujratHM);
        map.put(TeamName.PUNE,puneHM);
        map.put(TeamName.MUMBAI,mumbaiHM);
        map.put(TeamName.HYDERABAD,hyderabadHM);
        map.put(TeamName.BANGALURU,bangaluruHM);
        map.put(TeamName.DELHI,delhiHM);
    }

    public static Map<String,TeamHASHMEN> getMAP(){
        return map;
    }
}


