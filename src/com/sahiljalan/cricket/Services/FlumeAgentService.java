package com.sahiljalan.cricket.Services;

import com.sahiljalan.cricket.Configuration.DefaultConf;
import org.apache.flume.node.Application;
import org.apache.log4j.BasicConfigurator;

import java.util.concurrent.CountDownLatch;

import static com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis.checkFlumeConfiguration;
import static com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis.generateFlumeConfigurationFile;
import static com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis.startCleaningOnly;

/**
 * Created by sahiljalan on 20/5/17.
 */
public class FlumeAgentService extends BaseHealthChecker {

    private Boolean flag=false;
    public FlumeAgentService(CountDownLatch latch){
        super("Flume Agent Service",latch);
    }

    @Override
    public void verifyService() throws Exception{

        try{
            if(!startCleaningOnly) {
                Thread.sleep(6000);
                /** Generate Flume Configuration File Using Defined Properties */
                generateFlumeConfigurationFile();

                Thread.sleep(1000);
                if (checkFlumeConfiguration()) {
                    System.out.println("Starting " + this.getServiceName());
                    String[] args1 = new String[]{"agent", "-n" + DefaultConf.getBattleCode(), "-f/home/sahiljalan/flume/conf/flume.conf"};
                    //BasicConfigurator.configure();
                    Application.main(args1);
                    System.setProperty("hadoop.home.dir", "/");
                    System.out.println(this.getServiceName() + " is UP");
                    System.out.println("\nPlease wait for few minute for fetching " +
                            "some data from the twitter server for the first run");
                    for (int i = 1; i <= 80; i++) {
                        System.out.print(".");
                        Thread.sleep(1000);
                    }
                } else {
                    System.out.println(this.getServiceName() + " is Down");
                    System.out.println("Reason : Configuration File is Not Set");
                    flag = true;
                }
            }
        }catch (Exception e){
            System.out.println(e);
        }
        if(flag)
            throw new Exception();

    }
}
