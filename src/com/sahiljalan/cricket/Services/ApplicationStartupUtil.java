package com.sahiljalan.cricket.Services;

import com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis.closeConnection;
import static com.sahiljalan.cricket.CricketAnalysis.CricketAnalysis.startCleaningOnly;

/**
 * Created by sahiljalan on 20/5/17.
 */
public class ApplicationStartupUtil
{
    //List of service checkers
    private static List<BaseHealthChecker> _services;

    static ExecutorService executorService;

    //This latch will be used to wait on
    private static CountDownLatch _latch;

    private ApplicationStartupUtil()
    {
    }

    private final static ApplicationStartupUtil INSTANCE = new ApplicationStartupUtil();

    public static ApplicationStartupUtil getInstance()
    {
        return INSTANCE;
    }

    public static boolean startStartupServices() throws Exception
    {
        //Initialize the latch with number of service checkers
        _latch = new CountDownLatch(3);

        //All add checker in lists
        _services = new ArrayList<>();
        _services.add(new HiveConnectionService(_latch));
        _services.add(new PreProcessingQueriesService(_latch));
        _services.add(new FlumeAgentService(_latch));
        //Start service checkers using executor framework
        executorService = Executors.newFixedThreadPool(_services.size());

        for(final BaseHealthChecker v : _services)
        {
            executorService.submit(v);
        }

        //Now wait till all services are checked
        _latch.await();

        //Services are file and now proceed startup
        for(final BaseHealthChecker v : _services)
        {
            if( ! v.isServiceUp())
            {
                return false;
            }
        }
        return true;


    }

    public static void shutdownStartupServices() {
        executorService.shutdown();// Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executorService.awaitTermination(50, TimeUnit.SECONDS)) {
                executorService.shutdownNow();// Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!executorService.awaitTermination(50, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
        try {
            closeConnection();
        } catch (SQLException e) {
            System.out.println("There is NO Connection Instance To Close");
        }
        System.out.println("Stopping All Services \nPlease wait....");
        System.exit(1);
    }


}
