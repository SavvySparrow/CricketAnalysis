package com.sahiljalan.cricket.Services;

import java.util.concurrent.CountDownLatch;

/**
 * Created by sahiljalan on 20/5/17.
 */
public abstract class BaseHealthChecker implements Runnable {

    private CountDownLatch latch;
    private String _serviceName;
    private boolean _serviceUp;

    //Get latch object in constructor so that after completing the task, thread can countDown() the latch
    public BaseHealthChecker(String serviceName, CountDownLatch latch)
    {
        super();
        this.latch = latch;
        this._serviceName = serviceName;
        this._serviceUp = false;
    }

    @Override
    public void run() {
        try {
            verifyService();
            _serviceUp = true;
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            _serviceUp = false;
        } finally {
            latch.countDown();
        }
    }

    public String getServiceName() {
        return _serviceName;
    }

    public boolean isServiceUp() {
        return _serviceUp;
    }
    //This method needs to be implemented by all specific service checker
    public abstract void verifyService() throws Exception;
}
