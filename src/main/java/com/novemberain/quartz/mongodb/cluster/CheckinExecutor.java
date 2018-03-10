package com.novemberain.quartz.mongodb.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CheckinExecutor {

    private static final Logger log = LoggerFactory.getLogger(CheckinExecutor.class);

    // Arbitrary value:
    private static final int INITIAL_DELAY = 0;
    private final Runnable checkinTask;
    private final long checkinIntervalMillis;
    private final String instanceId;

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    public CheckinExecutor(Runnable checkinTask, long checkinIntervalMillis, String instanceId) {
        this.checkinTask = checkinTask;
        this.checkinIntervalMillis = checkinIntervalMillis;
        this.instanceId = instanceId;
    }

    /**
     * Start execution of CheckinTask.
     */
    public void start() {
        log.info("Starting check-in task for scheduler instance: {}", instanceId);
        executor.scheduleAtFixedRate(checkinTask, INITIAL_DELAY, checkinIntervalMillis, MILLISECONDS);
    }

    /**
     * Stop execution of CheckinTask.
     */
    public void shutdown() {
        log.info("Stopping CheckinExecutor for scheduler instance: {}", instanceId);
        executor.shutdown();
    }
}
