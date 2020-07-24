package com.novemberain.quartz.mongodb.util;

import com.novemberain.quartz.mongodb.Constants;
import com.novemberain.quartz.mongodb.cluster.Scheduler;
import com.novemberain.quartz.mongodb.dao.SchedulerDao;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ExpiryCalculator {

    private static final Logger log = LoggerFactory.getLogger(ExpiryCalculator.class);

    private final SchedulerDao schedulerDao;
    private final Clock clock;
    private final long jobTimeoutMillis;
    private final long triggerTimeoutMillis;
    private final boolean isClustered;

    public ExpiryCalculator(SchedulerDao schedulerDao, Clock clock,
                            long jobTimeoutMillis, long triggerTimeoutMillis, boolean isClustered) {
        this.schedulerDao = schedulerDao;
        this.clock = clock;
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.triggerTimeoutMillis = triggerTimeoutMillis;
        this.isClustered = isClustered;
    }

    public boolean isJobLockExpired(Document lock) {
        return isLockExpired(lock, jobTimeoutMillis) || hasDefunctScheduler(lock);
    }

    public boolean isTriggerLockExpired(Document lock) {
        String schedulerId = lock.getString(Constants.LOCK_INSTANCE_ID);
        return isLockExpired(lock, triggerTimeoutMillis) || hasDefunctScheduler(schedulerId);
    }

    private boolean hasDefunctScheduler(String schedulerId) {

        Scheduler scheduler = schedulerDao.findInstance(schedulerId);
        if (scheduler == null) {
            log.debug("No such scheduler: {}", schedulerId);
            return false;
        }
        return scheduler.isDefunct(clock.millis()) && schedulerDao.isNotSelf(scheduler);

    }

    private boolean hasDefunctScheduler(Document lock) {
        String schedulerId = lock.getString(Constants.LOCK_INSTANCE_ID);
        return hasDefunctScheduler(schedulerId);
    }

    private boolean isLockExpired(Document lock, long timeoutMillis) {
        Date lockTime = lock.getDate(Constants.LOCK_TIME);
        long elapsedTime = clock.millis() - lockTime.getTime();
        return (elapsedTime > timeoutMillis);
    }
}
