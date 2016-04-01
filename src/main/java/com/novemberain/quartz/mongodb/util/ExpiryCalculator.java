package com.novemberain.quartz.mongodb.util;

import com.novemberain.quartz.mongodb.Constants;
import com.novemberain.quartz.mongodb.cluster.Scheduler;
import com.novemberain.quartz.mongodb.dao.SchedulerDao;
import org.bson.Document;

import java.util.Date;

public class ExpiryCalculator {

    private final SchedulerDao schedulerDao;
    private final Clock clock;
    private final long jobTimeoutMillis;
    private final long triggerTimeoutMillis;

    public ExpiryCalculator(SchedulerDao schedulerDao, Clock clock,
                            long jobTimeoutMillis, long triggerTimeoutMillis) {
        this.schedulerDao = schedulerDao;
        this.clock = clock;
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.triggerTimeoutMillis = triggerTimeoutMillis;
    }

    public boolean isJobLockExpired(Document lock) {
        return isLockExpired(lock, jobTimeoutMillis);
    }

    public boolean isTriggerLockExpired(Document lock) {
        return hasDefunctScheduler(lock) && isLockExpired(lock, triggerTimeoutMillis);
    }

    private boolean hasDefunctScheduler(Document lock) {
        Scheduler scheduler = schedulerDao.findInstance(lock.getString(Constants.LOCK_INSTANCE_ID));
        return (scheduler != null) && scheduler.isDefunct(clock.millis());
    }

    private boolean isLockExpired(Document lock, long timeoutMillis) {
        Date lockTime = lock.getDate(Constants.LOCK_TIME);
        long elapsedTime = clock.millis() - lockTime.getTime();
        return (elapsedTime > timeoutMillis);
    }
}
