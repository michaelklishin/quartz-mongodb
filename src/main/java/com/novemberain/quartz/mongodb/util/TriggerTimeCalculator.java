package com.novemberain.quartz.mongodb.util;

import com.novemberain.quartz.mongodb.Constants;
import org.bson.Document;

import java.util.Date;

public class TriggerTimeCalculator {

    private Clock clock;
    private long jobTimeoutMillis;
    private long triggerTimeoutMillis;

    public TriggerTimeCalculator(Clock clock, long jobTimeoutMillis, long triggerTimeoutMillis) {
        this.clock = clock;
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.triggerTimeoutMillis = triggerTimeoutMillis;
    }

    public boolean isJobLockExpired(Document lock) {
        return isLockExpired(lock, jobTimeoutMillis);
    }

    public boolean isTriggerLockExpired(Document lock) {
        return isLockExpired(lock, triggerTimeoutMillis);
    }

    private boolean isLockExpired(Document lock, long timeoutMillis) {
        Date lockTime = lock.getDate(Constants.LOCK_TIME);
        long elapsedTime = clock.millis() - lockTime.getTime();
        return (elapsedTime > timeoutMillis);
    }
}
