package com.novemberain.quartz.mongodb.util;

import com.novemberain.quartz.mongodb.Constants;
import org.bson.Document;

import java.util.Date;

public class TriggerTimeCalculator {

    private long jobTimeoutMillis;
    private long triggerTimeoutMillis;
    private long misfireThreshold;

    public TriggerTimeCalculator(long jobTimeoutMillis, long triggerTimeoutMillis, long misfireThreshold) {
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.triggerTimeoutMillis = triggerTimeoutMillis;
        this.misfireThreshold = misfireThreshold;
    }

    public boolean isJobLockExpired(Document lock) {
        return isLockExpired(lock, jobTimeoutMillis);
    }

    public boolean isNotMisfired(Date fireTime) {
        return calculateMisfireTime() < fireTime.getTime();
    }

    public boolean isTriggerLockExpired(Document lock) {
        return isLockExpired(lock, triggerTimeoutMillis);
    }

    private long calculateMisfireTime() {
        long misfireTime = System.currentTimeMillis();
        if (misfireThreshold > 0) {
            misfireTime -= misfireThreshold;
        }
        return misfireTime;
    }

    private boolean isLockExpired(Document lock, long timeoutMillis) {
        Date lockTime = lock.getDate(Constants.LOCK_TIME);
        long elapsedTime = System.currentTimeMillis() - lockTime.getTime();
        return (elapsedTime > timeoutMillis);
    }
}
