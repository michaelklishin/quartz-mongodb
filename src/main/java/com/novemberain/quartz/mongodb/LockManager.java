package com.novemberain.quartz.mongodb;

import com.mongodb.MongoWriteException;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.util.ExpiryCalculator;
import org.bson.Document;
import org.quartz.JobDetail;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockManager {

    private static final Logger log = LoggerFactory.getLogger(LockManager.class);

    private LocksDao locksDao;
    private ExpiryCalculator expiryCalculator;

    public LockManager(LocksDao locksDao, ExpiryCalculator expiryCalculator) {
        this.locksDao = locksDao;
        this.expiryCalculator = expiryCalculator;
    }

    /**
     * Lock job if it doesn't allow concurrent executions.
     *
     * @param job    job to lock
     */
    public void lockJob(JobDetail job) {
        if (job.isConcurrentExectionDisallowed()) {
            locksDao.lockJob(job);
        }
    }

    public void unlockAcquiredTrigger(OperableTrigger trigger) {
        locksDao.unlockTrigger(trigger);
    }

    /**
     * Unlock job that have existing, expired lock.
     *
     * @param job    job to potentially unlock
     */
    public void unlockExpired(JobDetail job) {
        Document existingLock = locksDao.findJobLock(job.getKey());
        if (existingLock != null) {
            if (expiryCalculator.isJobLockExpired(existingLock)) {
                log.debug("Removing expired lock for job {}", job.getKey());
                locksDao.remove(existingLock);
            }
        }
    }

    /**
     * Try to lock given trigger, ignoring errors.
     * @param key    trigger to lock
     * @return true when successfully locked, false otherwise
     */
    public boolean tryLock(TriggerKey key) {
        try {
            locksDao.lockTrigger(key);
            return true;
        } catch (MongoWriteException e) {
            log.info("Failed to lock trigger {}, reason: {}", key, e.getError());
        }
        return false;
    }

    /**
     * Relock trigger if its lock has expired.
     *
     * @param key    trigger to lock
     * @return true when successfully relocked
     */
    public boolean relockExpired(TriggerKey key) {
        Document existingLock = locksDao.findTriggerLock(key);
        if (existingLock != null) {
            if (expiryCalculator.isTriggerLockExpired(existingLock)) {
                // When a scheduler is defunct then its triggers become expired
                // after sometime and can be recovered by other schedulers.
                // To check that a trigger is owned by a defunct scheduler we evaluate
                // its LOCK_TIME and try to reassign it to this scheduler.
                // Relock may not be successful when some other scheduler has done
                // it first.
                log.info("Trigger {} is expired - re-locking", key);
                return locksDao.relock(key, existingLock.getDate(Constants.LOCK_TIME));
            } else {
                log.info("Trigger {} hasn't expired yet. Lock time: {}",
                        key, existingLock.getDate(Constants.LOCK_TIME));
            }
        } else {
            log.warn("Error retrieving expired lock from the database for trigger {}. Maybe it was deleted", key);
        }
        return false;
    }
}
