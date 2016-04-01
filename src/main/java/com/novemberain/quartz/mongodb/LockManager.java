package com.novemberain.quartz.mongodb;

import com.mongodb.MongoWriteException;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.util.ExpiryCalculator;
import org.bson.Document;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
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

    /**
     * Try to lock the trigger with retry when found expired lock on it.
     *
     * @param trigger       trigger to lock
     * @return true when successfully locked
     */
    public boolean tryLockWithExpiredTakeover(OperableTrigger trigger) {
        return tryLock(trigger) || relockExpired(trigger);
    }

    public void unlockAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
        try {
            locksDao.unlockTrigger(trigger);
        } catch (Exception e) {
            throw new JobPersistenceException(e.getLocalizedMessage(), e);
        }
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

    private boolean tryLock(OperableTrigger trigger) {
        try {
            locksDao.lockTrigger(trigger);
            return true;
        } catch (MongoWriteException e) {
            log.info("Failed to lock trigger {}, reason: {}", trigger.getKey(), e.getError());
        }
        return false;
    }

    private boolean relockExpired(OperableTrigger trigger) {
        Document existingLock = locksDao.findTriggerLock(trigger.getKey());
        if (existingLock != null && expiryCalculator.isTriggerLockExpired(existingLock)) {
            // When a scheduler is defunct then its triggers become expired
            // after sometime and can be recovered by other schedulers.
            // To check that a trigger is owned by a defunct scheduler we evaluate
            // its LOCK_TIME and try to reassign it to this scheduler.
            // Relock may not be successful when some other scheduler has done
            // it first.
            log.info("Trigger {} is expired - re-locking", trigger.getKey());
            return locksDao.relock(trigger.getKey(), existingLock.getDate(Constants.LOCK_TIME));
        } else {
            log.warn("Error retrieving expired lock from the database. Maybe it was deleted");
        }
        return false;
    }
}
