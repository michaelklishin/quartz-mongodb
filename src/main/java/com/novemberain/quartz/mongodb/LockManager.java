package com.novemberain.quartz.mongodb;

import com.mongodb.MongoWriteException;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.util.TriggerTimeCalculator;
import org.bson.Document;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockManager {

    private static final Logger log = LoggerFactory.getLogger(LockManager.class);

    private LocksDao locksDao;
    private TriggerTimeCalculator timeCalculator;

    public LockManager(LocksDao locksDao, TriggerTimeCalculator timeCalculator) {
        this.locksDao = locksDao;
        this.timeCalculator = timeCalculator;
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
        if (tryLock(trigger)) {
            return true;
        }

        if (unlockExpired(trigger)) {
            log.info("Retrying trigger acquisition: {}", trigger.getKey());
            return tryLock(trigger);
        }
        return false;
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
            if (timeCalculator.isJobLockExpired(existingLock)) {
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

    private boolean unlockExpired(OperableTrigger trigger) {
        Document existingLock = locksDao.findTriggerLock(trigger.getKey());
        if (existingLock != null) {
            // support for trigger lock expiration
            if (timeCalculator.isTriggerLockExpired(existingLock)) {
                log.warn("Lock for trigger {} is expired - removing lock", trigger.getKey());
                locksDao.unlockTrigger(trigger);
            }
            return true;
        } else {
            log.warn("Error retrieving expired lock from the database. Maybe it was deleted");
            return false;
        }
    }
}
