package com.novemberain.quartz.mongodb;

import com.mongodb.MongoWriteException;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.util.TriggerTimeCalculator;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.novemberain.quartz.mongodb.util.Keys.createLockFilter;
import static com.novemberain.quartz.mongodb.util.Keys.lockToBson;

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
     * @param triggerDoc    to create trigger lock's filter
     * @param trigger       trigger to lock
     * @return true when successfully locked
     */
    public boolean tryLockWithExpiredTakeover(Document triggerDoc, OperableTrigger trigger) {
        if (tryLock(trigger)) {
            return true;
        }

        if (unlockExpired(trigger, triggerDoc)) {
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
        Bson lockFilter = createLockFilter(job);
        Document existingLock = locksDao.findLock(lockFilter);
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

    private boolean unlockExpired(OperableTrigger trigger, Document triggerDoc) {
        //TODO triggerDoc is not needed here, because filter can be created from the trigger
        Document triggerLock = lockToBson(triggerDoc);
        Document existingLock = locksDao.findLock(triggerLock);
        if (existingLock != null) {
            // support for trigger lock expirations
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
