package com.novemberain.quartz.mongodb;

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
}
