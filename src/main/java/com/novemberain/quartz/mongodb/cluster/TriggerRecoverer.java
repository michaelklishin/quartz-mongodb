package com.novemberain.quartz.mongodb.cluster;

import com.novemberain.quartz.mongodb.JobConverter;
import com.novemberain.quartz.mongodb.LockManager;
import com.novemberain.quartz.mongodb.TriggerAndJobPersister;
import com.novemberain.quartz.mongodb.dao.JobDao;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.dao.TriggerDao;
import org.bson.Document;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TriggerRecoverer {

    private static final Logger log = LoggerFactory.getLogger(TriggerRecoverer.class);

    private final LocksDao locksDao;
    private final TriggerAndJobPersister persister;
    private final LockManager lockManager;
    private final TriggerDao triggerDao;
    private final JobDao jobDao;

    public TriggerRecoverer(LocksDao locksDao, TriggerAndJobPersister persister,
                            LockManager lockManager, TriggerDao triggerDao, JobDao jobDao) {
        this.locksDao = locksDao;
        this.persister = persister;
        this.lockManager = lockManager;
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
    }

    public void recover() throws JobPersistenceException {
        removeOwnNonRecoverableTriggers();
    }

    private void removeOwnNonRecoverableTriggers() throws JobPersistenceException {
        locksDao.updateOwnLocks();

        cleanUpFailedOneShotTriggers();
    }

    private void cleanUpFailedOneShotTriggers() throws JobPersistenceException {
        for (TriggerKey key : locksDao.findOwnTriggersLocks()) {
            OperableTrigger trigger = triggerDao.getTrigger(key);
            if (wasOneShotTrigger(trigger)) {
                cleanUpFailedRun(trigger);

            }
        }
    }

    private void cleanUpFailedRun(OperableTrigger trigger) throws JobPersistenceException {
        //TODO check if it's the same as getJobDataMap?
        Document jobDoc = jobDao.getJob(trigger.getJobKey());
        boolean recover = jobDoc.getBoolean(JobConverter.JOB_REQUESTS_RECOVERY, false);
        if (!recover) {
            persister.removeTrigger(trigger.getKey());
            lockManager.unlockAcquiredTrigger(trigger);
        }
    }

    private boolean wasOneShotTrigger(OperableTrigger trigger) {
        return (trigger != null) && (trigger.getNextFireTime() == null);
    }
}
