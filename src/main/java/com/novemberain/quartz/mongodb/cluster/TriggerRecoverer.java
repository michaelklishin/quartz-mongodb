package com.novemberain.quartz.mongodb.cluster;

import com.novemberain.quartz.mongodb.LockManager;
import com.novemberain.quartz.mongodb.TriggerAndJobPersister;
import com.novemberain.quartz.mongodb.dao.JobDao;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.dao.TriggerDao;
import com.novemberain.quartz.mongodb.trigger.MisfireHandler;
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
    private final RecoveryTriggerFactory recoveryTriggerFactory;
    private final MisfireHandler misfireHandler;

    public TriggerRecoverer(LocksDao locksDao, TriggerAndJobPersister persister,
                            LockManager lockManager, TriggerDao triggerDao,
                            JobDao jobDao, RecoveryTriggerFactory recoveryTriggerFactory,
                            MisfireHandler misfireHandler) {
        this.locksDao = locksDao;
        this.persister = persister;
        this.lockManager = lockManager;
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
        this.recoveryTriggerFactory = recoveryTriggerFactory;
        this.misfireHandler = misfireHandler;
    }

    public void recover() throws JobPersistenceException {
        for (TriggerKey key : locksDao.findOwnTriggersLocks()) {
            OperableTrigger trigger = triggerDao.getTrigger(key);
            if (trigger == null) {
                continue;
            }

            // Make the trigger's lock fresh for other nodes,
            // so they don't recover it.
            if (locksDao.updateOwnLock(trigger.getKey())) {
                doRecovery(trigger);
                lockManager.unlockAcquiredTrigger(trigger);
            }
        }
    }

    /**
     * Do recovery procedure after failed run of given trigger.
     *
     * @param trigger    trigger to recover
     * @return recovery trigger or null if its job doesn't want that
     * @throws JobPersistenceException
     */
    public OperableTrigger doRecovery(OperableTrigger trigger) throws JobPersistenceException {
        OperableTrigger recoveryTrigger = null;
        if (jobDao.requestsRecovery(trigger.getJobKey())) {
            recoveryTrigger = recoverTrigger(trigger);
            if (!wasOneShotTrigger(trigger)) {
                updateMisfires(trigger);
            }
        } else if (wasOneShotTrigger(trigger)) {
            cleanUpFailedRun(trigger);
        } else {
            updateMisfires(trigger);
        }
        return recoveryTrigger;
    }

    private OperableTrigger recoverTrigger(OperableTrigger trigger)
            throws JobPersistenceException {
        log.info("Recovering trigger: {}", trigger.getKey());
        OperableTrigger recoveryTrigger = recoveryTriggerFactory.from(trigger);
        persister.storeTrigger(recoveryTrigger, false);
        return recoveryTrigger;
    }

    private void updateMisfires(OperableTrigger trigger) throws JobPersistenceException {
        if (misfireHandler.applyMisfireOnRecovery(trigger)) {
            log.info("Misfire applied. Replacing trigger: {}", trigger.getKey());
            persister.storeTrigger(trigger, true);
        } else {
            //TODO should complete trigger?
            log.warn("Recovery misfire not applied for trigger: {}",
                    trigger.getKey());
//            storeTrigger(conn, trig,
//                    null, true, STATE_COMPLETE, forceState, recovering);
//            schedSignaler.notifySchedulerListenersFinalized(trig);
        }
    }

    private void cleanUpFailedRun(OperableTrigger trigger) {
        persister.removeTrigger(trigger.getKey());
    }

    private boolean wasOneShotTrigger(OperableTrigger trigger) {
        return trigger.getNextFireTime() == null
                && trigger.getStartTime().equals(trigger.getFinalFireTime());
    }
}
