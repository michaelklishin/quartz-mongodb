package com.novemberain.quartz.mongodb;

import com.mongodb.MongoWriteException;
import com.novemberain.quartz.mongodb.cluster.TriggerRecoverer;
import com.novemberain.quartz.mongodb.dao.CalendarDao;
import com.novemberain.quartz.mongodb.dao.JobDao;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.dao.TriggerDao;
import com.novemberain.quartz.mongodb.trigger.MisfireHandler;
import com.novemberain.quartz.mongodb.trigger.TriggerConverter;
import org.bson.Document;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TriggerRunner {

    private static final Logger log = LoggerFactory.getLogger(TriggerRunner.class);

    private static final Comparator<OperableTrigger> NEXT_FIRE_TIME_COMPARATOR
            = new Comparator<OperableTrigger>() {
        @Override
        public int compare(OperableTrigger o1, OperableTrigger o2) {
            return (int) (o1.getNextFireTime().getTime() - o2.getNextFireTime().getTime());
        }
    };

    private MisfireHandler misfireHandler;
    private TriggerAndJobPersister persister;
    private TriggerDao triggerDao;
    private TriggerConverter triggerConverter;
    private LockManager lockManager;
    private TriggerRecoverer recoverer;
    private JobDao jobDao;
    private LocksDao locksDao;
    private CalendarDao calendarDao;

    public TriggerRunner(TriggerAndJobPersister persister, TriggerDao triggerDao, JobDao jobDao, LocksDao locksDao,
                         CalendarDao calendarDao, MisfireHandler misfireHandler,
                         TriggerConverter triggerConverter, LockManager lockManager,
                         TriggerRecoverer recoverer) {
        this.persister = persister;
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
        this.locksDao = locksDao;
        this.calendarDao = calendarDao;
        this.misfireHandler = misfireHandler;
        this.triggerConverter = triggerConverter;
        this.lockManager = lockManager;
        this.recoverer = recoverer;
    }

    public List<OperableTrigger> acquireNext(long noLaterThan, int maxCount, long timeWindow)
            throws JobPersistenceException {
        Date noLaterThanDate = new Date(noLaterThan + timeWindow);

        log.debug("Finding up to {} triggers which have time less than {}",
                maxCount, noLaterThanDate);

        List<OperableTrigger> triggers = acquireNextTriggers(noLaterThanDate, maxCount);

        // Because we are handling a batch, we may have done multiple queries and while the result for each
        // query is in fire order, the result for the whole might not be, so sort them again

        Collections.sort(triggers, NEXT_FIRE_TIME_COMPARATOR);

        return triggers;
    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
            throws JobPersistenceException {
        List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>(triggers.size());

        for (OperableTrigger trigger : triggers) {
            log.debug("Fired trigger {}", trigger.getKey());

            TriggerFiredBundle bundle = createTriggerFiredBundle(trigger);

            if (hasJobDetail(bundle)) {
                JobDetail job = bundle.getJobDetail();
                try {
                    lockManager.lockJob(job);
                    results.add(new TriggerFiredResult(bundle));
                    persister.storeTrigger(trigger, true);
                } catch (MongoWriteException dk) {
                    log.debug("Job disallows concurrent execution and is already running {}", job.getKey());
                    locksDao.unlockTrigger(trigger);
                    lockManager.unlockExpired(job);
                }
            }

        }
        return results;
    }

    private List<OperableTrigger> acquireNextTriggers(Date noLaterThanDate, int maxCount)
            throws JobPersistenceException {
        Map<TriggerKey, OperableTrigger> triggers = new HashMap<>();

        for (Document triggerDoc : triggerDao.findEligibleToRun(noLaterThanDate, maxCount)) {
            if (acquiredEnough(triggers, maxCount)) {
                break;
            }

            TriggerKey key = triggerConverter.createTriggerKey(triggerDoc);

            if (lockManager.tryLock(key)) {
                OperableTrigger trigger = joinTriggerWithJob(triggers, triggerDoc);

                if (trigger == null) continue;

                if (prepareForFire(noLaterThanDate, trigger)) {
                    log.info("Acquired trigger: {}", trigger.getKey());
                    triggers.put(trigger.getKey(), trigger);
                } else {
                    lockManager.unlockAcquiredTrigger(trigger);
                }
            } else if (lockManager.relockExpired(key)) {
                OperableTrigger trigger = joinTriggerWithJob(triggers, triggerDoc);

                if (trigger == null) continue;

                log.info("Recovering trigger: {}", trigger.getKey());
                OperableTrigger recoveryTrigger = recoverer.doRecovery(trigger);
                lockManager.unlockAcquiredTrigger(trigger);
                if (recoveryTrigger != null && lockManager.tryLock(recoveryTrigger.getKey())) {
                    log.info("Acquired trigger: {}", recoveryTrigger.getKey());
                    triggers.put(recoveryTrigger.getKey(), recoveryTrigger);
                }
            }
        }

        return new ArrayList<OperableTrigger>(triggers.values());
    }

    private OperableTrigger joinTriggerWithJob(Map<TriggerKey, OperableTrigger> triggers, Document triggerDoc) throws JobPersistenceException {
        OperableTrigger trigger = triggerConverter.toTriggerWithOptionalJob(triggerDoc);

        if (cannotAcquire(triggers, trigger)) {
            return null;
        }

        if (trigger.getJobKey() == null) {
            log.error("Error retrieving job for trigger {}, setting trigger state to ERROR.", trigger.getKey());
            triggerDao.transferState(trigger.getKey(), Constants.STATE_WAITING, Constants.STATE_ERROR);
            return null;
        }
        return trigger;
    }

    private boolean prepareForFire(Date noLaterThanDate, OperableTrigger trigger)
            throws JobPersistenceException {
        //TODO don't remove when recovering trigger
        if (persister.removeTriggerWithoutNextFireTime(trigger)) {
            return false;
        }

        if (notAcquirableAfterMisfire(noLaterThanDate, trigger)) {
            return false;
        }
        return true;
    }

    private boolean acquiredEnough(Map<TriggerKey, OperableTrigger> triggers, int maxCount) {
        return maxCount <= triggers.size();
    }

    private boolean cannotAcquire(Map<TriggerKey, OperableTrigger> triggers, OperableTrigger trigger) {
        if (trigger == null) {
            return true;
        }

        if (triggers.containsKey(trigger.getKey())) {
            log.debug("Skipping trigger {} as we have already acquired it.", trigger.getKey());
            return true;
        }
        return false;
    }

    private TriggerFiredBundle createTriggerFiredBundle(OperableTrigger trigger)
            throws JobPersistenceException {
        Calendar cal = calendarDao.retrieveCalendar(trigger.getCalendarName());
        if (expectedCalendarButNotFound(trigger, cal)) {
            return null;
        }

        Date prevFireTime = trigger.getPreviousFireTime();
        trigger.triggered(cal);

        return new TriggerFiredBundle(retrieveJob(trigger), trigger, cal,
                isRecovering(trigger), new Date(),
                trigger.getPreviousFireTime(), prevFireTime,
                trigger.getNextFireTime());
    }

    private boolean expectedCalendarButNotFound(OperableTrigger trigger, Calendar cal) {
        return trigger.getCalendarName() != null && cal == null;
    }

    private boolean isRecovering(OperableTrigger trigger) {
        return trigger.getKey().getGroup().equals(Scheduler.DEFAULT_RECOVERY_GROUP);
    }

    private boolean hasJobDetail(TriggerFiredBundle bundle) {
        return (bundle != null) && (bundle.getJobDetail() != null);
    }

    private boolean notAcquirableAfterMisfire(Date noLaterThanDate, OperableTrigger trigger)
            throws JobPersistenceException {
        if (misfireHandler.applyMisfire(trigger)) {
            persister.storeTrigger(trigger, true);

            log.debug("Misfire trigger {}.", trigger.getKey());

            if (persister.removeTriggerWithoutNextFireTime(trigger)) {
                return true;
            }

            // The trigger has misfired and was rescheduled, its firetime may be too far in the future
            // and we don't want to hang the quartz scheduler thread up on <code>sigLock.wait(timeUntilTrigger);</code>
            // so, check again that the trigger is due to fire
            if (trigger.getNextFireTime().after(noLaterThanDate)) {
                log.debug("Skipping trigger {} as it misfired and was scheduled for {}.",
                        trigger.getKey(), trigger.getNextFireTime());
                return true;
            }
        }
        return false;
    }

    private JobDetail retrieveJob(OperableTrigger trigger) throws JobPersistenceException {
        try {
            return jobDao.retrieveJob(trigger.getJobKey());
        } catch (JobPersistenceException e) {
            locksDao.unlockTrigger(trigger);
            throw e;
        }
    }
}
