package com.novemberain.quartz.mongodb;

import com.mongodb.MongoWriteException;
import com.novemberain.quartz.mongodb.dao.CalendarDao;
import com.novemberain.quartz.mongodb.dao.JobDao;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.dao.TriggerDao;
import com.novemberain.quartz.mongodb.trigger.MisfireHandler;
import com.novemberain.quartz.mongodb.trigger.TriggerConverter;
import com.novemberain.quartz.mongodb.util.*;
import org.bson.Document;
import org.quartz.*;
import org.quartz.Calendar;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.novemberain.quartz.mongodb.util.Keys.*;

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
    private TriggerTimeCalculator timeCalculator;
    private TriggerAndJobPersister persister;
    private TriggerDao triggerDao;
    private TriggerConverter triggerConverter;
    private LockManager lockManager;
    private JobDao jobDao;
    private LocksDao locksDao;
    private CalendarDao calendarDao;
    private SchedulerSignaler signaler;

    public TriggerRunner(TriggerAndJobPersister persister, TriggerDao triggerDao, JobDao jobDao, LocksDao locksDao,
                         CalendarDao calendarDao, SchedulerSignaler signaler,
                         TriggerTimeCalculator timeCalculator, MisfireHandler misfireHandler,
                         TriggerConverter triggerConverter, LockManager lockManager) {
        this.persister = persister;
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
        this.locksDao = locksDao;
        this.calendarDao = calendarDao;
        this.signaler = signaler;
        this.timeCalculator = timeCalculator;
        this.misfireHandler = misfireHandler;
        this.triggerConverter = triggerConverter;
        this.lockManager = lockManager;
    }

    public List<OperableTrigger> acquireNext(long noLaterThan, int maxCount, long timeWindow)
            throws JobPersistenceException {
        Date noLaterThanDate = new Date(noLaterThan + timeWindow);

        log.debug("Finding up to {} triggers which have time less than {}",
                maxCount, noLaterThanDate);

        Map<TriggerKey, OperableTrigger> triggers = new HashMap<TriggerKey, OperableTrigger>();

        acquireNextTriggers(triggers, noLaterThanDate, maxCount);

        List<OperableTrigger> triggerList = new LinkedList<OperableTrigger>(triggers.values());

        // Because we are handling a batch, we may have done multiple queries and while the result for each
        // query is in fire order, the result for the whole might not be, so sort them again

        Collections.sort(triggerList, NEXT_FIRE_TIME_COMPARATOR);

        return triggerList;
    }

    public void releaseAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
        lockManager.unlockAcquiredTrigger(trigger);
    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>();

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

    public void triggeredJobComplete(OperableTrigger trigger, JobDetail job,
                                     CompletedExecutionInstruction executionInstruction)
            throws JobPersistenceException {
        log.debug("Trigger completed {}", trigger.getKey());

        if (job.isPersistJobDataAfterExecution()) {
            if (job.getJobDataMap().isDirty()) {
                log.debug("Job data map dirty, will store {}", job.getKey());
                jobDao.storeJobInMongo(job, true);
            }
        }

        if (job.isConcurrentExectionDisallowed()) {
            locksDao.unlockJob(job);
        }

        process(trigger, executionInstruction);

        locksDao.unlockTrigger(trigger);
    }

    private void acquireNextTriggers(Map<TriggerKey, OperableTrigger> triggers,
                                     Date noLaterThanDate, int maxCount)
            throws JobPersistenceException {
        for (Document triggerDoc : triggerDao.findEligibleToRun(noLaterThanDate)) {
            if (acquiredEnough(triggers, maxCount)) {
                break;
            }

            OperableTrigger trigger = triggerConverter.toTrigger(triggerDoc);

            try {
                if (cannotAcquire(triggers, trigger)) {
                    continue;
                }

                if (notAcquirableAfterMisfire(noLaterThanDate, trigger)) {
                    continue;
                }

                locksDao.lockTrigger(triggerDoc, trigger);

                log.info("Acquired trigger {}", trigger.getKey());
                triggers.put(trigger.getKey(), trigger);
            } catch (MongoWriteException e) {
                // someone else acquired this lock. Move on.
                log.info("Failed to acquire trigger {} due to a lock, reason: {}",
                        trigger.getKey(), e.getError());

                unlockExpiredAndAcquireNext(triggers, noLaterThanDate, maxCount, triggerDoc, trigger);
            }
        }
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

        return persister.removeTriggerWithoutNextFireTime(trigger);
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
                false, new Date(), trigger.getPreviousFireTime(), prevFireTime,
                trigger.getNextFireTime());
    }

    private boolean expectedCalendarButNotFound(OperableTrigger trigger, Calendar cal) {
        return trigger.getCalendarName() != null && cal == null;
    }

    private boolean hasJobDetail(TriggerFiredBundle bundle) {
        return (bundle != null) && (bundle.getJobDetail() != null);
    }

    private boolean isTriggerDeletionRequested(CompletedExecutionInstruction triggerInstCode) {
        return triggerInstCode == CompletedExecutionInstruction.DELETE_TRIGGER;
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

    private void process(OperableTrigger trigger, CompletedExecutionInstruction executionInstruction)
            throws JobPersistenceException {
        // check for trigger deleted during execution...
        OperableTrigger dbTrigger = triggerDao.getTrigger(trigger.getKey());
        if (dbTrigger != null) {
            if (isTriggerDeletionRequested(executionInstruction)) {
                if (trigger.getNextFireTime() == null) {
                    // double check for possible reschedule within job
                    // execution, which would cancel the need to delete...
                    if (dbTrigger.getNextFireTime() == null) {
                        persister.removeTrigger(trigger.getKey());
                    }
                } else {
                    persister.removeTrigger(trigger.getKey());
                    signaler.signalSchedulingChange(0L);
                }
            } else if (executionInstruction == CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            } else if (executionInstruction == CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            } else if (executionInstruction == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            } else if (executionInstruction == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            }
        }
    }

    private JobDetail retrieveJob(OperableTrigger trigger) throws JobPersistenceException {
        try {
            return jobDao.retrieveJob(trigger.getJobKey());
        } catch (JobPersistenceException e) {
            locksDao.unlockTrigger(trigger);
            throw e;
        }
    }

    private void unlockExpiredAndAcquireNext(Map<TriggerKey, OperableTrigger> triggers,
                                             Date noLaterThanDate, int maxCount,
                                             Document triggerDoc, OperableTrigger trigger)
            throws JobPersistenceException {
        Document filter = lockToBson(triggerDoc);
        Document existingLock = locksDao.findLock(filter);
        if (existingLock != null) {
            // support for trigger lock expirations
            if (timeCalculator.isTriggerLockExpired(existingLock)) {
                log.warn("Lock for trigger {} is expired - removing lock and retrying trigger acquisition",
                        trigger.getKey());
                locksDao.unlockTrigger(trigger);
                acquireNextTriggers(triggers, noLaterThanDate, maxCount - triggers.size());
            }
        } else {
            log.warn("Error retrieving expired lock from the database. Maybe it was deleted");
            acquireNextTriggers(triggers, noLaterThanDate, maxCount - triggers.size());
        }
    }
}
