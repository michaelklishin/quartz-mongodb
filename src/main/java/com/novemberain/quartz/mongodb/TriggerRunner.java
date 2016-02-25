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
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
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
    private TriggerDao triggerDao;
    private TriggerConverter triggerConverter;
    private JobDao jobDao;
    private LocksDao locksDao;
    private CalendarDao calendarDao;
    private SchedulerSignaler signaler;
    private String instanceId;

    public TriggerRunner(TriggerDao triggerDao, JobDao jobDao, LocksDao locksDao,
                         CalendarDao calendarDao, SchedulerSignaler signaler,
                         String instanceId, TriggerTimeCalculator timeCalculator,
                         MisfireHandler misfireHandler, TriggerConverter triggerConverter) {
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
        this.locksDao = locksDao;
        this.calendarDao = calendarDao;
        this.signaler = signaler;
        this.instanceId = instanceId;
        this.timeCalculator = timeCalculator;
        this.misfireHandler = misfireHandler;
        this.triggerConverter = triggerConverter;
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

    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        final List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();
        final Document doc = jobDao.getJob(jobKey);
        if (doc != null) {
            for (Document item : triggerDao.findByJobId(doc.get("_id"))) {
                triggers.add(triggerConverter.toTrigger(item));
            }
        }
        return triggers;
    }

    public void releaseAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
        try {
            unlock(trigger);
        } catch (Exception e) {
            throw new JobPersistenceException(e.getLocalizedMessage(), e);
        }
    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>();

        for (OperableTrigger trigger : triggers) {
            log.debug("Fired trigger {}", trigger.getKey());

            TriggerFiredBundle bundle = createTriggerFiredBundle(trigger);

            if (hasJobDetail(bundle)) {
                JobDetail job = bundle.getJobDetail();
                try {
                    lockJob(job);
                    results.add(new TriggerFiredResult(bundle));
                    storeTrigger(trigger, true);
                } catch (MongoWriteException dk) {
                    log.debug("Job disallows concurrent execution and is already running {}", job.getKey());
                    unlock(trigger);
                    unlock(job);
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
            log.debug("Removing lock for job {}", job.getKey());
            locksDao.remove(createLockFilter(job));
        }

        process(trigger, executionInstruction);

        unlock(trigger);
    }

    public boolean removeTrigger(TriggerKey triggerKey) {
        Bson filter = Keys.toFilter(triggerKey);
        Document trigger = triggerDao.findTrigger(filter);
        if (trigger != null) {
            removeOrphanedJob(trigger);
            //TODO: check if can .deleteOne(filter) here
            triggerDao.remove(filter);
            return true;
        }
        return false;
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        //FIXME return boolean allFound = true when all removed
        for (TriggerKey key : triggerKeys) {
            removeTrigger(key);
        }
        return false;
    }

    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger)
            throws JobPersistenceException {
        OperableTrigger oldTrigger = retrieveTrigger(triggerKey);
        if (oldTrigger == null) {
            return false;
        }

        if (!oldTrigger.getJobKey().equals(newTrigger.getJobKey())) {
            throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
        }

        removeOldTrigger(triggerKey);
        copyOldJobDataMap(newTrigger, oldTrigger);
        storeNewTrigger(newTrigger, oldTrigger);

        return true;
    }

    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        Document doc = triggerDao.findTrigger(Keys.toFilter(triggerKey));
        if (doc == null) {
            return null;
        }
        return triggerConverter.toTrigger(triggerKey, doc);
    }

    public void storeTrigger(OperableTrigger newTrigger, ObjectId jobId, boolean replaceExisting)
            throws JobPersistenceException {
        Document trigger = triggerConverter.toDocument(newTrigger, jobId);
        if (replaceExisting) {
            trigger.remove("_id");
            triggerDao.replace(newTrigger.getKey(), trigger);
        } else {
            triggerDao.insert(trigger, newTrigger);
        }
    }

    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
            throws JobPersistenceException {
        if (newTrigger.getJobKey() == null) {
            throw new JobPersistenceException("Trigger must be associated with a job. Please specify a JobKey.");
        }

        Document doc = jobDao.getJob(Keys.toFilter(newTrigger.getJobKey()));
        if (doc != null) {
            storeTrigger(newTrigger, doc.getObjectId("_id"), replaceExisting);
        } else {
            throw new JobPersistenceException("Could not find job with key " + newTrigger.getJobKey());
        }
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
                if (trigger == null) {
                    continue;
                }

                if (triggers.containsKey(trigger.getKey())) {
                    log.debug("Skipping trigger {} as we have already acquired it.", trigger.getKey());
                    continue;
                }

                if (trigger.getNextFireTime() == null) {
                    log.debug("Skipping trigger {} as it has no next fire time.", trigger.getKey());

                    // No next fire time, so delete it
                    removeTrigger(trigger.getKey());
                    continue;
                }

                // deal with misfires
                if (misfireHandler.applyMisfire(trigger)) {
                    storeTrigger(trigger, true);

                    log.debug("Misfire trigger {}.", trigger.getKey());

                    Date nextFireTime = trigger.getNextFireTime();

                    if (nextFireTime == null) {
                        log.debug("Removing trigger {} as it has no next fire time after the misfire was applied.",
                                trigger.getKey());

                        // No next fire time, so delete it
                        removeTrigger(trigger.getKey());
                        continue;
                    }

                    // The trigger has misfired and was rescheduled, its firetime may be too far in the future
                    // and we don't want to hang the quartz scheduler thread up on <code>sigLock.wait(timeUntilTrigger);</code>
                    // so, check again that the trigger is due to fire
                    if (nextFireTime.after(noLaterThanDate)) {
                        log.debug("Skipping trigger {} as it misfired and was scheduled for {}.",
                                trigger.getKey(), trigger.getNextFireTime());
                        continue;
                    }
                }

                log.info("Inserting lock for trigger {}", trigger.getKey());

                Document lock = createTriggerDbLock(triggerDoc, instanceId);
                locksDao.insertLock(lock);

                log.info("Acquired trigger {}", trigger.getKey());
                triggers.put(trigger.getKey(), trigger);

            } catch (MongoWriteException e) {
                // someone else acquired this lock. Move on.
                log.info("Failed to acquire trigger {} due to a lock, reason: {}",
                        trigger.getKey(), e.getError());

                Document filter = lockToBson(triggerDoc);
                Document existingLock = locksDao.findLock(filter);
                if (existingLock != null) {
                    // support for trigger lock expirations
                    if (timeCalculator.isTriggerLockExpired(existingLock)) {
                        log.warn("Lock for trigger {} is expired - removing lock and retrying trigger acquisition",
                                trigger.getKey());
                        unlock(trigger);
                        acquireNextTriggers(triggers, noLaterThanDate, maxCount - triggers.size());
                    }
                } else {
                    log.warn("Error retrieving expired lock from the database. Maybe it was deleted");
                    acquireNextTriggers(triggers, noLaterThanDate, maxCount - triggers.size());
                }
            }
        }
    }

    private boolean acquiredEnough(Map<TriggerKey, OperableTrigger> triggers, int maxCount) {
        return maxCount <= triggers.size();
    }

    private void copyOldJobDataMap(OperableTrigger newTrigger, OperableTrigger trigger) {
        // Copy across the job data map from the old trigger to the new one.
        newTrigger.getJobDataMap().putAll(trigger.getJobDataMap());
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

    private boolean isOrphan(Document job) {
        return (job != null) && isNotDurable(job) && triggerDao.hasLastTrigger(job);
    }

    private boolean isNotDurable(Document job) {
        return !job.containsKey(JobConverter.JOB_DURABILITY) ||
                job.get(JobConverter.JOB_DURABILITY).toString().equals("false");
    }

    private boolean isTriggerDeletionRequested(CompletedExecutionInstruction triggerInstCode) {
        return triggerInstCode == CompletedExecutionInstruction.DELETE_TRIGGER;
    }

    private void lockJob(JobDetail job) {
        if (job.isConcurrentExectionDisallowed()) {
            log.debug("Inserting lock for job {}", job.getKey());
            Document lock = createJobLock(job, instanceId);
            locksDao.insertLock(lock);
        }
    }

    private void process(OperableTrigger trigger, CompletedExecutionInstruction executionInstruction)
            throws JobPersistenceException {
        // check for trigger deleted during execution...
        OperableTrigger dbTrigger = retrieveTrigger(trigger.getKey());
        if (dbTrigger != null) {
            if (isTriggerDeletionRequested(executionInstruction)) {
                if (trigger.getNextFireTime() == null) {
                    // double check for possible reschedule within job
                    // execution, which would cancel the need to delete...
                    if (dbTrigger.getNextFireTime() == null) {
                        removeTrigger(trigger.getKey());
                    }
                } else {
                    removeTrigger(trigger.getKey());
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

    private void removeOldTrigger(TriggerKey triggerKey) {
        // Can't call remove trigger as if the job is not durable, it will remove the job too
        Bson filter = toFilter(triggerKey);
        if (triggerDao.exists(filter)) {
            triggerDao.remove(filter);
        }
    }

    // If the removal of the Trigger results in an 'orphaned' Job that is not 'durable',
    // then the job should be removed also.
    private void removeOrphanedJob(Document trigger) {
        if (trigger.containsKey(Constants.TRIGGER_JOB_ID)) {
            // There is only 1 job per trigger so no need to look further.
            Document job = jobDao.getById(trigger.get(Constants.TRIGGER_JOB_ID));
            if (isOrphan(job)) {
                jobDao.remove(job);
            }
        } else {
            log.debug("The trigger had no associated jobs");
        }
    }

    private JobDetail retrieveJob(OperableTrigger trigger) throws JobPersistenceException {
        try {
            return jobDao.retrieveJob(trigger.getJobKey());
        } catch (JobPersistenceException e) {
            unlock(trigger);
            throw e;
        }
    }

    private void storeNewTrigger(OperableTrigger newTrigger, OperableTrigger oldTrigger)
            throws JobPersistenceException {
        try {
            storeTrigger(newTrigger, false);
        } catch (JobPersistenceException jpe) {
            storeTrigger(oldTrigger, false);
            throw jpe;
        }
    }

    private void unlock(JobDetail job) {
        // Find the existing lock and if still present, and expired, then remove it.
        Bson lock = createLockFilter(job);
        Document existingLock = locksDao.findLock(lock);
        if (existingLock != null) {
            if (timeCalculator.isJobLockExpired(existingLock)) {
                log.debug("Removing expired lock for job {}", job.getKey());
                locksDao.remove(existingLock);
            }
        }
    }

    private void unlock(OperableTrigger trigger) {
        log.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
        Bson lock = Keys.toFilter(trigger.getKey());

        // Comment this out, as expired trigger locks should be deleted by any another instance
        // lock.put(LOCK_INSTANCE_ID, instanceId);

        locksDao.remove(lock);
        log.info("Trigger lock {}.{} removed.", trigger.getKey(), instanceId);
    }
}
