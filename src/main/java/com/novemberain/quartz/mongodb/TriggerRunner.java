package com.novemberain.quartz.mongodb;

import com.mongodb.MongoWriteException;
import com.novemberain.quartz.mongodb.dao.CalendarDao;
import com.novemberain.quartz.mongodb.dao.JobDao;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.dao.TriggerDao;
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

import java.io.IOException;
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

    private List<TriggerPersistenceHelper> persistenceHelpers = Arrays.asList(
            new SimpleTriggerPersistenceHelper(),
            new CalendarIntervalTriggerPersistenceHelper(),
            new CronTriggerPersistenceHelper(),
            new DailyTimeIntervalTriggerPersistenceHelper());

    private TriggerDao triggerDao;
    private JobDao jobDao;
    private LocksDao locksDao;
    private CalendarDao calendarDao;
    private SchedulerSignaler signaler;
    private long triggerTimeoutMillis;
    private long misfireThreshold;
    private long jobTimeoutMillis;
    private String instanceId;

    public TriggerRunner(TriggerDao triggerDao, JobDao jobDao, LocksDao locksDao,
                         CalendarDao calendarDao, SchedulerSignaler signaler,
                         long triggerTimeoutMillis, long misfireThreshold, long jobTimeoutMillis,
                         String instanceId) {
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
        this.locksDao = locksDao;
        this.calendarDao = calendarDao;
        this.signaler = signaler;
        this.triggerTimeoutMillis = triggerTimeoutMillis;
        this.misfireThreshold = misfireThreshold;
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.instanceId = instanceId;
    }

    public List<OperableTrigger> acquireNext(long noLaterThan, int maxCount, long timeWindow)
            throws JobPersistenceException {
        Date noLaterThanDate = new Date(noLaterThan + timeWindow);

        if (log.isDebugEnabled()) {
            log.debug("Finding up to {} triggers which have time less than {}",
                    maxCount, noLaterThanDate);
        }

        Map<TriggerKey, OperableTrigger> triggers = new HashMap<TriggerKey, OperableTrigger>();

        doAcquireNextTriggers(triggers, noLaterThanDate, maxCount);

        List<OperableTrigger> triggerList = new LinkedList<OperableTrigger>(triggers.values());

        // Because we are handling a batch, we may have done multiple queries and while the result for each
        // query is in fire order, the result for the whole might not be, so sort them again

        Collections.sort(triggerList, NEXT_FIRE_TIME_COMPARATOR);

        return triggerList;
    }

    private void doAcquireNextTriggers(Map<TriggerKey, OperableTrigger> triggers,
                                       Date noLaterThanDate, int maxCount)
            throws JobPersistenceException {
        QueryHelper queryHelper = new QueryHelper();
        Bson query = queryHelper.createNextTriggerQuery(noLaterThanDate);

        for (Document triggerDoc : triggerDao.findEligibleToRun(query)) {
            if (maxCount <= triggers.size()) {
                break;
            }

            OperableTrigger trigger = toTrigger(triggerDoc);

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
                if (applyMisfire(trigger)) {
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
                    if (isTriggerLockExpired(existingLock)) {
                        log.warn("Lock for trigger {} is expired - removing lock and retrying trigger acquisition",
                                trigger.getKey());
                        removeTriggerLock(trigger);
                        doAcquireNextTriggers(triggers, noLaterThanDate, maxCount - triggers.size());
                    }
                } else {
                    log.warn("Error retrieving expired lock from the database. Maybe it was deleted");
                    doAcquireNextTriggers(triggers, noLaterThanDate, maxCount - triggers.size());
                }
            }
        }
    }

    private boolean applyMisfire(OperableTrigger trigger) throws JobPersistenceException {
        long misfireTime = System.currentTimeMillis();
        if (misfireThreshold > 0) {
            misfireTime -= misfireThreshold;
        }

        Date tnft = trigger.getNextFireTime();
        if (tnft == null || tnft.getTime() > misfireTime
                || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
            return false;
        }

        org.quartz.Calendar cal = null;
        if (trigger.getCalendarName() != null) {
            cal = calendarDao.retrieveCalendar(trigger.getCalendarName());
        }

        signaler.notifyTriggerListenersMisfired((OperableTrigger) trigger.clone());

        trigger.updateAfterMisfire(cal);

        if (trigger.getNextFireTime() == null) {
            signaler.notifySchedulerListenersFinalized(trigger);
        } else if (tnft.equals(trigger.getNextFireTime())) {
            return false;
        }

        storeTrigger(trigger, true);
        return true;
    }

    private boolean isTriggerLockExpired(Document lock) {
        Date lockTime = lock.getDate(Constants.LOCK_TIME);
        long elaspedTime = System.currentTimeMillis() - lockTime.getTime();
        return (elaspedTime > triggerTimeoutMillis);
    }

    public void releaseAcquired(OperableTrigger trigger) throws JobPersistenceException {
        try {
            removeTriggerLock(trigger);
        } catch (Exception e) {
            throw new JobPersistenceException(e.getLocalizedMessage(), e);
        }
    }

    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>();

        for (OperableTrigger trigger : triggers) {
            log.debug("Fired trigger {}", trigger.getKey());
            Calendar cal = null;
            if (trigger.getCalendarName() != null) {
                cal = calendarDao.retrieveCalendar(trigger.getCalendarName());
                if (cal == null)
                    continue;
            }

            Date prevFireTime = trigger.getPreviousFireTime();
            trigger.triggered(cal);

            TriggerFiredBundle bundle = new TriggerFiredBundle(retrieveJob(trigger), trigger, cal,
                    false, new Date(), trigger.getPreviousFireTime(), prevFireTime,
                    trigger.getNextFireTime());

            JobDetail job = bundle.getJobDetail();

            if (job != null) {

                try {
                    if (job.isConcurrentExectionDisallowed()) {
                        log.debug("Inserting lock for job {}", job.getKey());
                        Document lock = new Document();
                        lock.put(KEY_NAME, "jobconcurrentlock:" + job.getKey().getName());
                        lock.put(KEY_GROUP, job.getKey().getGroup());
                        lock.put(Constants.LOCK_INSTANCE_ID, instanceId);
                        lock.put(Constants.LOCK_TIME, new Date());
                        locksDao.insertLock(lock);
                    }

                    results.add(new TriggerFiredResult(bundle));
                    storeTrigger(trigger, true);
                } catch (MongoWriteException dk) {
                    log.debug("Job disallows concurrent execution and is already running {}", job.getKey());

                    removeTriggerLock(trigger);

                    // Find the existing lock and if still present, and expired, then remove it.
                    Bson lock = createLockFilter(job);
                    Document existingLock = locksDao.findLock(lock);
                    if (existingLock != null) {
                        if (isJobLockExpired(existingLock)) {
                            log.debug("Removing expired lock for job {}", job.getKey());
                            locksDao.remove(existingLock);
                        }
                    }
                }
            }

        }
        return results;
    }

    public void triggeredComplete(OperableTrigger trigger, JobDetail job,
                                  CompletedExecutionInstruction triggerInstCode)
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

        // check for trigger deleted during execution...
        OperableTrigger trigger2 = retrieveTrigger(trigger.getKey());
        if (trigger2 != null) {
            if (triggerInstCode == CompletedExecutionInstruction.DELETE_TRIGGER) {
                if (trigger.getNextFireTime() == null) {
                    // double check for possible reschedule within job
                    // execution, which would cancel the need to delete...
                    if (trigger2.getNextFireTime() == null) {
                        removeTrigger(trigger.getKey());
                    }
                } else {
                    removeTrigger(trigger.getKey());
                    signaler.signalSchedulingChange(0L);
                }
            } else if (triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            } else if (triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            } else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            } else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            }
        }

        removeTriggerLock(trigger);
    }

    private JobDetail retrieveJob(OperableTrigger trigger) throws JobPersistenceException {
        try {
            return jobDao.retrieveJob(trigger.getJobKey());
        } catch (JobPersistenceException e) {
            removeTriggerLock(trigger);
            throw e;
        }
    }

    public boolean removeTrigger(TriggerKey triggerKey) {
        // If the removal of the Trigger results in an 'orphaned' Job that is not 'durable',
        // then the job should be removed also.
        Bson filter = Keys.toFilter(triggerKey);
        Document trigger = triggerDao.findTrigger(filter);
        if (trigger != null) {
            if (trigger.containsKey(Constants.TRIGGER_JOB_ID)) {
                // There is only 1 job per trigger so no need to look further.
                Document job = jobDao.getById(trigger.get(Constants.TRIGGER_JOB_ID));
                // Remove the orphaned job if it's durable and has no other triggers associated with it,
                // remove it
                if (job != null && (!job.containsKey(Constants.JOB_DURABILITY) || job.get(Constants.JOB_DURABILITY).toString().equals("false"))) {
                    if (triggerDao.hasLastTrigger(job)) {
                        jobDao.remove(job);
                    }
                }
            } else {
                log.debug("The trigger had no associated jobs");
            }
            //TODO: check if can .deleteOne(filter) here
            triggerDao.remove(filter);

            return true;
        }

        return false;
    }

    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        for (TriggerKey key : triggerKeys) {
            removeTrigger(key);
        }
        return false;
    }

    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        Document doc = triggerDao.findTrigger(Keys.toFilter(triggerKey));
        if (doc == null) {
            return null;
        }
        return toTrigger(triggerKey, doc);
    }

    public void storeTrigger(OperableTrigger newTrigger, ObjectId jobId, boolean replaceExisting)
            throws JobPersistenceException {
        Document trigger = convertToBson(newTrigger, jobId);
        if (newTrigger.getJobDataMap().size() > 0) {
            try {
                String jobDataString = SerialUtils.serialize(newTrigger.getJobDataMap());
                trigger.put(Constants.JOB_DATA, jobDataString);
            } catch (IOException ioe) {
                throw new JobPersistenceException("Could not serialise job data map on the trigger for " + newTrigger.getKey(), ioe);
            }
        }

        TriggerPersistenceHelper tpd = triggerPersistenceDelegateFor(newTrigger);
        trigger = tpd.injectExtraPropertiesForInsert(newTrigger, trigger);

        if (replaceExisting) {
            trigger.remove("_id");
            triggerDao.replace(newTrigger.getKey(), trigger);
        } else {
            triggerDao.insert(trigger, newTrigger);
        }
    }

    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        final List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();
        final Document doc = jobDao.getJob(jobKey);
        if (doc == null) {
            return triggers;
        }

        for (Document item : triggerDao.findByJobId(doc.get("_id"))) {
            triggers.add(toTrigger(item));
        }

        return triggers;
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

    private boolean isJobLockExpired(Document lock) {
        Date lockTime = lock.getDate(Constants.LOCK_TIME);
        long elaspedTime = System.currentTimeMillis() - lockTime.getTime();
        return (elaspedTime > jobTimeoutMillis);
    }

    private void removeTriggerLock(OperableTrigger trigger) {
        log.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
        Bson lock = Keys.toFilter(trigger.getKey());

        // Comment this out, as expired trigger locks should be deleted by any another instance
        // lock.put(LOCK_INSTANCE_ID, instanceId);

        locksDao.remove(lock);
        log.info("Trigger lock {}.{} removed.", trigger.getKey(), instanceId);
    }

    private OperableTrigger toTrigger(Document doc) throws JobPersistenceException {
        TriggerKey key = new TriggerKey(doc.getString(KEY_NAME), doc.getString(KEY_GROUP));
        return toTrigger(key, doc);
    }

    private OperableTrigger toTrigger(TriggerKey triggerKey, Document triggerDoc)
            throws JobPersistenceException {
        OperableTrigger trigger;
        try {
            @SuppressWarnings("unchecked")
            Class<OperableTrigger> triggerClass = (Class<OperableTrigger>) getTriggerClassLoader()
                    .loadClass(triggerDoc.getString(Constants.TRIGGER_CLASS));
            trigger = triggerClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException("Could not find trigger class " + triggerDoc.get(Constants.TRIGGER_CLASS));
        } catch (Exception e) {
            throw new JobPersistenceException("Could not instantiate trigger class " + triggerDoc.get(Constants.TRIGGER_CLASS));
        }

        TriggerPersistenceHelper tpd = triggerPersistenceDelegateFor(trigger);

        trigger.setKey(triggerKey);
        trigger.setCalendarName(triggerDoc.getString(Constants.TRIGGER_CALENDAR_NAME));
        trigger.setDescription(triggerDoc.getString(Constants.TRIGGER_DESCRIPTION));
        trigger.setFireInstanceId(triggerDoc.getString(Constants.TRIGGER_FIRE_INSTANCE_ID));
        trigger.setMisfireInstruction(triggerDoc.getInteger(Constants.TRIGGER_MISFIRE_INSTRUCTION));
        trigger.setNextFireTime(triggerDoc.getDate(Constants.TRIGGER_NEXT_FIRE_TIME));
        trigger.setPreviousFireTime(triggerDoc.getDate(Constants.TRIGGER_PREVIOUS_FIRE_TIME));
        trigger.setPriority(triggerDoc.getInteger(Constants.TRIGGER_PRIORITY));

        String jobDataString = triggerDoc.getString(Constants.JOB_DATA);

        if (jobDataString != null) {
            try {
                SerialUtils.deserialize(trigger.getJobDataMap(), jobDataString);
            } catch (IOException e) {
                throw new JobPersistenceException("Could not deserialize job data for trigger " + triggerDoc.get(Constants.TRIGGER_CLASS));
            }
        }

        try {
            trigger.setStartTime(triggerDoc.getDate(Constants.TRIGGER_START_TIME));
            trigger.setEndTime(triggerDoc.getDate(Constants.TRIGGER_END_TIME));
        } catch (IllegalArgumentException e) {
            //Ignore illegal arg exceptions thrown by triggers doing JIT validation of start and endtime
            log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e);
        }


        try {
            trigger.setStartTime(triggerDoc.getDate(Constants.TRIGGER_START_TIME));
            trigger.setEndTime(triggerDoc.getDate(Constants.TRIGGER_END_TIME));
        } catch (IllegalArgumentException e) {
            //Ignore illegal arg exceptions thrown by triggers doing JIT validation of start and endtime
            log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e);
        }

        trigger = tpd.setExtraPropertiesAfterInstantiation(trigger, triggerDoc);

        Document job = jobDao.getById(triggerDoc.get(Constants.TRIGGER_JOB_ID));
        if (job != null) {
            trigger.setJobKey(new JobKey(job.getString(KEY_NAME), job.getString(KEY_GROUP)));
            return trigger;
        } else {
            // job was deleted
            return null;
        }
    }

    private ClassLoader getTriggerClassLoader() {
        return Job.class.getClassLoader();
    }

    private TriggerPersistenceHelper triggerPersistenceDelegateFor(OperableTrigger trigger) {
        TriggerPersistenceHelper result = null;

        for (TriggerPersistenceHelper d : persistenceHelpers) {
            if (d.canHandleTriggerType(trigger)) {
                result = d;
                break;
            }
        }

        assert result != null;
        return result;
    }
}
