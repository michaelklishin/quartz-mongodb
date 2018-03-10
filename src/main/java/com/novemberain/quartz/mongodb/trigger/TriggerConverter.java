package com.novemberain.quartz.mongodb.trigger;

import com.novemberain.quartz.mongodb.Constants;
import com.novemberain.quartz.mongodb.JobDataConverter;
import com.novemberain.quartz.mongodb.dao.JobDao;
import com.novemberain.quartz.mongodb.dao.TriggerDao;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.novemberain.quartz.mongodb.util.Keys.KEY_GROUP;
import static com.novemberain.quartz.mongodb.util.Keys.KEY_NAME;

public class TriggerConverter {

    private static final String TRIGGER_CALENDAR_NAME = "calendarName";
    private static final String TRIGGER_CLASS = "class";
    private static final String TRIGGER_DESCRIPTION = "description";
    private static final String TRIGGER_END_TIME = "endTime";
    private static final String TRIGGER_FINAL_FIRE_TIME = "finalFireTime";
    private static final String TRIGGER_FIRE_INSTANCE_ID = "fireInstanceId";
    private static final String TRIGGER_MISFIRE_INSTRUCTION = "misfireInstruction";
    private static final String TRIGGER_PREVIOUS_FIRE_TIME = "previousFireTime";
    private static final String TRIGGER_PRIORITY = "priority";
    private static final String TRIGGER_START_TIME = "startTime";

    private static final Logger log = LoggerFactory.getLogger(TriggerConverter.class);

    private JobDao jobDao;
    private final JobDataConverter jobDataConverter;

    public TriggerConverter(JobDao jobDao, JobDataConverter jobDataConverter) {
        this.jobDao = jobDao;
        this.jobDataConverter = jobDataConverter;
    }

    /**
     * Converts trigger into document.
     * Depending on the config, job data map can be stored
     * as a {@code base64} encoded (default) or plain object.
     */
    public Document toDocument(OperableTrigger newTrigger, ObjectId jobId)
            throws JobPersistenceException {
        Document trigger = convertToBson(newTrigger, jobId);
        jobDataConverter.toDocument(newTrigger.getJobDataMap(), trigger);

        TriggerPropertiesConverter tpd = TriggerPropertiesConverter.getConverterFor(newTrigger);
        trigger = tpd.injectExtraPropertiesForInsert(newTrigger, trigger);
        return trigger;
    }

    /**
     * Restore trigger from Mongo Document.
     *
     * @param triggerKey {@link TriggerKey} instance.
     * @param triggerDoc mongo {@link Document} to read from.
     * @return trigger from Document or null when trigger has no associated job
     * @throws JobPersistenceException if could not construct trigger instance
     * or could not deserialize job data map.
     */
    public OperableTrigger toTrigger(TriggerKey triggerKey, Document triggerDoc)
            throws JobPersistenceException {
        OperableTrigger trigger = toTriggerWithOptionalJob(triggerKey, triggerDoc);
        if ( trigger.getJobKey() == null) {
            return null;
        }
        return trigger;
    }

    /**
     * Restore trigger from Mongo Document.
     *
     * @param triggerKey {@link TriggerKey} instance.
     * @param triggerDoc mongo {@link Document} to read from.
     * @return trigger from Document even if no associated job exists
     * @throws JobPersistenceException if could not construct trigger instance
     * or could not deserialize job data map.
     */
    public OperableTrigger toTriggerWithOptionalJob(TriggerKey triggerKey, Document triggerDoc) throws JobPersistenceException {
        OperableTrigger trigger = createNewInstance(triggerDoc);

        TriggerPropertiesConverter tpd = TriggerPropertiesConverter.getConverterFor(trigger);

        loadCommonProperties(triggerKey, triggerDoc, trigger);

        jobDataConverter.toJobData(triggerDoc, trigger.getJobDataMap());

        loadStartAndEndTimes(triggerDoc, trigger);

        tpd.setExtraPropertiesAfterInstantiation(trigger, triggerDoc);

        Object jobId = triggerDoc.get(Constants.TRIGGER_JOB_ID);
        Document job = jobDao.getById(jobId);

        if (job != null) {
            trigger.setJobKey(new JobKey(job.getString(KEY_NAME), job.getString(KEY_GROUP)));
        }
        return trigger;
    }

    public OperableTrigger toTrigger(Document doc) throws JobPersistenceException {
        TriggerKey key = new TriggerKey(doc.getString(KEY_NAME), doc.getString(KEY_GROUP));
        return toTrigger(key, doc);
    }

    public OperableTrigger toTriggerWithOptionalJob(Document doc) throws JobPersistenceException {
        TriggerKey key = new TriggerKey(doc.getString(KEY_NAME), doc.getString(KEY_GROUP));
        return toTriggerWithOptionalJob(key, doc);
    }

    private Document convertToBson(OperableTrigger newTrigger, ObjectId jobId) {
        Document trigger = new Document();
        trigger.put(Constants.TRIGGER_STATE, Constants.STATE_WAITING);
        trigger.put(TRIGGER_CALENDAR_NAME, newTrigger.getCalendarName());
        trigger.put(TRIGGER_CLASS, newTrigger.getClass().getName());
        trigger.put(TRIGGER_DESCRIPTION, newTrigger.getDescription());
        trigger.put(TRIGGER_END_TIME, newTrigger.getEndTime());
        trigger.put(TRIGGER_FINAL_FIRE_TIME, newTrigger.getFinalFireTime());
        trigger.put(TRIGGER_FIRE_INSTANCE_ID, newTrigger.getFireInstanceId());
        trigger.put(Constants.TRIGGER_JOB_ID, jobId);
        trigger.put(KEY_NAME, newTrigger.getKey().getName());
        trigger.put(KEY_GROUP, newTrigger.getKey().getGroup());
        trigger.put(TRIGGER_MISFIRE_INSTRUCTION, newTrigger.getMisfireInstruction());
        trigger.put(Constants.TRIGGER_NEXT_FIRE_TIME, newTrigger.getNextFireTime());
        trigger.put(TRIGGER_PREVIOUS_FIRE_TIME, newTrigger.getPreviousFireTime());
        trigger.put(TRIGGER_PRIORITY, newTrigger.getPriority());
        trigger.put(TRIGGER_START_TIME, newTrigger.getStartTime());
        return trigger;
    }

    private OperableTrigger createNewInstance(Document triggerDoc) throws JobPersistenceException {
        String triggerClassName = triggerDoc.getString(TRIGGER_CLASS);
        try {
            @SuppressWarnings("unchecked")
            Class<OperableTrigger> triggerClass = (Class<OperableTrigger>) getTriggerClassLoader()
                    .loadClass(triggerClassName);
            return triggerClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException("Could not find trigger class " + triggerClassName);
        } catch (Exception e) {
            throw new JobPersistenceException("Could not instantiate trigger class " + triggerClassName);
        }
    }

    private ClassLoader getTriggerClassLoader() {
        return Job.class.getClassLoader();
    }

    private void loadCommonProperties(TriggerKey triggerKey, Document triggerDoc, OperableTrigger trigger) {
        trigger.setKey(triggerKey);
        trigger.setCalendarName(triggerDoc.getString(TRIGGER_CALENDAR_NAME));
        trigger.setDescription(triggerDoc.getString(TRIGGER_DESCRIPTION));
        trigger.setFireInstanceId(triggerDoc.getString(TRIGGER_FIRE_INSTANCE_ID));
        trigger.setMisfireInstruction(triggerDoc.getInteger(TRIGGER_MISFIRE_INSTRUCTION));
        trigger.setNextFireTime(triggerDoc.getDate(Constants.TRIGGER_NEXT_FIRE_TIME));
        trigger.setPreviousFireTime(triggerDoc.getDate(TRIGGER_PREVIOUS_FIRE_TIME));
        trigger.setPriority(triggerDoc.getInteger(TRIGGER_PRIORITY));
    }

    private void loadStartAndEndTimes(Document triggerDoc, OperableTrigger trigger) {
        loadStartAndEndTime(triggerDoc, trigger);
    }

    private void loadStartAndEndTime(Document triggerDoc, OperableTrigger trigger) {
        try {
            trigger.setStartTime(triggerDoc.getDate(TRIGGER_START_TIME));
            trigger.setEndTime(triggerDoc.getDate(TRIGGER_END_TIME));
        } catch (IllegalArgumentException e) {
            //Ignore illegal arg exceptions thrown by triggers doing JIT validation of start and endtime
            log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e);
        }
    }
}
