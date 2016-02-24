package com.novemberain.quartz.mongodb.trigger;

import com.novemberain.quartz.mongodb.Constants;
import com.novemberain.quartz.mongodb.TriggerPersistenceHelper;
import com.novemberain.quartz.mongodb.dao.JobDao;
import com.novemberain.quartz.mongodb.util.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.novemberain.quartz.mongodb.util.Keys.KEY_GROUP;
import static com.novemberain.quartz.mongodb.util.Keys.KEY_NAME;
import static com.novemberain.quartz.mongodb.util.Keys.convertToBson;

public class TriggerConverter {

    private static final Logger log = LoggerFactory.getLogger(TriggerConverter.class);

    private List<TriggerPersistenceHelper> persistenceHelpers = Arrays.asList(
            new SimpleTriggerPersistenceHelper(),
            new CalendarIntervalTriggerPersistenceHelper(),
            new CronTriggerPersistenceHelper(),
            new DailyTimeIntervalTriggerPersistenceHelper());

    private JobDao jobDao;

    public TriggerConverter(JobDao jobDao) {
        this.jobDao = jobDao;
    }

    public Document toDocument(OperableTrigger newTrigger, ObjectId jobId)
            throws JobPersistenceException {
        Document trigger = convertToBson(newTrigger, jobId);
        if (newTrigger.getJobDataMap().size() > 0) {
            try {
                String jobDataString = SerialUtils.serialize(newTrigger.getJobDataMap());
                trigger.put(Constants.JOB_DATA, jobDataString);
            } catch (IOException ioe) {
                throw new JobPersistenceException("Could not serialise job data map on the trigger for "
                        + newTrigger.getKey(), ioe);
            }
        }

        TriggerPersistenceHelper tpd = getPersistenceDelegateFor(newTrigger);
        trigger = tpd.injectExtraPropertiesForInsert(newTrigger, trigger);
        return trigger;
    }

    public OperableTrigger toTrigger(TriggerKey triggerKey, Document triggerDoc)
            throws JobPersistenceException {
        OperableTrigger trigger = createNewInstance(triggerDoc);

        TriggerPersistenceHelper tpd = getPersistenceDelegateFor(trigger);

        loadCommonProperties(triggerKey, triggerDoc, trigger);

        loadJobData(triggerDoc, trigger);

        loadStartAndEndTimes(triggerDoc, trigger);

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

    public OperableTrigger toTrigger(Document doc) throws JobPersistenceException {
        TriggerKey key = new TriggerKey(doc.getString(KEY_NAME), doc.getString(KEY_GROUP));
        return toTrigger(key, doc);
    }

    private OperableTrigger createNewInstance(Document triggerDoc) throws JobPersistenceException {
        String triggerClassName = triggerDoc.getString(Constants.TRIGGER_CLASS);
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

    private TriggerPersistenceHelper getPersistenceDelegateFor(OperableTrigger trigger) {
        TriggerPersistenceHelper result = null;

        for (TriggerPersistenceHelper d : persistenceHelpers) {
            if (d.canHandleTriggerType(trigger)) {
                result = d;
                break;
            }
        }

        return result;
    }

    private ClassLoader getTriggerClassLoader() {
        return Job.class.getClassLoader();
    }

    private void loadCommonProperties(TriggerKey triggerKey, Document triggerDoc, OperableTrigger trigger) {
        trigger.setKey(triggerKey);
        trigger.setCalendarName(triggerDoc.getString(Constants.TRIGGER_CALENDAR_NAME));
        trigger.setDescription(triggerDoc.getString(Constants.TRIGGER_DESCRIPTION));
        trigger.setFireInstanceId(triggerDoc.getString(Constants.TRIGGER_FIRE_INSTANCE_ID));
        trigger.setMisfireInstruction(triggerDoc.getInteger(Constants.TRIGGER_MISFIRE_INSTRUCTION));
        trigger.setNextFireTime(triggerDoc.getDate(Constants.TRIGGER_NEXT_FIRE_TIME));
        trigger.setPreviousFireTime(triggerDoc.getDate(Constants.TRIGGER_PREVIOUS_FIRE_TIME));
        trigger.setPriority(triggerDoc.getInteger(Constants.TRIGGER_PRIORITY));
    }

    private void loadJobData(Document triggerDoc, OperableTrigger trigger)
            throws JobPersistenceException {
        String jobDataString = triggerDoc.getString(Constants.JOB_DATA);

        if (jobDataString != null) {
            try {
                SerialUtils.deserialize(trigger.getJobDataMap(), jobDataString);
            } catch (IOException e) {
                throw new JobPersistenceException("Could not deserialize job data for trigger "
                        + triggerDoc.get(Constants.TRIGGER_CLASS));
            }
        }
    }

    private void loadStartAndEndTimes(Document triggerDoc, OperableTrigger trigger) {
        loadStartAndEndTime(triggerDoc, trigger);
        loadStartAndEndTime(triggerDoc, trigger);
    }

    private void loadStartAndEndTime(Document triggerDoc, OperableTrigger trigger) {
        try {
            trigger.setStartTime(triggerDoc.getDate(Constants.TRIGGER_START_TIME));
            trigger.setEndTime(triggerDoc.getDate(Constants.TRIGGER_END_TIME));
        } catch (IllegalArgumentException e) {
            //Ignore illegal arg exceptions thrown by triggers doing JIT validation of start and endtime
            log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e);
        }
    }
}
