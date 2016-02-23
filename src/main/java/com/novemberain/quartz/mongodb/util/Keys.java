package com.novemberain.quartz.mongodb.util;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.novemberain.quartz.mongodb.Constants;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.quartz.utils.Key;

import java.util.Date;

public class Keys {

    public static final String KEY_NAME = "keyName";
    public static final String KEY_GROUP = "keyGroup";
    public static final Bson KEY_AND_GROUP_FIELDS = Projections.include(KEY_GROUP, KEY_NAME);

    public static Bson createLockFilter(JobDetail job) {
        return Filters.and(
                Filters.eq(KEY_NAME, "jobconcurrentlock:" + job.getKey().getName()),
                Filters.eq(KEY_GROUP, job.getKey().getGroup()));
    }

    public static Bson toFilter(Key<?> key) {
        return Filters.and(
                Filters.eq(KEY_NAME, key.getName()),
                Filters.eq(KEY_GROUP, key.getGroup()));
    }

    public static JobKey toJobKey(Document dbo) {
        return new JobKey(dbo.getString(KEY_NAME), dbo.getString(KEY_GROUP));
    }

    public static TriggerKey toTriggerKey(Document dbo) {
        return new TriggerKey(dbo.getString(KEY_NAME), dbo.getString(KEY_GROUP));
    }

    public static Document convertToBson(JobDetail newJob, JobKey key) {
        Document job = new Document();
        job.put(KEY_NAME, key.getName());
        job.put(KEY_GROUP, key.getGroup());
        job.put(KEY_NAME, key.getName());
        job.put(KEY_GROUP, key.getGroup());
        job.put(Constants.JOB_DESCRIPTION, newJob.getDescription());
        job.put(Constants.JOB_CLASS, newJob.getJobClass().getName());
        job.put(Constants.JOB_DURABILITY, newJob.isDurable());
        job.putAll(newJob.getJobDataMap());
        return job;
    }

    public static Document convertToBson(OperableTrigger newTrigger, ObjectId jobId) {
        Document trigger = new Document();
        trigger.put(Constants.TRIGGER_STATE, Constants.STATE_WAITING);
        trigger.put(Constants.TRIGGER_CALENDAR_NAME, newTrigger.getCalendarName());
        trigger.put(Constants.TRIGGER_CLASS, newTrigger.getClass().getName());
        trigger.put(Constants.TRIGGER_DESCRIPTION, newTrigger.getDescription());
        trigger.put(Constants.TRIGGER_END_TIME, newTrigger.getEndTime());
        trigger.put(Constants.TRIGGER_FINAL_FIRE_TIME, newTrigger.getFinalFireTime());
        trigger.put(Constants.TRIGGER_FIRE_INSTANCE_ID, newTrigger.getFireInstanceId());
        trigger.put(Constants.TRIGGER_JOB_ID, jobId);
        trigger.put(KEY_NAME, newTrigger.getKey().getName());
        trigger.put(KEY_GROUP, newTrigger.getKey().getGroup());
        trigger.put(Constants.TRIGGER_MISFIRE_INSTRUCTION, newTrigger.getMisfireInstruction());
        trigger.put(Constants.TRIGGER_NEXT_FIRE_TIME, newTrigger.getNextFireTime());
        trigger.put(Constants.TRIGGER_PREVIOUS_FIRE_TIME, newTrigger.getPreviousFireTime());
        trigger.put(Constants.TRIGGER_PRIORITY, newTrigger.getPriority());
        trigger.put(Constants.TRIGGER_START_TIME, newTrigger.getStartTime());
        return trigger;
    }

    public static Document lockToBson(Document doc) {
        Document lock = new Document();
        lock.put(KEY_NAME, doc.get(KEY_NAME));
        lock.put(KEY_GROUP, doc.get(KEY_GROUP));
        return lock;
    }

    public static Document createTriggerDbLock(Document doc, String instanceId) {
        Document lock = lockToBson(doc);
        lock.put(Constants.LOCK_INSTANCE_ID, instanceId);
        lock.put(Constants.LOCK_TIME, new Date());
        return lock;
    }
}
