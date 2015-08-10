package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
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

  public static BasicDBObject keyToDBObject(Key<?> key) {
    BasicDBObject job = new BasicDBObject();
    job.put(KEY_NAME, key.getName());
    job.put(KEY_GROUP, key.getGroup());
    return job;
  }

  public static JobKey dbObjectToJobKey(DBObject dbo) {
    return new JobKey((String) dbo.get(KEY_NAME), (String) dbo.get(KEY_GROUP));
  }

  public static TriggerKey dbObjectToTriggerKey(DBObject dbo) {
    return new TriggerKey((String) dbo.get(KEY_NAME), (String) dbo.get(KEY_GROUP));
  }

  public static BasicDBObject convertToBson(JobDetail newJob, JobKey key) {
    BasicDBObject job = new BasicDBObject();
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

  public static BasicDBObject convertToBson(OperableTrigger newTrigger, ObjectId jobId) {
    BasicDBObject trigger = new BasicDBObject();
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

  public static BasicDBObject lockToBson(DBObject dbObj) {
    BasicDBObject lock = new BasicDBObject();
    lock.put(KEY_NAME, dbObj.get(KEY_NAME));
    lock.put(KEY_GROUP, dbObj.get(KEY_GROUP));
    return lock;
  }

  public static BasicDBObject createTriggerDbLock(DBObject dbObj, String instanceId) {
    BasicDBObject lock = lockToBson(dbObj);
    lock.put(Constants.LOCK_INSTANCE_ID, instanceId);
    lock.put(Constants.LOCK_TIME, new Date());
    return lock;
  }
}
