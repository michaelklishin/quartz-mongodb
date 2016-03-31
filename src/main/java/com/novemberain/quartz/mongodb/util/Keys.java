package com.novemberain.quartz.mongodb.util;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.novemberain.quartz.mongodb.Constants;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.utils.Key;

import java.util.Date;

public class Keys {

    public static final String KEY_NAME = "keyName";
    public static final String KEY_GROUP = "keyGroup";
    public static final Bson KEY_AND_GROUP_FIELDS = Projections.include(KEY_GROUP, KEY_NAME);

    public static Bson createJobLockFilter(JobKey key) {
        return createLockFilter(key, getJobLockName(key));
    }

    public static Bson createTriggerLockFilter(TriggerKey triggerKey) {
        return createLockFilter(triggerKey, triggerKey.getName());
    }

    public static Bson createRelockFilter(TriggerKey key, Date lockTime) {
        return Filters.and(
                createTriggerLockFilter(key),
                createLockTimeFilter(lockTime));
    }

    public static Document createJobLock(JobKey jobKey, String instanceId, Date lockTime) {
        return createLock(instanceId, jobKey.getGroup(), getJobLockName(jobKey), lockTime);
    }

    public static Document createTriggerLock(TriggerKey triggerKey, String instanceId, Date lockTime) {
        return createLock(instanceId, triggerKey.getGroup(), triggerKey.getName(), lockTime);
    }

    public static Bson toFilter(Key<?> key) {
        return Filters.and(
                Filters.eq(KEY_GROUP, key.getGroup()),
                Filters.eq(KEY_NAME, key.getName()));
    }

    public static JobKey toJobKey(Document dbo) {
        return new JobKey(dbo.getString(KEY_NAME), dbo.getString(KEY_GROUP));
    }

    public static TriggerKey toTriggerKey(Document dbo) {
        return new TriggerKey(dbo.getString(KEY_NAME), dbo.getString(KEY_GROUP));
    }

    private static Document createLock(String instanceId, String group, String name, Date lockTime) {
        Document lock = new Document();
        lock.put(KEY_GROUP, group);
        lock.put(KEY_NAME, name);
        lock.put(Constants.LOCK_INSTANCE_ID, instanceId);
        lock.put(Constants.LOCK_TIME, lockTime);
        return lock;
    }

    public static Document createLockUpdateDocument(String instanceId, Date newLockTime) {
        return new Document("$set", new Document()
                .append(Constants.LOCK_INSTANCE_ID, instanceId)
                .append(Constants.LOCK_TIME, newLockTime));
    }

    private static <T> Bson createLockFilter(Key<T> key, String name) {
        return Filters.and(
                Filters.eq(KEY_GROUP, key.getGroup()),
                Filters.eq(KEY_NAME, name));
    }

    private static Bson createLockTimeFilter(Date lockTime) {
        return Filters.eq(Constants.LOCK_TIME, lockTime);
    }

    private static String getJobLockName(JobKey jobKey) {
        return "jobconcurrentlock:" + jobKey.getName();
    }
}
