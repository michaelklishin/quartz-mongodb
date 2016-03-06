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

    public static Document createJobLock(JobKey jobKey, String instanceId) {
        return createLock(instanceId, jobKey.getGroup(), getJobLockName(jobKey));
    }

    public static Document createTriggerLock(TriggerKey triggerKey, String instanceId) {
        return createLock(instanceId, triggerKey.getGroup(), triggerKey.getName());
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

    private static Document createLock(String instanceId, String group, String name) {
        Document lock = new Document();
        lock.put(KEY_GROUP, group);
        lock.put(KEY_NAME, name);
        lock.put(Constants.LOCK_INSTANCE_ID, instanceId);
        lock.put(Constants.LOCK_TIME, new Date());
        return lock;
    }

    private static <T> Bson createLockFilter(Key<T> key, String name) {
        return Filters.and(
                Filters.eq(KEY_GROUP, key.getGroup()),
                Filters.eq(KEY_NAME, name));
    }

    private static String getJobLockName(JobKey jobKey) {
        return "jobconcurrentlock:" + jobKey.getName();
    }
}
