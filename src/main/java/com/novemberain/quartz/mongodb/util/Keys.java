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

    public enum LockType { t, j }

    public static final String LOCK_TYPE = "type";
    public static final String KEY_NAME = "keyName";
    public static final String KEY_GROUP = "keyGroup";

    public static final Bson KEY_AND_GROUP_FIELDS = Projections.include(KEY_GROUP, KEY_NAME);

    public static Bson createJobLockFilter(JobKey key) {
        return createLockFilter(LockType.j, key);
    }

    public static Bson createTriggerLockFilter(TriggerKey triggerKey) {
        return createLockFilter(LockType.t, triggerKey);
    }

    public static Bson createTriggersLocksFilter(String instanceId) {
        return Filters.and(
                Filters.eq(Constants.LOCK_INSTANCE_ID, instanceId),
                Filters.eq(LOCK_TYPE, LockType.t.name()));
    }

    public static Bson createLockRefreshFilter(String instanceId) {
        return Filters.eq(Constants.LOCK_INSTANCE_ID, instanceId);
    }

    public static Bson createRelockFilter(TriggerKey key, Date lockTime) {
        return Filters.and(
                createTriggerLockFilter(key),
                createLockTimeFilter(lockTime));
    }

    public static Document createJobLock(JobKey jobKey, String instanceId, Date lockTime) {
        return createLock(LockType.j, instanceId, jobKey, lockTime);
    }

    public static Document createTriggerLock(TriggerKey triggerKey, String instanceId, Date lockTime) {
        return createLock(LockType.t, instanceId, triggerKey, lockTime);
    }

    public static Bson toFilter(Key<?> key) {
        return Filters.and(
                Filters.eq(KEY_GROUP, key.getGroup()),
                Filters.eq(KEY_NAME, key.getName()));
    }

    public static Bson toFilter(Key<?> key, String instanceId) {
        return Filters.and(
                Filters.eq(KEY_GROUP, key.getGroup()),
                Filters.eq(KEY_NAME, key.getName()),
                Filters.eq(Constants.LOCK_INSTANCE_ID, instanceId));
    }

    public static JobKey toJobKey(Document dbo) {
        return new JobKey(dbo.getString(KEY_NAME), dbo.getString(KEY_GROUP));
    }

    public static TriggerKey toTriggerKey(Document dbo) {
        return new TriggerKey(dbo.getString(KEY_NAME), dbo.getString(KEY_GROUP));
    }

    private static Document createLock(LockType type, String instanceId, Key<?> key, Date lockTime) {
        Document lock = new Document();
        lock.put(LOCK_TYPE, type.name());
        lock.put(KEY_GROUP, key.getGroup());
        lock.put(KEY_NAME, key.getName());
        lock.put(Constants.LOCK_INSTANCE_ID, instanceId);
        lock.put(Constants.LOCK_TIME, lockTime);
        return lock;
    }

    public static Document createLockUpdateDocument(String instanceId, Date newLockTime) {
        return new Document("$set", new Document()
                .append(Constants.LOCK_INSTANCE_ID, instanceId)
                .append(Constants.LOCK_TIME, newLockTime));
    }

    private static <T> Bson createLockFilter(LockType type, Key<T> key) {
        return Filters.and(
                Filters.eq(LOCK_TYPE, type.name()),
                Filters.eq(KEY_GROUP, key.getGroup()),
                Filters.eq(KEY_NAME, key.getName()));
    }

    private static Bson createLockTimeFilter(Date lockTime) {
        return Filters.eq(Constants.LOCK_TIME, lockTime);
    }
}
