package com.novemberain.quartz.mongodb.util;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.novemberain.quartz.mongodb.Constants;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
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
