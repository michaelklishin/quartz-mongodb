package com.novemberain.quartz.mongodb.dao;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.novemberain.quartz.mongodb.util.Keys;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.novemberain.quartz.mongodb.Constants.LOCK_INSTANCE_ID;
import static com.novemberain.quartz.mongodb.util.Keys.createJobLock;
import static com.novemberain.quartz.mongodb.util.Keys.createJobLockFilter;
import static com.novemberain.quartz.mongodb.util.Keys.createTriggerLock;

public class LocksDao {

    private static final Logger log = LoggerFactory.getLogger(LocksDao.class);

    private final MongoCollection<Document> locksCollection;
    public final String instanceId;

    public LocksDao(MongoCollection<Document> locksCollection, String instanceId) {
        this.locksCollection = locksCollection;
        this.instanceId = instanceId;
    }

    public MongoCollection<Document> getCollection() {
        return locksCollection;
    }

    public void createIndex() {
        locksCollection.createIndex(
                Keys.KEY_AND_GROUP_FIELDS,
                new IndexOptions().unique(true));

        // Need this to stop table scan when removing all locks
        locksCollection.createIndex(Projections.include(LOCK_INSTANCE_ID));

        // remove all locks for this instance on startup
        locksCollection.deleteMany(Filters.eq(LOCK_INSTANCE_ID, instanceId));

    }

    public void dropIndex() {
        locksCollection.dropIndex("keyName_1_keyGroup_1");
    }

    public Document findJobLock(JobKey job) {
        Bson filter = createJobLockFilter(job);
        return locksCollection.find(filter).first();
    }

    public Document findTriggerLock(TriggerKey trigger) {
        Bson filter = Keys.createTriggerLockFilter(trigger);
        return locksCollection.find(filter).first();
    }

    public void lockJob(JobDetail job) {
        log.debug("Inserting lock for job {}", job.getKey());
        Document lock = createJobLock(job.getKey(), instanceId);
        insertLock(lock);
    }

    public void lockTrigger(OperableTrigger trigger) {
        log.info("Inserting lock for trigger {}", trigger.getKey());
        Document lock = createTriggerLock(trigger.getKey(), instanceId);
        insertLock(lock);
    }

    public void remove(Document lock) {
        locksCollection.deleteMany(lock);
    }

    public void unlockTrigger(OperableTrigger trigger) {
        log.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
        Bson lock = Keys.toFilter(trigger.getKey());

        // Comment this out, as expired trigger locks should be deleted by any another instance
        // lock.put(LOCK_INSTANCE_ID, instanceId);

        remove(lock);
        log.info("Trigger lock {}.{} removed.", trigger.getKey(), instanceId);
    }

    public void unlockJob(JobDetail job) {
        log.debug("Removing lock for job {}", job.getKey());
        remove(createJobLockFilter(job.getKey()));
    }

    private void insertLock(Document lock) {
        // A lock needs to be written with FSYNCED to be 100% effective across multiple servers
        locksCollection.withWriteConcern(WriteConcern.FSYNCED).insertOne(lock);
    }

    private void remove(Bson filter) {
        locksCollection.deleteMany(filter);
    }
}
