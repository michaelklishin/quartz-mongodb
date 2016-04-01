package com.novemberain.quartz.mongodb.dao;

import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import com.novemberain.quartz.mongodb.util.Clock;
import com.novemberain.quartz.mongodb.util.Keys;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

import static com.novemberain.quartz.mongodb.Constants.LOCK_INSTANCE_ID;
import static com.novemberain.quartz.mongodb.util.Keys.createJobLock;
import static com.novemberain.quartz.mongodb.util.Keys.createJobLockFilter;
import static com.novemberain.quartz.mongodb.util.Keys.createTriggerLock;

public class LocksDao {

    private static final Logger log = LoggerFactory.getLogger(LocksDao.class);

    private final MongoCollection<Document> locksCollection;
    private Clock clock;
    public final String instanceId;

    public LocksDao(MongoCollection<Document> locksCollection, Clock clock, String instanceId) {
        this.locksCollection = locksCollection;
        this.clock = clock;
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
        Document lock = createJobLock(job.getKey(), instanceId, clock.now());
        insertLock(lock);
    }

    public void lockTrigger(OperableTrigger trigger) {
        log.info("Inserting lock for trigger {}", trigger.getKey());
        Document lock = createTriggerLock(trigger.getKey(), instanceId, clock.now());
        insertLock(lock);
    }

    /**
     * Lock given trigger iff its <b>lockTime</b> haven't changed.
     *
     * <p>Update is performed using "Update document if current" pattern
     * to update iff document in DB hasn't changed - haven't been relocked
     * by other scheduler.</p>
     *
     * @param key         identifies trigger lock
     * @param lockTime    expected current lockTime
     * @return false when not found or caught an exception
     */
    public boolean relock(TriggerKey key, Date lockTime) {
        UpdateResult updateResult;
        try {
            updateResult = locksCollection.withWriteConcern(WriteConcern.FSYNCED)
                    .updateOne(
                            Keys.createRelockFilter(key, lockTime),
                            Keys.createLockUpdateDocument(instanceId, clock.now()));
        } catch (MongoException e) {
            log.error("Relock failed because: " + e.getMessage(), e);
            return false;
        }

        if (updateResult.getModifiedCount() == 1) {
            log.info("Scheduler {} relocked the trigger: {}", instanceId, key);
            return true;
        }
        log.info("Scheduler {} couldn't relock the trigger {} with lock time: {}",
                instanceId, key, lockTime.getTime());
        return false;
    }

    public void remove(Document lock) {
        locksCollection.deleteMany(lock);
    }

    /**
     * Unlock the trigger if it still belongs to the current scheduler.
     *
     * @param trigger    to unlock
     */
    public void unlockTrigger(OperableTrigger trigger) {
        log.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
        remove(Keys.toFilter(trigger.getKey(), instanceId));
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
