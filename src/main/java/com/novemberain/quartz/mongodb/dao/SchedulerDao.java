package com.novemberain.quartz.mongodb.dao;

import com.mongodb.Block;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.novemberain.quartz.mongodb.cluster.Scheduler;
import com.novemberain.quartz.mongodb.util.Clock;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static com.mongodb.client.model.Sorts.ascending;

public class SchedulerDao {

    private static final Logger log = LoggerFactory.getLogger(SchedulerDao.class);

    public static final String SCHEDULER_NAME_FIELD = "schedulerName";
    public static final String INSTANCE_ID_FIELD = "instanceId";
    public static final String LAST_CHECKIN_TIME_FIELD = "lastCheckinTime";
    public static final String CHECKIN_INTERVAL_FIELD = "checkinInterval";

    public final MongoCollection<Document> schedulerCollection;

    public final String schedulerName;
    public final String instanceId;
    public final long clusterCheckinIntervalMillis;
    public final Clock clock;

    private final Bson schedulerFilter;

    public SchedulerDao(MongoCollection<Document> schedulerCollection, String schedulerName,
                        String instanceId, long clusterCheckinIntervalMillis, Clock clock) {
        this.schedulerCollection = schedulerCollection;
        this.schedulerName = schedulerName;
        this.instanceId = instanceId;
        this.schedulerFilter = createSchedulerFilter(schedulerName, instanceId);
        this.clusterCheckinIntervalMillis = clusterCheckinIntervalMillis;
        this.clock = clock;
    }

    public MongoCollection<Document> getCollection() {
        return schedulerCollection;
    }

    public void createIndex() {
        schedulerCollection.createIndex(
                Projections.include(SCHEDULER_NAME_FIELD, INSTANCE_ID_FIELD),
                new IndexOptions().unique(true));
    }

    /**
     * Checks-in in cluster to inform other nodes that its alive.
     */
    public void checkIn() {
        long lastCheckinTime = clock.millis();

        log.debug("Saving node data: name='{}', id='{}', checkin time={}, interval={}",
                schedulerName, instanceId, lastCheckinTime, clusterCheckinIntervalMillis);

        // If not found Mongo will create a new entry with content from filter and update.
        Document update = createUpdateClause(lastCheckinTime);

        // An entry needs to be written with FSYNCED to be 100% effective across multiple servers
        UpdateResult result = schedulerCollection
                //TODO shouldn't be WriteConcern.REPLICA_ACKNOWLEDGED?
                .withWriteConcern(WriteConcern.FSYNCED)
                .updateOne(schedulerFilter, update, new UpdateOptions().upsert(true));

        log.debug("Node {}:{} check-in result: {}", schedulerName, instanceId, result);
    }

    /**
     * Return all schedulers ordered ascending by last check-in time.
     * @return
     */
    public List<Scheduler> getAllByCheckinTime() {
        final List<Scheduler> schedulers = new LinkedList<Scheduler>();
        schedulerCollection
                .find()
                .sort(ascending(LAST_CHECKIN_TIME_FIELD))
                .forEach(createResultConverter(schedulers));
        return schedulers;
    }

    /**
     * Remove selected scheduler instance entry from database.
     *
     * @param schedulerName    scheduler' name
     * @param instanceId       instance id
     */
    public void remove(String schedulerName, String instanceId) {
        log.info("Removing scheduler: {}:{}", schedulerName, instanceId);
        DeleteResult result = schedulerCollection
                .withWriteConcern(WriteConcern.FSYNCED)
                .deleteOne(createSchedulerFilter(schedulerName, instanceId));
        log.debug("Result of removing {}:{}: {}", schedulerName, instanceId, result);
    }

    private Bson createSchedulerFilter(String schedulerName, String instanceId) {
        return Filters.and(
                Filters.eq(SCHEDULER_NAME_FIELD, schedulerName),
                Filters.eq(INSTANCE_ID_FIELD, instanceId));
    }

    private Document createUpdateClause(long lastCheckinTime) {
        return new Document("$set", new Document()
                    .append(LAST_CHECKIN_TIME_FIELD, lastCheckinTime)
                    .append(CHECKIN_INTERVAL_FIELD, clusterCheckinIntervalMillis));
    }

    private Block<Document> createResultConverter(final List<Scheduler> schedulers) {
        return new Block<Document>() {
            @Override
            public void apply(Document document) {
                schedulers.add(toScheduler(document));
            }
        };
    }

    private Scheduler toScheduler(Document document) {
        return new Scheduler(
                document.getString(SCHEDULER_NAME_FIELD),
                document.getString(INSTANCE_ID_FIELD),
                document.getLong(LAST_CHECKIN_TIME_FIELD),
                document.getLong(CHECKIN_INTERVAL_FIELD));
    }
}