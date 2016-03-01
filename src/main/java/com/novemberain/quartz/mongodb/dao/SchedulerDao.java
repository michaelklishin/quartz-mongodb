package com.novemberain.quartz.mongodb.dao;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.novemberain.quartz.mongodb.util.Clock;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerDao {

    private static final Logger log = LoggerFactory.getLogger(SchedulerDao.class);

    private static final String SCHEDULER_NAME_FIELD = "schedulerName";
    private static final String INSTANCE_ID_FIELD = "instanceId";
    private static final String LAST_CHECKIN_TIME_FIELD = "lastCheckinTime";
    private static final String CHECKIN_INTERVAL_FIELD = "checkinInterval";

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
        this.schedulerFilter = Filters.and(
                Filters.eq(SCHEDULER_NAME_FIELD, schedulerName),
                Filters.eq(INSTANCE_ID_FIELD, instanceId));
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
        log.info("Cluster checkin scheduler {}:{}, checkin time: {}, interval: {}",
                schedulerName, instanceId, lastCheckinTime, clusterCheckinIntervalMillis);

        // If not found Mongo will create a new entry with content from filter and update.
        Document update = createUpdateClause(lastCheckinTime);

        // An entry needs to be written with FSYNCED to be 100% effective across multiple servers
        UpdateResult result = schedulerCollection
                //TODO shouldn't be WriteConcern.REPLICA_ACKNOWLEDGED?
                .withWriteConcern(WriteConcern.FSYNCED)
                .updateOne(schedulerFilter, update, new UpdateOptions().upsert(true));

        log.info("Node {}:{} check-in result: {}", schedulerName, instanceId, result);
    }

    private Document createUpdateClause(long lastCheckinTime) {
        return new Document("$set", new Document()
                    .append(LAST_CHECKIN_TIME_FIELD, lastCheckinTime)
                    .append(CHECKIN_INTERVAL_FIELD, clusterCheckinIntervalMillis));
    }
}
