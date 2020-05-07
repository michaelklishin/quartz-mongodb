package com.novemberain.quartz.mongodb.dao;

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
import java.util.function.Consumer;

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

        UpdateResult result = schedulerCollection
                .updateOne(schedulerFilter, update, new UpdateOptions().upsert(true));

        log.debug("Node {}:{} check-in result: {}", schedulerName, instanceId, result);
    }

    /**
     * @return Scheduler or null when not found
     */
    public Scheduler findInstance(String instanceId) {
        log.debug("Finding scheduler instance: {}", instanceId);
        Document doc = schedulerCollection
                .find(createSchedulerFilter(schedulerName, instanceId))
                .first();

        Scheduler scheduler = null;
        if (doc != null) {
            scheduler = toScheduler(doc);
            log.debug("Returning scheduler instance '{}' with last checkin time: {}",
                    scheduler.getInstanceId(), scheduler.getLastCheckinTime());
        } else {
            log.info("Scheduler instance '{}' not found.");
        }
        return scheduler;
    }

    public boolean isNotSelf(Scheduler scheduler) {
        return !instanceId.equals(scheduler.getInstanceId());
    }

    /**
     * Return all scheduler instances in ascending order by last check-in time.
     *
     * @return schedler instances ordered by last check-in time
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
     * The scheduler is selected based on its name, instanceId, and lastCheckinTime.
     * If the last check-in time is different, then it is not removed, for it might
     * have gotten back to live.
     *
     * @param instanceId       instance id
     * @param lastCheckinTime  last time scheduler has checked in
     *
     * @return when removed successfully
     */
    public boolean remove(String instanceId, long lastCheckinTime) {
        log.info("Removing scheduler: {},{},{}",
                schedulerName, instanceId, lastCheckinTime);
        DeleteResult result = schedulerCollection
                .deleteOne(createSchedulerFilter(
                        schedulerName, instanceId, lastCheckinTime));

        log.info("Result of removing scheduler ({},{},{}): {}",
                schedulerName, instanceId, lastCheckinTime, result);
        return result.getDeletedCount() == 1;
    }

    private Bson createSchedulerFilter(String schedulerName, String instanceId, long lastCheckinTime) {
        return Filters.and(
                Filters.eq(SCHEDULER_NAME_FIELD, schedulerName),
                Filters.eq(INSTANCE_ID_FIELD, instanceId),
                Filters.eq(LAST_CHECKIN_TIME_FIELD, lastCheckinTime));
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

    private Consumer<Document> createResultConverter(final List<Scheduler> schedulers) {
        return document -> schedulers.add(toScheduler(document));
    }

    private Scheduler toScheduler(Document document) {
        return new Scheduler(
                document.getString(SCHEDULER_NAME_FIELD),
                document.getString(INSTANCE_ID_FIELD),
                document.getLong(LAST_CHECKIN_TIME_FIELD),
                document.getLong(CHECKIN_INTERVAL_FIELD));
    }
}
