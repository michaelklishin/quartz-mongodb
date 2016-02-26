package com.novemberain.quartz.mongodb.dao;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.novemberain.quartz.mongodb.Constants;
import com.novemberain.quartz.mongodb.trigger.TriggerConverter;
import com.novemberain.quartz.mongodb.util.Keys;
import com.novemberain.quartz.mongodb.util.QueryHelper;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.mongodb.client.model.Sorts.ascending;
import static com.novemberain.quartz.mongodb.util.Keys.KEY_GROUP;
import static com.novemberain.quartz.mongodb.util.Keys.toFilter;

public class TriggerDao {

    private static final Logger log = LoggerFactory.getLogger(TriggerDao.class);

    private MongoCollection<Document> triggerCollection;
    private QueryHelper queryHelper;
    private TriggerConverter triggerConverter;

    public TriggerDao(MongoCollection<Document> triggerCollection, QueryHelper queryHelper,
                      TriggerConverter triggerConverter) {
        this.triggerCollection = triggerCollection;
        this.queryHelper = queryHelper;
        this.triggerConverter = triggerConverter;
    }

    public void createIndex() {
        triggerCollection.createIndex(Keys.KEY_AND_GROUP_FIELDS,
                new IndexOptions().unique(true));
    }

    public void dropIndex() {
        triggerCollection.dropIndex("keyName_1_keyGroup_1");
    }

    public void clear() {
        triggerCollection.deleteMany(new Document());
    }

    public boolean exists(Bson filter) {
        return triggerCollection.count(filter) > 0;
    }

    public FindIterable<Document> findEligibleToRun(Date noLaterThanDate) {
        Bson query = createNextTriggerQuery(noLaterThanDate);
        if (log.isInfoEnabled()) {
            log.info("Found {} triggers which are eligible to be run.", getCount(query));
        }
        return triggerCollection.find(query).sort(ascending(Constants.TRIGGER_NEXT_FIRE_TIME));
    }

    public Document findTrigger(Bson filter) {
        return triggerCollection.find(filter).first();
    }

    public int getCount() {
        return (int) triggerCollection.count();
    }

    public List<String> getGroupNames() {
        return triggerCollection.distinct(KEY_GROUP, String.class).into(new ArrayList<String>());
    }

    public String getState(TriggerKey triggerKey) {
        Document doc = findTrigger(triggerKey);
        return doc.getString(Constants.TRIGGER_STATE);
    }

    public List<OperableTrigger> getTriggersForJob(Document doc) throws JobPersistenceException {
        final List<OperableTrigger> triggers = new LinkedList<OperableTrigger>();
        if (doc != null) {
            for (Document item : findByJobId(doc.get("_id"))) {
                triggers.add(triggerConverter.toTrigger(item));
            }
        }
        return triggers;
    }

    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) {
        Set<TriggerKey> keys = new HashSet<TriggerKey>();
        Bson query = queryHelper.matchingKeysConditionFor(matcher);
        for (Document doc : triggerCollection.find(query).projection(Keys.KEY_AND_GROUP_FIELDS)) {
            keys.add(Keys.toTriggerKey(doc));
        }
        return keys;
    }

    public boolean hasLastTrigger(Document job) {
        List<Document> referencedTriggers = triggerCollection
                .find(Filters.eq(Constants.TRIGGER_JOB_ID, job.get("_id")))
                .limit(2)
                .into(new ArrayList<Document>(2));
        return referencedTriggers.size() == 1;
    }

    public void insert(Document trigger, OperableTrigger offendingTrigger)
            throws ObjectAlreadyExistsException {
        try {
            triggerCollection.insertOne(trigger);
        } catch (MongoWriteException key) {
            throw new ObjectAlreadyExistsException(offendingTrigger);
        }
    }

    public void pause(TriggerKey triggerKey) {
        triggerCollection.updateOne(Keys.toFilter(triggerKey),
                updateThatSetsTriggerStateTo(Constants.STATE_PAUSED));
    }

    public void pauseAll() {
        triggerCollection.updateMany(new Document(), updateThatSetsTriggerStateTo(Constants.STATE_PAUSED));
    }

    public void pauseByJobId(ObjectId jobId) {
        triggerCollection.updateMany(new Document(Constants.TRIGGER_JOB_ID, jobId),
                updateThatSetsTriggerStateTo(Constants.STATE_PAUSED));
    }

    public void pauseGroups(List<String> groups) {
        triggerCollection.updateMany(queryHelper.inGroups(groups),
                updateThatSetsTriggerStateTo(Constants.STATE_PAUSED));
    }

    public void pauseMatching(GroupMatcher<TriggerKey> matcher) {
        triggerCollection.updateMany(
                queryHelper.matchingKeysConditionFor(matcher),
                updateThatSetsTriggerStateTo(Constants.STATE_PAUSED),
                new UpdateOptions().upsert(false));
    }

    public void remove(Bson filter) {
        triggerCollection.deleteMany(filter);
    }

    public void removeByJobId(Object id) {
        triggerCollection.deleteMany(Filters.eq(Constants.TRIGGER_JOB_ID, id));
    }

    public void replace(TriggerKey triggerKey, Document trigger) {
        triggerCollection.replaceOne(toFilter(triggerKey), trigger);
    }

    public void resume(TriggerKey triggerKey) {
        triggerCollection.updateOne(Keys.toFilter(triggerKey),
                updateThatSetsTriggerStateTo(Constants.STATE_WAITING));
    }

    public void resumeAll() {
        triggerCollection.updateMany(new Document(),
                updateThatSetsTriggerStateTo(Constants.STATE_WAITING));
    }

    public void resumeByJobId(ObjectId jobId) {
        triggerCollection.updateMany(new Document(Constants.TRIGGER_JOB_ID, jobId),
                updateThatSetsTriggerStateTo(Constants.STATE_WAITING));
    }

    public void resumeGroups(List<String> groups) {
        triggerCollection.updateMany(queryHelper.inGroups(groups),
                updateThatSetsTriggerStateTo(Constants.STATE_WAITING));
    }

    public void resumeMatching(GroupMatcher<TriggerKey> matcher) {
        triggerCollection.updateMany(
                queryHelper.matchingKeysConditionFor(matcher),
                updateThatSetsTriggerStateTo(Constants.STATE_WAITING),
                new UpdateOptions().upsert(false));
    }

    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        Document doc = findTrigger(Keys.toFilter(triggerKey));
        if (doc == null) {
            return null;
        }
        return triggerConverter.toTrigger(triggerKey, doc);
    }

    public MongoCollection<Document> getCollection() {
        return triggerCollection;
    }

    private Bson createNextTriggerQuery(Date noLaterThanDate) {
        return Filters.and(
                Filters.lte(Constants.TRIGGER_NEXT_FIRE_TIME, noLaterThanDate),
                Filters.eq(Constants.TRIGGER_STATE, Constants.STATE_WAITING));
    }

    private FindIterable<Document> findByJobId(Object jobId) {
        return triggerCollection.find(Filters.eq(Constants.TRIGGER_JOB_ID, jobId));
    }

    private Document findTrigger(TriggerKey key) {
        return findTrigger(toFilter(key));
    }

    private long getCount(Bson query) {
        return triggerCollection.count(query);
    }

    private Bson updateThatSetsTriggerStateTo(String state) {
        return new Document("$set", new Document(Constants.TRIGGER_STATE, state));
    }
}
