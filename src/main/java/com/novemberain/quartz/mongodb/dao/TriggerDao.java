package com.novemberain.quartz.mongodb.dao;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.novemberain.quartz.mongodb.Constants;
import com.novemberain.quartz.mongodb.Keys;
import com.novemberain.quartz.mongodb.MongoDBJobStore;
import com.novemberain.quartz.mongodb.QueryHelper;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.mongodb.client.model.Sorts.ascending;
import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;
import static com.novemberain.quartz.mongodb.Keys.toFilter;

public class TriggerDao {

    private MongoCollection<Document> triggerCollection;
    private QueryHelper queryHelper;

    public TriggerDao(MongoCollection<Document> triggerCollection, QueryHelper queryHelper) {
        this.triggerCollection = triggerCollection;
        this.queryHelper = queryHelper;
    }

    public void createIndex() {
        triggerCollection.createIndex(MongoDBJobStore.KEY_AND_GROUP_FIELDS,
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

    public FindIterable<Document> findByJobId(Object jobId) {
        return triggerCollection.find(Filters.eq(Constants.TRIGGER_JOB_ID, jobId));
    }

    public FindIterable<Document> findEligibleToRun(Bson query) {
        return triggerCollection.find(query).sort(ascending(Constants.TRIGGER_NEXT_FIRE_TIME));
    }

    public Document findTrigger(Bson filter) {
        return triggerCollection.find(filter).first();
    }

    public Document findTrigger(TriggerKey key) {
        return findTrigger(toFilter(key));
    }

    public int getCount() {
        return (int) triggerCollection.count();
    }

    public long getCount(Bson query) {
        return triggerCollection.count(query);
    }

    public List<String> getGroupNames() {
        return triggerCollection.distinct(KEY_GROUP, String.class).into(new ArrayList<String>());
    }

    public TriggerState getState(TriggerKey triggerKey) {
        Document doc = findTrigger(triggerKey);
        return triggerStateForValue(doc.getString(Constants.TRIGGER_STATE));
    }

    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) {
        Set<TriggerKey> keys = new HashSet<TriggerKey>();
        Bson query = queryHelper.matchingKeysConditionFor(matcher);
        for (Document doc : triggerCollection.find(query).projection(MongoDBJobStore.KEY_AND_GROUP_FIELDS)) {
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

    private TriggerState triggerStateForValue(String ts) {
        if (ts == null) {
            return TriggerState.NONE;
        }

        if (ts.equals(Constants.STATE_DELETED)) {
            return TriggerState.NONE;
        }

        if (ts.equals(Constants.STATE_COMPLETE)) {
            return TriggerState.COMPLETE;
        }

        if (ts.equals(Constants.STATE_PAUSED)) {
            return TriggerState.PAUSED;
        }

        if (ts.equals(Constants.STATE_PAUSED_BLOCKED)) {
            return TriggerState.PAUSED;
        }

        if (ts.equals(Constants.STATE_ERROR)) {
            return TriggerState.ERROR;
        }

        if (ts.equals(Constants.STATE_BLOCKED)) {
            return TriggerState.BLOCKED;
        }

        // waiting or acquired
        return TriggerState.NORMAL;
    }

    public Bson updateThatSetsTriggerStateTo(String state) {
        return new Document("$set", new Document(Constants.TRIGGER_STATE, state));
    }

    public MongoCollection<Document> getCollection() {
        return triggerCollection;
    }
}
