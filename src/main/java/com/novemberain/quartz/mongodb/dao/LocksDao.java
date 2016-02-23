package com.novemberain.quartz.mongodb.dao;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.DeleteResult;
import com.novemberain.quartz.mongodb.Keys;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.novemberain.quartz.mongodb.Constants.LOCK_INSTANCE_ID;

public class LocksDao {

    private final MongoCollection<Document> locksCollection;
    private final String instanceId;

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

    public Document findLock(Bson lock) {
        return locksCollection.find(lock).first();
    }

    public void insertLock(Document lock) {
        // A lock needs to be written with FSYNCED to be 100% effective across multiple servers
        locksCollection.withWriteConcern(WriteConcern.FSYNCED).insertOne(lock);
    }

    public DeleteResult remove(Bson filter) {
        return locksCollection.deleteMany(filter);
    }

    public void remove(Document lock) {
        locksCollection.deleteMany(lock);
    }
}
