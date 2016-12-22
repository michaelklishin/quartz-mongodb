package com.novemberain.quartz.mongodb

import com.mongodb.MongoClient
import com.mongodb.MongoClientOptions
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import org.bson.Document

class MongoHelper {

    static testDatabaseName = 'quartz_mongodb_test'

    static DEFAULT_MONGO_PORT = 12345

    static MongoClient client = new MongoClient(
            new ServerAddress('localhost', DEFAULT_MONGO_PORT),
            MongoClientOptions.builder()
                    .writeConcern(WriteConcern.JOURNALED)
                    .build())

    static MongoDatabase testDatabase = client.getDatabase(testDatabaseName)

    static Map<String,MongoCollection<Document>> collections = [
            calendars    : testDatabase.getCollection('quartz_calendars'),
            locks        : testDatabase.getCollection('quartz_locks'),
            jobs         : testDatabase.getCollection('quartz_jobs'),
            jobGroups    : testDatabase.getCollection('quartz_paused_job_groups'),
            schedulers   : testDatabase.getCollection('quartz_schedulers'),
            triggers     : testDatabase.getCollection('quartz_triggers'),
            triggerGroups: testDatabase.getCollection('quartz_paused_trigger_groups')
    ]

    static def dropTestDB() {
        testDatabase.drop()
    }

    static def clearColl(String colKey) {
        collections[colKey].deleteMany(new Document())
    }

    static def purgeCollections() {
        //Remove all data from Quartz MongoDB collections.
        clearColl('triggers')
        clearColl('jobs')
        clearColl('locks')
        clearColl('calendars')
        clearColl('schedulers')
        clearColl('triggerGroups')
        clearColl('jobGroups')
    }

    /**
     * Adds a new scheduler entry created from given map.
     */
    static def addScheduler(Map dataMap) {
        collections['schedulers'].insertOne(new Document(dataMap))
    }

    /**
     * Adds a new Job entry created from given map.
     */
    static def addJob(Map dataMap) {
        collections['jobs'].insertOne(new Document(dataMap))
    }

    /**
     * Adds a new Lock entry created from given map.
     */
    static def addLock(Map dataMap) {
        collections['locks'].insertOne(new Document(dataMap))
    }

    /**
     * Adds a new Trigger entry created from given map.
     */
    static def addTrigger(Map dataMap) {
        collections['triggers'].insertOne(new Document(dataMap))
    }

    /**
     * Return number of elements in a collection.
     */
    static def getCount(String col) {
        collections[col].count()
    }

    /**
     * Return calendars collection as MongoCollection.
     */
    static def getCalendarsColl() {
        collections['calendars']
    }

    /**
     * Return locks collection as MongoCollection.
     */
    static def getLocksColl() {
        collections['locks']
    }

    /**
     * Return schedulers collection as MongoCollection.
     */
    static def getSchedulersColl() {
        collections['schedulers']
    }

    static def getTriggersColl() {
        collections['triggers']
    }

    /**
     * Return the first document from given collection.
     */
    static def Document getFirst(String col) {
        getFirst(col, [:])
    }

    static def Document getFirst(String col, Map amap) {
        collections[col].find(new Document(amap)).first()
    }

    /**
     * Return all documents from given collection.
     */
    static def Collection<Document> findAll(String col) {
        collections[col].find(new Document()).into([])
    }
}
