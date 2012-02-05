package com.mulesoft.quartz.mongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.junit.Before;
import org.quartz.simpl.SimpleClassLoadHelper;

public class MongoDBJobStoreTest {
  protected MongoDBJobStore store;
  protected DBCollection jobsCollection;
  protected DBCollection triggersCollection;

  public MongoDBJobStoreTest() {
  }

  public DBCollection getJobsCollection() {
    return jobsCollection;
  }

  public MongoDBJobStore getStore() {
    return store;
  }

  public DBCollection getTriggersCollection() {
    return triggersCollection;
  }

  @Before
  public void setUpJobStore() throws Exception {
    Mongo mongo = new Mongo("127.0.0.1");
    DB db = mongo.getDB("quartz");
    db.getCollection("quartz_jobs").drop();
    db.getCollection("quartz_triggers").drop();
    db.getCollection("quartz_locks").drop();

    store = new MongoDBJobStore();
    store.setInstanceName("test");
    store.setDbName("quartz");
    store.setAddresses("localhost");
    store.initialize(new SimpleClassLoadHelper(), null);
    jobsCollection = store.getJobCollection();
    triggersCollection = store.getTriggerCollection();
  }
}