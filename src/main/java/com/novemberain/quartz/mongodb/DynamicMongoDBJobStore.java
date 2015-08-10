package com.novemberain.quartz.mongodb;

import clojure.lang.DynamicClassLoader;

import com.mongodb.MongoClient;

public class DynamicMongoDBJobStore extends MongoDBJobStore {

  public DynamicMongoDBJobStore() {
    super();
  }

  public DynamicMongoDBJobStore(MongoClient mongo) {
    super(mongo);
  }

  public DynamicMongoDBJobStore(final String mongoUri, final String username, final String password){
    super(mongoUri, username, password);
  }

  @Override
  protected ClassLoader getJobClassLoader() {
    // makes it possible for Quartz to load and instantiate jobs that are defined
    // using defrecord without AOT compilation.
    return new DynamicClassLoader();
  }
}
