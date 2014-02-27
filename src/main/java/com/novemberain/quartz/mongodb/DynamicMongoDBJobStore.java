package com.novemberain.quartz.mongodb;

import clojure.lang.DynamicClassLoader;
import com.novemberain.quartz.mongodb.MongoDBJobStore;

public class DynamicMongoDBJobStore extends MongoDBJobStore implements org.quartz.spi.JobStore {
  @Override
  protected ClassLoader getJobClassLoader() {
    // makes it possible for Quartz to load and instantiate jobs that are defined
    // using defrecord without AOT compilation.
    return new DynamicClassLoader();
  }
}
