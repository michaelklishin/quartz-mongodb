package com.novemberain.quartz.mongodb;

import com.mongodb.client.MongoClient;
import com.novemberain.quartz.mongodb.clojure.DynamicClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;

public class DynamicMongoDBJobStore extends MongoDBJobStore {

    public DynamicMongoDBJobStore() {
        super();
    }

    public DynamicMongoDBJobStore(MongoClient mongo) {
        super(mongo);
    }

    public DynamicMongoDBJobStore(String mongoUri, String username, String password) {
        super(mongoUri, username, password);
    }

    @Override
    protected ClassLoadHelper getClassLoaderHelper(ClassLoadHelper original) {
        return new DynamicClassLoadHelper();
    }
}
