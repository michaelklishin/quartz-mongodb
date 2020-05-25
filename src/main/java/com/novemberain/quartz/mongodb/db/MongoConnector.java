package com.novemberain.quartz.mongodb.db;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.Closeable;

/**
 * Interface through which quartz-mongodb interacts with MongoDB.
 */
public interface MongoConnector extends Closeable {

    /**
     * Quartz-mongodb will call this method to get the instance of {@link MongoCollection} for internal uses.
     * The collection is expected to be fully configured with correct {@link WriteConcern}.
     *
     * @param collectionName collection name.
     * @return instance of {@link MongoCollection}.
     */
    MongoCollection<Document> getCollection(final String collectionName);

    /**
     * Quartz-mongodb will call this method when shutting down.
     * Implementation can close {@link MongoClient} here.
     */
    @Override
    void close();
}
