package com.novemberain.quartz.mongodb.db;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.quartz.SchedulerConfigException;

import java.util.List;
import java.util.Optional;

/**
 * The implementation of {@link MongoConnector} that owns the lifecycle of {@link MongoClient}.
 */
public class InternalMongoConnector implements MongoConnector {

    private final WriteConcern writeConcern;
    private final MongoClient mongoClient;
    private final MongoDatabase database;

    /**
     * Constructs an instance of {@link InternalMongoConnector}.
     *
     * @param writeConcern instance of {@link WriteConcern}. Each {@link MongoCollection} produced by
     *                     {@link #getCollection(String)} will be configured with this write concern.
     * @param mongoClient  {@link MongoClient} that we just created.
     * @param dbName       name of the database that will be used to produce collections.
     */
    private InternalMongoConnector(final WriteConcern writeConcern, final MongoClient mongoClient,
                                   final String dbName) {
        this.writeConcern = writeConcern;
        this.mongoClient = mongoClient;
        this.database = mongoClient.getDatabase(dbName);
    }

    /**
     * Constructs an instance of {@link InternalMongoConnector} from connection URI.
     *
     * @param writeConcern instance of {@link WriteConcern}. Each {@link MongoCollection} produced by
     *                     {@link #getCollection(String)} will be configured with this write concern.
     * @param uri          MongoDB connection URI.
     * @param dbName       name of the database that will be used to produce collections.
     * @throws SchedulerConfigException if failed to create instance of MongoClient.
     */
    public InternalMongoConnector(final WriteConcern writeConcern, final String uri,
                                  final String dbName) throws SchedulerConfigException {
        this(writeConcern, uri, dbName, null);
    }

    /**
     * Constructs an instance of {@link InternalMongoConnector} from connection URI and option builder.
     *
     * @param writeConcern instance of {@link WriteConcern}. Each {@link MongoCollection} produced by
     *                     {@link #getCollection(String)} will be configured with this write concern.
     * @param uri          MongoDB connection URI.
     * @param dbName       name of the database that will be used to produce collections.
     * @param settingsBuilder default settings builder.
     * @throws SchedulerConfigException if failed to create instance of MongoClient.
     */
    public InternalMongoConnector(final WriteConcern writeConcern, final String uri,
                                  final String dbName, MongoClientSettings.Builder settingsBuilder) throws SchedulerConfigException {
        this(writeConcern, createClient(uri, settingsBuilder), dbName);
    }

    /**
     * Constructs an instance of {@link InternalMongoConnector}.
     *
     * @param writeConcern    instance of {@link WriteConcern}. Each {@link MongoCollection} produced by
     *                        {@link #getCollection(String)} will be configured with this write concern.
     * @param seeds           list of server addresses.
     * @param credentials     credentials used to authenticate all connections.
     * @param settingsBuilder default settings builder.
     * @param dbName          name of the database that will be used to produce collections.
     * @throws SchedulerConfigException if failed to create instance of MongoClient.
     */
    public InternalMongoConnector(final WriteConcern writeConcern, final List<ServerAddress> seeds,
                                  final Optional<MongoCredential> credentials,
                                  final MongoClientSettings.Builder settingsBuilder,
                                  final String dbName) throws SchedulerConfigException {
        this(writeConcern, createClient(seeds, credentials, settingsBuilder), dbName);
    }

    @Override
    public MongoCollection<Document> getCollection(String collectionName) {
        return database.getCollection(collectionName).withWriteConcern(writeConcern);
    }

    @Override
    public void close() {
        mongoClient.close();
    }

    /**
     * Creates an instance of MongoClient from MongoClientSettings wrapping exception.
     */
    private static MongoClient createClient(final MongoClientSettings settings) throws SchedulerConfigException {
        try {
            return MongoClients.create(settings);
        } catch (final MongoException e) {
            throw new SchedulerConfigException("MongoDB driver thrown an exception.", e);
        }
    }

    /**
     * Creates an instance of MongoClient from string URI and settings builder wrapping exception.
     */
    private static MongoClient createClient(final String uri, final MongoClientSettings.Builder settingsBuilder) throws SchedulerConfigException {
        final ConnectionString mongoUri;
        try {
            mongoUri = new ConnectionString(uri);
        } catch (final MongoException e) {
            throw new SchedulerConfigException("Invalid mongo client uri.", e);
        }
        return createClient(settingsBuilder.applyConnectionString(mongoUri).build());
    }

    /**
     * Creates an instance of MongoClient from server addresses, credentials and settings builder wrapping exception.
     */
    private static MongoClient createClient(final List<ServerAddress> seeds,
                                            final Optional<MongoCredential> credentials,
                                            final MongoClientSettings.Builder settingsBuilder) throws SchedulerConfigException {
        try {
            settingsBuilder
                    .applyToClusterSettings(builder -> builder.hosts(seeds));

            credentials.ifPresent(settingsBuilder::credential);

            return MongoClients.create(settingsBuilder.build());
        } catch (MongoException e) {
            throw new SchedulerConfigException("MongoDB driver thrown an exception.", e);
        }
    }

}
