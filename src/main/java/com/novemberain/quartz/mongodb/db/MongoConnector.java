package com.novemberain.quartz.mongodb.db;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.quartz.SchedulerConfigException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The responsibility of this class is create a MongoClient with given parameters.
 */
public class MongoConnector {

    private MongoClient mongo;

    private MongoConnector() {
        // use the builder
    }

    public void shutdown() {
        mongo.close();
    }

    public MongoDatabase selectDatabase(String dbName) {
        return mongo.getDatabase(dbName);
    }

    public static MongoConnectorBuilder builder() {
        return new MongoConnectorBuilder();
    }

    public static class MongoConnectorBuilder {
        private MongoConnector connector = new MongoConnector();
        private MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();

        private String mongoUri;
        private String username;
        private String password;
        private String[] addresses;
        private String dbName;
        private String authDbName;
        private int writeTimeout;

        public MongoConnector build() throws SchedulerConfigException {
            connect();
            return connector;
        }

        public MongoConnectorBuilder withClient(MongoClient mongo) {
            connector.mongo = mongo;
            return this;
        }

        public MongoConnectorBuilder withUri(String mongoUri) {
            this.mongoUri = mongoUri;
            return this;
        }

        public MongoConnectorBuilder withCredentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public MongoConnectorBuilder withAddresses(String[] addresses) {
            this.addresses = addresses;
            return this;
        }

        private void connect() throws SchedulerConfigException {
            if (connector.mongo == null) {
                initializeMongo();
            } else {
                if (mongoUri != null  || username != null || password != null || addresses != null){
                    throw new SchedulerConfigException("Configure either a Mongo instance or MongoDB connection parameters.");
                }
            }
        }

        private void initializeMongo() throws SchedulerConfigException {
            connector.mongo = connectToMongoDB();
            if (connector.mongo == null) {
                throw new SchedulerConfigException("Could not connect to MongoDB! Please check that quartz-mongodb configuration is correct.");
            }
            setWriteConcern();
        }

        private MongoClient connectToMongoDB() throws SchedulerConfigException {
            if (mongoUri == null && (addresses == null || addresses.length == 0)) {
                throw new SchedulerConfigException("At least one MongoDB address or a MongoDB URI must be specified .");
            }

            if (mongoUri != null) {
                return connectToMongoDB(mongoUri);
            }

            return createClient();
        }

        private MongoClient createClient() throws SchedulerConfigException {
            MongoClientOptions options = createOptions();
            List<MongoCredential> credentials = createCredentials();
            List<ServerAddress> serverAddresses = collectServerAddresses();
            try {
                return new MongoClient(serverAddresses, credentials, options);
            } catch (MongoException e) {
                throw new SchedulerConfigException("Could not connect to MongoDB", e);
            }
        }

        private MongoClientOptions createOptions() {
            return optionsBuilder.build();
        }

        private List<MongoCredential> createCredentials() {
            List<MongoCredential> credentials = new ArrayList<MongoCredential>(1);
            if (username != null) {
                if (authDbName != null) {
                    // authenticating to db which gives access to all other dbs (role - readWriteAnyDatabase)
                    // by default in mongo it should be "admin"
                    credentials.add(MongoCredential.createCredential(username, authDbName, password.toCharArray()));
                } else {
                    credentials.add(MongoCredential.createCredential(username, dbName, password.toCharArray()));
                }
            }
            return credentials;
        }

        private List<ServerAddress> collectServerAddresses() {
            List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
            for (String a : addresses) {
                serverAddresses.add(new ServerAddress(a));
            }
            return serverAddresses;
        }

        private MongoClient connectToMongoDB(final String mongoUriAsString) throws SchedulerConfigException {
            try {
                return new MongoClient(new MongoClientURI(mongoUriAsString));
            } catch (final MongoException e) {
                throw new SchedulerConfigException("MongoDB driver thrown an exception", e);
            }
        }

        private void setWriteConcern() {
            // Use MAJORITY to make sure that writes (locks, updates, check-ins)
            // are propagated to secondaries in a Replica Set. It allows us to
            // have consistent state in case of failure of the primary.
            //
            // Since MongoDB 3.2, when MAJORITY is used and protocol version == 1
            // for replica set, then Journaling in enabled by default for primary
            // and secondaries.
            WriteConcern writeConcern = WriteConcern.MAJORITY
                    .withWTimeout(writeTimeout, TimeUnit.MILLISECONDS)
                    .withJournal(true);
            connector.mongo.setWriteConcern(writeConcern);
        }

        public MongoConnectorBuilder withAuthDatabaseName(String authDbName) {
            this.authDbName = authDbName;
            return this;
        }

        public MongoConnectorBuilder withDatabaseName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public MongoConnectorBuilder withMaxConnectionsPerHost(Integer maxConnectionsPerHost) {
            if (maxConnectionsPerHost != null) {
                optionsBuilder.connectionsPerHost(maxConnectionsPerHost);
            }
            return this;
        }

        public MongoConnectorBuilder withConnectTimeoutMillis(Integer connectTimeoutMillis) {
            if (connectTimeoutMillis != null) {
                optionsBuilder.connectTimeout(connectTimeoutMillis);
            }
            return this;
        }

        public MongoConnectorBuilder withSocketTimeoutMillis(Integer socketTimeoutMillis) {
            if (socketTimeoutMillis != null) {
                optionsBuilder.socketTimeout(socketTimeoutMillis);
            }
            return this;
        }

        public MongoConnectorBuilder withSocketKeepAlive(Boolean socketKeepAlive) {
            if (socketKeepAlive != null) {
                optionsBuilder.socketKeepAlive(socketKeepAlive);
            }
            return this;
        }

        public MongoConnectorBuilder withThreadsAllowedToBlockForConnectionMultiplier(
                Integer threadsAllowedToBlockForConnectionMultiplier) {
            if (threadsAllowedToBlockForConnectionMultiplier != null) {
                optionsBuilder.threadsAllowedToBlockForConnectionMultiplier(
                        threadsAllowedToBlockForConnectionMultiplier);
            }
            return this;
        }

        public MongoConnectorBuilder withSSL(Boolean enableSSL, Boolean sslInvalidHostNameAllowed) {
            if (enableSSL != null) {
                optionsBuilder.sslEnabled(enableSSL);
                if (sslInvalidHostNameAllowed != null) {
                    optionsBuilder.sslInvalidHostNameAllowed(sslInvalidHostNameAllowed);
                }
            }
            return this;
        }

        public MongoConnectorBuilder withWriteTimeout(int writeTimeout) {
            this.writeTimeout = writeTimeout;
            return this;
        }
    }
}
