package com.novemberain.quartz.mongodb.db;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.quartz.SchedulerConfigException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Builder for {@link MongoConnector}.
 */
public class MongoConnectorBuilder {

    private static final String PARAM_NOT_ALLOWED = "'%s' parameter is not allowed. %s";
    private MongoConnector connector;
    private Integer writeTimeout;
    private MongoDatabase database;
    private MongoClient client;
    private String dbName;
    private String uri;
    private String[] addresses;
    private String username;
    private String password;
    private String authDbName;
    private Integer maxConnectionsPerHost;
    private Integer connectTimeoutMillis;
    private Integer socketTimeoutMillis;
    private Boolean socketKeepAlive;
    private Integer threadsAllowedToBlockForConnectionMultiplier;
    private Boolean enableSSL;
    private Boolean sslInvalidHostNameAllowed;

    /**
     * Use {@link #builder()}.
     */
    private MongoConnectorBuilder() {
    }

    /**
     * Creates a builder.
     *
     * @return new builder instance.
     */
    public static MongoConnectorBuilder builder() {
        return new MongoConnectorBuilder();
    }

    /**
     * Builds MongoConnector from current settings.
     *
     * @return an instance of {@link MongoConnector}.
     * @throws SchedulerConfigException if builder was in invalid state.
     */
    public MongoConnector build() throws SchedulerConfigException {
        if (connector != null) {
            // User implemented MongoConnector himself, highest priority.
            validateForConnector();
            return connector;
        }

        // Options below require WriteConcern
        final WriteConcern writeConcern = createWriteConcern();

        if (database != null) {
            // User passed MongoDatabase instance.
            validateForDatabase();
            return new ExternalMongoConnector(writeConcern, database);
        }

        // Options below require database name
        checkNotNull(dbName, "'Database name' parameter is required.");

        if (client != null) {
            // User passed MongoClient instance.
            validateForClient();
            return new ExternalMongoConnector(writeConcern, client, dbName);
        }

        if (uri != null) {
            // User passed URI.
            validateForUri();
            return new InternalMongoConnector(writeConcern, uri, dbName);
        }

        checkNotNull(addresses, "At least one MongoDB address or a MongoDB URI must be specified.");
        final List<ServerAddress> serverAddresses = collectServerAddresses();
        final Optional<MongoCredential> credentials = createCredentials();
        final MongoClientOptions options = createOptions();
        return new InternalMongoConnector(writeConcern, serverAddresses, credentials, options, dbName);
    }

    private List<ServerAddress> collectServerAddresses() {
        final List<ServerAddress> serverAddresses = new ArrayList<>(addresses.length);
        for (final String address : addresses) {
            serverAddresses.add(new ServerAddress(address));
        }
        return serverAddresses;
    }

    private Optional<MongoCredential> createCredentials() {
        if (username != null) {
            final MongoCredential cred;
            if (authDbName != null) {
                // authenticating to db which gives access to all other dbs (role - readWriteAnyDatabase)
                // by default in mongo it should be "admin"
                cred = MongoCredential.createCredential(username, authDbName, password.toCharArray());
            } else {
                cred = MongoCredential.createCredential(username, dbName, password.toCharArray());
            }
            return Optional.of(cred);
        }
        return Optional.empty();
    }

    private MongoClientOptions createOptions() {
        final MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();
        if (maxConnectionsPerHost != null) {
            optionsBuilder.connectionsPerHost(maxConnectionsPerHost);
        }
        if (connectTimeoutMillis != null) {
            optionsBuilder.connectTimeout(connectTimeoutMillis);
        }
        if (socketTimeoutMillis != null) {
            optionsBuilder.socketTimeout(socketTimeoutMillis);
        }
        if (socketKeepAlive != null) {
            // enabled by default,
            // ignored per MongoDB Java client deprecations
        }
        if (threadsAllowedToBlockForConnectionMultiplier != null) {
            optionsBuilder.threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier);
        }
        if (enableSSL != null) {
            optionsBuilder.sslEnabled(enableSSL);
            if (sslInvalidHostNameAllowed != null) {
                optionsBuilder.sslInvalidHostNameAllowed(sslInvalidHostNameAllowed);
            }
        }
        return optionsBuilder.build();
    }

    private WriteConcern createWriteConcern() throws SchedulerConfigException {
        // Use MAJORITY to make sure that writes (locks, updates, check-ins)
        // are propagated to secondaries in a Replica Set. It allows us to
        // have consistent state in case of failure of the primary.
        //
        // Since MongoDB 3.2, when MAJORITY is used and protocol version == 1
        // for replica set, then Journaling in enabled by default for primary
        // and secondaries.
        checkNotNull(writeTimeout, "Write timeout is expected.");
        return WriteConcern.MAJORITY.withWTimeout(writeTimeout, TimeUnit.MILLISECONDS)
                .withJournal(true);
    }

    private void validateForConnector() throws SchedulerConfigException {
        final String suffix = "'Connector' parameter is used.";
        checkIsNull(database, paramNotAllowed("Database", suffix));
        checkIsNull(client, paramNotAllowed("Client", suffix));
        checkIsNull(dbName, paramNotAllowed("Database name", suffix));
        checkIsNull(uri, paramNotAllowed("URI", suffix));
        checkServersCredsOptionsAreNull(suffix);
    }

    private void validateForDatabase() throws SchedulerConfigException {
        final String suffix = "'Database' parameter is used.";
        checkIsNull(client, paramNotAllowed("Client", suffix));
        checkIsNull(dbName, paramNotAllowed("Database name", suffix));
        checkIsNull(uri, paramNotAllowed("URI", suffix));
        checkServersCredsOptionsAreNull(suffix);
    }

    private void validateForClient() throws SchedulerConfigException {
        final String suffix = "'Client' parameter is used.";
        checkIsNull(uri, paramNotAllowed("URI", suffix));
        checkServersCredsOptionsAreNull(suffix);
    }

    private void validateForUri() throws SchedulerConfigException {
        checkServersCredsOptionsAreNull("'URI' parameter is used.");
    }

    private static <T> T checkNotNull(final T reference, final String message) throws SchedulerConfigException {
        if (reference == null) {
            throw new SchedulerConfigException(message);
        }
        return reference;
    }

    private static void checkIsNull(final Object reference, final String message) throws SchedulerConfigException {
        if (reference != null) {
            throw new SchedulerConfigException(message);
        }
    }

    private void checkServersCredsOptionsAreNull(final String suffix) throws SchedulerConfigException {
        checkIsNull(addresses, paramNotAllowed("Addresses array", suffix));
        checkIsNull(username, paramNotAllowed("Username", suffix));
        checkIsNull(password, paramNotAllowed("Password", suffix));
        checkIsNull(authDbName, paramNotAllowed("Auth database name", suffix));
        checkIsNull(maxConnectionsPerHost, paramNotAllowed("Max connections per host", suffix));
        checkIsNull(connectTimeoutMillis, paramNotAllowed("Connect timeout millis", suffix));
        checkIsNull(socketTimeoutMillis, paramNotAllowed("Socket timeout millis", suffix));
        checkIsNull(socketKeepAlive, paramNotAllowed("Socket keepAlive", suffix));
        checkIsNull(threadsAllowedToBlockForConnectionMultiplier,
                paramNotAllowed("Threads allowed to block for connection multiplier", suffix));
        checkIsNull(enableSSL, paramNotAllowed("Enable ssl", suffix));
        checkIsNull(sslInvalidHostNameAllowed, paramNotAllowed("SSL invalid hostname allowed", suffix));
    }

    private static String paramNotAllowed(final String paramName, final String suffix) {
        return String.format(PARAM_NOT_ALLOWED, paramName, suffix);
    }

    // mutators below

    public MongoConnectorBuilder withConnector(final MongoConnector connector) {
        this.connector = connector;
        return this;
    }

    public MongoConnectorBuilder withWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
        return this;
    }

    public MongoConnectorBuilder withDatabase(final MongoDatabase database) {
        this.database = database;
        return this;
    }

    public MongoConnectorBuilder withClient(final MongoClient client) {
        this.client = client;
        return this;
    }

    public MongoConnectorBuilder withDatabaseName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public MongoConnectorBuilder withUri(final String uri) {
        this.uri = uri;
        return this;
    }

    public MongoConnectorBuilder withAddresses(final String[] addresses) {
        this.addresses = addresses;
        return this;
    }

    public MongoConnectorBuilder withCredentials(final String username, final String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    public MongoConnectorBuilder withAuthDatabaseName(String authDbName) {
        this.authDbName = authDbName;
        return this;
    }

    public MongoConnectorBuilder withMaxConnectionsPerHost(final Integer maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
        return this;
    }

    public MongoConnectorBuilder withConnectTimeoutMillis(final Integer connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    public MongoConnectorBuilder withSocketTimeoutMillis(final Integer socketTimeoutMillis) {
        this.socketTimeoutMillis = socketTimeoutMillis;
        return this;
    }

    public MongoConnectorBuilder withSocketKeepAlive(final Boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
        return this;
    }

    public MongoConnectorBuilder withThreadsAllowedToBlockForConnectionMultiplier(
            final Integer threadsAllowedToBlockForConnectionMultiplier) {
        this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
        return this;
    }

    public MongoConnectorBuilder withSSL(final Boolean enableSSL, final Boolean sslInvalidHostNameAllowed) {
        this.enableSSL = enableSSL;
        this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
        return this;
    }
}
