package com.novemberain.quartz.mongodb.db;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.quartz.SchedulerConfigException;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

/**
 * Builder for {@link MongoConnector}.
 */
public class MongoConnectorBuilder {

    private static final String PARAM_NOT_ALLOWED = "'%s' parameter is not allowed. %s";
    private MongoConnector connector;
    private String writeConcernW;
    private Integer writeConcernWriteTimeout;
    private MongoDatabase database;
    private MongoClient client;
    private String dbName;
    private String uri;
    private String[] addresses;
    private String username;
    private String password;
    private String authDbName;
    private Integer maxConnections;
    private Integer connectTimeoutMillis;
    private Integer readTimeoutMillis;
    private Boolean socketKeepAlive;
    private Boolean enableSSL;
    private Boolean sslInvalidHostNameAllowed;
    private String trustStorePath;
    private String trustStorePassword;
    private String trustStoreType;
    private String keyStorePath;
    private String keyStorePassword;
    private String keyStoreType;
    private SSLContextFactory sslContextFactory = new SSLContextFactory();

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
        resolveDbNameByUriIfNull();
        checkNotNull(dbName, "'Database name' is required, as parameter or in MongoDB URI path.");

        if (client != null) {
            // User passed MongoClient instance.
            validateForClient();
            return new ExternalMongoConnector(writeConcern, client, dbName);
        }

        final MongoClientSettings.Builder settingsBuilder = createSettingsBuilder();
        if (uri != null) {
            // User passed URI.
            validateForUri();
            return new InternalMongoConnector(writeConcern, uri, dbName, settingsBuilder);
        }

        checkNotNull(addresses, "At least one MongoDB address or a MongoDB URI must be specified.");
        final List<ServerAddress> serverAddresses = collectServerAddresses();
        final Optional<MongoCredential> credentials = createCredentials();
        return new InternalMongoConnector(writeConcern, serverAddresses, credentials, settingsBuilder, dbName);
    }

    private void resolveDbNameByUriIfNull() {
        if (dbName == null && uri != null) {
            String path = URI.create(uri).getPath();
            if (path != null && path.startsWith("/") && path.length() > 1) {
                dbName = path.substring(1);
            }
        }
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

    private MongoClientSettings.Builder createSettingsBuilder() throws SchedulerConfigException {
        final MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
        if (maxConnections != null) {
            settingsBuilder.applyToConnectionPoolSettings(builder -> builder.maxSize(maxConnections));
        }
        if (connectTimeoutMillis != null) {
            settingsBuilder.applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS));
        }
        if (readTimeoutMillis != null) {
            settingsBuilder.applyToSocketSettings(builder -> builder.readTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS));
        }
        if (socketKeepAlive != null) {
            // enabled by default,
            // ignored per MongoDB Java client deprecations
        }
        hanldeSSLContext(settingsBuilder);
        return settingsBuilder;
    }

    private void hanldeSSLContext(MongoClientSettings.Builder optionsBuilder) throws SchedulerConfigException {
        try {
            SSLContext sslContext = sslContextFactory.getSSLContext(trustStorePath, trustStorePassword, trustStoreType,
                    keyStorePath, keyStorePassword, keyStoreType);
            if (sslContext == null) {
                if (enableSSL != null) {
                    optionsBuilder.applyToSslSettings(builder -> builder.enabled(enableSSL));
                    if (sslInvalidHostNameAllowed != null) {
                        optionsBuilder.applyToSslSettings(builder -> builder.invalidHostNameAllowed(sslInvalidHostNameAllowed));
                    }
                }
            } else {
                optionsBuilder.applyToSslSettings(builder -> builder.enabled(true));
                if (sslInvalidHostNameAllowed != null) {
                    optionsBuilder.applyToSslSettings(builder -> builder.invalidHostNameAllowed(sslInvalidHostNameAllowed));
                }
                optionsBuilder.applyToSslSettings(builder -> builder.context(sslContext));
            }
        } catch (SSLException e) {
            throw new SchedulerConfigException("Cannot setup SSL context", e);
        }
    }

    private WriteConcern createWriteConcern() throws SchedulerConfigException {
        checkNotNull(writeConcernWriteTimeout, "Write timeout is expected.");

        if(writeConcernW != null) {
            return WriteConcern.valueOf(writeConcernW)
               .withWTimeout(writeConcernWriteTimeout, TimeUnit.MILLISECONDS)
               .withJournal(true);
        }

        // Default:
        // Use MAJORITY to make sure that writes (locks, updates, check-ins)
        // are propagated to secondaries in a Replica Set. It allows us to
        // have consistent state in case of failure of the primary.
        //
        // Since MongoDB 3.2, when MAJORITY is used and protocol version == 1
        // for replica set, then Journaling in enabled by default for primary
        // and secondaries.
        return WriteConcern.MAJORITY.withWTimeout(writeConcernWriteTimeout, TimeUnit.MILLISECONDS)
                .withJournal(true);
    }

    private void validateForConnector() throws SchedulerConfigException {
        final String suffix = "'Connector' parameter is used.";
        checkIsNull(database, paramNotAllowed("Database", suffix));
        checkIsNull(client, paramNotAllowed("Client", suffix));
        checkIsNull(dbName, paramNotAllowed("Database name", suffix));
        checkIsNull(uri, paramNotAllowed("URI", suffix));
        checkServerPropertiesAreNull(suffix);
        checkConnectionOptionsAreNull(suffix);
    }

    private void validateForDatabase() throws SchedulerConfigException {
        final String suffix = "'Database' parameter is used.";
        checkIsNull(client, paramNotAllowed("Client", suffix));
        checkIsNull(dbName, paramNotAllowed("Database name", suffix));
        checkIsNull(uri, paramNotAllowed("URI", suffix));
        checkServerPropertiesAreNull(suffix);
        checkConnectionOptionsAreNull(suffix);
    }

    private void validateForClient() throws SchedulerConfigException {
        final String suffix = "'Client' parameter is used.";
        checkIsNull(uri, paramNotAllowed("URI", suffix));
        checkServerPropertiesAreNull(suffix);
        checkConnectionOptionsAreNull(suffix);
    }

    private void validateForUri() throws SchedulerConfigException {
        checkServerPropertiesAreNull("'URI' parameter is used.");
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

    private void checkServerPropertiesAreNull(final String suffix) throws SchedulerConfigException {
        checkIsNull(addresses, paramNotAllowed("Addresses array", suffix));
        checkIsNull(username, paramNotAllowed("Username", suffix));
        checkIsNull(password, paramNotAllowed("Password", suffix));
        checkIsNull(authDbName, paramNotAllowed("Auth database name", suffix));
    }

    private void checkConnectionOptionsAreNull(final String suffix) throws SchedulerConfigException {
        checkIsNull(maxConnections, paramNotAllowed("Max connections", suffix));
        checkIsNull(connectTimeoutMillis, paramNotAllowed("Connect timeout millis", suffix));
        checkIsNull(readTimeoutMillis, paramNotAllowed("Socket timeout millis", suffix));
        checkIsNull(socketKeepAlive, paramNotAllowed("Socket keepAlive", suffix));
        checkIsNull(enableSSL, paramNotAllowed("Enable ssl", suffix));
        checkIsNull(sslInvalidHostNameAllowed, paramNotAllowed("SSL invalid hostname allowed", suffix));
        checkIsNull(trustStorePath, paramNotAllowed("TrustStore path", suffix));
        checkIsNull(trustStorePassword, paramNotAllowed("TrustStore password", suffix));
        checkIsNull(trustStoreType, paramNotAllowed("TrustStore type", suffix));
        checkIsNull(keyStorePath, paramNotAllowed("KeyStore path", suffix));
        checkIsNull(keyStorePassword, paramNotAllowed("KeyStore password", suffix));
        checkIsNull(keyStoreType, paramNotAllowed("KeyStore type", suffix));
    }

    private static String paramNotAllowed(final String paramName, final String suffix) {
        return String.format(PARAM_NOT_ALLOWED, paramName, suffix);
    }

    // mutators below

    public MongoConnectorBuilder withConnector(final MongoConnector connector) {
        this.connector = connector;
        return this;
    }

    public MongoConnectorBuilder withWriteConcernWriteTimeout(int writeConcernWriteTimeout) {
        this.writeConcernWriteTimeout = writeConcernWriteTimeout;
        return this;
    }

    public MongoConnectorBuilder withWriteConcernW(String writeConcernW) {
        this.writeConcernW = writeConcernW;
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

    public MongoConnectorBuilder withMaxConnections(final Integer maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    }

    public MongoConnectorBuilder withConnectTimeoutMillis(final Integer connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    public MongoConnectorBuilder withReadTimeoutMillis(final Integer readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
        return this;
    }

    public MongoConnectorBuilder withSocketKeepAlive(final Boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
        return this;
    }

    public MongoConnectorBuilder withSSL(final Boolean enableSSL, final Boolean sslInvalidHostNameAllowed) {
        this.enableSSL = enableSSL;
        this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
        return this;
    }

    public MongoConnectorBuilder withTrustStore(String trustStorePath, String trustStorePassword, String trustStoreType) {
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.trustStoreType = trustStoreType;
        return this;
    }

    public MongoConnectorBuilder withKeyStore(String keyStorePath, String keyStorePassword, String keyStoreType) {
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.keyStoreType = keyStoreType;
        return this;
    }

}
