/*
 * $Id: MongoDBJobStore.java 253170 2014-01-06 02:28:03Z waded $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.novemberain.quartz.mongodb;

import com.mongodb.*;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.novemberain.quartz.mongodb.dao.*;
import com.novemberain.quartz.mongodb.db.MongoConnector;
import com.novemberain.quartz.mongodb.trigger.MisfireHandler;
import com.novemberain.quartz.mongodb.util.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MongoDBJobStore implements JobStore, Constants {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Deprecated
    private static MongoClient overriddenMongo;

    /**
     * @deprecated use {@link #MongoDBJobStore(MongoClient)}
     */
    @Deprecated
    public static void overrideMongo(MongoClient mongo) {
        overriddenMongo = mongo;
    }

    private MongoConnector mongoConnector;
    private TriggerStateManager triggerStateManager;
    private TriggerRunner triggerRunner;

    private MongoClient mongo;
    private String collectionPrefix = "quartz_";
    private String dbName;
    private String authDbName;
    private CalendarDao calendarDao;
    private JobDao jobDao;
    private LocksDao locksDao;
    private PausedJobGroupsDao pausedJobGroupsDao;
    private PausedTriggerGroupsDao pausedTriggerGroupsDao;
    private TriggerDao triggerDao;
    private String instanceId;
    private String[] addresses;
    private String mongoUri;
    private String username;
    private String password;
    private long misfireThreshold = 5000;
    private long triggerTimeoutMillis = 10 * 60 * 1000L;
    private long jobTimeoutMillis = 10 * 60 * 1000L;

    // Options for the Mongo client.
    private Boolean mongoOptionSocketKeepAlive;
    private Integer mongoOptionMaxConnectionsPerHost;
    private Integer mongoOptionConnectTimeoutMillis;
    private Integer mongoOptionSocketTimeoutMillis; // read timeout
    private Integer mongoOptionThreadsAllowedToBlockForConnectionMultiplier;
    private Boolean mongoOptionEnableSSL;
    private Boolean mongoOptionSslInvalidHostNameAllowed;

    private QueryHelper queryHelper = new QueryHelper();

    public MongoDBJobStore() {
    }

    public MongoDBJobStore(final MongoClient mongo) {
        this.mongo = mongo;
    }

    public MongoDBJobStore(final String mongoUri, final String username, final String password) {
        this.mongoUri = mongoUri;
        this.username = username;
        this.password = password;
    }

    /**
     * Override to change class loading mechanism, to e.g. dynamic
     * @param original    default provided by Quartz
     * @return loader to use for loading of Quartz Jobs' classes
     */
    protected ClassLoadHelper getClassLoaderHelper(ClassLoadHelper original) {
        return original;
    }

    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
            throws SchedulerConfigException {
        mongoConnector = MongoConnector.builder()
                .withClient(mongo)
                .withOverriddenMongo(overriddenMongo)
                .withUri(mongoUri)
                .withCredentials(username, password)
                .withAddresses(addresses)
                .withDatabaseName(dbName)
                .withAuthDatabaseName(authDbName)
                .withMaxConnectionsPerHost(mongoOptionMaxConnectionsPerHost)
                .withConnectTimeoutMillis(mongoOptionConnectTimeoutMillis)
                .withSocketTimeoutMillis(mongoOptionSocketTimeoutMillis)
                .withSocketKeepAlive(mongoOptionSocketKeepAlive)
                .withThreadsAllowedToBlockForConnectionMultiplier(mongoOptionThreadsAllowedToBlockForConnectionMultiplier)
                .withSSL(mongoOptionEnableSSL, mongoOptionSslInvalidHostNameAllowed)
                .build();

        MongoDatabase db = mongoConnector.selectDatabase(dbName);

        JobLoader jobLoader = new JobLoader(getClassLoaderHelper(loadHelper));
        initializeCollections(db, jobLoader);

        ensureIndexes();

        triggerStateManager = new TriggerStateManager(triggerDao, jobDao,
                pausedJobGroupsDao, pausedTriggerGroupsDao, queryHelper);

        MisfireHandler misfireHandler = new MisfireHandler(calendarDao, signaler, misfireThreshold);
        TriggerTimeCalculator timeCalculator = new TriggerTimeCalculator(jobTimeoutMillis,
                triggerTimeoutMillis);
        triggerRunner = new TriggerRunner(triggerDao, jobDao, locksDao, calendarDao, signaler,
                instanceId, timeCalculator, misfireHandler);
    }

    @Override
    public void schedulerStarted() throws SchedulerException {
        // No-op
    }

    @Override
    public void schedulerPaused() {
        // No-op
    }

    @Override
    public void schedulerResumed() {
    }

    @Override
    public void shutdown() {
        mongoConnector.shutdown();
    }

    @Override
    public boolean supportsPersistence() {
        return true;
    }

    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        // this will vary...
        return 200;
    }

    @Override
    public boolean isClustered() {
        return false;
    }

    /**
     * Job and Trigger storage Methods
     */
    @Override
    public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
            throws JobPersistenceException {
        ObjectId jobId = jobDao.storeJobInMongo(newJob, false);

        log.debug("Storing job {} and trigger {}", newJob.getKey(), newTrigger.getKey());
        triggerRunner.storeTrigger(newTrigger, jobId, false);
    }

    @Override
    public void storeJob(JobDetail newJob, boolean replaceExisting)
            throws JobPersistenceException {
        jobDao.storeJobInMongo(newJob, replaceExisting);
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, List<Trigger>> triggersAndJobs, boolean replace)
            throws JobPersistenceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        Bson keyObject = Keys.toFilter(jobKey);
        Document item = jobDao.getJob(keyObject);
        if (item != null) {
            jobDao.remove(keyObject);
            triggerDao.removeByJobId(item.get("_id"));
            return true;
        }
        return false;
    }

    @Override
    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        for (JobKey key : jobKeys) {
            removeJob(key);
        }
        return false;
    }

    @Override
    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return jobDao.retrieveJob(jobKey);
    }

    @Override
    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
            throws JobPersistenceException {
        triggerRunner.storeTrigger(newTrigger, replaceExisting);
    }

    @Override
    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return triggerRunner.removeTrigger(triggerKey);
    }

    @Override
    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        return triggerRunner.removeTriggers(triggerKeys);
    }

    @Override
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        OperableTrigger trigger = retrieveTrigger(triggerKey);
        if (trigger == null) {
            return false;
        }

        if (!trigger.getJobKey().equals(newTrigger.getJobKey())) {
            throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
        }

        // Can't call remove trigger as if the job is not durable, it will remove the job too
        Bson filter = Keys.toFilter(triggerKey);
        if (triggerDao.exists(filter)) {
            triggerDao.remove(filter);
        }

        // Copy across the job data map from the old trigger to the new one.
        newTrigger.getJobDataMap().putAll(trigger.getJobDataMap());

        try {
            storeTrigger(newTrigger, false);
        } catch (JobPersistenceException jpe) {
            storeTrigger(trigger, false); // put previous trigger back...
            throw jpe;
        }
        return true;
    }

    @Override
    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return triggerRunner.retrieveTrigger(triggerKey);
    }

    @Override
    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return jobDao.exists(jobKey);
    }

    @Override
    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        return triggerDao.exists(Keys.toFilter(triggerKey));
    }

    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {
        jobDao.clear();
        triggerDao.clear();
        calendarDao.clear();
        pausedJobGroupsDao.remove();
        pausedTriggerGroupsDao.remove();
    }

    @Override
    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
            throws JobPersistenceException {
        // TODO implement updating triggers
        if (updateTriggers) {
            throw new UnsupportedOperationException("Updating triggers is not supported.");
        }

        calendarDao.store(name, calendar);
    }

    @Override
    public boolean removeCalendar(String calName) throws JobPersistenceException {
        return calendarDao.remove(calName);
    }

    @Override
    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        return calendarDao.retrieveCalendar(calName);
    }

    @Override
    public int getNumberOfJobs() throws JobPersistenceException {
        return jobDao.getCount();
    }

    @Override
    public int getNumberOfTriggers() throws JobPersistenceException {
        return triggerDao.getCount();
    }

    @Override
    public int getNumberOfCalendars() throws JobPersistenceException {
        return calendarDao.getCount();
    }

    @Override
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        return jobDao.getJobKeys(matcher);
    }

    @Override
    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return triggerDao.getTriggerKeys(matcher);
    }

    @Override
    public List<String> getJobGroupNames() throws JobPersistenceException {
        return jobDao.getGroupNames();
    }

    @Override
    public List<String> getTriggerGroupNames() throws JobPersistenceException {
        return triggerDao.getGroupNames();
    }

    @Override
    public List<String> getCalendarNames() throws JobPersistenceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        return triggerRunner.getTriggersForJob(jobKey);
    }

    @Override
    public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        return triggerDao.getState(triggerKey);
    }

    @Override
    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        triggerStateManager.pause(triggerKey);
    }

    @Override
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return triggerStateManager.pause(matcher);
    }

    @Override
    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        triggerStateManager.resume(triggerKey);
    }

    @Override
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return triggerStateManager.resume(matcher);
    }

    @Override
    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        return triggerStateManager.getPausedTriggerGroups();
    }

    // only for tests
    public Set<String> getPausedJobGroups() throws JobPersistenceException {
        return pausedJobGroupsDao.getPausedGroups();
    }

    @Override
    public void pauseAll() throws JobPersistenceException {
        triggerStateManager.pauseAll();
    }

    @Override
    public void resumeAll() throws JobPersistenceException {
        triggerStateManager.resumeAll();
    }

    @Override
    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        triggerStateManager.pauseJob(jobKey);
    }

    @Override
    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        return triggerStateManager.pauseJobs(groupMatcher);
    }

    @Override
    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        triggerStateManager.resume(jobKey);
    }

    @Override
    public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        return triggerStateManager.resumeJobs(groupMatcher);
    }

    @Override
    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
            throws JobPersistenceException {
        return triggerRunner.acquireNext(noLaterThan, maxCount, timeWindow);
    }

    @Override
    public void releaseAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
        triggerRunner.releaseAcquired(trigger);
    }

    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
            throws JobPersistenceException {
        return triggerRunner.triggersFired(triggers);
    }

    @Override
    public void triggeredJobComplete(OperableTrigger trigger, JobDetail job,
                                     CompletedExecutionInstruction triggerInstCode)
            throws JobPersistenceException {
        triggerRunner.triggeredJobComplete(trigger, job, triggerInstCode);
    }

    @Override
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    @Override
    public void setInstanceName(String schedName) {
        // No-op
    }

    @Override
    public void setThreadPoolSize(int poolSize) {
        // No-op
    }

    public void setAddresses(String addresses) {
        this.addresses = addresses.split(",");
    }

    public MongoCollection<Document> getJobCollection() {
        return jobDao.getCollection();
    }

    public MongoCollection<Document> getTriggerCollection() {
        return triggerDao.getCollection();
    }

    public MongoCollection<Document> getCalendarCollection() {
        return calendarDao.getCollection();
    }

    public MongoCollection<Document> getLocksCollection() {
        return locksDao.getCollection();
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setCollectionPrefix(String prefix) {
        collectionPrefix = prefix + "_";
    }

    public void setMongoUri(final String mongoUri) {
        this.mongoUri = mongoUri;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setMisfireThreshold(long misfireThreshold) {
        this.misfireThreshold = misfireThreshold;
    }

    public void setTriggerTimeoutMillis(long triggerTimeoutMillis) {
        this.triggerTimeoutMillis = triggerTimeoutMillis;
    }

    public void setJobTimeoutMillis(long jobTimeoutMillis) {
        this.jobTimeoutMillis = jobTimeoutMillis;
    }

    private void initializeCollections(MongoDatabase db, JobLoader jobLoader) {
        jobDao = new JobDao(db.getCollection(collectionPrefix + "jobs"), queryHelper, jobLoader);
        triggerDao = new TriggerDao(db.getCollection(collectionPrefix + "triggers"), queryHelper);
        calendarDao = new CalendarDao(db.getCollection(collectionPrefix + "calendars"));
        locksDao = new LocksDao(db.getCollection(collectionPrefix + "locks"), instanceId);

        pausedJobGroupsDao = new PausedJobGroupsDao(db.getCollection(collectionPrefix + "paused_job_groups"));
        pausedTriggerGroupsDao = new PausedTriggerGroupsDao(db.getCollection(collectionPrefix + "paused_trigger_groups"));
    }

    /**
     * Initializes the indexes for the scheduler collections.
     *
     * @throws SchedulerConfigException if an error occurred communicating with the MongoDB server.
     */
    private void ensureIndexes() throws SchedulerConfigException {
        try {
      /*
       * Indexes are to be declared as group then name.  This is important as the quartz API allows
       * for the searching of jobs and triggers using a group matcher.  To be able to use the compound
       * index using group alone (as the API allows), group must be the first key in that index.
       * 
       * To be consistent, all such indexes are ensured in the order group then name.  The previous
       * indexes are removed after we have "ensured" the new ones.
       */

            jobDao.createIndex();
            triggerDao.createIndex();
            locksDao.createIndex();
            calendarDao.createIndex();

            try {
                // Drop the old indexes that were declared as name then group rather than group then name
                jobDao.dropIndex();
                triggerDao.dropIndex();
                locksDao.dropIndex();
            } catch (MongoCommandException cfe) {
                // Ignore, the old indexes have already been removed
            }
        } catch (MongoException e) {
            throw new SchedulerConfigException("Error while initializing the indexes", e);
        }
    }

    public void setMongoOptionMaxConnectionsPerHost(int maxConnectionsPerHost) {
        this.mongoOptionMaxConnectionsPerHost = maxConnectionsPerHost;
    }

    public void setMongoOptionConnectTimeoutMillis(int maxConnectWaitTime) {
        this.mongoOptionConnectTimeoutMillis = maxConnectWaitTime;
    }

    public void setMongoOptionSocketTimeoutMillis(int socketTimeoutMillis) {
        this.mongoOptionSocketTimeoutMillis = socketTimeoutMillis;
    }

    public void setMongoOptionThreadsAllowedToBlockForConnectionMultiplier(int threadsAllowedToBlockForConnectionMultiplier) {
        this.mongoOptionThreadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
    }

    public void setMongoOptionSocketKeepAlive(boolean socketKeepAlive) {
        this.mongoOptionSocketKeepAlive = socketKeepAlive;
    }

    public void setMongoOptionEnableSSL(boolean enableSSL) {
        this.mongoOptionEnableSSL = enableSSL;
    }

    public void setMongoOptionSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
        this.mongoOptionSslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
    }

    public String getAuthDbName() {
        return authDbName;
    }

    public void setAuthDbName(String authDbName) {
        this.authDbName = authDbName;
    }
}
