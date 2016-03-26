package com.novemberain.quartz.mongodb;

import com.mongodb.client.MongoDatabase;
import com.novemberain.quartz.mongodb.dao.*;
import com.novemberain.quartz.mongodb.db.MongoConnector;
import com.novemberain.quartz.mongodb.trigger.MisfireHandler;
import com.novemberain.quartz.mongodb.trigger.TriggerConverter;
import com.novemberain.quartz.mongodb.util.Clock;
import com.novemberain.quartz.mongodb.util.QueryHelper;
import com.novemberain.quartz.mongodb.util.TriggerTimeCalculator;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;

public class MongoStoreAssembler {

    public MongoConnector mongoConnector;
    public JobCompleteHandler jobCompleteHandler;
    public LockManager lockManager;
    public TriggerStateManager triggerStateManager;
    public TriggerRunner triggerRunner;
    public TriggerAndJobPersister persister;

    public CalendarDao calendarDao;
    public JobDao jobDao;
    public LocksDao locksDao;
    public SchedulerDao schedulerDao;
    public PausedJobGroupsDao pausedJobGroupsDao;
    public PausedTriggerGroupsDao pausedTriggerGroupsDao;
    public TriggerDao triggerDao;

    private QueryHelper queryHelper = new QueryHelper();

    public void build(MongoDBJobStore jobStore, ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        mongoConnector = MongoConnector.builder()
                .withClient(jobStore.mongo)
                .withUri(jobStore.mongoUri)
                .withCredentials(jobStore.username, jobStore.password)
                .withAddresses(jobStore.addresses)
                .withDatabaseName(jobStore.dbName)
                .withAuthDatabaseName(jobStore.authDbName)
                .withMaxConnectionsPerHost(jobStore.mongoOptionMaxConnectionsPerHost)
                .withConnectTimeoutMillis(jobStore.mongoOptionConnectTimeoutMillis)
                .withSocketTimeoutMillis(jobStore.mongoOptionSocketTimeoutMillis)
                .withSocketKeepAlive(jobStore.mongoOptionSocketKeepAlive)
                .withThreadsAllowedToBlockForConnectionMultiplier(
                        jobStore.mongoOptionThreadsAllowedToBlockForConnectionMultiplier)
                .withSSL(jobStore.mongoOptionEnableSSL, jobStore.mongoOptionSslInvalidHostNameAllowed)
                .build();

        MongoDatabase db = mongoConnector.selectDatabase(jobStore.dbName);

        JobConverter jobConverter = new JobConverter(jobStore.getClassLoaderHelper(loadHelper));
        jobDao = new JobDao(db.getCollection(jobStore.collectionPrefix + "jobs"), queryHelper, jobConverter);

        TriggerConverter triggerConverter = new TriggerConverter(jobDao);
        triggerDao = new TriggerDao(db.getCollection(jobStore.collectionPrefix + "triggers"), queryHelper, triggerConverter);

        calendarDao = new CalendarDao(db.getCollection(jobStore.collectionPrefix + "calendars"));
        locksDao = new LocksDao(db.getCollection(jobStore.collectionPrefix + "locks"), jobStore.instanceId);
        pausedJobGroupsDao = new PausedJobGroupsDao(db.getCollection(jobStore.collectionPrefix + "paused_job_groups"));
        pausedTriggerGroupsDao = new PausedTriggerGroupsDao(db.getCollection(jobStore.collectionPrefix + "paused_trigger_groups"));
        schedulerDao = new SchedulerDao(db.getCollection(jobStore.collectionPrefix + "schedulers"),
                jobStore.schedulerName, jobStore.instanceId, jobStore.clusterCheckinIntervalMillis,
                Clock.SYSTEM_CLOCK);

        MisfireHandler misfireHandler = new MisfireHandler(calendarDao, signaler, jobStore.misfireThreshold);
        TriggerTimeCalculator timeCalculator = new TriggerTimeCalculator(jobStore.jobTimeoutMillis,
                jobStore.triggerTimeoutMillis);

        persister = new TriggerAndJobPersister(triggerDao, jobDao, triggerConverter);

        jobCompleteHandler = new JobCompleteHandler(persister, signaler, jobDao, locksDao, triggerDao);

        lockManager = new LockManager(locksDao, timeCalculator);

        triggerStateManager = new TriggerStateManager(triggerDao, jobDao,
                pausedJobGroupsDao, pausedTriggerGroupsDao, queryHelper);
        triggerRunner = new TriggerRunner(persister, triggerDao, jobDao, locksDao, calendarDao,
                misfireHandler, triggerConverter, lockManager);
    }
}
