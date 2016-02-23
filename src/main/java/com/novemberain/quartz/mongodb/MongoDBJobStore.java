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

import java.io.IOException;
import java.util.*;

import static com.novemberain.quartz.mongodb.util.Keys.*;

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
  private ClassLoadHelper loadHelper;
  private String instanceId;
  private String[] addresses;
  private String mongoUri;
  private String username;
  private String password;
  private SchedulerSignaler signaler;
  protected long misfireThreshold = 5000l;
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

  private List<TriggerPersistenceHelper> persistenceHelpers = Arrays.asList(
          new SimpleTriggerPersistenceHelper(),
          new CalendarIntervalTriggerPersistenceHelper(),
          new CronTriggerPersistenceHelper(),
          new DailyTimeIntervalTriggerPersistenceHelper());

  private QueryHelper queryHelper = new QueryHelper();

  public MongoDBJobStore(){
  }

  public MongoDBJobStore(final MongoClient mongo){
    this.mongo = mongo;
  }

  public MongoDBJobStore(final String mongoUri, final String username, final String password) {
    this.mongoUri = mongoUri;
    this.username = username;
    this.password = password;
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
          throws SchedulerConfigException {
    this.loadHelper = loadHelper;
    this.signaler = signaler;

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
    initializeCollections(db);
    triggerStateManager = new TriggerStateManager(triggerDao, jobDao,
            pausedJobGroupsDao, pausedTriggerGroupsDao, queryHelper);
    ensureIndexes();
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
    storeTrigger(newTrigger, jobId, false);
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
    Document doc = jobDao.getJob(jobKey);
    if (doc == null) {
      //Return null if job does not exist, per interface
      return null;
    }

    JobLoader jobLoader = new JobLoader(getJobClassLoader());
    return jobLoader.loadJobDetail(doc);
  }

  @Override
  public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws
      JobPersistenceException {
    if (newTrigger.getJobKey() == null) {
      throw new JobPersistenceException("Trigger must be associated with a job. Please specify a JobKey.");
    }

    Document doc = jobDao.getJob(Keys.toFilter(newTrigger.getJobKey()));
    if (doc != null) {
      storeTrigger(newTrigger, doc.getObjectId("_id"), replaceExisting);
    } else {
      throw new JobPersistenceException("Could not find job with key " + newTrigger.getJobKey());
    }
  }

  // If the removal of the Trigger results in an 'orphaned' Job that is not 'durable',
  // then the job should be removed also.
  @Override
  public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    Bson filter = Keys.toFilter(triggerKey);
    Document trigger = triggerDao.findTrigger(filter);
    if (trigger != null) {
      if (trigger.containsKey(TRIGGER_JOB_ID)) {
        // There is only 1 job per trigger so no need to look further.
        Document job = jobDao.getById(trigger.get(TRIGGER_JOB_ID));
        // Remove the orphaned job if it's durable and has no other triggers associated with it,
        // remove it
        if (job != null && (!job.containsKey(JOB_DURABILITY) || job.get(JOB_DURABILITY).toString().equals("false"))) {
          if (triggerDao.hasLastTrigger(job)) {
            jobDao.remove(job);
          }
        }
      } else {
        log.debug("The trigger had no associated jobs");
      }
      //TODO: check if can .deleteOne(filter) here
      triggerDao.remove(filter);

      return true;
    }

    return false;
  }

  @Override
  public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
    for (TriggerKey key : triggerKeys) {
      removeTrigger(key);
    }
    return false;
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
    } catch(JobPersistenceException jpe) {
      storeTrigger(trigger, false); // put previous trigger back...
      throw jpe;
    }
    return true;
  }

  @Override
  public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    Document doc = triggerDao.findTrigger(Keys.toFilter(triggerKey));
    if (doc == null) {
      return null;
    }
    return toTrigger(triggerKey, doc);
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
    // TODO
    throw new UnsupportedOperationException();
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
    final List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();
    final Document doc = jobDao.getJob(jobKey);
    if (doc  == null) {
      return triggers;
    }
    
    for (Document item : triggerDao.findByJobId(doc.get("_id"))) {
      triggers.add(toTrigger(item));
    }

    return triggers;
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
    
    Date noLaterThanDate = new Date(noLaterThan + timeWindow);
    
    if (log.isDebugEnabled()) {
      log.debug("Finding up to {} triggers which have time less than {}", maxCount, noLaterThanDate);
    }
    
    Map<TriggerKey, OperableTrigger> triggers = new HashMap<TriggerKey, OperableTrigger>();
    
    doAcquireNextTriggers(triggers, noLaterThanDate, maxCount);
    
    List<OperableTrigger> triggerList = new LinkedList<OperableTrigger>(triggers.values());

    // Because we are handling a batch, we may have done multiple queries and while the result for each
    // query is in fire order, the result for the whole might not be, so sort them again
    
    Collections.sort(triggerList, new Comparator<OperableTrigger>() {

      @Override
      public int compare(OperableTrigger o1, OperableTrigger o2) {
        return (int) (o1.getNextFireTime().getTime() - o2.getNextFireTime().getTime());
      }
    });
    
    return triggerList;
  }
  
  private void doAcquireNextTriggers(Map<TriggerKey, OperableTrigger> triggers, Date noLaterThanDate, int maxCount)
      throws JobPersistenceException {
    QueryHelper queryHelper = new QueryHelper();
    Bson query = queryHelper.createNextTriggerQuery(noLaterThanDate);

    log.info("Found {} triggers which are eligible to be run.", triggerDao.getCount(query));

    for (Document triggerDoc : triggerDao.findEligibleToRun(query)) {
      if (maxCount <= triggers.size()) {
          break;
      }

      OperableTrigger trigger = toTrigger(triggerDoc);

      try {

        if (trigger == null) {
          continue;
        }

        if (triggers.containsKey(trigger.getKey())) {
          log.debug("Skipping trigger {} as we have already acquired it.", trigger.getKey());
          continue;
        }
        
        if (trigger.getNextFireTime() == null) {
          log.debug("Skipping trigger {} as it has no next fire time.", trigger.getKey());
          
          // No next fire time, so delete it
          removeTrigger(trigger.getKey());
          continue;
        }

        // deal with misfires
        if (applyMisfire(trigger)) {
          log.debug("Misfire trigger {}.", trigger.getKey());

          Date nextFireTime = trigger.getNextFireTime();
          
          if (nextFireTime == null) {
            log.debug("Removing trigger {} as it has no next fire time after the misfire was applied.", trigger.getKey());
            
            // No next fire time, so delete it
            removeTrigger(trigger.getKey());
            continue;
          }
          
          // The trigger has misfired and was rescheduled, its firetime may be too far in the future
          // and we don't want to hang the quartz scheduler thread up on <code>sigLock.wait(timeUntilTrigger);</code> 
          // so, check again that the trigger is due to fire
          if (nextFireTime.after(noLaterThanDate)) {
            log.debug("Skipping trigger {} as it misfired and was scheduled for {}.",
                    trigger.getKey(), trigger.getNextFireTime());
            continue;
          }
        }
        
        log.info("Inserting lock for trigger {}", trigger.getKey());

        Document lock = createTriggerDbLock(triggerDoc, instanceId);
        locksDao.insertLock(lock);

        log.info("Acquired trigger {}", trigger.getKey());
        triggers.put(trigger.getKey(), trigger);
        
      } catch (MongoWriteException e) {
        // someone else acquired this lock. Move on.
        log.info("Failed to acquire trigger {} due to a lock, reason: {}",
                trigger.getKey(), e.getError());

        Document filter = lockToBson(triggerDoc);
        Document existingLock = locksDao.findLock(filter);
        if (existingLock != null) {
          // support for trigger lock expirations
          if (isTriggerLockExpired(existingLock)) {
            log.warn("Lock for trigger {} is expired - removing lock and retrying trigger acquisition", trigger.getKey());
            removeTriggerLock(trigger);
            doAcquireNextTriggers(triggers, noLaterThanDate, maxCount - triggers.size());
          }
        } else {
          log.warn("Error retrieving expired lock from the database. Maybe it was deleted");
          doAcquireNextTriggers(triggers, noLaterThanDate, maxCount - triggers.size());
        }
      }
    }
  }

  @Override
  public void releaseAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
    try {
      removeTriggerLock(trigger);
    } catch (Exception e) {
      throw new JobPersistenceException(e.getLocalizedMessage(), e);
    }
  }

  @Override
  public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {

    List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>();

    for (OperableTrigger trigger : triggers) {
      log.debug("Fired trigger {}", trigger.getKey());
      Calendar cal = null;
      if (trigger.getCalendarName() != null) {
        cal = retrieveCalendar(trigger.getCalendarName());
        if (cal == null)
          continue;
      }

      Date prevFireTime = trigger.getPreviousFireTime();
      trigger.triggered(cal);

      TriggerFiredBundle bundle = new TriggerFiredBundle(retrieveJob(
          trigger), trigger, cal,
          false, new Date(), trigger.getPreviousFireTime(), prevFireTime,
          trigger.getNextFireTime());

      JobDetail job = bundle.getJobDetail();
      
      if (job != null) {
        
        try {
          if (job.isConcurrentExectionDisallowed()) {
            log.debug("Inserting lock for job {}", job.getKey());
            Document lock = new Document();
            lock.put(KEY_NAME, "jobconcurrentlock:" + job.getKey().getName());
            lock.put(KEY_GROUP, job.getKey().getGroup());
            lock.put(LOCK_INSTANCE_ID, instanceId);
            lock.put(LOCK_TIME, new Date());
            locksDao.insertLock(lock);
          }
          
          results.add(new TriggerFiredResult(bundle));
          storeTrigger(trigger, true);
        } catch (MongoWriteException dk) {
            log.debug("Job disallows concurrent execution and is already running {}", job.getKey());
          
          // Remove the trigger lock
          removeTriggerLock(trigger);
          
          // Find the existing lock and if still present, and expired, then remove it.
          Bson lock = createLockFilter(job);
          Document existingLock = locksDao.findLock(lock);
          if (existingLock != null) {
            if (isJobLockExpired(existingLock)) {
              log.debug("Removing expired lock for job {}", job.getKey());
              locksDao.remove(existingLock);
            }
          }
        }
      }

    }
    return results;
  }

  @Override
  public void triggeredJobComplete(OperableTrigger trigger, JobDetail job,
                                   CompletedExecutionInstruction triggerInstCode)
      throws JobPersistenceException {
    
    log.debug("Trigger completed {}", trigger.getKey());
    
    if (job.isPersistJobDataAfterExecution()) {
      if (job.getJobDataMap().isDirty()) {
        log.debug("Job data map dirty, will store {}", job.getKey());
        jobDao.storeJobInMongo(job, true);
      }
    }
    
    if (job.isConcurrentExectionDisallowed()) {
      log.debug("Removing lock for job {}", job.getKey());
      locksDao.remove(createLockFilter(job));
    }
    
    // check for trigger deleted during execution...
    OperableTrigger trigger2 = retrieveTrigger(trigger.getKey());
    if (trigger2 != null) {
      if (triggerInstCode == CompletedExecutionInstruction.DELETE_TRIGGER) {
        if (trigger.getNextFireTime() == null) {
          // double check for possible reschedule within job
          // execution, which would cancel the need to delete...
          if (trigger2.getNextFireTime() == null) {
            removeTrigger(trigger.getKey());
          }
        } else {
          removeTrigger(trigger.getKey());
          signaler.signalSchedulingChange(0L);
        }
      } else if (triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
        // TODO: need to store state
        signaler.signalSchedulingChange(0L);
      } else if (triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
        // TODO: need to store state
        signaler.signalSchedulingChange(0L);
      } else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
        // TODO: need to store state
        signaler.signalSchedulingChange(0L);
      } else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
        // TODO: need to store state
        signaler.signalSchedulingChange(0L);
      }
    }

    removeTriggerLock(trigger);
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

  public long getMisfireThreshold() {
    return misfireThreshold;
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

  //
  // Implementation
  //

  private void initializeCollections(MongoDatabase db) {
    jobDao = new JobDao(db.getCollection(collectionPrefix + "jobs"), queryHelper);
    triggerDao = new TriggerDao(db.getCollection(collectionPrefix + "triggers"), queryHelper);
    calendarDao = new CalendarDao(db.getCollection(collectionPrefix + "calendars"));
    locksDao = new LocksDao(db.getCollection(collectionPrefix + "locks"), instanceId);

    pausedJobGroupsDao = new PausedJobGroupsDao(db.getCollection(collectionPrefix + "paused_job_groups"));
    pausedTriggerGroupsDao = new PausedTriggerGroupsDao(db.getCollection(collectionPrefix + "paused_trigger_groups"));
  }

  protected OperableTrigger toTrigger(Document doc) throws JobPersistenceException {
    TriggerKey key = new TriggerKey(doc.getString(KEY_NAME), doc.getString(KEY_GROUP));
    return toTrigger(key, doc);
  }

  protected OperableTrigger toTrigger(TriggerKey triggerKey, Document triggerDoc) throws JobPersistenceException {
    OperableTrigger trigger;
    try {
      @SuppressWarnings("unchecked")
      Class<OperableTrigger> triggerClass = (Class<OperableTrigger>) getTriggerClassLoader()
              .loadClass(triggerDoc.getString(TRIGGER_CLASS));
      trigger = triggerClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new JobPersistenceException("Could not find trigger class " + triggerDoc.get(TRIGGER_CLASS));
    } catch (Exception e) {
      throw new JobPersistenceException("Could not instantiate trigger class " + triggerDoc.get(TRIGGER_CLASS));
    }

    TriggerPersistenceHelper tpd = triggerPersistenceDelegateFor(trigger);

    trigger.setKey(triggerKey);
    trigger.setCalendarName(triggerDoc.getString(TRIGGER_CALENDAR_NAME));
    trigger.setDescription(triggerDoc.getString(TRIGGER_DESCRIPTION));
    trigger.setFireInstanceId(triggerDoc.getString(TRIGGER_FIRE_INSTANCE_ID));
    trigger.setMisfireInstruction(triggerDoc.getInteger(TRIGGER_MISFIRE_INSTRUCTION));
    trigger.setNextFireTime(triggerDoc.getDate(TRIGGER_NEXT_FIRE_TIME));
    trigger.setPreviousFireTime(triggerDoc.getDate(TRIGGER_PREVIOUS_FIRE_TIME));
    trigger.setPriority(triggerDoc.getInteger(TRIGGER_PRIORITY));
    
    String jobDataString = triggerDoc.getString(JOB_DATA);
    
    if (jobDataString != null) {
      try {
        SerialUtils.deserialize(trigger.getJobDataMap(), jobDataString);
      } catch (IOException e) {
        throw new JobPersistenceException("Could not deserialize job data for trigger " + triggerDoc.get(TRIGGER_CLASS));
      }
    }
    
    try {
        trigger.setStartTime(triggerDoc.getDate(TRIGGER_START_TIME));
        trigger.setEndTime(triggerDoc.getDate(TRIGGER_END_TIME));
    } catch(IllegalArgumentException e) {
        //Ignore illegal arg exceptions thrown by triggers doing JIT validation of start and endtime
        log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e);
    }


    try {
        trigger.setStartTime(triggerDoc.getDate(TRIGGER_START_TIME));
        trigger.setEndTime(triggerDoc.getDate(TRIGGER_END_TIME));
    } catch(IllegalArgumentException e) {
        //Ignore illegal arg exceptions thrown by triggers doing JIT validation of start and endtime
        log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e);
    }

    trigger = tpd.setExtraPropertiesAfterInstantiation(trigger, triggerDoc);

    Document job = jobDao.getById(triggerDoc.get(TRIGGER_JOB_ID));
    if (job != null) {
      trigger.setJobKey(new JobKey(job.getString(KEY_NAME), job.getString(KEY_GROUP)));
      return trigger;
    } else {
      // job was deleted
      return null;
    }
  }

  protected ClassLoader getTriggerClassLoader() {
    return org.quartz.Job.class.getClassLoader();
  }

  private TriggerPersistenceHelper triggerPersistenceDelegateFor(OperableTrigger trigger) {
    TriggerPersistenceHelper result = null;

    for (TriggerPersistenceHelper d : persistenceHelpers) {
      if (d.canHandleTriggerType(trigger)) {
        result = d;
        break;
      }
    }

    assert result != null;
    return result;
  }

  protected boolean isTriggerLockExpired(Document lock) {
    Date lockTime = lock.getDate(LOCK_TIME);
    long elaspedTime = System.currentTimeMillis() - lockTime.getTime();
    return (elaspedTime > triggerTimeoutMillis);
  }

  protected boolean isJobLockExpired(Document lock) {
    Date lockTime = lock.getDate(LOCK_TIME);
    long elaspedTime = System.currentTimeMillis() - lockTime.getTime();
    return (elaspedTime > jobTimeoutMillis);
  }

  protected boolean applyMisfire(OperableTrigger trigger) throws JobPersistenceException {
    long misfireTime = System.currentTimeMillis();
    if (getMisfireThreshold() > 0) {
      misfireTime -= getMisfireThreshold();
    }

    Date tnft = trigger.getNextFireTime();
    if (tnft == null || tnft.getTime() > misfireTime
        || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
      return false;
    }

    Calendar cal = null;
    if (trigger.getCalendarName() != null) {
      cal = retrieveCalendar(trigger.getCalendarName());
    }

    signaler.notifyTriggerListenersMisfired((OperableTrigger) trigger.clone());

    trigger.updateAfterMisfire(cal);

    if (trigger.getNextFireTime() == null) {
      signaler.notifySchedulerListenersFinalized(trigger);
    } else if (tnft.equals(trigger.getNextFireTime())) {
      return false;
    }

    storeTrigger(trigger, true);
    return true;
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
    } catch (MongoException e){
      throw new SchedulerConfigException("Error while initializing the indexes", e);
    }
  }

  protected void storeTrigger(OperableTrigger newTrigger, ObjectId jobId, boolean replaceExisting)
          throws JobPersistenceException {
    Document trigger = convertToBson(newTrigger, jobId);
    if (newTrigger.getJobDataMap().size() > 0) {
      try {
        String jobDataString = SerialUtils.serialize(newTrigger.getJobDataMap());
        trigger.put(JOB_DATA, jobDataString);
      } catch (IOException ioe) {
        throw new JobPersistenceException("Could not serialise job data map on the trigger for " + newTrigger.getKey(), ioe);
      }
    }
    
    TriggerPersistenceHelper tpd = triggerPersistenceDelegateFor(newTrigger);
    trigger = tpd.injectExtraPropertiesForInsert(newTrigger, trigger);

    if (replaceExisting) {
      trigger.remove("_id");
      triggerDao.replace(newTrigger.getKey(), trigger);
    } else {
      triggerDao.insert(trigger, newTrigger);
    }
  }

  protected void removeTriggerLock(OperableTrigger trigger) {
    log.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
    Bson lock = Keys.toFilter(trigger.getKey());

    // Comment this out, as expired trigger locks should be deleted by any another instance
    // lock.put(LOCK_INSTANCE_ID, instanceId);

    locksDao.remove(lock);
    log.info("Trigger lock {}.{} removed.", trigger.getKey(), instanceId);
  }

  protected ClassLoader getJobClassLoader() {
    return loadHelper.getClassLoader();
  }

  private JobDetail retrieveJob(OperableTrigger trigger) throws JobPersistenceException {
    try {
      return retrieveJob(trigger.getJobKey());
    } catch (JobPersistenceException e) {
      removeTriggerLock(trigger);
      throw e;
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
