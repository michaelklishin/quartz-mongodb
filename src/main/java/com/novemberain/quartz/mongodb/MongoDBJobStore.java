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
import com.mongodb.client.model.*;
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

import static com.mongodb.client.model.Sorts.ascending;
import static com.novemberain.quartz.mongodb.Keys.*;

public class MongoDBJobStore implements JobStore, Constants {
  protected final Logger log = LoggerFactory.getLogger(getClass());

  private static final Bson KEY_AND_GROUP_FIELDS = Projections.include(KEY_GROUP, KEY_NAME);

  @Deprecated
  private static MongoClient overriddenMongo;

  /**
   * @deprecated use {@link #MongoDBJobStore(MongoClient)}
   */
  @Deprecated
  public static void overrideMongo(MongoClient mongo) {
    overriddenMongo = mongo;
  }

  private MongoClient mongo;
  private String collectionPrefix = "quartz_";
  private String dbName;
  private String authDbName;
  private MongoCollection<Document> jobCollection;
  private MongoCollection<Document> triggerCollection;
  private MongoCollection<Document> calendarCollection;
  private MongoCollection<Document> locksCollection;
  private MongoCollection<Document> pausedTriggerGroupsCollection;
  private MongoCollection<Document> pausedJobGroupsCollection;
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

  private List<TriggerPersistenceHelper> persistenceHelpers;
  private QueryHelper queryHelper;

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
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
    this.loadHelper = loadHelper;
    this.signaler = signaler;
    if (this.mongo == null) {
      initializeMongo();
    } else {
      if (mongoUri != null  || username != null || password != null || addresses != null){
        throw new SchedulerConfigException("Configure either a Mongo instance or MongoDB connection parameters.");
      }
    }

    MongoDatabase db = selectDatabase(this.mongo);
    initializeCollections(db);
    ensureIndexes();

    initializeHelpers();
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
    mongo.close();
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
    ObjectId jobId = storeJobInMongo(newJob, false);

    log.debug("Storing job {} and trigger {}", newJob.getKey(), newTrigger.getKey());
    storeTrigger(newTrigger, jobId, false);
  }

  @Override
  public void storeJob(JobDetail newJob, boolean replaceExisting)
          throws JobPersistenceException {
    storeJobInMongo(newJob, replaceExisting);
  }

  @Override
  public void storeJobsAndTriggers(Map<JobDetail, List<Trigger>> triggersAndJobs, boolean replace)
      throws JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
    Bson keyObject = Keys.toFilter(jobKey);
    Document item = jobCollection.find(keyObject).first();
    if (item != null) {
      jobCollection.deleteMany(keyObject);
      triggerCollection.deleteMany(Filters.eq(TRIGGER_JOB_ID, item.get("_id")));
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
    Document doc = findJobDocumentByKey(jobKey);
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

    Document doc = jobCollection.find(Keys.toFilter(newTrigger.getJobKey())).first();
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
    List<Document> triggers = triggerCollection.find(filter).limit(2).into(new ArrayList<Document>(2));
    if (triggers.size() > 0) {
      Document trigger = triggers.get(0);
      if (trigger.containsKey(TRIGGER_JOB_ID)) {
        // There is only 1 job per trigger so no need to look further.
        Document job = jobCollection.find(Filters.eq("_id", trigger.get(TRIGGER_JOB_ID))).first();
        // Remove the orphaned job if it's durable and has no other triggers associated with it,
        // remove it
        if (job != null && (!job.containsKey(JOB_DURABILITY) || job.get(JOB_DURABILITY).toString().equals("false"))) {
          List<Document> referencedTriggers = triggerCollection
                  .find(Filters.eq(TRIGGER_JOB_ID, job.get("_id")))
                  .limit(2)
                  .into(new ArrayList<Document>(2));
          if (referencedTriggers.size() == 1) {
            jobCollection.deleteMany(job);
          }
        }
      } else {
        log.debug("The trigger had no associated jobs");
      }
      //TODO: check if can .deleteOne(filter) here
      triggerCollection.deleteMany(filter);

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
    if (triggerCollection.count(filter) > 0) {
      triggerCollection.deleteMany(filter);
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
    Document doc = triggerCollection.find(Keys.toFilter(triggerKey)).first();
    if (doc == null) {
      return null;
    }
    return toTrigger(triggerKey, doc);
  }

  @Override
  public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
    return jobCollection.count(Keys.toFilter(jobKey)) > 0;
  }

  @Override
  public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
    return triggerCollection.count(Keys.toFilter(triggerKey)) > 0;
  }

  @Override
  public void clearAllSchedulingData() throws JobPersistenceException {
    //TODO: consider using coll.drop() here
    jobCollection.deleteMany(new Document());
    triggerCollection.deleteMany(new Document());
    calendarCollection.deleteMany(new Document());
    pausedJobGroupsCollection.deleteMany(new Document());
    pausedTriggerGroupsCollection.deleteMany(new Document());
  }

  @Override
  public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
      throws JobPersistenceException {
    // TODO implement updating triggers
    if (updateTriggers) {
      throw new UnsupportedOperationException("Updating triggers is not supported.");
    }

    Document doc = new Document(CALENDAR_NAME, name)
            .append(CALENDAR_SERIALIZED_OBJECT, SerialUtils.serialize(calendar));
    calendarCollection.insertOne(doc);
  }

  @Override
  public boolean removeCalendar(String calName) throws JobPersistenceException {
    Bson searchObj = Filters.eq(CALENDAR_NAME, calName);
    if (calendarCollection.count(searchObj) > 0) {
      calendarCollection.deleteMany(searchObj);
      return true;
    }
    return false;
  }

  @Override
  public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumberOfJobs() throws JobPersistenceException {
    return (int) jobCollection.count();
  }

  @Override
  public int getNumberOfTriggers() throws JobPersistenceException {
    return (int) triggerCollection.count();
  }

  @Override
  public int getNumberOfCalendars() throws JobPersistenceException {
    return (int) calendarCollection.count();
  }

  public int getNumberOfLocks() {
    return (int) locksCollection.count();
  }

  @Override
  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
    Set<JobKey> keys = new HashSet<JobKey>();
    Bson query = queryHelper.matchingKeysConditionFor(matcher);
    for (Document doc : jobCollection.find(query).projection(KEY_AND_GROUP_FIELDS)) {
        keys.add(Keys.toJobKey(doc));
    }
    return keys;
  }

  @Override
  public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
    Set<TriggerKey> keys = new HashSet<TriggerKey>();
    Bson query = queryHelper.matchingKeysConditionFor(matcher);
    for (Document doc : triggerCollection.find(query).projection(KEY_AND_GROUP_FIELDS)) {
        keys.add(Keys.toTriggerKey(doc));
    }
    return keys;
  }

  @Override
  public List<String> getJobGroupNames() throws JobPersistenceException {
    return jobCollection.distinct(KEY_GROUP, String.class).into(new ArrayList<String>());
  }

  @Override
  public List<String> getTriggerGroupNames() throws JobPersistenceException {
    return triggerCollection.distinct(KEY_GROUP, String.class).into(new ArrayList<String>());
  }

  @Override
  public List<String> getCalendarNames() throws JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
    final List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();
    final Document doc = findJobDocumentByKey(jobKey);
    if (doc  == null) {
      return triggers;
    }
    
    for (Document item : triggerCollection.find(Filters.eq(TRIGGER_JOB_ID, doc.get("_id")))) {
      triggers.add(toTrigger(item));
    }

    return triggers;
  }

  @Override
  public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
    Document doc = findTriggerDocumentByKey(triggerKey);
    return triggerStateForValue(doc.getString(TRIGGER_STATE));
  }

  @Override
  public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    triggerCollection.updateOne(Keys.toFilter(triggerKey), updateThatSetsTriggerStateTo(STATE_PAUSED));
  }

  @Override
  public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
    triggerCollection.updateMany(
            queryHelper.matchingKeysConditionFor(matcher),
            updateThatSetsTriggerStateTo(STATE_PAUSED),
            new UpdateOptions().upsert(false));

    final GroupHelper groupHelper = new GroupHelper(triggerCollection, queryHelper);
    final Set<String> set = groupHelper.groupsThatMatch(matcher);
    markTriggerGroupsAsPaused(set);

    return set;
  }

  @Override
  public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    // TODO: port blocking behavior and misfired triggers handling from StdJDBCDelegate in Quartz
    triggerCollection.updateOne(Keys.toFilter(triggerKey), updateThatSetsTriggerStateTo(STATE_WAITING));
  }

  @Override
  public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
    triggerCollection.updateMany(
            queryHelper.matchingKeysConditionFor(matcher),
            updateThatSetsTriggerStateTo(STATE_WAITING),
            new UpdateOptions().upsert(false));

    final GroupHelper groupHelper = new GroupHelper(triggerCollection, queryHelper);
    final Set<String> set = groupHelper.groupsThatMatch(matcher);
    unmarkTriggerGroupsAsPaused(set);
    return set;
  }

  @Override
  public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
    return pausedTriggerGroupsCollection.distinct(KEY_GROUP, String.class).into(new HashSet<String>());
  }

  public Set<String> getPausedJobGroups() throws JobPersistenceException {
    return pausedJobGroupsCollection.distinct(KEY_GROUP, String.class).into(new HashSet<String>());
  }

  @Override
  public void pauseAll() throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(triggerCollection, queryHelper);
    triggerCollection.updateMany(new Document(), updateThatSetsTriggerStateTo(STATE_PAUSED));
    this.markTriggerGroupsAsPaused(groupHelper.allGroups());
  }

  @Override
  public void resumeAll() throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(triggerCollection, queryHelper);
    triggerCollection.updateMany(new Document(), updateThatSetsTriggerStateTo(STATE_WAITING));
    this.unmarkTriggerGroupsAsPaused(groupHelper.allGroups());
  }

  @Override
  public void pauseJob(JobKey jobKey) throws JobPersistenceException {
    final ObjectId jobId = findJobDocumentByKey(jobKey).getObjectId("_id");
    final TriggerGroupHelper groupHelper = new TriggerGroupHelper(triggerCollection, queryHelper);
    List<String> groups = groupHelper.groupsForJobId(jobId);
    triggerCollection.updateMany(new Document(TRIGGER_JOB_ID, jobId), updateThatSetsTriggerStateTo(STATE_PAUSED));
    this.markTriggerGroupsAsPaused(groups);
  }

  @Override
  public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
    final TriggerGroupHelper groupHelper = new TriggerGroupHelper(triggerCollection, queryHelper);
    List<String> groups = groupHelper.groupsForJobIds(idsFrom(findJobDocumentsThatMatch(groupMatcher)));
    triggerCollection.updateMany(queryHelper.inGroups(groups), updateThatSetsTriggerStateTo(STATE_PAUSED));
    this.markJobGroupsAsPaused(groups);

    return groups;
  }

  @Override
  public void resumeJob(JobKey jobKey) throws JobPersistenceException {
    final ObjectId jobId = findJobDocumentByKey(jobKey).getObjectId("_id");
    // TODO: port blocking behavior and misfired triggers handling from StdJDBCDelegate in Quartz
    triggerCollection.updateMany(new Document(TRIGGER_JOB_ID, jobId), updateThatSetsTriggerStateTo(STATE_WAITING));
  }

  @Override
  public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
    final TriggerGroupHelper groupHelper = new TriggerGroupHelper(triggerCollection, queryHelper);
    List<String> groups = groupHelper.groupsForJobIds(idsFrom(findJobDocumentsThatMatch(groupMatcher)));
    triggerCollection.updateMany(queryHelper.inGroups(groups), updateThatSetsTriggerStateTo(STATE_WAITING));
    this.unmarkJobGroupsAsPaused(groups);

    return groups;
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

    log.info("Found {} triggers which are eligible to be run.", triggerCollection.count(query));

    for (Document triggerDoc : triggerCollection.find(query).sort(ascending(TRIGGER_NEXT_FIRE_TIME))) {
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
        // A lock needs to be written with FSYNCED to be 100% effective across multiple servers
        locksCollection.withWriteConcern(WriteConcern.FSYNCED).insertOne(lock);
        
        log.info("Acquired trigger {}", trigger.getKey());
        triggers.put(trigger.getKey(), trigger);
        
      } catch (MongoWriteException e) {
        // someone else acquired this lock. Move on.
        log.info("Failed to acquire trigger {} due to a lock, reason: {}",
                trigger.getKey(), e.getError());

        Document filter = lockToBson(triggerDoc);
        Document existingLock = locksCollection.find(filter).first();
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
            // A lock needs to be written with FSYNCED to be 100% effective across multiple servers
            locksCollection.withWriteConcern(WriteConcern.FSYNCED).insertOne(lock);
          }
          
          results.add(new TriggerFiredResult(bundle));
          storeTrigger(trigger, true);
        } catch (MongoWriteException dk) {
            log.debug("Job disallows concurrent execution and is already running {}", job.getKey());
          
          // Remove the trigger lock
          removeTriggerLock(trigger);
          
          // Find the existing lock and if still present, and expired, then remove it.
          Bson lock = createLockFilter(job);
          Document existingLock = locksCollection.find(lock).first();
          if (existingLock != null) {
            if (isJobLockExpired(existingLock)) {
              log.debug("Removing expired lock for job {}", job.getKey());
              locksCollection.deleteMany(existingLock);
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
        storeJobInMongo(job, true);
      }
    }
    
    if (job.isConcurrentExectionDisallowed()) {
      log.debug("Removing lock for job {}", job.getKey());
      locksCollection.deleteMany(createLockFilter(job));
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

    private Bson createLockFilter(JobDetail job) {
        return Filters.and(
                Filters.eq(KEY_NAME, "jobconcurrentlock:" + job.getKey().getName()),
                Filters.eq(KEY_GROUP, job.getKey().getGroup()));
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
    return jobCollection;
  }

  public MongoCollection<Document> getTriggerCollection() {
    return triggerCollection;
  }

  public MongoCollection<Document> getCalendarCollection() {
    return calendarCollection;
  }

  public MongoCollection<Document> getLocksCollection() {
    return locksCollection;
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

  private void initializeMongo() throws SchedulerConfigException {
    if (overriddenMongo != null) {
      this.mongo = overriddenMongo;
    } else {
      this.mongo = connectToMongoDB();
    }
    if (this.mongo == null) {
      throw new SchedulerConfigException("Could not connect to MongoDB! Please check that quartz-mongodb configuration is correct.");
    }
  }

  private void initializeCollections(MongoDatabase db) {
    jobCollection = db.getCollection(collectionPrefix + "jobs");
    triggerCollection = db.getCollection(collectionPrefix + "triggers");
    calendarCollection = db.getCollection(collectionPrefix + "calendars");
    locksCollection = db.getCollection(collectionPrefix + "locks");

    pausedTriggerGroupsCollection = db.getCollection(collectionPrefix + "paused_trigger_groups");
    pausedJobGroupsCollection = db.getCollection(collectionPrefix + "paused_job_groups");
  }

  private MongoDatabase selectDatabase(MongoClient mongo) {
    // MongoDB defaults are insane, set a reasonable write concern explicitly. MK.
    // But we would be insane not to override this when writing lock records. LB.
    mongo.setWriteConcern(WriteConcern.JOURNALED);
    return mongo.getDatabase(dbName);
  }

  private MongoClient connectToMongoDB() throws SchedulerConfigException {
    if (mongoUri == null && (addresses == null || addresses.length == 0)) {
      throw new SchedulerConfigException("At least one MongoDB address or a MongoDB URI must be specified .");
    }

    if(mongoUri != null) {
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

  private List<ServerAddress> collectServerAddresses() {
    List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
    for (String a : addresses) {
      serverAddresses.add(new ServerAddress(a));
    }
    return serverAddresses;
  }

  private MongoClientOptions createOptions() {
    MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();
    optionsBuilder.writeConcern(WriteConcern.SAFE);

    if (mongoOptionMaxConnectionsPerHost != null) optionsBuilder.connectionsPerHost(mongoOptionMaxConnectionsPerHost);
    if (mongoOptionConnectTimeoutMillis != null) optionsBuilder.connectTimeout(mongoOptionConnectTimeoutMillis);
    if (mongoOptionSocketTimeoutMillis != null) optionsBuilder.socketTimeout(mongoOptionSocketTimeoutMillis);
    if (mongoOptionSocketKeepAlive != null) optionsBuilder.socketKeepAlive(mongoOptionSocketKeepAlive);
    if (mongoOptionThreadsAllowedToBlockForConnectionMultiplier != null) {
      optionsBuilder.threadsAllowedToBlockForConnectionMultiplier(mongoOptionThreadsAllowedToBlockForConnectionMultiplier);
    }
    if (mongoOptionEnableSSL != null) {
      optionsBuilder.sslEnabled(mongoOptionEnableSSL);
      if (mongoOptionSslInvalidHostNameAllowed != null) {
        optionsBuilder.sslInvalidHostNameAllowed(mongoOptionSslInvalidHostNameAllowed);
      }
    }

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

  private MongoClient connectToMongoDB(final String mongoUriAsString) throws SchedulerConfigException {
    try {
      return new MongoClient(new MongoClientURI(mongoUriAsString));
   } catch (final MongoException e) {
      throw new SchedulerConfigException("MongoDB driver thrown an exception", e);
    }
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

    Document job = jobCollection.find(Filters.eq("_id", triggerDoc.get(TRIGGER_JOB_ID))).first();
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

      jobCollection.createIndex(KEY_AND_GROUP_FIELDS, new IndexOptions().unique(true));
      triggerCollection.createIndex(KEY_AND_GROUP_FIELDS, new IndexOptions().unique(true));
      locksCollection.createIndex(KEY_AND_GROUP_FIELDS, new IndexOptions().unique(true));

      // Need this to stop table scan when removing all locks
      locksCollection.createIndex(Projections.include(LOCK_INSTANCE_ID));
      
      // remove all locks for this instance on startup
      locksCollection.deleteMany(Filters.eq(LOCK_INSTANCE_ID, instanceId));

      calendarCollection.createIndex(Projections.include(CALENDAR_NAME), new IndexOptions().unique(true));

      try {
        // Drop the old indexes that were declared as name then group rather than group then name
        jobCollection.dropIndex("keyName_1_keyGroup_1");
        triggerCollection.dropIndex("keyName_1_keyGroup_1");
        locksCollection.dropIndex("keyName_1_keyGroup_1");
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
      triggerCollection.replaceOne(toFilter(newTrigger.getKey()), trigger);
    } else {
      try {
        triggerCollection.insertOne(trigger);
      } catch (MongoWriteException key) {
        throw new ObjectAlreadyExistsException(newTrigger);
      }
    }
  }

  protected ObjectId storeJobInMongo(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException {
    JobKey key = newJob.getKey();

    Bson keyDbo = toFilter(key);
    Document job = Keys.convertToBson(newJob, key);

    Document object = jobCollection.find(keyDbo).first();

    ObjectId objectId = null;
    if (object != null && replaceExisting) {
      jobCollection.replaceOne(keyDbo, job);
    } else if (object == null) {
      try {
        jobCollection.insertOne(job);
        objectId = job.getObjectId("_id");
      } catch (MongoWriteException e) {
        // Fine, find it and get its id.
        object = jobCollection.find(keyDbo).first();
        objectId = object.getObjectId("_id");
      }
    } else {
      objectId = object.getObjectId("_id");
    }

    return objectId;
  }

  protected void removeTriggerLock(OperableTrigger trigger) {
    log.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
    Bson lock = Keys.toFilter(trigger.getKey());

    // Comment this out, as expired trigger locks should be deleted by any another instance
    // lock.put(LOCK_INSTANCE_ID, instanceId);

    locksCollection.deleteMany(lock);
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

  protected Document findJobDocumentByKey(JobKey key) {
    return jobCollection.find(toFilter(key)).first();
  }

  protected Document findTriggerDocumentByKey(TriggerKey key) {
    return triggerCollection.find(toFilter(key)).first();
  }

  private void initializeHelpers() {
    this.persistenceHelpers = new ArrayList<TriggerPersistenceHelper>();

    persistenceHelpers.add(new SimpleTriggerPersistenceHelper());
    persistenceHelpers.add(new CalendarIntervalTriggerPersistenceHelper());
    persistenceHelpers.add(new CronTriggerPersistenceHelper());
    persistenceHelpers.add(new DailyTimeIntervalTriggerPersistenceHelper());

    this.queryHelper = new QueryHelper();
  }

  private TriggerState triggerStateForValue(String ts) {
    if (ts == null) {
      return TriggerState.NONE;
    }

    if (ts.equals(STATE_DELETED)) {
      return TriggerState.NONE;
    }

    if (ts.equals(STATE_COMPLETE)) {
      return TriggerState.COMPLETE;
    }

    if (ts.equals(STATE_PAUSED)) {
      return TriggerState.PAUSED;
    }

    if (ts.equals(STATE_PAUSED_BLOCKED)) {
      return TriggerState.PAUSED;
    }

    if (ts.equals(STATE_ERROR)) {
      return TriggerState.ERROR;
    }

    if (ts.equals(STATE_BLOCKED)) {
      return TriggerState.BLOCKED;
    }

    // waiting or acquired
    return TriggerState.NORMAL;
  }

  private Bson updateThatSetsTriggerStateTo(String state) {
    return new Document("$set", new Document(TRIGGER_STATE, state));
  }

  private void markTriggerGroupsAsPaused(Collection<String> groups) {
    List<Document> list = new ArrayList<Document>();
    for (String s : groups) {
      list.add(new Document(KEY_GROUP, s));
    }
    pausedTriggerGroupsCollection.insertMany(list);
  }

  private void unmarkTriggerGroupsAsPaused(Collection<String> groups) {
    pausedTriggerGroupsCollection.deleteMany(Filters.in(KEY_GROUP, groups));
  }

  private void markJobGroupsAsPaused(List<String> groups) {
    if (groups == null) {
      throw new IllegalArgumentException("groups cannot be null!");
    }
    List<Document> list = new ArrayList<Document>();
    for (String s : groups) {
      list.add(new Document(KEY_GROUP, s));
    }
    pausedJobGroupsCollection.insertMany(list);
  }

  private void unmarkJobGroupsAsPaused(Collection<String> groups) {
    pausedJobGroupsCollection.deleteMany(Filters.in(KEY_GROUP, groups));
  }


  private Collection<ObjectId> idsFrom(Collection<Document> docs) {
    // so much repetitive code would be gone if Java collections just had .map and .filter…
    List<ObjectId> list = new ArrayList<ObjectId>();
    for (Document doc : docs) {
      list.add(doc.getObjectId("_id"));
    }
    return list;
  }

  private Collection<Document> findJobDocumentsThatMatch(GroupMatcher<JobKey> matcher) {
    final GroupHelper groupHelper = new GroupHelper(jobCollection, queryHelper);
    return groupHelper.inGroupsThatMatch(matcher);
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
