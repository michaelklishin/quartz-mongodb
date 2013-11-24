/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.novemberain.quartz.mongodb;

import com.mongodb.*;
import com.mongodb.MongoException.DuplicateKey;

import org.bson.types.ObjectId;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;
import java.util.*;

import static com.novemberain.quartz.mongodb.Keys.*;

public class MongoDBJobStore implements JobStore, Constants {
  protected final Logger log = LoggerFactory.getLogger(getClass());

  public static final DBObject KEY_AND_GROUP_FIELDS = BasicDBObjectBuilder.start().
      append(KEY_GROUP, 1).
      append(KEY_NAME, 1).
      get();

  private Mongo mongo;
  private String collectionPrefix = "quartz_";
  private String dbName;
  private DBCollection jobCollection;
  private DBCollection triggerCollection;
  private DBCollection calendarCollection;
  private ClassLoadHelper loadHelper;
  private DBCollection locksCollection;
  private DBCollection pausedTriggerGroupsCollection;
  private DBCollection pausedJobGroupsCollection;
  private String instanceId;
  private String[] addresses;
  private String mongoUri;
  private String username;
  private String password;
  private SchedulerSignaler signaler;
  protected long misfireThreshold = 5000l;
  private long triggerTimeoutMillis = 10 * 60 * 1000L;

  private List<TriggerPersistenceHelper> persistenceHelpers;
  private QueryHelper queryHelper;

  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
    this.loadHelper = loadHelper;
    this.signaler = signaler;

    if (mongoUri == null && (addresses == null || addresses.length == 0)) {
      throw new SchedulerConfigException("At least one MongoDB address or a MongoDB URI must be specified .");
    }

    this.mongo = connectToMongoDB();
    if (this.mongo == null) {
      throw new SchedulerConfigException("Could not connect to MongoDB! Please check that quartz-mongodb configuration is correct.");
    }

    DB db = selectDatabase(this.mongo);
    initializeCollections(db);
    ensureIndexes();

    initializeHelpers();
  }

  public void schedulerStarted() throws SchedulerException {
    // No-op
  }

  public void schedulerPaused() {
    // No-op
  }

  public void schedulerResumed() {
  }

  public void shutdown() {
    mongo.close();
  }

  public boolean supportsPersistence() {
    return true;
  }

  public long getEstimatedTimeToReleaseAndAcquireTrigger() {
    // this will vary...
    return 200;
  }

  public boolean isClustered() {
    return false;
  }

  /**
   * Job and Trigger storage Methods
   */
  public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws
      JobPersistenceException {
    ObjectId jobId = storeJobInMongo(newJob, false);

    log.debug("Storing job {} and trigger {}", newJob.getKey(), newTrigger.getKey());
    storeTrigger(newTrigger, jobId, false);
  }

  public void storeJob(JobDetail newJob, boolean replaceExisting) throws
      JobPersistenceException {
    storeJobInMongo(newJob, replaceExisting);
  }

  public void storeJobsAndTriggers(Map<JobDetail, List<Trigger>> triggersAndJobs, boolean replace)
      throws JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
    BasicDBObject keyObject = Keys.keyToDBObject(jobKey);
    DBCursor find = jobCollection.find(keyObject);
    if (find.hasNext()) {
      DBObject jobObj = find.next();
      jobCollection.remove(keyObject);
      triggerCollection.remove(new BasicDBObject(TRIGGER_JOB_ID, jobObj.get("_id")));

      return true;
    }

    return false;
  }

  public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
    for (JobKey key : jobKeys) {
      removeJob(key);
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
    DBObject dbObject = findJobDocumentByKey(jobKey);
    if (dbObject == null) {
      //Return null if job does not exist, per interface
      return null;
    }

    try {
      Class<Job> jobClass = (Class<Job>) getJobClassLoader().loadClass((String) dbObject.get(JOB_CLASS));

      JobBuilder builder = JobBuilder.newJob(jobClass)
          .withIdentity((String) dbObject.get(JOB_KEY_NAME), (String) dbObject.get(JOB_KEY_GROUP))
          .withDescription((String) dbObject.get(JOB_DESCRIPTION));

      JobDataMap jobData = new JobDataMap();
      for (String key : dbObject.keySet()) {
        if (!key.equals(JOB_KEY_NAME)
            && !key.equals(JOB_KEY_GROUP)
            && !key.equals(JOB_CLASS)
            && !key.equals(JOB_DESCRIPTION)
            && !key.equals("_id")) {
          jobData.put(key, dbObject.get(key));
        }
      }

      return builder.usingJobData(jobData).build();
    } catch (ClassNotFoundException e) {
      throw new JobPersistenceException("Could not load job class " + dbObject.get(JOB_CLASS), e);
    }
  }

  public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws
      JobPersistenceException {
    if (newTrigger.getJobKey() == null) {
      throw new JobPersistenceException("Trigger must be associated with a job. Please specify a JobKey.");
    }

    DBObject dbObject = jobCollection.findOne(Keys.keyToDBObject(newTrigger.getJobKey()));
    if (dbObject != null) {
      storeTrigger(newTrigger, (ObjectId) dbObject.get("_id"), replaceExisting);
    } else {
      throw new JobPersistenceException("Could not find job with key " + newTrigger.getJobKey());
    }
  }

  // If the removal of the Trigger results in an 'orphaned' Job that is not 'durable',
  // then the job should be removed also.
  public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    BasicDBObject dbObject = Keys.keyToDBObject(triggerKey);
    DBCursor triggers = triggerCollection.find(dbObject);
    if (triggers.count() > 0) {
      DBObject trigger = triggers.next();
      if (trigger.containsField(TRIGGER_JOB_ID)) {
        // There is only 1 job per trigger so no need to look further.
        DBObject job = jobCollection.findOne(new BasicDBObject("_id", trigger.get(TRIGGER_JOB_ID)));
        // Remove the orphaned job if it's durable and has no other triggers associated with it,
        // remove it
        if (!job.containsField(JOB_DURABILITY) || job.get(JOB_DURABILITY).toString().equals("false")) {
          DBCursor referencedTriggers = triggerCollection.find(new BasicDBObject(TRIGGER_JOB_ID, job.get("_id")));
          if (referencedTriggers != null && referencedTriggers.count() <= 1) {
            jobCollection.remove(job);
          }
        }
      } else {
        log.debug("The trigger had no associated jobs");
      }
      triggerCollection.remove(dbObject);

      return true;
    }

    return false;
  }

  public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
    for (TriggerKey key : triggerKeys) {
      removeTrigger(key);
    }
    return false;
  }

  public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
    OperableTrigger trigger = retrieveTrigger(triggerKey);
    if(trigger == null) {
      return false;
    }

    if (!trigger.getJobKey().equals(newTrigger.getJobKey())) {
      throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
    }

    // Can't call remove trigger as if the job is not durable, it will remove the job too
    BasicDBObject dbObject = Keys.keyToDBObject(triggerKey);
    DBCursor triggers = triggerCollection.find(dbObject);
    if (triggers.count() > 0) {
      triggerCollection.remove(dbObject);
    }
    
    try {
      storeTrigger(newTrigger, false);
    } catch(JobPersistenceException jpe) {
      storeTrigger(trigger, false); // put previous trigger back...
      throw jpe;
    }
    return true;
  }

  public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    DBObject dbObject = triggerCollection.findOne(Keys.keyToDBObject(triggerKey));
    if (dbObject == null) {
      return null;
    }
    return toTrigger(triggerKey, dbObject);
  }

  public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
    return jobCollection.count(Keys.keyToDBObject(jobKey)) > 0;
  }

  public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
    return triggerCollection.count(Keys.keyToDBObject(triggerKey)) > 0;
  }

  public void clearAllSchedulingData() throws JobPersistenceException {
    jobCollection.remove(new BasicDBObject());
    triggerCollection.remove(new BasicDBObject());
    calendarCollection.remove(new BasicDBObject());
    pausedJobGroupsCollection.remove(new BasicDBObject());
    pausedTriggerGroupsCollection.remove(new BasicDBObject());
  }

  public void storeCalendar(String name,
                            Calendar calendar,
                            boolean replaceExisting,
                            boolean updateTriggers)
      throws JobPersistenceException {
    // TODO
    if (updateTriggers) {
      throw new UnsupportedOperationException("Updating triggers is not supported.");
    }

    BasicDBObject dbObject = new BasicDBObject();
    dbObject.put(CALENDAR_NAME, name);
    dbObject.put(CALENDAR_SERIALIZED_OBJECT, serialize(calendar));

    calendarCollection.insert(dbObject);
  }

  public boolean removeCalendar(String calName) throws JobPersistenceException {
    BasicDBObject searchObj = new BasicDBObject(CALENDAR_NAME, calName);
    if (calendarCollection.count(searchObj) > 0) {
      calendarCollection.remove(searchObj);
      return true;
    }
    return false;
  }

  public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
    // TODO
    throw new UnsupportedOperationException();
  }

  public int getNumberOfJobs() throws JobPersistenceException {
    return (int) jobCollection.count();
  }

  public int getNumberOfTriggers() throws JobPersistenceException {
    return (int) triggerCollection.count();
  }

  public int getNumberOfCalendars() throws JobPersistenceException {
    return (int) calendarCollection.count();
  }

  public int getNumberOfLocks() {
    return (int) locksCollection.count();
  }

  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
    DBCursor cursor = jobCollection.find(queryHelper.matchingKeysConditionFor(matcher), KEY_AND_GROUP_FIELDS);

    Set<JobKey> result = new HashSet<JobKey>();
    while (cursor.hasNext()) {
      DBObject dbo = cursor.next();
      JobKey key = Keys.dbObjectToJobKey(dbo);
      result.add(key);
    }

    return result;
  }

  public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
    DBCursor cursor = triggerCollection.find(queryHelper.matchingKeysConditionFor(matcher), KEY_AND_GROUP_FIELDS);

    Set<TriggerKey> result = new HashSet<TriggerKey>();
    while (cursor.hasNext()) {
      DBObject dbo = cursor.next();
      TriggerKey key = Keys.dbObjectToTriggerKey(dbo);
      result.add(key);
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  public List<String> getJobGroupNames() throws JobPersistenceException {
    return new ArrayList<String>(jobCollection.distinct(KEY_GROUP));
  }

  @SuppressWarnings("unchecked")
  public List<String> getTriggerGroupNames() throws JobPersistenceException {
    return new ArrayList<String>(triggerCollection.distinct(KEY_GROUP));
  }

  public List<String> getCalendarNames() throws JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
    DBObject dbObject = findJobDocumentByKey(jobKey);

    List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();
    DBCursor cursor = triggerCollection.find(new BasicDBObject(TRIGGER_JOB_ID, dbObject.get("_id")));
    while (cursor.hasNext()) {
      triggers.add(toTrigger(cursor.next()));
    }

    return triggers;
  }

  public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
    DBObject doc = findTriggerDocumentByKey(triggerKey);

    return triggerStateForValue((String) doc.get(TRIGGER_STATE));
  }

  public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    triggerCollection.update(Keys.keyToDBObject(triggerKey), updateThatSetsTriggerStateTo(STATE_PAUSED));
  }

  public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(triggerCollection, queryHelper);
    triggerCollection.update(queryHelper.matchingKeysConditionFor(matcher), updateThatSetsTriggerStateTo(STATE_PAUSED), false, true);

    final Set<String> set = groupHelper.groupsThatMatch(matcher);
    markTriggerGroupsAsPaused(set);

    return set;
  }

  public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    // TODO: port blocking behavior and misfired triggers handling from StdJDBCDelegate in Quartz
    triggerCollection.update(Keys.keyToDBObject(triggerKey), updateThatSetsTriggerStateTo(STATE_WAITING));
  }

  public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(triggerCollection, queryHelper);
    triggerCollection.update(queryHelper.matchingKeysConditionFor(matcher), updateThatSetsTriggerStateTo(STATE_WAITING), false, true);

    final Set<String> set = groupHelper.groupsThatMatch(matcher);
    this.unmarkTriggerGroupsAsPaused(set);
    return set;
  }

  @SuppressWarnings("unchecked")
  public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
    return new HashSet<String>(pausedTriggerGroupsCollection.distinct(KEY_GROUP));
  }

  @SuppressWarnings("unchecked")
  public Set<String> getPausedJobGroups() throws JobPersistenceException {
    return new HashSet<String>(pausedJobGroupsCollection.distinct(KEY_GROUP));
  }

  public void pauseAll() throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(triggerCollection, queryHelper);
    triggerCollection.update(new BasicDBObject(), updateThatSetsTriggerStateTo(STATE_PAUSED));
    this.markTriggerGroupsAsPaused(groupHelper.allGroups());
  }

  public void resumeAll() throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(triggerCollection, queryHelper);
    triggerCollection.update(new BasicDBObject(), updateThatSetsTriggerStateTo(STATE_WAITING));
    this.unmarkTriggerGroupsAsPaused(groupHelper.allGroups());
  }


  public void pauseJob(JobKey jobKey) throws JobPersistenceException {
    final ObjectId jobId = (ObjectId) findJobDocumentByKey(jobKey).get("_id");
    final TriggerGroupHelper groupHelper = new TriggerGroupHelper(triggerCollection, queryHelper);
    List<String> groups = groupHelper.groupsForJobId(jobId);
    triggerCollection.update(new BasicDBObject(TRIGGER_JOB_ID, jobId), updateThatSetsTriggerStateTo(STATE_PAUSED));
    this.markTriggerGroupsAsPaused(groups);
  }

  public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
    final TriggerGroupHelper groupHelper = new TriggerGroupHelper(triggerCollection, queryHelper);
    List<String> groups = groupHelper.groupsForJobIds(idsFrom(findJobDocumentsThatMatch(groupMatcher)));
    triggerCollection.update(queryHelper.inGroups(groups), updateThatSetsTriggerStateTo(STATE_PAUSED));
    this.markJobGroupsAsPaused(groups);

    return groups;
  }

  public void resumeJob(JobKey jobKey) throws JobPersistenceException {
    final ObjectId jobId = (ObjectId) findJobDocumentByKey(jobKey).get("_id");
    // TODO: port blocking behavior and misfired triggers handling from StdJDBCDelegate in Quartz
    triggerCollection.update(new BasicDBObject(TRIGGER_JOB_ID, jobId), updateThatSetsTriggerStateTo(STATE_WAITING));
  }

  public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
    final TriggerGroupHelper groupHelper = new TriggerGroupHelper(triggerCollection, queryHelper);
    List<String> groups = groupHelper.groupsForJobIds(idsFrom(findJobDocumentsThatMatch(groupMatcher)));
    triggerCollection.update(queryHelper.inGroups(groups), updateThatSetsTriggerStateTo(STATE_WAITING));
    this.unmarkJobGroupsAsPaused(groups);

    return groups;
  }


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
      }});
    
    return triggerList;
  }
  
  private void doAcquireNextTriggers(Map<TriggerKey, OperableTrigger> triggers, Date noLaterThanDate, int maxCount)
      throws JobPersistenceException {
    BasicDBObject query = new BasicDBObject();
    query.put(TRIGGER_NEXT_FIRE_TIME, new BasicDBObject("$lte", noLaterThanDate));

    DBCursor cursor = triggerCollection.find(query);

    BasicDBObject sort = new BasicDBObject();
    sort.put(TRIGGER_NEXT_FIRE_TIME, Integer.valueOf(1));
    cursor.sort(sort);

    log.debug("Found {} triggers which are eligible to be run.", cursor.count());

    while (cursor.hasNext() && maxCount > triggers.size()) {
      DBObject dbObj = cursor.next();

      OperableTrigger trigger = toTrigger(dbObj);

      try {

        if (trigger == null) {
          continue;
        }

        if (triggers.containsKey(trigger.getKey()))
        {
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
          if (nextFireTime.after(noLaterThanDate))
          {
            log.debug("Skipping trigger {} as it misfired and was scheduled for {}.", trigger.getKey(), trigger.getNextFireTime());
            continue;
          }
        }
        
        log.debug("Inserting lock for trigger {}", trigger.getKey());
        
        BasicDBObject lock = new BasicDBObject();
        lock.put(LOCK_KEY_NAME, dbObj.get(KEY_NAME));
        lock.put(LOCK_KEY_GROUP, dbObj.get(KEY_GROUP));
        lock.put(LOCK_INSTANCE_ID, instanceId);
        lock.put(LOCK_TIME, new Date());
        locksCollection.insert(lock);
        
        log.debug("Aquired trigger {}", trigger.getKey());
        triggers.put(trigger.getKey(), trigger);
        
      } catch (DuplicateKey e) {

        // someone else acquired this lock. Move on.
        log.debug("Failed to acquire trigger {} due to a lock", trigger.getKey());

        BasicDBObject lock = new BasicDBObject();
        lock.put(LOCK_KEY_NAME, dbObj.get(KEY_NAME));
        lock.put(LOCK_KEY_GROUP, dbObj.get(KEY_GROUP));

        DBObject existingLock;
        DBCursor lockCursor = locksCollection.find(lock);
        if (lockCursor.hasNext()) {
          existingLock = lockCursor.next();
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

  public void releaseAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
    try {
      removeTriggerLock(trigger);
    } catch (Exception e) {
      throw new JobPersistenceException(e.getLocalizedMessage(), e);
    }
  }

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

      TriggerFiredBundle bndle = new TriggerFiredBundle(retrieveJob(
          trigger), trigger, cal,
          false, new Date(), trigger.getPreviousFireTime(), prevFireTime,
          trigger.getNextFireTime());

      JobDetail job = bndle.getJobDetail();
      
      if (job != null) {
        if (job.isConcurrentExectionDisallowed()) {
          throw new UnsupportedOperationException("ConcurrentExecutionDisallowed is not supported currently.");
        }
        results.add(new TriggerFiredResult(bndle));

        trigger.triggered(cal);
        storeTrigger(trigger, true);
      }

    }
    return results;
  }

  public void triggeredJobComplete(OperableTrigger trigger,
                                   JobDetail jobDetail,
                                   CompletedExecutionInstruction triggerInstCode)
      throws JobPersistenceException {
    log.debug("Trigger completed {}", trigger.getKey());
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

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public void setInstanceName(String schedName) {
    // No-op
  }

  public void setThreadPoolSize(int poolSize) {
    // No-op
  }

  public void setAddresses(String addresses) {
    this.addresses = addresses.split(",");
  }

  public DBCollection getJobCollection() {
    return jobCollection;
  }

  public DBCollection getTriggerCollection() {
    return triggerCollection;
  }

  public DBCollection getCalendarCollection() {
    return calendarCollection;
  }

  public DBCollection getLocksCollection() {
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


  //
  // Implementation
  //

  private void initializeCollections(DB db) {
    jobCollection = db.getCollection(collectionPrefix + "jobs");
    triggerCollection = db.getCollection(collectionPrefix + "triggers");
    calendarCollection = db.getCollection(collectionPrefix + "calendars");
    locksCollection = db.getCollection(collectionPrefix + "locks");

    pausedTriggerGroupsCollection = db.getCollection(collectionPrefix + "paused_trigger_groups");
    pausedJobGroupsCollection = db.getCollection(collectionPrefix + "paused_job_groups");
  }

  private DB selectDatabase(Mongo mongo) {
    DB db = mongo.getDB(dbName);
    // MongoDB defaults are insane, set a reasonable write concern explicitly. MK.
    db.setWriteConcern(WriteConcern.JOURNAL_SAFE);
    if (username != null) {
      db.authenticate(username, password.toCharArray());
    }
    return db;
  }

  private Mongo connectToMongoDB() throws SchedulerConfigException {
	if(mongoUri != null){
		return connectToMongoDB(mongoUri);
	}
    MongoOptions options = new MongoOptions();
    options.safe = true;

    try {
      ArrayList<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
      for (String a : addresses) {
        serverAddresses.add(new ServerAddress(a));
      }
      return new Mongo(serverAddresses, options);

    } catch (UnknownHostException e) {
      throw new SchedulerConfigException("Could not connect to MongoDB", e);
    } catch (MongoException e) {
      throw new SchedulerConfigException("Could not connect to MongoDB", e);
    }
  }

  private Mongo connectToMongoDB(final String mongoUriAsString) throws SchedulerConfigException {
	  try {
		  return new MongoClient(new MongoClientURI(mongoUriAsString));
	 } catch (final UnknownHostException e) {
		 throw new SchedulerConfigException("Could not connect to MongoDB", e);
	 } catch (final MongoException e) {
          throw new SchedulerConfigException("MongoDB driver thrown an exception", e);
      }
  }

  protected OperableTrigger toTrigger(DBObject dbObj) throws JobPersistenceException {
    TriggerKey key = new TriggerKey((String) dbObj.get(KEY_NAME), (String) dbObj.get(KEY_GROUP));
    return toTrigger(key, dbObj);
  }

  @SuppressWarnings("unchecked")
  protected OperableTrigger toTrigger(TriggerKey triggerKey, DBObject dbObject) throws JobPersistenceException {
    OperableTrigger trigger;
    try {
      Class<OperableTrigger> triggerClass = (Class<OperableTrigger>) getTriggerClassLoader().loadClass((String) dbObject.get(TRIGGER_CLASS));
      trigger = triggerClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new JobPersistenceException("Could not find trigger class " + dbObject.get(TRIGGER_CLASS));
    } catch (Exception e) {
      throw new JobPersistenceException("Could not instantiate trigger class " + dbObject.get(TRIGGER_CLASS));
    }

    TriggerPersistenceHelper tpd = triggerPersistenceDelegateFor(trigger);

    trigger.setKey(triggerKey);
    trigger.setCalendarName((String) dbObject.get(TRIGGER_CALENDAR_NAME));
    trigger.setDescription((String) dbObject.get(TRIGGER_DESCRIPTION));
    trigger.setFireInstanceId((String) dbObject.get(TRIGGER_FIRE_INSTANCE_ID));
    trigger.setMisfireInstruction((Integer) dbObject.get(TRIGGER_MISFIRE_INSTRUCTION));
    trigger.setNextFireTime((Date) dbObject.get(TRIGGER_NEXT_FIRE_TIME));
    trigger.setPreviousFireTime((Date) dbObject.get(TRIGGER_PREVIOUS_FIRE_TIME));
    trigger.setPriority((Integer) dbObject.get(TRIGGER_PRIORITY));
    
    try {
        trigger.setStartTime((Date) dbObject.get(TRIGGER_START_TIME));
        trigger.setEndTime((Date) dbObject.get(TRIGGER_END_TIME));
	} catch(IllegalArgumentException e) {
        //Ignore illegal arg exceptions thrown by triggers doing JIT validation of start and endtime
        log.warn("Trigger had illegal start / end time combination: {}", trigger.getKey(), e)
    }


    trigger = tpd.setExtraPropertiesAfterInstantiation(trigger, dbObject);

    DBObject job = jobCollection.findOne(new BasicDBObject("_id", dbObject.get(TRIGGER_JOB_ID)));
    if (job != null) {
      trigger.setJobKey(new JobKey((String) job.get(JOB_KEY_NAME), (String) job.get(JOB_KEY_GROUP)));
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

  protected boolean isTriggerLockExpired(DBObject lock) {
    Date lockTime = (Date) lock.get(LOCK_TIME);
    long elaspedTime = System.currentTimeMillis() - lockTime.getTime();
    return (elaspedTime > triggerTimeoutMillis);
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


  private Object serialize(Calendar calendar) throws JobPersistenceException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    try {
      ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
      objectStream.writeObject(calendar);
      objectStream.close();
      return byteStream.toByteArray();
    } catch (IOException e) {
      throw new JobPersistenceException("Could not serialize Calendar.", e);
    }
  }

  /**
   * Initializes the indexes for the scheduler collections.
   *
   * @throws SchedulerConfigException if an error occurred communicating with the MongoDB server.
   */
  private void ensureIndexes() throws SchedulerConfigException {
    try {
      BasicDBObject keys = new BasicDBObject();
      keys.put(JOB_KEY_NAME, 1);
      keys.put(JOB_KEY_GROUP, 1);
	  jobCollection.ensureIndex(keys, null, true);

	  keys = new BasicDBObject();
	  keys.put(KEY_NAME, 1);
	  keys.put(KEY_GROUP, 1);
	  triggerCollection.ensureIndex(keys, null, true);

	  keys = new BasicDBObject();
	  keys.put(LOCK_KEY_NAME, 1);
	  keys.put(LOCK_KEY_GROUP, 1);
	  locksCollection.ensureIndex(keys, null, true);
	  // remove all locks for this instance on startup
	  locksCollection.remove(new BasicDBObject(LOCK_INSTANCE_ID, instanceId));

	  keys = new BasicDBObject();
	  keys.put(CALENDAR_NAME, 1);
	  calendarCollection.ensureIndex(keys, null, true);
	} catch(final MongoException e){
	  throw new SchedulerConfigException("Error while initializing the indexes", e);
	}
  }

  protected void storeTrigger(OperableTrigger newTrigger, ObjectId jobId, boolean replaceExisting) throws ObjectAlreadyExistsException {
    BasicDBObject trigger = new BasicDBObject();
    trigger.put(TRIGGER_STATE, STATE_WAITING);
    trigger.put(TRIGGER_CALENDAR_NAME, newTrigger.getCalendarName());
    trigger.put(TRIGGER_CLASS, newTrigger.getClass().getName());
    trigger.put(TRIGGER_DESCRIPTION, newTrigger.getDescription());
    trigger.put(TRIGGER_END_TIME, newTrigger.getEndTime());
    trigger.put(TRIGGER_FINAL_FIRE_TIME, newTrigger.getFinalFireTime());
    trigger.put(TRIGGER_FIRE_INSTANCE_ID, newTrigger.getFireInstanceId());
    trigger.put(TRIGGER_JOB_ID, jobId);
    trigger.put(KEY_NAME, newTrigger.getKey().getName());
    trigger.put(KEY_GROUP, newTrigger.getKey().getGroup());
    trigger.put(TRIGGER_MISFIRE_INSTRUCTION, newTrigger.getMisfireInstruction());
    trigger.put(TRIGGER_NEXT_FIRE_TIME, newTrigger.getNextFireTime());
    trigger.put(TRIGGER_PREVIOUS_FIRE_TIME, newTrigger.getPreviousFireTime());
    trigger.put(TRIGGER_PRIORITY, newTrigger.getPriority());
    trigger.put(TRIGGER_START_TIME, newTrigger.getStartTime());

    TriggerPersistenceHelper tpd = triggerPersistenceDelegateFor(newTrigger);
    trigger = (BasicDBObject) tpd.injectExtraPropertiesForInsert(newTrigger, trigger);

    try {
      triggerCollection.insert(trigger);
    } catch (DuplicateKey key) {
      if (replaceExisting) {
        trigger.remove("_id");
        triggerCollection.update(keyToDBObject(newTrigger.getKey()), trigger);
      } else {
        throw new ObjectAlreadyExistsException(newTrigger);
      }
    }
  }

  protected ObjectId storeJobInMongo(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException {
    JobKey key = newJob.getKey();

    BasicDBObject keyDbo = keyToDBObject(key);
    BasicDBObject job = keyToDBObject(key);

    job.put(JOB_KEY_NAME, key.getName());
    job.put(JOB_KEY_GROUP, key.getGroup());
    job.put(JOB_DESCRIPTION, newJob.getDescription());
    job.put(JOB_CLASS, newJob.getJobClass().getName());
    job.put(JOB_DURABILITY, newJob.isDurable());

    job.putAll(newJob.getJobDataMap());

    boolean existing = jobCollection.findOne(keyDbo) != null;
    try {
      if (existing && replaceExisting) {
        jobCollection.update(keyDbo, job);
      } else {
        jobCollection.insert(job);
      }

      return (ObjectId) job.get("_id");
    } catch (DuplicateKey e) {
      throw new ObjectAlreadyExistsException(e.getMessage());
    }
  }

  protected void removeTriggerLock(OperableTrigger trigger) {
    log.debug("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
    BasicDBObject lock = new BasicDBObject();
    lock.put(LOCK_KEY_NAME, trigger.getKey().getName());
    lock.put(LOCK_KEY_GROUP, trigger.getKey().getGroup());

    // Coment this out, as expired trigger locks should be deleted by any another instance
    // lock.put(LOCK_INSTANCE_ID, instanceId);

    locksCollection.remove(lock);
    log.debug("Trigger lock {}.{} removed.", trigger.getKey(), instanceId);
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

  protected DBObject findJobDocumentByKey(JobKey key) {
    return jobCollection.findOne(keyToDBObject(key));
  }

  protected DBObject findTriggerDocumentByKey(TriggerKey key) {
    return triggerCollection.findOne(keyToDBObject(key));
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

  private DBObject updateThatSetsTriggerStateTo(String state) {
    return BasicDBObjectBuilder.
        start("$set", new BasicDBObject(TRIGGER_STATE, state)).
        get();
  }

  private void markTriggerGroupsAsPaused(Collection<String> groups) {
    List<DBObject> list = new ArrayList<DBObject>();
    for (String s : groups) {
      list.add(new BasicDBObject(KEY_GROUP, s));
    }
    pausedTriggerGroupsCollection.insert(list);
  }

  private void unmarkTriggerGroupsAsPaused(Collection<String> groups) {
    pausedTriggerGroupsCollection.remove(QueryBuilder.start(KEY_GROUP).in(groups).get());
  }

  private void markJobGroupsAsPaused(List<String> groups) {
    if (groups == null) {
      throw new IllegalArgumentException("groups cannot be null!");
    }
    List<DBObject> list = new ArrayList<DBObject>();
    for (String s : groups) {
      list.add(new BasicDBObject(KEY_GROUP, s));
    }
    pausedJobGroupsCollection.insert(list);
  }

  private void unmarkJobGroupsAsPaused(Collection<String> groups) {
    pausedJobGroupsCollection.remove(QueryBuilder.start(KEY_GROUP).in(groups).get());
  }


  private Collection<ObjectId> idsFrom(Collection<DBObject> docs) {
    // so much repetitive code would be gone if Java collections just had .map and .filter…
    List<ObjectId> list = new ArrayList<ObjectId>();
    for (DBObject doc : docs) {
      list.add((ObjectId) doc.get("_id"));
    }
    return list;
  }

  private Collection<DBObject> findJobDocumentsThatMatch(GroupMatcher<JobKey> matcher) {
    final GroupHelper groupHelper = new GroupHelper(jobCollection, queryHelper);
    return groupHelper.inGroupsThatMatch(matcher);
  }
}
