package com.mulesoft.quartz.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.*;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.StdScheduler;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;

import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;
import static org.quartz.SimpleScheduleBuilder.repeatSecondlyForTotalCount;
import static org.quartz.SimpleScheduleBuilder.repeatSecondlyForever;
import static org.quartz.TriggerBuilder.newTrigger;

/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

public class SimpleScheduleTriggerIntegrationTest {

  public static int COUNTER = 0;

  protected Scheduler scheduler;

  protected Mongo mongo;

  protected DBCollection triggerCollection;

  protected DBCollection jobCollection;

  protected DBCollection locksCollection;

  protected JobStore jobStore;

  @Before
  public void setUp() throws Exception {
    COUNTER = 0;
    resetMongo();
    scheduler = createScheduler(true);
  }

  @After
  public void tearDown() throws SchedulerException {
    if (scheduler != null) {
      scheduler.shutdown();
    }
  }

  public void createJobStoreSpy(Scheduler schedulerToSpy) throws Exception {
    Field schedField = StdScheduler.class.getDeclaredField("sched");
    schedField.setAccessible(true);
    QuartzScheduler quartzScheduler = (QuartzScheduler) schedField.get(schedulerToSpy);

    Field resourcesField = QuartzScheduler.class.getDeclaredField("resources");
    resourcesField.setAccessible(true);
    QuartzSchedulerResources resources = (QuartzSchedulerResources) resourcesField.get(quartzScheduler);

    jobStore = resources.getJobStore();
    jobStore = spy(jobStore);
    resources.setJobStore(jobStore);
  }

  /**
   * Creates a second scheduler for concurrency scenarios.
   *
   * @return
   * @throws Exception
   */
  protected Scheduler createSecondScheduler() throws Exception {
    return createScheduler(false);
  }

  /**
   * Recreates the scheduler for testing shutdown/startup scenarios.
   *
   * @throws Exception
   */
  protected void recreateScheduler() throws Exception {
    scheduler = createScheduler(true);
  }

  protected Scheduler createScheduler(boolean createSpy) throws Exception {
    StdSchedulerFactory factory = new StdSchedulerFactory();
    Properties props = new Properties();
    props.put(StdSchedulerFactory.PROP_JOB_STORE_CLASS, MongoDBJobStore.class.getName());
    props.put(StdSchedulerFactory.PROP_JOB_STORE_PREFIX + ".addresses", "127.0.0.1");
    props.put(StdSchedulerFactory.PROP_JOB_STORE_PREFIX + ".dbName", "quartz");
    props.put(StdSchedulerFactory.PROP_JOB_STORE_PREFIX + ".collectionPrefix", "test");
    props.put(StdSchedulerFactory.PROP_THREAD_POOL_PREFIX + ".threadCount", "1");

    factory.initialize(props);
    Scheduler scheduler = factory.getScheduler();

    if (createSpy) {
      createJobStoreSpy(scheduler);
    }

    scheduler.start();
    return scheduler;
  }

  protected void resetMongo() throws UnknownHostException {
    mongo = new Mongo("127.0.0.1");
    DB db = mongo.getDB("quartz");
    jobCollection = db.getCollection("test_jobs");
    jobCollection.drop();
    triggerCollection = db.getCollection("test_triggers");
    triggerCollection.drop();
    locksCollection = db.getCollection("test_locks");
    locksCollection.drop();
  }

  @Test
  public void testJobStorage() throws Exception {
    JobDetail job = JobBuilder.newJob(IncrementJob.class)
        .storeDurably()
        .withIdentity("name", "group")
        .build();

    OperableTrigger trigger = (OperableTrigger) newTrigger()
        .withIdentity("name", "group")
        .forJob(job)
        .startNow()
        .withSchedule(repeatSecondlyForTotalCount(10))
        .build();

    scheduler.scheduleJob(job, trigger);

    assertEquals(1, jobCollection.find().count());
    assertEquals(1, triggerCollection.find().count());

    // wait for acquire to be called and the job to finish
    verify(jobStore, timeout(2000).times(1)).triggeredJobComplete((OperableTrigger) anyObject(), (JobDetail) anyObject(), (CompletedExecutionInstruction) anyObject());

    assertTrue(COUNTER > 0);

    scheduler.deleteJob(job.getKey());

    assertEquals(0, jobCollection.find().count());
    assertEquals(0, triggerCollection.find().count());
  }

  /**
   * Ensure that we apply misfires correctly when the scheduler starts after being shut down.
   *
   * @throws Exception
   */
  @Test
  public void testMisfiresAfterShutdown() throws Exception {
    JobDetail job = JobBuilder.newJob(IncrementJob.class)
        .storeDurably()
        .withIdentity("name", "group")
        .build();

    OperableTrigger trigger = (OperableTrigger) newTrigger()
        .withIdentity("name", "group")
        .forJob(job)
        .startNow()
        .withSchedule(repeatSecondlyForever().withMisfireHandlingInstructionNextWithRemainingCount())
        .build();

    scheduler.scheduleJob(job, trigger);

    assertEquals(1, jobCollection.find().count());
    assertEquals(1, triggerCollection.find().count());

    // shut down the scheduler to simulate some misfires
    scheduler.shutdown();
    int before = COUNTER;

    Thread.sleep(5000);

    // resume scheduler with misfires
    recreateScheduler();

    Thread.sleep(5000);

    // if misfires are applied correctly, we won't have more than 5 + before
    assertTrue(COUNTER > 0);
    assertTrue(COUNTER <= before + 5);
    scheduler.deleteJob(job.getKey());

    assertEquals(0, jobCollection.find().count());
    assertEquals(0, triggerCollection.find().count());
  }

  @Test
  public void testFireWhileSchedulerIsDown() throws Exception {
    JobDetail job = JobBuilder.newJob(IncrementJob.class)
        .storeDurably()
        .withIdentity("name", "group")
        .build();

    OperableTrigger trigger = (OperableTrigger) newTrigger()
        .withIdentity("name", "group")
        .forJob(job)
        .startNow()
        .withSchedule(repeatSecondlyForTotalCount(1).withMisfireHandlingInstructionFireNow())
        .build();

    scheduler.standby();
    scheduler.scheduleJob(job, trigger);
    scheduler.shutdown();

    recreateScheduler();

    // wait for acquire to be called and the job to finish
    verify(jobStore, timeout(2000).times(1)).triggeredJobComplete((OperableTrigger) anyObject(), (JobDetail) anyObject(), (CompletedExecutionInstruction) anyObject());

    assertTrue(COUNTER > 0);
  }

  /**
   * Ensure that this job only fires once if there are multiple scheduler nodes.
   *
   * @throws Exception
   */
  @Test
  public void testTwoSchedulers() throws Exception {
    JobDetail job = JobBuilder.newJob(IncrementJob.class)
        .storeDurably()
        .withIdentity("name", "group")
        .build();

    OperableTrigger trigger = (OperableTrigger) newTrigger()
        .withIdentity("name", "group")
        .forJob(job)
        .startNow()
        .withSchedule(repeatSecondlyForever())
        .build();

    long start = System.currentTimeMillis();
    scheduler.scheduleJob(job, trigger);

    Scheduler scheduler2 = createSecondScheduler();

    Thread.sleep(10000);

    scheduler.shutdown();
    scheduler2.shutdown();

    assertTrue(COUNTER > 1);
    long elapsed = System.currentTimeMillis() - start;
    System.out.println("GOT " + COUNTER);
    assertTrue("Got too many counts. " + COUNTER, COUNTER <= ((elapsed / 1000) + 1));
  }

  /**
   * Verifies that all your jobs actually run if you schedule them. We ran into issues
   * when acquireNextTriggers returns stuff out of order, so this verifies that things get run
   * as they should.
   */
  @Test
  public void testMultipleJobs() throws Exception {
    createTrigger("job1");
    createTrigger("job2");
    createTrigger("job3");

    verify(jobStore, times(3)).storeJobAndTrigger((JobDetail) anyObject(), (OperableTrigger) anyObject());

    assertEquals(3, jobCollection.find().count());
    assertEquals(3, triggerCollection.find().count());

    // wait for acquire to be called and the job to be stored for each job
    verify(jobStore, timeout(2000).times(3)).triggeredJobComplete((OperableTrigger) anyObject(), (JobDetail) anyObject(), (CompletedExecutionInstruction) anyObject());
  }

  /**
   * Verifies that old trigger locks expire after a period of time.
   */
  @Test
  public void testTriggerExpiration() throws Exception {

    // add a lock for the trigger in the DB that is older than the expiration period of 10 mins
    BasicDBObject lock = new BasicDBObject();
    lock.put("keyName", "triggerExpirationTest");
    lock.put("keyGroup", "triggerExpirationTest");
    lock.put("instanceId", "NON_CLUSTERED");
    lock.put("time", new Date(System.currentTimeMillis() - (19 * 60 * 1000L)));
    locksCollection.save(lock);

    // create the trigger with the same name
    createTrigger("triggerExpirationTest");

    // wait for the lock to expire and the trigger to be aquired
    verify(jobStore, timeout(2000).times(1)).triggeredJobComplete((OperableTrigger) anyObject(), (JobDetail) anyObject(), (CompletedExecutionInstruction) anyObject());
  }

  private JobDetail createTrigger(String name) throws SchedulerException {
    JobDetail job = JobBuilder.newJob(IncrementJob.class)
        .storeDurably()
        .withIdentity(name, name)
        .build();

    OperableTrigger trigger = (OperableTrigger) newTrigger()
        .withIdentity(name, name)
        .forJob(job)
        .startNow()
        .withSchedule(repeatSecondlyForever())
        .build();

    scheduler.scheduleJob(job, trigger);
    return job;
  }

  public static class IncrementJob implements Job {

    public void execute(JobExecutionContext context) throws JobExecutionException {
      COUNTER++;
    }

  }
}
