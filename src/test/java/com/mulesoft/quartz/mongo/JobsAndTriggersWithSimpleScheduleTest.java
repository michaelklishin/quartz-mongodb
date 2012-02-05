package com.mulesoft.quartz.mongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.simpl.SimpleClassLoadHelper;
import org.quartz.spi.OperableTrigger;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.quartz.SimpleScheduleBuilder.repeatMinutelyForever;
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

public class JobsAndTriggersWithSimpleScheduleTest extends MongoDBJobStoreTest {
  @Test
  public void testJobStorage() throws Exception {
    assertEquals(0, jobsCollection.count());
    assertEquals(0, store.getNumberOfJobs());

    JobDetail job = JobBuilder.newJob()
        .storeDurably()
        .usingJobData("key", "value")
        .withIdentity("name", "group")
        .build();

    store.storeJob(job, false);
    assertEquals(1, jobsCollection.count());
    assertEquals(1, store.getNumberOfJobs());

    try {
      store.storeJob(job, false);
      fail("Expected jobs collection to have a duplicate");
    } catch (ObjectAlreadyExistsException e) {
      // expected
    }

    OperableTrigger trigger = (OperableTrigger) newTrigger()
        .withIdentity("name", "group")
        .forJob(job)
        .startAt(new Date())
        .withSchedule(repeatMinutelyForever())
        .build();

    assertEquals(0, triggersCollection.count());
    assertEquals(0, store.getNumberOfTriggers());

    store.storeTrigger(trigger, false);
    assertEquals(1, triggersCollection.count());
    assertEquals(1, store.getNumberOfTriggers());

    try {
      store.storeTrigger(trigger, false);
      fail("Should not be able to store the same trigger twice");
    } catch (ObjectAlreadyExistsException e) {
      // expected
    }

    OperableTrigger trigger2 = (OperableTrigger) newTrigger()
        .withIdentity("name2", "group")
        .forJob(job)
        .startNow()
        .withSchedule(repeatMinutelyForever())
        .build();

    store.storeTrigger(trigger2, false);
    assertEquals(2, triggersCollection.count());
    assertEquals(2, store.getNumberOfTriggers());

    JobDetail job2 = store.retrieveJob(job.getKey());
    assertEquals("name", job2.getKey().getName());
    assertEquals("group", job2.getKey().getGroup());
    assertEquals(1, job2.getJobDataMap().size());
    assertEquals("value", job2.getJobDataMap().get("key"));

    trigger2 = store.retrieveTrigger(trigger.getKey());
    assertEquals("name", trigger2.getKey().getName());
    assertEquals("group", trigger2.getKey().getGroup());
  }
}
