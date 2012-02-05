package com.mulesoft.quartz.mongo;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.spi.OperableTrigger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.quartz.CalendarIntervalScheduleBuilder.calendarIntervalSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) Michael S. Klishin
 *
 * The software in this package is published under the terms of the Apache License, Version 2.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

public class JobsAndTriggersWithCalendarIntervalScheduleTest extends MongoDBJobStoreTest {
  @Test
  public void testStoringJobsAndTriggers() throws Exception {
    assertEquals(0, jobsCollection.count());
    assertEquals(0, store.getNumberOfJobs());

    JobDetail job = JobBuilder.newJob()
        .storeDurably()
        .usingJobData("rowId", 10)
        .withIdentity("shaveAllYaks", "yakCare")
        .build();

    store.storeJob(job, false);
    assertEquals(1, jobsCollection.count());
    assertEquals(1, store.getNumberOfJobs());

    OperableTrigger trigger = (OperableTrigger) newTrigger()
        .withIdentity("yakShavingSchedule1", "yakCare")
        .forJob(job)
        .startNow()
        .withSchedule(calendarIntervalSchedule().withIntervalInHours(3))
        .build();

    assertEquals(0, triggersCollection.count());
    assertEquals(0, store.getNumberOfTriggers());

    store.storeTrigger(trigger, false);
    assertEquals(1, triggersCollection.count());
    assertEquals(1, store.getNumberOfTriggers());

    DBObject loaded = triggersCollection.findOne(BasicDBObjectBuilder.start()
        .append("keyName", "yakShavingSchedule1")
        .append("keyGroup", "yakCare").get());
    assertNotNull(loaded);
    System.out.println(loaded);
    assertEquals(Integer.valueOf(3), (Integer)loaded.get("repeatInterval"));
  }
}
