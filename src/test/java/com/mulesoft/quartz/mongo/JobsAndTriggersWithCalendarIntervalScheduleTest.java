package com.mulesoft.quartz.mongo;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;

import java.util.List;

import static org.junit.Assert.*;
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
        .endAt(DateTime.now().plusYears(2).toDate())
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

    assertEquals(Integer.valueOf(3), (Integer) loaded.get("repeatInterval"));
    assertEquals("HOUR", (String)loaded.get("repeatIntervalUnit"));
  }

  @Test
  public void testLoadingAndInstantiatingTriggers() throws Exception {
    assertEquals(0, store.getNumberOfJobs());

    JobDetail job = JobBuilder.newJob()
        .storeDurably()
        .usingJobData("rowId", 10)
        .withIdentity("shaveAllYaks", "yakCare")
        .build();

    store.storeJob(job, false);
    assertEquals(1, store.getNumberOfJobs());

    OperableTrigger trigger = (OperableTrigger) newTrigger()
        .withIdentity("yakShavingSchedule1", "yakCare")
        .forJob(job)
        .startNow()
        .withSchedule(calendarIntervalSchedule().withIntervalInHours(3))
        .build();

    // this value is not set by the builder, but is used in the query condition,
    // so we need to have it. MK.
    trigger.setNextFireTime(DateTime.now().plusHours(3).toDate());

    assertEquals(0, store.getNumberOfTriggers());
    store.storeTrigger(trigger, false);
    assertEquals(1, store.getNumberOfTriggers());

    long twoMonthsFromNow = (DateTime.now().plusMonths(2).toDate().getTime());
    List<OperableTrigger> list = store.acquireNextTriggers(twoMonthsFromNow, 10, twoMonthsFromNow);
    assertEquals(1, list.size());

    CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl)list.get(0);

    assertEquals(3, t.getRepeatInterval());
    assertEquals(DateBuilder.IntervalUnit.HOUR, t.getRepeatIntervalUnit());
  }
}
