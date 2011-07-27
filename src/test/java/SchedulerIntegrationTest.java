import static org.quartz.SimpleScheduleBuilder.*;
import static org.quartz.TriggerBuilder.newTrigger;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mulesoft.quartz.MongoDBJobStore;

import java.net.UnknownHostException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.OperableTrigger;

/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

public class SchedulerIntegrationTest extends Assert {

    public static int COUNTER = 0;
    
    private Scheduler scheduler;

    private Mongo mongo;

    private DBCollection triggerCollection;

    private DBCollection jobCollection;

    private DBCollection locksCollection;

    @Before
    public void setUp() throws Exception {
        COUNTER = 0; 
        resetMongo();
        scheduler = createNewScheduler();
    }

    protected Scheduler createNewScheduler() throws SchedulerException {
        StdSchedulerFactory factory = new StdSchedulerFactory();
        Properties props = new Properties();
        props.put(StdSchedulerFactory.PROP_JOB_STORE_CLASS, MongoDBJobStore.class.getName());
        props.put(StdSchedulerFactory.PROP_JOB_STORE_PREFIX + ".addresses", "localhost");
        props.put(StdSchedulerFactory.PROP_JOB_STORE_PREFIX + ".dbName", "quartz");
        props.put(StdSchedulerFactory.PROP_JOB_STORE_PREFIX + ".collectionPrefix", "test");
        props.put(StdSchedulerFactory.PROP_THREAD_POOL_PREFIX + ".threadCount", "1");
        
        factory.initialize(props);
        Scheduler scheduler = factory.getScheduler();
        scheduler.start();
        return scheduler;
    }

    protected void resetMongo() throws UnknownHostException {
        mongo = new Mongo("localhost");
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
            .usingJobData("key", "value")
            .withIdentity("name", "group")
            .build();
        
        OperableTrigger trigger = (OperableTrigger)newTrigger()
            .withIdentity("name", "group")
            .forJob(job)
            .startNow()
            .withSchedule(repeatSecondlyForTotalCount(10))
            .build();
        
        scheduler.scheduleJob(job, trigger);
        
        assertEquals(1, jobCollection.find().count());
        assertEquals(1, triggerCollection.find().count());
        
        Thread.sleep(2000);
        
        assertTrue(COUNTER > 0);
        
        scheduler.deleteJob(job.getKey());
        
        assertEquals(0, jobCollection.find().count());
        assertEquals(0, triggerCollection.find().count());
    }

    @Test
    public void testFireWhileSchedulerIsDown() throws Exception {
        JobDetail job = JobBuilder.newJob(IncrementJob.class)
            .storeDurably()
            .usingJobData("key", "value")
            .withIdentity("name", "group")
            .build();
        
        OperableTrigger trigger = (OperableTrigger)newTrigger()
            .withIdentity("name", "group")
            .forJob(job)
            .startNow()
            .withSchedule(repeatSecondlyForTotalCount(1).withMisfireHandlingInstructionFireNow())
            .build();
        
        scheduler.standby();
        scheduler.scheduleJob(job, trigger);
        scheduler.shutdown();
       
        scheduler = createNewScheduler();
        
        Thread.sleep(1000);
        
        assertTrue(COUNTER > 0);
    }

    /**
     * Ensure that this job only fires once if there are multiple scheduler nodes.
     * @throws Exception
     */
    @Test
    public void testTwoSchedulers() throws Exception {
        JobDetail job = JobBuilder.newJob(IncrementJob.class)
            .storeDurably()
            .usingJobData("key", "value")
            .withIdentity("name", "group")
            .build();
        
        OperableTrigger trigger = (OperableTrigger)newTrigger()
            .withIdentity("name", "group")
            .forJob(job)
            .startNow()
            .withSchedule(repeatSecondlyForever())
            .build();
        
        long start = System.currentTimeMillis();
        scheduler.scheduleJob(job, trigger);
        
        Scheduler scheduler2 = createNewScheduler();
        
        Thread.sleep(10000);
        
        scheduler.shutdown();
        scheduler2.shutdown();
        
        assertTrue(COUNTER > 1);
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("GOT " + COUNTER);
        assertTrue("Got too many counts. " + COUNTER, COUNTER <= ((elapsed / 1000) + 1));
    }

    public static class IncrementJob implements Job {

        public void execute(JobExecutionContext context) throws JobExecutionException {
            COUNTER++;
        }
        
    }
}
