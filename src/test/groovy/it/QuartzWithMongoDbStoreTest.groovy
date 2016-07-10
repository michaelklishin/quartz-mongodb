package it

import com.novemberain.quartz.mongodb.QuartzHelper
import org.quartz.Job
import org.quartz.JobBuilder
import org.quartz.JobDetail
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.quartz.JobKey
import org.quartz.Scheduler
import org.quartz.SimpleScheduleBuilder
import org.quartz.Trigger
import org.quartz.TriggerBuilder
import org.quartz.TriggerKey
import org.quartz.impl.matchers.GroupMatcher
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import static org.quartz.impl.matchers.GroupMatcher.groupEquals

/**
 * These are integration tests from Quartzite.
 * They do a decent job of exercising most of the underlying store operations
 * so we just reuse it here.
 */
class QuartzWithMongoDbStoreTest extends Specification {

    @Shared def Scheduler scheduler

    def setupSpec() {
        scheduler = QuartzHelper.startDefaultScheduler()
    }

    def cleanupSpec() {
        scheduler.shutdown()
    }

    def 'scheduler should be started'() {
        expect:
        scheduler.isStarted()
    }

    // Case 1:
    static latch1 = new CountDownLatch(10)
    public static class JobA implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            latch1.countDown()
        }
    }

    def 'test basic periodic execution with a job'() {
        given:
        def job = JobBuilder.newJob(JobA)
                .withIdentity(JobA.getName(), 'tests')
                .build()
        def trigger = TriggerBuilder.newTrigger()
                .startNow()
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(10)
                .withIntervalInMilliseconds(200))
                .build()

        when:
        scheduler.scheduleJob(job, trigger)
        def j = scheduler.getJobDetail(new JobKey(JobA.getName(), 'tests'))

        then:
        j.getKey() != null
        j.getDescription() == null
        j.getJobDataMap() != null
        latch1.await()
    }

    // Case 2:
    static counter2 = new AtomicInteger(0)
    public static class JobB implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            counter2.incrementAndGet()
        }
    }

    def 'test unscheduling of a job'() {
        given:
        def jk = new JobKey(JobB.getName(), 'tests')
        def tk = new TriggerKey('clojurewerkz.quartzite.test.execution.trigger2', 'tests')
        def job = JobBuilder.newJob(JobB)
                .withIdentity(JobB.name, 'tests')
                .build()
        def trigger = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity('clojurewerkz.quartzite.test.execution.trigger2', 'tests')
                .withDescription('just a trigger')
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(10)
                .withIntervalInMilliseconds(400))
                .build()

        expect:
        !scheduler.checkExists(jk)
        !scheduler.checkExists(tk)

        when:
        scheduler.scheduleJob(job, trigger)

        then:
        scheduler.checkExists(jk)
        scheduler.checkExists(tk)
        scheduler.getTrigger(tk) != null
        scheduler.getJobDetail(jk) != null

        when:
        def t = scheduler.getTrigger(tk)

        then:
        t.getKey() != null
        t.getDescription() != null
        t.getStartTime() != null
        t.getNextFireTime() != null
        scheduler.getJobDetail(jk) != null
        scheduler.getJobDetail(new JobKey('ab88fsyd7f', 'k28s8d77s')) == null
        scheduler.getTrigger(new TriggerKey('ab88fsyd7f')) == null
        !scheduler.getTriggerKeys(groupEquals('tests')).isEmpty()
        !matchingTriggers(groupEquals('tests')).isEmpty()
        !scheduler.getJobKeys(groupEquals('tests')).isEmpty()
        !matchingJobs(groupEquals('tests')).isEmpty()

        when:
        Thread.sleep(2000)

        then:
        scheduler.unscheduleJob(tk)
        !scheduler.checkExists(jk)
        !scheduler.checkExists(tk)

        when:
        Thread.sleep(2000)

        then:
        counter2.get() < 7
    }

    def List<JobDetail> matchingJobs(GroupMatcher matcher) {
        scheduler.getJobKeys(matcher).collect { scheduler.getJobDetail(it) }
    }

    def List<Trigger> matchingTriggers(GroupMatcher matcher) {
        scheduler.getTriggerKeys(matcher).collect { scheduler.getTrigger(it) }
    }
}