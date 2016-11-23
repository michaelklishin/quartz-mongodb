package it

import com.novemberain.quartz.mongodb.MongoHelper
import com.novemberain.quartz.mongodb.QuartzHelper
import org.quartz.CalendarIntervalScheduleBuilder
import org.quartz.Job
import org.quartz.JobBuilder
import org.quartz.JobDetail
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.quartz.JobKey
import org.quartz.ObjectAlreadyExistsException
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

import static com.novemberain.quartz.mongodb.QuartzHelper.inSeconds
import static org.quartz.impl.matchers.GroupMatcher.groupEquals

/**
 * These are integration tests from Quartzite.
 * They do a decent job of exercising most of the underlying store operations
 * so we just reuse it here.
 */
class QuartzWithMongoDbStoreTest extends Specification {

    @Shared def Scheduler scheduler

    def setupSpec() {
        MongoHelper.dropTestDB()
        scheduler = QuartzHelper.startDefaultScheduler()
    }

    def cleanupSpec() {
        scheduler.shutdown()
    }

    def setup() {
        MongoHelper.purgeCollections()
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

    // Case 3:
    static counter3 = new AtomicInteger(0)
    public static class JobC implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            counter3.incrementAndGet()
        }
    }

    def 'test manual triggering of a job'() {
        given:
        def jk = new JobKey(JobC.name, "tests")
        def job = JobBuilder.newJob(JobC).withIdentity(jk).build()
        def trigger = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity('clojurewerkz.quartzite.test.execution.trigger3', 'tests')
                .withDescription('just a trigger')
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(10)
                .withIntervalInSeconds(2))
                .build()

        when:
        scheduler.scheduleJob(job, trigger)
        scheduler.triggerJob(jk)
        Thread.sleep(500)

        then:
        counter3.get() == 2
    }

    // Case 4:
    static counter4 = Collections.synchronizedMap([:])
    public static class JobD implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            counter4.putAll(ctx.getMergedJobDataMap())
        }
    }

    def 'test job data access'() {
        given:
        def jk = new JobKey(JobD.name, 'tests')
        def job = JobBuilder.newJob(JobD)
                .withIdentity(jk)
                .usingJobData('jobKey', 'jobValue')
                .build()
        def trigger = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity('clojurewerkz.quartzite.test.execution.trigger4', 'tests')
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(10)
                .withIntervalInSeconds(2))
                .build()

        when:
        scheduler.scheduleJob(job, trigger)
        scheduler.triggerJob(jk)
        Thread.sleep(1000)

        then:
        counter4 == [jobKey: 'jobValue']
    }

    // Case 5:
    static counter5 = new AtomicInteger(0)
    public static class JobE implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            counter5.addAndGet(ctx.getMergedJobDataMap().getInt('jobKey'))
        }
    }

    def 'test job pausing resuming and unscheduling'() {
        given:
        def jk = new JobKey(JobE.name, 'jobs.unscheduling')
        def tk = new TriggerKey('test.execution.trigger5', 'triggers.unscheduling')
        def job = JobBuilder.newJob(JobE)
                .withIdentity(jk)
                .usingJobData('jobKey', 2)
                .build()
        def trigger = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(tk)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(10)
                .withIntervalInSeconds(1))
                .build()

        when:
        scheduler.scheduleJob(job, trigger)
        scheduler.pauseJob(jk)
        scheduler.resumeJob(jk)
        scheduler.pauseJobs(groupEquals('jobs.unscheduling'))
        scheduler.resumeJobs(groupEquals('jobs.unscheduling'))
        scheduler.pauseTrigger(tk)
        scheduler.resumeTrigger(tk)
        scheduler.pauseTriggers(groupEquals('triggers.unscheduling'))
        scheduler.resumeTriggers(groupEquals('triggers.unscheduling'))
        scheduler.pauseAll()
        scheduler.resumeAll()
        scheduler.unscheduleJob(tk)
        Thread.sleep(300)

        and:
        scheduler.unscheduleJobs([tk])
        scheduler.deleteJob(jk)
        scheduler.deleteJobs([jk])
        Thread.sleep(3000)
        // With start-now policy some executions
        // manages to get through. In part this test is supposed
        // to demonstrate it as much as test unscheduling/pausing functions. MK.

        then:
        counter5.get() < 10
    }

    // Case 6:
    static latch6 = new CountDownLatch(3)
    public static class JobF implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            latch6.countDown()
        }
    }

    def 'test basic periodic execution with calendar interval schedule'() {
        given:
        def job = JobBuilder.newJob(JobF).withIdentity(JobF.name, 'tests').build()
        def trigger = TriggerBuilder.newTrigger()
                .startNow()
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInSeconds(2))
                .build()

        when:
        scheduler.scheduleJob(job, trigger)

        then:
        latch6.await()
    }

    // Case 7:
    static counter7 = new AtomicInteger(0)
    public static class JobG implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            counter7.incrementAndGet()
        }
    }

    def 'test double scheduling'() {
        given:
        def job = JobBuilder.newJob(JobG).withIdentity(JobG.name, 'tests').build()
        def trigger = TriggerBuilder.newTrigger()
                .startAt(inSeconds(2))
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInSeconds(2))
                .build()

        when:
        def date = scheduler.scheduleJob(job, trigger)

        then:
        date != null

        when:
        scheduler.scheduleJob(job, trigger)

        then: 'schedule will raise an exception'
        thrown(ObjectAlreadyExistsException)

        and: 'but maybe-schedule will not'
        !maybeSchedule(job, trigger)
        !maybeSchedule(job, trigger)
        !maybeSchedule(job, trigger)

        when:
        Thread.sleep(7000)

        then:
        counter7.get() == 3
    }

    def List<JobDetail> matchingJobs(GroupMatcher matcher) {
        scheduler.getJobKeys(matcher).collect { scheduler.getJobDetail(it) }
    }

    def List<Trigger> matchingTriggers(GroupMatcher matcher) {
        scheduler.getTriggerKeys(matcher).collect { scheduler.getTrigger(it) }
    }

    def boolean maybeSchedule(JobDetail job, Trigger trigger) {
        if (scheduler.checkExists(job.getKey()) || scheduler.checkExists(trigger.getKey())) {
            return false
        }
        scheduler.scheduleJob(job, trigger) != null
    }

    def 'schedule via scheduleJobs method'(){
        given:
        def jobKey = new JobKey('foo', 'bar')
        def job = JobBuilder.newJob(JobH).withIdentity(jobKey).build()
        def trigger = TriggerBuilder.newTrigger().build()
        def jobsAndTriggers = [:]
        jobsAndTriggers.put(job, [ trigger ] as Set)
        when:
        scheduler.scheduleJobs(jobsAndTriggers, false)
        then:
        scheduler.getJobDetail(jobKey)
        scheduler.getTriggersOfJob(jobKey) == [trigger]
    }

    public static class JobH implements Job {
      @Override
      void execute(JobExecutionContext ctx) throws JobExecutionException {
      }
  }
}