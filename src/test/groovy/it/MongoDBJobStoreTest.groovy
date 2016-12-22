package it

import com.novemberain.quartz.mongodb.MongoDBJobStore
import com.novemberain.quartz.mongodb.MongoHelper
import org.bson.Document
import org.bson.types.ObjectId
import org.quartz.CalendarIntervalScheduleBuilder
import org.quartz.CronScheduleBuilder
import org.quartz.DailyTimeIntervalScheduleBuilder
import org.quartz.Job
import org.quartz.JobBuilder
import org.quartz.JobDetail
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.quartz.JobKey
import org.quartz.SimpleScheduleBuilder
import org.quartz.TimeOfDay
import org.quartz.TriggerBuilder
import org.quartz.TriggerKey
import org.quartz.impl.matchers.GroupMatcher
import org.quartz.simpl.SimpleClassLoadHelper
import org.quartz.spi.OperableTrigger
import spock.lang.Specification
import spock.lang.Subject

import static com.novemberain.quartz.mongodb.QuartzHelper.in2Months
import static com.novemberain.quartz.mongodb.QuartzHelper.inSeconds
import static org.quartz.DateBuilder.IntervalUnit.*
import static org.quartz.Trigger.TriggerState.*
import static org.quartz.Trigger.TriggerState.PAUSED

class MongoDBJobStoreTest extends Specification {

    @Subject
    def store = makeStore()

    def setupSpec() {
        MongoHelper.dropTestDB()
    }

    def setup() {
        MongoHelper.purgeCollections()
    }

    def 'should store a job'() {
        given:
        def job = makeJob('test-storing-jobs')
        def key = new JobKey('test-storing-jobs', 'tests')

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJob(job, false)

        then:
        MongoHelper.getCount('jobs') == 1
        store.getNumberOfJobs() == 1
        store.getJobKeys(GroupMatcher.groupEquals('tests')) == [key] as Set
        store.getJobKeys(GroupMatcher.groupEquals('sabbra-cadabra')).isEmpty()
        store.getJobKeys(GroupMatcher.groupStartsWith('te')) == [key] as Set
        store.getJobKeys(GroupMatcher.groupEndsWith('sts')) == [key] as Set
        store.getJobKeys(GroupMatcher.groupContains('es')) == [key] as Set
    }

    def 'should store trigger with simple schedule'() {
        given:
        def desc = 'just a trigger'
        def job = makeJob('test-storing-triggers1')
        def tk = new TriggerKey('test-storing-triggers1', 'tests')
        def tr = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(tk)
                .withDescription(desc)
                .forJob(job)
                .withSchedule(SimpleScheduleBuilder.newInstance().withRepeatCount(10).withIntervalInMilliseconds(400))
                .build() as OperableTrigger
        def key = new TriggerKey('test-storing-triggers1', 'tests')

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJob(job, false)
        store.storeTrigger(tr, false)

        then:
        store.getTriggerState(tk) == NORMAL
        MongoHelper.getCount('jobs') == 1
        MongoHelper.getCount('triggers') == 1
        store.getNumberOfTriggers() == 1
        def m = firstTrigger('test-storing-triggers1')
        m.state == 'waiting'
        m.description == desc
        m.repeatInterval == 400
        m.timesTriggered == 0
        m.containsKey('startTime')
        m.containsKey('finalFireTime')
        m.containsKey('nextFireTime') && m.nextFireTime == null
        m.containsKey('endTime') && m.endTime == null
        m.containsKey('previousFireTime') && m.previousFireTime == null
        hasJob(m.jobId)
        store.getTriggerKeys(GroupMatcher.groupEquals('tests')) == [key] as Set
        store.getTriggerKeys(GroupMatcher.groupEquals('sabbra-cadabra')).isEmpty()
        store.getTriggerKeys(GroupMatcher.groupStartsWith('te')) == [key] as Set
        store.getTriggerKeys(GroupMatcher.groupEndsWith('sts')) == [key] as Set
        store.getTriggerKeys(GroupMatcher.groupContains('es')) == [key] as Set
    }

    def 'should store trigger with cron schedule'() {
        given:
        def desc = 'just a trigger that uses a cron expression schedule'
        def job = makeJob('test-storing-triggers2')
        def cronExp = '0 0 15 L-1 * ?'
        def tr = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity('test-storing-triggers2', 'tests')
                .withDescription(desc)
                .endAt(in2Months())
                .forJob(job)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExp))
                .build() as OperableTrigger

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJob(job, false)
        store.storeTrigger(tr, false)

        then:
        MongoHelper.getCount('jobs') == 1
        MongoHelper.getCount('triggers') == 1
        store.getNumberOfTriggers() == 1
        def m = firstTrigger('test-storing-triggers2')
        m.description == desc
        m.cronExpression == cronExp
        m.containsKey('startTime')
        m.containsKey('timezone')
        m.containsKey('nextFireTime') && m.nextFireTime == null
        m.containsKey('previousFireTime') && m.previousFireTime == null
        //TODO check: m.containsKey('repeatInterval') && m.repeatInterval == null
        //TODO m.containsKey('timesTriggered') && m.timesTriggered == null
        hasJob(m.jobId)
    }

    def 'should store triggers with daily interval schedule'() {
        given:
        def desc = 'just a trigger that uses a daily interval schedule'
        def job = makeJob('test-storing-triggers3')
        def tr = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity('test-storing-triggers3', 'tests')
                .withDescription(desc)
                .endAt(in2Months())
                .forJob(job)
                .withSchedule(DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule()
                .onEveryDay()
                .startingDailyAt(TimeOfDay.hourMinuteAndSecondOfDay(9, 00, 00))
                .endingDailyAt(TimeOfDay.hourMinuteAndSecondOfDay(18, 00, 00))
                .withIntervalInHours(2))
                .build() as OperableTrigger

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJob(job, false)
        store.storeTrigger(tr, false)

        then:
        MongoHelper.getCount('jobs') == 1
        MongoHelper.getCount('triggers') == 1
        store.getNumberOfTriggers() == 1
        def m = firstTrigger('test-storing-triggers3')
        m.endTimeOfDay.hour == 18
        m.endTimeOfDay.minute == 0
        m.endTimeOfDay.second == 0
        m.startTimeOfDay.hour == 9
        m.startTimeOfDay.minute == 0
        m.startTimeOfDay.second == 0
        m.description == desc
        m.repeatInterval == 2
        m.repeatIntervalUnit == HOUR.name()
        m.containsKey('startTime')
        m.containsKey('endTime')
        m.containsKey('nextFireTime') && m.nextFireTime == null
        m.containsKey('previousFireTime') && m.previousFireTime == null
        hasJob(m.jobId)
    }

    def 'should store trigger with calendar interval schedule'() {
        given:
        def desc = 'just a trigger that uses a daily interval schedule'
        def job = makeJob('test-storing-triggers4')
        def tr = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity('test-storing-triggers4', 'tests')
                .withDescription(desc)
                .endAt(in2Months())
                .forJob(job)
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInHours(4))
                .build() as OperableTrigger

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJob(job, false)
        store.storeTrigger(tr, false)

        then:
        MongoHelper.getCount('jobs') == 1
        MongoHelper.getCount('triggers') == 1
        store.getNumberOfTriggers() == 1
        def m = firstTrigger('test-storing-triggers4')
        m.description == desc
        m.repeatInterval == 4
        m.repeatIntervalUnit == HOUR.name()
        m.containsKey('startTime')
        m.containsKey('endTime')
        m.containsKey('nextFireTime') && m.nextFireTime == null
        m.containsKey('previousFireTime') && m.previousFireTime == null
        hasJob(m.jobId)
    }

    def 'should pause trigger'() {
        given:
        def job = makeJob('test-pause-trigger')
        def tk = new TriggerKey('test-pause-trigger', 'tests')
        def tr = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity('test-pause-trigger', 'tests')
                .endAt(in2Months())
                .forJob(job)
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInHours(4))
                .build() as OperableTrigger

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJob(job, false)
        store.storeTrigger(tr, false)
        store.pauseTrigger(tk)

        then:
        firstTrigger('test-pause-trigger') != null
        store.getTriggerState(tk) == PAUSED

        when:
        store.resumeTrigger(tk)

        then:
        firstTrigger('test-pause-trigger') != null
        store.getTriggerState(tk) == NORMAL
    }

    def 'should pause triggers'() {
        given:
        def job = makeJob('job-in-test-pause-triggers', 'main-tests')
        def tk1 = new TriggerKey('test-pause-triggers1', 'main-tests')
        def tr1 = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(tk1)
                .endAt(in2Months())
                .forJob(job)
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInHours(4))
                .build() as OperableTrigger
        def tk2 = new TriggerKey('test-pause-triggers2', 'alt-tests')
        def tr2 = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(tk2)
                .endAt(in2Months())
                .forJob(job)
                .withSchedule(SimpleScheduleBuilder.newInstance()
                .withRepeatCount(10)
                .withIntervalInMilliseconds(400))
                .build() as OperableTrigger

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJob(job, false)
        store.storeTrigger(tr1, false)
        store.storeTrigger(tr2, false)

        then:
        MongoHelper.getCount('triggers') == 2
        store.getNumberOfTriggers() == 2

        when:
        store.pauseTriggers(GroupMatcher.groupStartsWith('main'))
        def m = firstTrigger('test-pause-triggers1', 'main-tests')

        then:
        m.state == 'paused'
        store.getTriggerState(tk1) == PAUSED

        when:
        m = firstTrigger('test-pause-triggers2', 'alt-tests')

        then:
        m.state == 'waiting'
        store.getTriggerState(tk2) == NORMAL
        store.getPausedTriggerGroups() == ['main-tests'] as Set

        when:
        store.resumeTriggers(GroupMatcher.groupStartsWith('main'))
        m = firstTrigger('test-pause-triggers1', 'main-tests')

        then:
        m.state == 'waiting'
        store.getTriggerState(tk1) == NORMAL
        store.getPausedTriggerGroups().isEmpty()
    }

    def 'should return job group names'() {
        given:
        def job1 = makeJob('job-in-test-job-group-names', 'test-job1')
        def job2 = makeJob('job-in-test-job-group-names', 'test-job2')

        when:
        store.storeJob(job1, false)
        store.storeJob(job2, false)
        def groups = store.getJobGroupNames()

        then:
        groups == ['test-job1', 'test-job2']
    }

    def 'should return trigger group names'() {
        given:
        def job1 = makeJob('job-in-test-trigger-group-names', 'test-job1')
        def tr1 = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity('trigger-in-test-trigger-group-names', 'test-trigger-1')
                .withDescription('description')
                .endAt(in2Months())
                .forJob(job1)
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInHours(4))
                .build() as OperableTrigger


        def job2 = makeJob('job-in-test-trigger-group-names', 'test-job2')
        def tr2 = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity('trigger-in-test-trigger-group-names', 'test-trigger-2')
                .withDescription('description')
                .endAt(in2Months())
                .forJob(job2)
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInHours(4))
                .build() as OperableTrigger

        when:
        store.storeJob(job1, false)
        store.storeTrigger(tr1, false)
        store.storeJob(job2, false)
        store.storeTrigger(tr2, false)

        then:
        store.getTriggerGroupNames() == ['test-trigger-1', 'test-trigger-2']
    }

    def 'should pause all triggers'() {
        given:
        def job = makeJob('job-in-test-pause-all-triggers', 'main-tests')
        def tk1 = new TriggerKey('test-pause-all-triggers1', 'main-tests')
        def tr1 = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(tk1)
                .endAt(in2Months())
                .forJob(job)
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInHours(4))
                .build() as OperableTrigger

        def tk2 = new TriggerKey('test-pause-all-triggers2', 'alt-tests')
        def tr2 = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(tk2)
                .endAt(in2Months())
                .forJob(job)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(10)
                .withIntervalInMilliseconds(400))
                .build() as OperableTrigger

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJob(job, false)
        store.storeTrigger(tr1, false)
        store.storeTrigger(tr2, false)
        and:
        store.pauseAll()
        def m1 = firstTrigger('test-pause-all-triggers1', 'main-tests')
        def m2 = firstTrigger('test-pause-all-triggers2', 'alt-tests')

        then:
        m1.state == 'paused'
        store.getTriggerState(tk1) == PAUSED
        m2.state == 'paused'
        store.getTriggerState(tk2) == PAUSED
        store.getPausedTriggerGroups() == ['main-tests', 'alt-tests'] as Set

        when:
        store.resumeAll()
        m1 = firstTrigger('test-pause-all-triggers1', 'main-tests')
        m2 = firstTrigger('test-pause-all-triggers2', 'alt-tests')

        then:
        m1.state == 'waiting'
        store.getTriggerState(tk1) == NORMAL
        m2.state == 'waiting'
        store.getTriggerState(tk2) == NORMAL
        store.getPausedTriggerGroups().isEmpty()
    }

    def 'should pause job'() {
        given:
        def jk = new JobKey('test-pause-job', 'tests')
        def job = makeJob('test-pause-job')
        def tk = new TriggerKey('test-pause-job', 'tests')
        def tr = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(tk)
                .endAt(in2Months())
                .forJob(job)
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInHours(4))
                .build() as OperableTrigger

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJob(job, false)
        store.storeTrigger(tr, false)
        store.pauseJob(jk)

        then:
        firstTrigger('test-pause-job').state == 'paused'
        store.getTriggerState(tk) == PAUSED

        when:
        store.resumeTrigger(tk)

        then:
        firstTrigger('test-pause-job').state == 'waiting'
        store.getTriggerState(tk) == NORMAL
    }

    def 'should pause jobs'() {
        given:
        def j1 = makeJob('job-in-test-pause-jobs1', 'main-tests')
        def tk1 = new TriggerKey('test-pause-jobs1', 'main-tests')
        def tr1 = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(tk1)
                .endAt(in2Months())
                .forJob(j1)
                .withSchedule(CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                .withIntervalInHours(4))
                .build() as OperableTrigger
        def j2 = makeJob('job-in-test-pause-jobs2', 'alt-tests')
        def tk2 = new TriggerKey('test-pause-jobs2', 'alt-tests')
        def tr2 = TriggerBuilder.newTrigger()
                .startNow()
                .withIdentity(tk2)
                .endAt(in2Months())
                .forJob(j2)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(10)
                .withIntervalInMilliseconds(400))
                .build() as OperableTrigger

        expect:
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('triggers') == 0

        when:
        store.storeJobAndTrigger(j1, tr1)
        store.storeJobAndTrigger(j2, tr2)

        then:
        MongoHelper.getCount('jobs') == 2
        MongoHelper.getCount('triggers') == 2
        store.getNumberOfJobs() == 2
        store.getNumberOfTriggers() == 2

        when:
        store.pauseJobs(GroupMatcher.groupStartsWith('main'))
        def m1 = firstTrigger('test-pause-jobs1', 'main-tests')
        def m2 = firstTrigger('test-pause-jobs2', 'alt-tests')

        then:
        m1.state == 'paused'
        store.getTriggerState(tk1) == PAUSED
        m2.state == 'waiting'
        store.getTriggerState(tk2) == NORMAL
        store.getPausedTriggerGroups().isEmpty()
        store.getPausedJobGroups() == ['main-tests'] as Set

        when:
        store.resumeJobs(GroupMatcher.groupStartsWith('main'))
        m1 = firstTrigger('test-pause-jobs1', 'main-tests')

        then:
        m1.state == 'waiting'
        store.getTriggerState(tk1) == NORMAL
        store.getPausedTriggerGroups().isEmpty()
    }

    def 'should acquire next trigger'() {
        // Tests whether can acquire next trigger in case of trigger lock.
        // It creates 3 triggers and tries to acquire them one-by-one, which results
        // in locking triggers one-by-one in subsequent calls. At the end all should
        // be locked and should not acquire more.
        given:
        def j1 = makeJob('job-in-test-acquire-next-trigger-job1', 'main-tests')
        def tk1 = new TriggerKey('test-acquire-next-trigger-trigger1', 'main-tests')
        def tr1 = TriggerBuilder.newTrigger()
                .startAt(inSeconds(2))
                .withIdentity(tk1)
                .endAt(in2Months())
                .forJob(j1)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(2)
                .withIntervalInSeconds(400))
                .build() as OperableTrigger

        def j2 = makeJob('job-in-test-acquire-next-trigger-job2', 'main-tests')
        def tk2 = new TriggerKey('test-acquire-next-trigger-trigger2', 'main-tests')
        def tr2 = TriggerBuilder.newTrigger()
                .startAt(inSeconds(5))
                .withIdentity(tk2)
                .endAt(in2Months())
                .forJob(j2)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(2)
                .withIntervalInSeconds(400))
                .build() as OperableTrigger

        def j3 = makeJob('job-in-test-acquire-next-trigger-job3', 'main-tests')
        def tk3 = new TriggerKey('test-acquire-next-trigger-trigger3', 'main-tests')
        def tr3 = TriggerBuilder.newTrigger()
                .startAt(inSeconds(10))
                .withIdentity(tk3)
                .endAt(in2Months())
                .forJob(j3)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                .withRepeatCount(2)
                .withIntervalInSeconds(400))
                .build() as OperableTrigger

        when:
        tr1.computeFirstFireTime(null)
        tr2.computeFirstFireTime(null)
        tr3.computeFirstFireTime(null)
        and:
        store.storeJob(j1 ,false)
        store.storeTrigger(tr1 ,false)
        store.storeJob(j2, false)
        store.storeTrigger(tr2, false)
        store.storeJob(j3 ,false)
        store.storeTrigger(tr3 ,false)

        then:
        store.acquireNextTriggers(10, 1, 0).isEmpty()

        when:
        def ff = tr1.getNextFireTime().getTime()

        then:
        tk1 == store.acquireNextTriggers(ff + 10000, 1, 0).get(0).getKey()
        tk2 == store.acquireNextTriggers(ff + 10000, 1, 0).get(0).getKey()
        tk3 == store.acquireNextTriggers(ff + 10000, 1, 0).get(0).getKey()
        store.acquireNextTriggers(ff + 10000, 1, 0).isEmpty()
    }

    def "should acquire new triggers and update state for those without existing job"() {
        given:
        def j1 = makeJob('job-in-test-acquire-next-trigger-job1', 'main-tests')
        def tk1 = new TriggerKey('test-acquire-next-trigger-trigger1', 'main-tests')
        def tr1 = TriggerBuilder.newTrigger()
                .startAt(inSeconds(2))
                .withIdentity(tk1)
                .forJob(j1)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule())
                .build() as OperableTrigger
        def triggerWithoutJobKey = new TriggerKey('triggerWithoutJob', 'main-tests')
        def triggerWithoutJob = TriggerBuilder.newTrigger()
                .startAt(inSeconds(5))
                .withIdentity(triggerWithoutJobKey)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule())
                .build() as OperableTrigger
        def j2 = makeJob('job-in-test-acquire-next-trigger-job2', 'main-tests')
        def tk2 = new TriggerKey('test-acquire-next-trigger-trigger2', 'main-tests')
        def tr2 = TriggerBuilder.newTrigger()
                .startAt(inSeconds(7))
                .withIdentity(tk2)
                .forJob(j2)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule())
                .build() as OperableTrigger

        tr1.computeFirstFireTime(null)
        triggerWithoutJob.computeFirstFireTime(null)
        tr2.computeFirstFireTime(null)

        and: "jobs with triggers are stored"
        store.storeJobAndTrigger(j1, tr1)
        store.storeJobAndTrigger(j2, tr2)
        // omits checks for job existing when string triggers:
        store.assembler.persister.storeTrigger(triggerWithoutJob, ObjectId.get(), false)
        def ff = tr1.getNextFireTime().getTime()

        when:
        def acquiredTriggers = store.acquireNextTriggers(ff + 10000, 2, 0)

        then:
        acquiredTriggers.size() == 2
        acquiredTriggers.get(0) == tr1
        acquiredTriggers.get(1) == tr2
        store.getTriggerState(triggerWithoutJob.key) == ERROR
    }

    def Document firstTrigger(String name) {
        firstTrigger(name, 'tests')
    }

    def Document firstTrigger(String name, String group) {
        MongoHelper.getFirst('triggers', [keyName: name, keyGroup: group])
    }

    def JobDetail makeJob(String name) {
        makeJob(name, 'tests')
    }

    def boolean hasJob(jobId) {
        MongoHelper.getFirst('jobs', [_id: jobId]) != null
    }


    def JobDetail makeJob(String name, String group) {
        JobBuilder.newJob(NoOpJob).withIdentity(name, group).build()
    }

    def makeStore() {
        def store = new MongoDBJobStore(
                instanceName: 'quartz_mongodb_test',
                dbName: 'quartz_mongodb_test',
                addresses: "127.0.0.1:${MongoHelper.DEFAULT_MONGO_PORT}")
        store.initialize(new SimpleClassLoadHelper(), null)
        store
    }

    class NoOpJob implements Job {

        @Override
        void execute(final JobExecutionContext context) throws JobExecutionException {
        }
    }
}