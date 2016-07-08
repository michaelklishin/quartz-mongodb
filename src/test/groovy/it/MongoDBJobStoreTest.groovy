package it

import com.novemberain.quartz.mongodb.MongoDBJobStore
import com.novemberain.quartz.mongodb.MongoHelper
import org.bson.Document
import org.quartz.CalendarIntervalScheduleBuilder
import org.quartz.CronScheduleBuilder
import org.quartz.DailyTimeIntervalScheduleBuilder
import org.quartz.JobBuilder
import org.quartz.JobDetail
import org.quartz.JobKey
import org.quartz.SimpleScheduleBuilder
import org.quartz.TimeOfDay
import org.quartz.Trigger
import org.quartz.TriggerBuilder
import org.quartz.TriggerKey
import org.quartz.impl.matchers.GroupMatcher
import org.quartz.jobs.NoOpJob
import org.quartz.simpl.SimpleClassLoadHelper
import org.quartz.spi.OperableTrigger
import spock.lang.Specification
import spock.lang.Subject

import static org.joda.time.DateTime.now
import static org.quartz.DateBuilder.IntervalUnit.*
import static org.quartz.Trigger.TriggerState.*
import static org.quartz.Trigger.TriggerState.PAUSED

class MongoDBJobStoreTest extends Specification {

    @Subject
    def store = makeStore()

    def setup() {
        MongoHelper.purgeCollections()
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

    def Date in2Months() {
        now().plusMonths(2).toDate()
    }

    def JobDetail makeJob(String name, String group) {
        JobBuilder.newJob(NoOpJob).withIdentity(name, group).build()
    }

    def makeStore() {
        def store = new MongoDBJobStore(
                instanceName: 'quartz_mongodb_test',
                dbName: 'quartz_mongodb_test',
                addresses: '127.0.0.1')
        store.initialize(new SimpleClassLoadHelper(), null)
        store
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

}