package com.novemberain.quartz.mongodb

import com.novemberain.quartz.mongodb.util.Keys
import org.bson.types.ObjectId
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class FailoverTest extends Specification {

    def setup() {
        MongoHelper.purgeCollections()
    }

    @Shared def quartzFinishWaittimeSecs = 2l

    def insertScheduler(id) {
        MongoHelper.addScheduler([
                instanceId     : id,
                schedulerName  : 'test cluster',
                lastCheckinTime: 1462806352702,
                checkinInterval: 7500l
        ])
    }

    def insertJob(jobName, recover) {
        MongoHelper.addJob([
                _id             : new ObjectId('00000000ee78252adaba4534'),
                keyName         : 'job',
                keyGroup        : 'g1',
                jobDescription  : null,
                jobClass        : 'com.novemberain.quartz.mongodb.FailoverTest$' + jobName,
                durability      : false,
                requestsRecovery: recover
        ])
    }

    def commonTriggerData = [
            state             : 'waiting',
            calendarName      : null,
            description       : null,
            endTime           : null,
            fireInstanceId    : null,
            jobId             : new ObjectId('00000000ee78252adaba4534'),
            keyName           : 't1',
            keyGroup          : 'g1',
            misfireInstruction: 0,
            nextFireTime      : null,
            priority          : 5,
            timesTriggered    : 1
    ]

    def insertTrigger() {
        def data = [:]
        data.putAll(commonTriggerData)
        data.putAll([
                class             : 'org.quartz.impl.triggers.CalendarIntervalTriggerImpl',
                startTime         : new Date(1462820481910),
                previousFireTime  : new Date(1462820481910),
                nextFireTime      : new Date(1462820483910),
                finalFireTime     : null,
                repeatIntervalUnit: 'SECOND',
                repeatInterval    : 2
        ])
        MongoHelper.addTrigger(data)
    }

    /**
     * Inserts a simple trigger.
     */
    def insertSimpleTrigger(Date fireTime, long repeatInterval, int repeatCount) {
        insertSimpleTrigger(fireTime, null, repeatInterval, repeatCount)
    }

    def insertSimpleTrigger(Date fireTime, finalFireTime, repeatInterval, repeatCount) {
        def data = [:]
        data.putAll(commonTriggerData)
        data.putAll([
                class           : 'org.quartz.impl.triggers.SimpleTriggerImpl',
                startTime       : fireTime,
                previousFireTime: fireTime,
                nextFireTime    : null,
                finalFireTime   : finalFireTime,
                repeatCount     : repeatCount,
                repeatInterval  : repeatInterval
        ])
        MongoHelper.addTrigger(data)
    }

    /**
     * Inserts trigger that has no next fire time.
     */
    def insertOneshotTrigger(Date fireTime) {
        insertSimpleTrigger(fireTime, 0, 0)
    }

    def insertTriggerLock(instanceId) {
        MongoHelper.addLock([
                (Keys.LOCK_TYPE)            : Keys.LockType.t.name(),
                (Keys.KEY_GROUP)            : 'g1',
                (Keys.KEY_NAME)             : 't1',
                (Constants.LOCK_INSTANCE_ID): instanceId,
                (Constants.LOCK_TIME)       : new Date(1462820481910)
        ])
    }

    public static class DeadJob1 implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            println('Executing DeadJob1')
            throw new IllegalStateException('Should not be executed!')
        }
    }

    def 'should execute one shot trigger only once'() {
        // Should remove own, one-shot triggers during startup.
        // This is needed, because otherwise there would be no way
        // to distinguish between own long-running jobs and own dead jobs.
        given:
        insertScheduler('single-node')
        insertJob('DeadJob1', false)
        insertOneshotTrigger(new Date(1462820481910))
        insertTriggerLock('single-node')
        def cluster = QuartzHelper.createCluster('single-node')

        when:
        TimeUnit.SECONDS.sleep(quartzFinishWaittimeSecs)

        then:
        MongoHelper.getCount('triggers') == 0
        MongoHelper.getCount('locks') == 0
        QuartzHelper.shutdown(cluster)
    }

    static job2RunSignaler = new CountDownLatch(1)

    public static class DeadJob2 implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            println('Executing DeadJob2')
            if (ctx.isRecovering()) {
                throw new IllegalStateException('Should not be in recovering state!')
            }
            job2RunSignaler.countDown()
        }
    }

    def 'should reexecute other trigger from failed execution'() {
        // Case: checks that time-based recurring trigger (has next fire time)
        // from dead-node is picked up for execution."
        given:
        insertScheduler('dead-node')
        insertJob('DeadJob2', false)
        insertTrigger()
        insertTriggerLock('dead-node')

        when:
        def cluster = QuartzHelper.createCluster('single-node')
        job2RunSignaler.await(2, TimeUnit.SECONDS)

        then:
        job2RunSignaler.getCount() == 0
        TimeUnit.SECONDS.sleep(quartzFinishWaittimeSecs)  // let Quartz finish job
        QuartzHelper.shutdown(cluster)
    }

    static job3RunSignaler = new CountDownLatch(1)

    public static class DeadJob3 implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            println('Executing DeadJob3')
            if (!ctx.isRecovering()) {
                throw new IllegalStateException('Should not be in recovering state!')
            }
            job3RunSignaler.countDown()
        }
    }

    def 'should recover own one shot trigger'() {
        // Case: own, one-shot trigger whose job requests recovery
        // should be run again.
        given:
        insertScheduler('single-node')
        insertJob('DeadJob3', true)
        insertOneshotTrigger(new Date(1462820481910))
        insertTriggerLock('single-node')

        when:
        def cluster = QuartzHelper.createCluster('single-node')
        job3RunSignaler.await(2, TimeUnit.SECONDS)

        then:
        job3RunSignaler.getCount() == 0
        TimeUnit.SECONDS.sleep(quartzFinishWaittimeSecs)  // let Quartz finish job
        MongoHelper.getCount('triggers') == 0
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('locks') == 0
        QuartzHelper.shutdown(cluster)
    }

    static job4RunSignaler = new CountDownLatch(3)

    public static class DeadJob4 implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            println("Executing DeadJob4. Recovering: ${ctx.isRecovering()}, refire count: ${ctx.getRefireCount()}")
            job4RunSignaler.countDown()
        }
    }

    def 'should recover own repeating trigger'() {
        // Case: own, repeating trigger whose job requests recovery
        // should be run times_left + 1 times.
        given:
        insertScheduler('single-node')
        insertJob('DeadJob4', true)
        insertTriggerLock('single-node')
        def repeatInterval = 1000l
        def repeatCount = 2
        def fireTime = new Date()
        def finalFireTime = fireTime.getTime() + (repeatInterval * (repeatCount + 1))
        insertSimpleTrigger(fireTime, finalFireTime, repeatInterval, repeatCount)

        when:
        def cluster = QuartzHelper.createCluster('single-node')
        job4RunSignaler.await(5, TimeUnit.SECONDS)

        then:
        job4RunSignaler.getCount() == 0
        TimeUnit.SECONDS.sleep(quartzFinishWaittimeSecs) // let Quartz finish job
        MongoHelper.getCount('triggers') == 0
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('locks') == 0
        QuartzHelper.shutdown(cluster)
    }

    static job5RunSignaler = new CountDownLatch(1)

    public static class DeadJob5 implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            println('Executing DeadJob5')
            if (!ctx.isRecovering()) {
                throw new IllegalStateException('Should be in recovering state!')
            }
            job5RunSignaler.countDown()
        }
    }

    def 'should recover dead node one shot trigger'() {
        // Case: other node's, one-shot trigger whose job requests recovery
        // should be run again.
        given:
        insertScheduler('dead-node')
        insertJob('DeadJob5', true)
        insertOneshotTrigger(new Date(1462820481910))
        insertTriggerLock('dead-node')

        when:
        def cluster = QuartzHelper.createCluster('single-node')
        job5RunSignaler.await(2, TimeUnit.SECONDS)

        then:
        job5RunSignaler.getCount() == 0
        TimeUnit.SECONDS.sleep(quartzFinishWaittimeSecs)  // let Quartz finish job
        MongoHelper.getCount('triggers') == 0
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('locks') == 0
        QuartzHelper.shutdown(cluster)
    }

    static job6RunSignaler = new CountDownLatch(3)

    public static class DeadJob6 implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            println("Executing DeadJob6. Recovering: ${ctx.isRecovering()}, refire count: ${ctx.getRefireCount()}")
            job6RunSignaler.countDown()
        }
    }

    def 'should recover dead node repeating trigger'() {
        // Case: other node's, repeating trigger whose job requests recovery
        // should be run times_left + 1 times.
        given:
        insertScheduler('dead-node')
        insertJob('DeadJob6', true)
        insertTriggerLock('dead-node')
        def repeatInterval = 1000l
        def repeatCount = 2
        def fireTime = new Date()
        def finalFireTime = fireTime.getTime() + (repeatInterval * (repeatCount + 1))
        insertSimpleTrigger(fireTime, finalFireTime, repeatInterval, repeatCount)

        when:
        def cluster = QuartzHelper.createCluster('single-node')
        job6RunSignaler.await(5, TimeUnit.SECONDS)

        then:
        job6RunSignaler.getCount() == 0
        TimeUnit.SECONDS.sleep(quartzFinishWaittimeSecs)  // let Quartz finish job
        MongoHelper.getCount('triggers') == 0
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('locks') == 0
        QuartzHelper.shutdown(cluster)
    }

    static job7RunSignaler = new CountDownLatch(3)

    public static class DeadJob7 implements Job {
        @Override
        void execute(JobExecutionContext ctx) throws JobExecutionException {
            println("Executing DeadJob7. Recovering: ${ctx.isRecovering()}, refire count: ${ctx.getRefireCount()}")
            job7RunSignaler.countDown()
        }
    }

    def 'should recover dead node repeating trigger by any node'() {
        // Case: other node's, repeating trigger whose job requests recovery
        // should be run times_left + 1 times, when cluster consists of more
        // than one node.
        given:
        insertScheduler('dead-node')
        insertJob('DeadJob7', true)
        insertTriggerLock('dead-node')
        def repeatInterval = 1000l
        def repeatCount = 2
        def fireTime = new Date()
        def finalFireTime = fireTime.getTime() + (repeatInterval * (repeatCount + 1))
        insertSimpleTrigger(fireTime, finalFireTime, repeatInterval, repeatCount)

        when:
        def cluster = QuartzHelper.createCluster('live-one', 'live-two')
        job7RunSignaler.await(5, TimeUnit.SECONDS)

        then:
        job7RunSignaler.getCount() == 0
        TimeUnit.SECONDS.sleep(quartzFinishWaittimeSecs)  // let Quartz finish job
        MongoHelper.getCount('triggers') == 0
        MongoHelper.getCount('jobs') == 0
        MongoHelper.getCount('locks') == 0
        QuartzHelper.shutdown(cluster)
    }

}