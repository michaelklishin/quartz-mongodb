package com.novemberain.quartz.mongodb.cluster

import spock.lang.Specification

class SchedulerTest extends Specification {

    def Scheduler createScheduler(long lastCheckinTime, long checkinInterval) {
        new Scheduler("name", "id", lastCheckinTime, checkinInterval)
    }

    def 'should tell when scheduler is alive'() {
        expect: 'Checkin time was within expected time frame'
        !createScheduler(0, 0).isDefunct(0)
        !createScheduler(0, 0).isDefunct(Scheduler.TIME_EPSILON)
        !createScheduler(1, 0).isDefunct(Scheduler.TIME_EPSILON + 1)
        !createScheduler(0, 1).isDefunct(Scheduler.TIME_EPSILON + 1)
        !createScheduler(1, 1).isDefunct(Scheduler.TIME_EPSILON + 2)
    }

    def 'should tell when scheduler is defunct'() {
        expect: '1 tick after expected checkin time'
        createScheduler(0, 0).isDefunct(Scheduler.TIME_EPSILON + 1)
        createScheduler(1, 0).isDefunct(Scheduler.TIME_EPSILON + 2)
        createScheduler(0, 1).isDefunct(Scheduler.TIME_EPSILON + 2)
        createScheduler(1, 1).isDefunct(Scheduler.TIME_EPSILON + 3)
    }
}