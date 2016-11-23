package com.novemberain.quartz.mongodb.dao

import com.novemberain.quartz.mongodb.Clocks
import com.novemberain.quartz.mongodb.MongoHelper
import com.novemberain.quartz.mongodb.util.Clock
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.atomic.AtomicInteger

class SchedulerDaoTest extends Specification {

    @Shared def schedulerName = 'TestSched'
    @Shared def instanceId = 'TestID'
    @Shared def clusterCheckinIntervalMillis = 5000l

    def testClock = Clocks.incClock()

    def setup() {
        MongoHelper.purgeCollections()
    }

    def 'should have passed collection'() {
        given:
        def dao = createDao()

        expect:
        MongoHelper.getSchedulersColl().is(dao.getCollection())
        dao.schedulerName == schedulerName
        dao.instanceId == instanceId
        dao.clusterCheckinIntervalMillis == clusterCheckinIntervalMillis
        dao.clock.is(testClock)
    }

    def 'should add new entry to collection'() {
        given:
        def counter = new AtomicInteger(0)
        def dao = createDao(Clocks.incClock(counter))

        when:
        dao.checkIn()

        then:
        counter.get() == 1
        MongoHelper.getCount('schedulers') == 1
        checkScheduler(dao, MongoHelper.getFirst('schedulers'), counter)
    }

    def 'should find scheduler instance'() {
        given:
        def dao = createDao()
        def id = 'id1'

        when:
        addEntry(id, 42l)

        then:
        dao.findInstance('xxx') == null

        when:
        def scheduler = dao.findInstance(id)

        then:
        scheduler.getName() == schedulerName
        scheduler.getCheckinInterval() == 100l
        scheduler.getInstanceId() == id
        scheduler.getLastCheckinTime() == 42l
    }

    def 'should update checkin time'() {
        given:
        def counter = new AtomicInteger(0)
        def dao = createDao(Clocks.incClock(counter))

        when:
        dao.checkIn()
        dao.checkIn()

        then:
        counter.get() == 2
        MongoHelper.getCount('schedulers') == 1
        checkScheduler(dao, MongoHelper.getFirst('schedulers'), 2)
    }

    def 'should update only own entry'() {
        given:
        def id1 = 'id1'
        def id2 = 'id2'
        def id1Counter = new AtomicInteger(0)
        def id2Counter = new AtomicInteger(100)
        def dao1 = createDao(id1, Clocks.incClock(id1Counter))
        def dao2 = createDao(id2, Clocks.incClock(id2Counter))

        when:
        dao1.checkIn()
        dao2.checkIn()

        then:
        MongoHelper.getCount('schedulers') == 2

        when:
        dao2.checkIn()

        then:
        id2Counter.get() == 102

        when:
        def entry1 = MongoHelper.getFirst('schedulers', [instanceId: id1])
        def entry2 = MongoHelper.getFirst('schedulers', [instanceId: id2])

        then:
        checkScheduler(dao1, entry1, id1, id1Counter.get())
        checkScheduler(dao2, entry2, id2, id2Counter.get())
    }

    def 'should remove selected entry'() {
        given:
        def id1 = 'id1'
        def id2 = 'id2'
        def id3 = 'id3'
        def dao = createDao(id1, testClock)

        when: 'Create entries for two scheduler instances'
        addEntry(id1, 1)
        addEntry(id2, 2)
        addEntry(id3, 3)

        then: 'Remove non-existing does nothing'
        !dao.remove('x-id', 1)
        MongoHelper.getCount('schedulers') == 3
        !dao.remove('id1', 4)
        MongoHelper.getCount('schedulers') == 3

        when: 'Remove the first one'
        def removed = dao.remove(id2, 2)

        then:
        removed
        MongoHelper.findAll('schedulers')*.get('instanceId') == ['id1', 'id3']

        when: 'Remove the second one'
        removed = dao.remove(id1, 1)

        then:
        removed
        MongoHelper.findAll('schedulers')*.get('instanceId') == ['id3']

        when: 'Remove the last one'
        removed = dao.remove(id3, 3)

        then:
        removed
        MongoHelper.getCount('schedulers') == 0
    }

    def 'should return empty list of entries'() {
        given:
        def dao = createDao()

        when:
        def schedulers = dao.getAllByCheckinTime()

        then:
        schedulers.isEmpty()
    }

    def 'should return entries in ascending ordered by last checkin time'() {
        given:
        addEntry('i1', 3)
        addEntry('i2', 1)
        addEntry('i3', 2)
        def dao = createDao()

        when:
        def schedulers = dao.getAllByCheckinTime()

        then:
        schedulers.size() == 3
        schedulers*.getName() == [schedulerName, schedulerName, schedulerName]
        schedulers*.getCheckinInterval() == [100l, 100l, 100l]
        schedulers*.getInstanceId() == ['i2', 'i3', 'i1']
        schedulers*.getLastCheckinTime() == [1, 2, 3]
    }

    def addEntry(String id, long checkinTime) {
        MongoHelper.addScheduler([
                (SchedulerDao.SCHEDULER_NAME_FIELD)   : schedulerName,
                (SchedulerDao.INSTANCE_ID_FIELD)      : id,
                (SchedulerDao.CHECKIN_INTERVAL_FIELD) : 100l,
                (SchedulerDao.LAST_CHECKIN_TIME_FIELD): checkinTime])
    }

    def createDao() {
        createDao(testClock)
    }

    def createDao(Clock clock) {
        createDao(instanceId, clock)
    }

    def createDao(String id, Clock clock) {
        def dao = new SchedulerDao(MongoHelper.getSchedulersColl(),
                schedulerName, id, clusterCheckinIntervalMillis, clock)
        dao.createIndex()
        dao
    }

    def void checkScheduler(dao, entry, expectedTimeMillis) {
        checkScheduler(dao, entry, instanceId, expectedTimeMillis)
    }

    def void checkScheduler(dao, entry, expectedId, expectedTimeMillis) {
        assert entry.get('schedulerName') == schedulerName
        assert entry.get('instanceId') == expectedId
        assert entry.get('checkinInterval') == dao.clusterCheckinIntervalMillis
        assert entry.get('lastCheckinTime') == expectedTimeMillis
    }
}