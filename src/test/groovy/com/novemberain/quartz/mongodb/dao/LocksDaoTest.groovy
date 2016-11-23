package com.novemberain.quartz.mongodb.dao

import com.mongodb.MongoWriteException
import com.novemberain.quartz.mongodb.Clocks
import com.novemberain.quartz.mongodb.MongoHelper
import com.novemberain.quartz.mongodb.util.Clock
import com.novemberain.quartz.mongodb.util.Keys
import org.quartz.TriggerKey
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.atomic.AtomicInteger

class LocksDaoTest extends Specification {

    @Shared def instanceId = 'locksDaoTestId'
    def testClock = Clocks.incClock()

    def setup() {
        MongoHelper.purgeCollections()
    }

    def 'should have passed collection and instanceId'() {
        given:
        def locksCollection = MongoHelper.getLocksColl()
        def dao = createDao()

        expect:
        dao.instanceId == instanceId
        dao.collection.is(locksCollection)
    }

    def 'should lock trigger'() {
        given:
        def clock = Clocks.incClock()
        def dao = createDao(clock)
        def tkey = new TriggerKey('n1', 'g1')

        when:
        dao.lockTrigger(tkey)

        then:
        def locks = MongoHelper.findAll('locks')
        locks.size() == 1
        assertLock(locks.first(), instanceId, 1)
    }

    def 'should throw an exception when locking trigger again'() {
        // There should be no possibility to lock a trigger again.
        given:
        def dao = createDao()
        def triggerKey = new TriggerKey('n1', 'g1')

        when:
        dao.lockTrigger(triggerKey)

        then:
        notThrown(MongoWriteException)

        when:
        dao.lockTrigger(triggerKey)

        then:
        thrown(MongoWriteException)
        def locks = MongoHelper.findAll('locks')
        locks.size() == 1
        locks.first().get(Keys.KEY_NAME) == 'n1'
        locks.first().get(Keys.KEY_GROUP) == 'g1'
    }

    def 'should relock trigger when found'() {
        given:
        def counter = new AtomicInteger(0)
        def clock = Clocks.incClock(counter)
        def otherId = 'defunct scheduler'
        def triggerKey = new TriggerKey('n1', 'g1')
        def dao = createDao(clock, otherId)

        when: 'Lock using other scheduler in time "1"'
        dao.lockTrigger(triggerKey)

        then:
        assertOneLock(otherId, 1)
        counter.get() == 1

        when: 'Using the new scheduler relock lock with time "1"'
        dao = createDao(clock)
        def relocked = dao.relock(triggerKey, new Date(counter.get()))

        then:
        relocked

        and: 'Still one lock, but with updated owner and time'
        assertOneLock(instanceId, 2)
        counter.get() == 2
    }

    def 'should not relock when already changed'() {
        // Don't relock when other scheduler have already done that.
        given:
        def counter = new AtomicInteger(0)
        def clock = Clocks.incClock(counter)
        def tkey = new TriggerKey('n1', 'g1')
        def otherId = 'defunct scheduler'
        def otherDao = createDao(clock, otherId)

        when: 'Lock using other scheduler in time 1'
        otherDao.lockTrigger(tkey)

        then: 'Update the lock with new time 2'
        otherDao.relock(tkey, new Date(counter.get()))
        counter.get() == 2

        when: 'Using the new scheduler try to relock with old time 1'
        def relocked = createDao(clock).relock(tkey, new Date(counter.get()-1))

        then:
        !relocked

        and: 'Still one lock, updated be previous scheduler'
        assertOneLock(otherId, 2)
        counter.get() == 3
    }

    def createDao() {
        createDao(testClock)
    }

    def createDao(Clock clock) {
        createDao(clock, instanceId)
    }

    def createDao(Clock clock, String id) {
        def dao = new LocksDao(MongoHelper.getLocksColl(), clock, id)
        dao.createIndex(true)
        dao
    }

    def void assertLock(lock, instanceId, time) {
        assertLock(lock, 'n1', 'g1', instanceId, time)
    }

    def void assertLock(lock, key, group, instanceId, time) {
        assert lock.get(Keys.LOCK_TYPE) == Keys.LockType.t.name()
        assert lock.get(Keys.KEY_NAME) == key
        assert lock.get(Keys.KEY_GROUP) == group
        assert lock.get('instanceId') == instanceId
        assert lock.get('time').getTime() == time
    }

    def void assertOneLock(String instanceId, long time) {
        def locks = MongoHelper.findAll('locks')
        locks.size() == 1
        assertLock(locks.first(), instanceId, time)
    }
}
