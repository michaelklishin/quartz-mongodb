package com.novemberain.quartz.mongodb.dao

import com.novemberain.quartz.mongodb.Constants
import com.novemberain.quartz.mongodb.MongoHelper
import com.novemberain.quartz.mongodb.trigger.TriggerConverter
import com.novemberain.quartz.mongodb.util.QueryHelper
import org.quartz.TriggerKey
import spock.lang.Specification

class TriggerDaoTest extends Specification {

    def triggerDao = new TriggerDao(MongoHelper.gerTriggersColl(),
            new QueryHelper(),
            Mock(TriggerConverter))

    def setup() {
        triggerDao.createIndex()
        MongoHelper.purgeCollections()
    }

    def "should transfer trigger state if trigger is in specified state"() {
        given:
        def triggerKey = new TriggerKey("name", "default")
        insertSimpleTrigger(triggerKey)
        def currentState = Constants.STATE_WAITING
        def finalState = Constants.STATE_ERROR

        when:
        triggerDao.transferState(triggerKey, currentState, finalState)

        then:
        triggerDao.getState(triggerKey) == finalState
    }

    def "should not transfer state if trigger has different state already"() {
        given:
        def triggerKey = new TriggerKey("name", "default")
        insertSimpleTrigger(triggerKey)
        def originalState = Constants.STATE_WAITING

        when:
        triggerDao.transferState(triggerKey, Constants.STATE_PAUSED, Constants.STATE_ERROR)

        then:
        triggerDao.getState(triggerKey) == originalState
    }

    private def insertSimpleTrigger(TriggerKey key) {
        def data = [:]
        data.putAll([
                state   : 'waiting',
                keyName : key.name,
                keyGroup: key.group,
                class   : 'org.quartz.impl.triggers.SimpleTriggerImpl'
        ])
        MongoHelper.addTrigger(data)
    }
}
