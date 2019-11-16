package com.novemberain.quartz.mongodb

import com.novemberain.quartz.mongodb.dao.JobDao
import com.novemberain.quartz.mongodb.dao.PausedJobGroupsDao
import com.novemberain.quartz.mongodb.dao.PausedTriggerGroupsDao
import com.novemberain.quartz.mongodb.dao.TriggerDao
import com.novemberain.quartz.mongodb.util.QueryHelper
import org.quartz.TriggerKey
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

class TriggerStateManagerTest extends Specification {

    def triggerDao = Mock(TriggerDao)
    def jobDao = Mock(JobDao)
    def pausedJobGroupsDao = Mock(PausedJobGroupsDao)
    def pausedTriggerGroupsDao = Mock(PausedTriggerGroupsDao)
    def queryHelper = new QueryHelper()

    @Shared
    def triggerKey = new TriggerKey("t-name", "t-group")

    @Subject
    def stateManager = new TriggerStateManager(triggerDao, jobDao, pausedJobGroupsDao,
            pausedTriggerGroupsDao, queryHelper)

    def 'resetTriggerFromErrorState should do nothing  when trigger is not in error state'() {
        given:
        triggerDao.getState(triggerKey) >> Constants.STATE_WAITING

        when:
        stateManager.resetTriggerFromErrorState(triggerKey)

        then:
        0 * pausedTriggerGroupsDao._
        0 * triggerDao.transferState(_, _, _)
        0 * triggerDao.setState(_, _)
    }

    @Unroll
    def 'resetTriggerFromErrorState should set #targetStated state when trigger is in error state'() {
        given:
        triggerDao.getState(triggerKey) >> Constants.STATE_ERROR

        when:
        stateManager.resetTriggerFromErrorState(triggerKey)

        then:
        1 * pausedTriggerGroupsDao.getPausedGroups() >> pausedGroups

        and:
        1 * triggerDao.transferState(triggerKey, Constants.STATE_ERROR, targetState)

        where:
        pausedGroups             || targetState
        ['g1', 'g2']             || Constants.STATE_WAITING
        ['g1', triggerKey.group] || Constants.STATE_PAUSED
    }
}
