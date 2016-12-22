package com.novemberain.quartz.mongodb.trigger

import com.novemberain.quartz.mongodb.Constants
import com.novemberain.quartz.mongodb.JobDataConverter
import com.novemberain.quartz.mongodb.dao.JobDao
import com.novemberain.quartz.mongodb.dao.TriggerDao
import com.novemberain.quartz.mongodb.util.Keys
import org.bson.Document
import org.quartz.TriggerKey
import org.quartz.impl.triggers.SimpleTriggerImpl
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject


class TriggerConverterTest extends Specification {

    @Shared def jobId = '57a6d36dee7825134cf47308'

    def jobDao = Mock(JobDao)
    def triggerDao = Mock(TriggerDao)

    @Subject
    def converter = new TriggerConverter(jobDao, new JobDataConverter(true))

    def 'should return null when job of trigger is gone'() {
        given:
        def doc = createTriggerDoc()
        1 * jobDao.getById(jobId) >> null

        when:
        def trigger = converter.toTrigger(doc)

        then:
        trigger == null

    }

    def 'should convert document to trigger'() {
        given:
        def jobDoc = new Document(Keys.KEY_NAME, 'job key')
                .append(Keys.KEY_GROUP, 'job group')
        def triggerDoc = createTriggerDoc()
        1 * jobDao.getById(jobId) >> jobDoc

        when:
        def trigger = converter.toTrigger(triggerDoc) as SimpleTriggerImpl

        then:
        trigger != null
        trigger.validate()
        trigger.getKey().getName() == 'tkey'
        trigger.getKey().getGroup() == 'tgroup'
        trigger.getDescription() == 'trigger desc'
        trigger.getFireInstanceId() == 'fire instance id'
        trigger.getJobKey().getName() == 'job key'
        trigger.getJobKey().getGroup() == 'job group'
        trigger.getStartTime().getTime() == 10
        trigger.getEndTime().getTime() == 11
        trigger.getPreviousFireTime().getTime() == 1
        trigger.getNextFireTime().getTime() == 2
        //trigger.getFinalFireTime().getTime() == 3
        trigger.getMisfireInstruction() == 0
        trigger.getPriority() == 5
        trigger.getCalendarName() == 'test calendar'
        trigger.getRepeatCount() == 10
        trigger.getRepeatInterval() == 400l
        trigger.getTimesTriggered() == 3
        trigger.getJobDataMap().size() == 1
        trigger.getJobDataMap().getString('trg param') == 'my message'
    }

    def createTriggerDoc() {
        new Document()
                .append('_id', '57a6d36dee7825134cf47309')
                .append('state', 'waiting')
                .append('calendarName', 'test calendar')
                .append('class', 'org.quartz.impl.triggers.SimpleTriggerImpl')
                .append('description', 'trigger desc')
                .append('fireInstanceId', 'fire instance id')
                .append('jobId', jobId)
                .append('keyName', 'tkey')
                .append('keyGroup', 'tgroup')
                .append('misfireInstruction', 0)
                .append('startTime', new Date(10))
                .append('endTime', new Date(11))
                .append('previousFireTime', new Date(1))
                .append('nextFireTime', new Date(2))
                .append('finalFireTime', new Date(3))
                .append('jobData', 'rO0ABXNyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhwP0AAAAAAAAx3CAAAABAAAAABdAAJdHJnIHBhcmFtdAAKbXkgbWVzc2FnZXg=')
                .append('priority', 5)
                .append('repeatCount', 10)
                .append('repeatInterval', 400l)
                .append('timesTriggered', 3)
    }
}