package com.novemberain.quartz.mongodb.dao

import com.novemberain.quartz.mongodb.MongoHelper
import org.quartz.impl.calendar.DailyCalendar
import spock.lang.Specification

import static com.novemberain.quartz.mongodb.dao.CalendarDao.CALENDAR_NAME
import static com.novemberain.quartz.mongodb.dao.CalendarDao.CALENDAR_SERIALIZED_OBJECT

class CalendarDaoTest extends Specification {

    def dao = new CalendarDao(MongoHelper.getCalendarsColl())

    def setup() {
        dao.createIndex()
        MongoHelper.purgeCollections()
    }

    def 'should store given calendar'() {
        given:
        def name = 'my cal'
        def calendar = new DailyCalendar('10:15', '10:30')

        when:
        dao.store(name, calendar)
        def stored = MongoHelper.getFirst('calendars')

        then:
        stored != null
        stored.getString(CALENDAR_NAME) == name
        stored.get(CALENDAR_SERIALIZED_OBJECT) != null
    }

    def 'should return null when calendar name is null'() {
        expect:
        dao.retrieveCalendar(null) == null
    }

    def 'should return null when there is no such calendar'() {
        expect:
        dao.retrieveCalendar('aoeu') == null
    }

    def 'should retrieve stored calendar'() {
        given:
        def name = 'my cal'
        def calendar = new DailyCalendar('10:15', '10:30')

        when:
        dao.store(name, calendar)
        def stored = dao.retrieveCalendar(name)

        then:
        // This comparison is not fantastic, but better than nothing:
        calendar.toString() == stored.toString()
    }

}