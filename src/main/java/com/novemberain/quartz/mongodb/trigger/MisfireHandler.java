package com.novemberain.quartz.mongodb.trigger;

import com.novemberain.quartz.mongodb.dao.CalendarDao;
import org.quartz.Calendar;
import org.quartz.JobPersistenceException;
import org.quartz.Trigger;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;

import java.util.Date;

/**
 * The responsibility of this class is to handle misfires.
 */
public class MisfireHandler {

    private final CalendarDao calendarDao;
    private final SchedulerSignaler signaler;
    private final long misfireThreshold;

    public MisfireHandler(CalendarDao calendarDao, SchedulerSignaler signaler, long misfireThreshold) {
        this.calendarDao = calendarDao;
        this.signaler = signaler;
        this.misfireThreshold = misfireThreshold;
    }

    public boolean applyMisfire(OperableTrigger trigger) throws JobPersistenceException {
        Date fireTime = trigger.getNextFireTime();
        if (isMisfireApplicable(trigger, fireTime)) {
            return false;
        }

        org.quartz.Calendar cal = retrieveCalendar(trigger);

        signaler.notifyTriggerListenersMisfired((OperableTrigger) trigger.clone());

        trigger.updateAfterMisfire(cal);

        if (trigger.getNextFireTime() == null) {
            signaler.notifySchedulerListenersFinalized(trigger);
        } else if (fireTime.equals(trigger.getNextFireTime())) {
            return false;
        }
        return true;
    }

    private long calculateMisfireTime() {
        long misfireTime = System.currentTimeMillis();
        if (misfireThreshold > 0) {
            misfireTime -= misfireThreshold;
        }
        return misfireTime;
    }

    private boolean isMisfireApplicable(OperableTrigger trigger, Date fireTime) {
        return fireTime == null || isNotMisfired(fireTime)
                || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY;
    }

    private boolean isNotMisfired(Date fireTime) {
        return calculateMisfireTime() < fireTime.getTime();
    }

    private Calendar retrieveCalendar(OperableTrigger trigger) {
        return calendarDao.retrieveCalendar(trigger.getCalendarName());
    }
}
