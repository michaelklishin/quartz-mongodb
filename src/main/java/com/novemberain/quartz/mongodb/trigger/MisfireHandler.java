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

    /**
     * Return true when misfire have been applied and trigger has next fire time.
     *
     * @param trigger    on which apply misfire logic
     * @return true when result of misfire is next fire time
     */
    public boolean applyMisfireOnRecovery(OperableTrigger trigger) throws JobPersistenceException {
        if (trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
            return false;
        }

        Calendar cal = null;
        if (trigger.getCalendarName() != null) {
            cal = retrieveCalendar(trigger);
        }

        signaler.notifyTriggerListenersMisfired(trigger);

        trigger.updateAfterMisfire(cal);

        return trigger.getNextFireTime() != null;
    }

    public boolean applyMisfire(OperableTrigger trigger) throws JobPersistenceException {
        Date fireTime = trigger.getNextFireTime();
        if (misfireIsNotApplicable(trigger, fireTime)) {
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

    private boolean misfireIsNotApplicable(OperableTrigger trigger, Date fireTime) {
        return fireTime == null || isNotMisfired(fireTime)
                || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY;
    }

    private boolean isNotMisfired(Date fireTime) {
        return calculateMisfireTime() < fireTime.getTime();
    }

    private Calendar retrieveCalendar(OperableTrigger trigger) throws JobPersistenceException {
        return calendarDao.retrieveCalendar(trigger.getCalendarName());
    }
}
