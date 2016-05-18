package com.novemberain.quartz.mongodb.trigger.properties;

import com.novemberain.quartz.mongodb.trigger.TriggerPropertiesConverter;
import org.bson.Document;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class CalendarIntervalTriggerPropertiesConverter extends TriggerPropertiesConverter {

    private static final String TRIGGER_REPEAT_INTERVAL_UNIT = "repeatIntervalUnit";
    private static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
    private static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";

    @Override
    protected boolean canHandle(OperableTrigger trigger) {
        return ((trigger instanceof CalendarIntervalTriggerImpl)
                && !((CalendarIntervalTriggerImpl) trigger).hasAdditionalProperties());
    }

    @Override
    public Document injectExtraPropertiesForInsert(OperableTrigger trigger, Document original) {
        CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl) trigger;

        return new Document(original)
                .append(TRIGGER_REPEAT_INTERVAL_UNIT, t.getRepeatIntervalUnit().name())
                .append(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval())
                .append(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered());
    }

    @Override
    public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Document stored) {
        CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl) trigger;

        String repeatIntervalUnit = stored.getString(TRIGGER_REPEAT_INTERVAL_UNIT);
        if (repeatIntervalUnit != null) {
            t.setRepeatIntervalUnit(IntervalUnit.valueOf(repeatIntervalUnit));
        }
        Integer repeatInterval = stored.getInteger(TRIGGER_REPEAT_INTERVAL);
        if (repeatInterval != null) {
            t.setRepeatInterval(repeatInterval);
        }
        Integer timesTriggered = stored.getInteger(TRIGGER_TIMES_TRIGGERED);
        if (timesTriggered != null) {
            t.setTimesTriggered(timesTriggered);
        }
    }
}
