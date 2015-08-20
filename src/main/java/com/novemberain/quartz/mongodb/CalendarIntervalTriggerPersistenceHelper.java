package com.novemberain.quartz.mongodb;

import org.bson.Document;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class CalendarIntervalTriggerPersistenceHelper implements TriggerPersistenceHelper {
  private static final String TRIGGER_REPEAT_INTERVAL_UNIT = "repeatIntervalUnit";
  private static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
  private static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";

  @Override
  public boolean canHandleTriggerType(OperableTrigger trigger) {
    return ((trigger instanceof CalendarIntervalTriggerImpl) && !((CalendarIntervalTriggerImpl) trigger).hasAdditionalProperties());
  }

  @Override
  public Document injectExtraPropertiesForInsert(OperableTrigger trigger, Document original) {
    CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl) trigger;

    return new Document(original).
        append(TRIGGER_REPEAT_INTERVAL_UNIT, t.getRepeatIntervalUnit().name()).
        append(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval()).
        append(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered());
  }

  @Override
  public OperableTrigger setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Document stored) {
    CalendarIntervalTriggerImpl t = (CalendarIntervalTriggerImpl) trigger;

    String repeatIntervalUnit = (String) stored.get(TRIGGER_REPEAT_INTERVAL_UNIT);
    if (repeatIntervalUnit != null) {
      t.setRepeatIntervalUnit(IntervalUnit.valueOf(repeatIntervalUnit));
    }
    Object repeatInterval = stored.get(TRIGGER_REPEAT_INTERVAL);
    if (repeatInterval != null) {
      t.setRepeatInterval((Integer) repeatInterval);
    }
    Object timesTriggered = stored.get(TRIGGER_TIMES_TRIGGERED);
    if (timesTriggered != null) {
      t.setTimesTriggered((Integer) timesTriggered);
    }

    return t;
  }
}
