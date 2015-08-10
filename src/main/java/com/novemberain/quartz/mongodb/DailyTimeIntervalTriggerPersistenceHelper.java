package com.novemberain.quartz.mongodb;

import org.bson.Document;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.DailyTimeIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;
import org.quartz.DateBuilder;

public class DailyTimeIntervalTriggerPersistenceHelper implements TriggerPersistenceHelper {
  private static final String TRIGGER_REPEAT_INTERVAL_UNIT = "repeatIntervalUnit";
  private static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
  private static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";
  private static final String TRIGGER_START_TIME_OF_DAY = "startTimeOfDay";
  private static final String TRIGGER_END_TIME_OF_DAY = "endTimeOfDay";

  @Override
  public boolean canHandleTriggerType(OperableTrigger trigger) {
    return ((trigger instanceof DailyTimeIntervalTrigger) && !((DailyTimeIntervalTriggerImpl) trigger).hasAdditionalProperties());
  }

  @Override
  public Document injectExtraPropertiesForInsert(OperableTrigger trigger, Document original) {
    DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl) trigger;

    return new Document(original).
        append(TRIGGER_REPEAT_INTERVAL_UNIT, t.getRepeatIntervalUnit().name()).
        append(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval()).
        append(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered()).
        append(TRIGGER_START_TIME_OF_DAY, toDocument(t.getStartTimeOfDay())).
        append(TRIGGER_END_TIME_OF_DAY, toDocument(t.getEndTimeOfDay()));
  }

  private Document toDocument(TimeOfDay tod) {
    return new Document().
        append("hour", tod.getHour()).
        append("minute", tod.getMinute()).
        append("second", tod.getSecond());
  }

  @Override
  public OperableTrigger setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Document stored) {
    DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl) trigger;

    Object interval_unit = stored.get(TRIGGER_REPEAT_INTERVAL_UNIT);
    if (interval_unit != null) {
      t.setRepeatIntervalUnit(DateBuilder.IntervalUnit.valueOf((String) interval_unit));
    }
    Object repeatInterval = stored.get(TRIGGER_REPEAT_INTERVAL);
    if (repeatInterval != null) {
      t.setRepeatInterval((Integer) repeatInterval);
    }
    Object timesTriggered = stored.get(TRIGGER_TIMES_TRIGGERED);
    if (timesTriggered != null) {
      t.setTimesTriggered((Integer) timesTriggered);
    }

    Document startTOD = (Document) stored.get(TRIGGER_START_TIME_OF_DAY);
    if (startTOD != null) {
      t.setStartTimeOfDay(fromDocument(startTOD));
    }
    Document endTOD = (Document) stored.get(TRIGGER_END_TIME_OF_DAY);
    if (endTOD != null) {
      t.setEndTimeOfDay(fromDocument(endTOD));
    }

    return t;
  }

  private TimeOfDay fromDocument(Document endTOD) {
    return new TimeOfDay((Integer) endTOD.get("hour"), (Integer) endTOD.get("minute"), (Integer) endTOD.get("second"));
  }
}
