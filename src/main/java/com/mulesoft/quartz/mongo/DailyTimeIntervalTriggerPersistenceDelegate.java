package com.mulesoft.quartz.mongo;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.DailyTimeIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class DailyTimeIntervalTriggerPersistenceDelegate implements TriggerPersistenceDelegate {
  private static final String TRIGGER_REPEAT_INTERVAL_UNIT = "repeatIntervalUnit";
  private static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
  private static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";
  private static final String TRIGGER_START_TIME_OF_DAY = "startTimeOfDay";
  private static final String TRIGGER_END_TIME_OF_DAY = "endTimeOfDay";  

  @Override
  public boolean canHandleTriggerType(OperableTrigger trigger) {
    return ((trigger instanceof DailyTimeIntervalTrigger) && !((DailyTimeIntervalTriggerImpl)trigger).hasAdditionalProperties());
  }

  @Override
  public DBObject injectExtraPropertiesForInsert(OperableTrigger trigger, DBObject original) {
    DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl)trigger;

    return BasicDBObjectBuilder.start(original.toMap()).
        append(TRIGGER_REPEAT_INTERVAL_UNIT, t.getRepeatIntervalUnit().name()).
        append(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval()).
        append(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered()).
        append(TRIGGER_START_TIME_OF_DAY, toDBObject(t.getStartTimeOfDay())).
        append(TRIGGER_END_TIME_OF_DAY, toDBObject(t.getEndTimeOfDay())).
        get();
  }

  private DBObject toDBObject(TimeOfDay tod) {
    return BasicDBObjectBuilder.start().
        append("hour", tod.getHour()).
        append("minute", tod.getMinute()).
        append("second", tod.getSecond()).
        get();
  }

  @Override
  public OperableTrigger setExtraPropertiesAfterInstantiation(OperableTrigger trigger, DBObject stored) {
    DailyTimeIntervalTriggerImpl t = (DailyTimeIntervalTriggerImpl)trigger;

    Object interval_unit = stored.get(TRIGGER_REPEAT_INTERVAL_UNIT);
    if (interval_unit != null) {
      t.setRepeatCount((Integer)interval_unit);
    }
    Object repeatInterval = stored.get(TRIGGER_REPEAT_INTERVAL);
    if (repeatInterval != null) {
      t.setRepeatInterval((Integer)repeatInterval);
    }
    Object timesTriggered = stored.get(TRIGGER_TIMES_TRIGGERED);
    if (timesTriggered != null) {
      t.setTimesTriggered((Integer)timesTriggered);
    }

    DBObject startTOD = (DBObject)stored.get(TRIGGER_START_TIME_OF_DAY);
    if (startTOD != null) {
      t.setStartTimeOfDay(fromDBObject(startTOD));
    }
    DBObject endTOD = (DBObject)stored.get(TRIGGER_END_TIME_OF_DAY);
    if (endTOD != null) {
      t.setEndTimeOfDay(fromDBObject(endTOD));
    }

    return t;
  }

  private TimeOfDay fromDBObject(DBObject endTOD) {
    return new TimeOfDay((Integer)endTOD.get("hour"), (Integer)endTOD.get("minute"), (Integer)endTOD.get("second"));
  }
}
