package com.novemberain.quartz.mongodb.util;

import com.novemberain.quartz.mongodb.TriggerPersistenceHelper;
import org.bson.Document;
import org.quartz.SimpleTrigger;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class SimpleTriggerPersistenceHelper implements TriggerPersistenceHelper {
  private static final String TRIGGER_REPEAT_COUNT = "repeatCount";
  private static final String TRIGGER_REPEAT_INTERVAL = "repeatInterval";
  private static final String TRIGGER_TIMES_TRIGGERED = "timesTriggered";

  @Override
  public boolean canHandleTriggerType(OperableTrigger trigger) {
    return ((trigger instanceof SimpleTriggerImpl) && !((SimpleTriggerImpl) trigger).hasAdditionalProperties());
  }

  @Override
  public Document injectExtraPropertiesForInsert(OperableTrigger trigger, Document original) {
    SimpleTrigger t = (SimpleTrigger) trigger;

    return new Document(original).
        append(TRIGGER_REPEAT_COUNT, t.getRepeatCount()).
        append(TRIGGER_REPEAT_INTERVAL, t.getRepeatInterval()).
        append(TRIGGER_TIMES_TRIGGERED, t.getTimesTriggered());
  }

  @Override
  public OperableTrigger setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Document stored) {
    SimpleTriggerImpl t = (SimpleTriggerImpl) trigger;

    Object repeatCount = stored.get(TRIGGER_REPEAT_COUNT);
    if (repeatCount != null) {
      t.setRepeatCount((Integer) repeatCount);
    }
    Object repeatInterval = stored.get(TRIGGER_REPEAT_INTERVAL);
    if (repeatInterval != null) {
      t.setRepeatInterval((Long) repeatInterval);
    }
    Object timesTriggered = stored.get(TRIGGER_TIMES_TRIGGERED);
    if (timesTriggered != null) {
      t.setTimesTriggered((Integer) timesTriggered);
    }

    return t;
  }
}
