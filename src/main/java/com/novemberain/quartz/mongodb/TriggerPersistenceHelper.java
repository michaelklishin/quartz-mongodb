package com.novemberain.quartz.mongodb;

import com.mongodb.DBObject;
import org.quartz.spi.OperableTrigger;

public interface TriggerPersistenceHelper {
  boolean canHandleTriggerType(OperableTrigger trigger);

  DBObject injectExtraPropertiesForInsert(OperableTrigger trigger, DBObject original);

  OperableTrigger setExtraPropertiesAfterInstantiation(OperableTrigger trigger, DBObject stored);
}
