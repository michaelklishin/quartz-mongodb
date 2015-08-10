package com.novemberain.quartz.mongodb;

import org.bson.Document;
import org.quartz.spi.OperableTrigger;

public interface TriggerPersistenceHelper {
  boolean canHandleTriggerType(OperableTrigger trigger);

  Document injectExtraPropertiesForInsert(OperableTrigger trigger, Document original);

  OperableTrigger setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Document stored);
}
