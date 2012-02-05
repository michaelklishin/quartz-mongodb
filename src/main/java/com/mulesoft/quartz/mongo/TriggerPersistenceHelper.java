package com.mulesoft.quartz.mongo;

import com.mongodb.DBObject;
import org.quartz.spi.OperableTrigger;

public interface TriggerPersistenceHelper {
  public boolean canHandleTriggerType(OperableTrigger trigger);

  public DBObject injectExtraPropertiesForInsert(OperableTrigger trigger, DBObject original);
  public OperableTrigger setExtraPropertiesAfterInstantiation(OperableTrigger trigger, DBObject stored);
}
