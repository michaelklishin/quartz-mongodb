package com.novemberain.quartz.mongodb.trigger;

import org.bson.Document;
import org.quartz.spi.OperableTrigger;

/**
 * Converts trigger type specific properties.
 */
public interface TriggerPropertiesConverter {

    boolean canHandleTriggerType(OperableTrigger trigger);

    Document injectExtraPropertiesForInsert(OperableTrigger trigger, Document original);

    OperableTrigger setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Document stored);
}
