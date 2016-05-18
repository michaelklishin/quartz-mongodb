package com.novemberain.quartz.mongodb.trigger.properties;

import com.novemberain.quartz.mongodb.trigger.TriggerPropertiesConverter;
import org.bson.Document;
import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;

import java.text.ParseException;
import java.util.TimeZone;

public class CronTriggerPropertiesConverter extends TriggerPropertiesConverter {

    private static final String TRIGGER_CRON_EXPRESSION = "cronExpression";
    private static final String TRIGGER_TIMEZONE = "timezone";

    @Override
    protected boolean canHandle(OperableTrigger trigger) {
        return ((trigger instanceof CronTriggerImpl)
                && !((CronTriggerImpl) trigger).hasAdditionalProperties());
    }

    @Override
    public Document injectExtraPropertiesForInsert(OperableTrigger trigger, Document original) {
        CronTrigger t = (CronTrigger) trigger;

        return new Document(original)
                .append(TRIGGER_CRON_EXPRESSION, t.getCronExpression())
                .append(TRIGGER_TIMEZONE, t.getTimeZone().getID());
    }

    @Override
    public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, Document stored) {
        CronTriggerImpl t = (CronTriggerImpl) trigger;

        String expression = stored.getString(TRIGGER_CRON_EXPRESSION);
        if (expression != null) {
            try {
                t.setCronExpression(new CronExpression(expression));
            } catch (ParseException e) {
                // no good handling strategy and
                // checked exceptions route sucks just as much.
            }
        }
        String tz = stored.getString(TRIGGER_TIMEZONE);
        if (tz != null) {
            t.setTimeZone(TimeZone.getTimeZone(tz));
        }
    }
}
