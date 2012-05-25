package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;

public class QueryHelper {
  public DBObject matchingKeysConditionFor(GroupMatcher matcher) {
    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

    final String compareToValue = matcher.getCompareToValue();
    switch (matcher.getCompareWithOperator()) {
      case EQUALS:
        builder.append("keyGroup", compareToValue);
        break;
      case STARTS_WITH:
        builder.append("keyGroup", startsWithRegexDBObject(compareToValue));
        break;
      case ENDS_WITH:
        builder.append("keyGroup", endsWithRegexDBObject(compareToValue));
      case CONTAINS:
        builder.append("keyGroup", containsWithRegexDBObject(compareToValue));
        break;
    }

    return builder.get();
  }

  public DBObject startsWithRegexDBObject(String compareToValue) {
    return BasicDBObjectBuilder.start().append("$regex", "^" + compareToValue + ".*").get();
  }

  public DBObject endsWithRegexDBObject(String compareToValue) {
    return BasicDBObjectBuilder.start().append("$regex", ".*" + compareToValue + "$").get();
  }

  public DBObject containsWithRegexDBObject(String compareToValue) {
    return BasicDBObjectBuilder.start().append("$regex", compareToValue).get();
  }

  public TriggerKey triggerKeyFromDBObject(DBObject dbo) {
    return new TriggerKey((String) dbo.get("keyName"), (String) dbo.get("keyGroup"));
  }

  public JobKey jobKeyFromDBObject(DBObject dbo) {
    return new JobKey((String) dbo.get("keyName"), (String) dbo.get("keyGroup"));
  }
}
