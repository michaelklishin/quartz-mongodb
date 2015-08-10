package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.Collection;
import java.util.Date;

import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;

public class QueryHelper {

  public BasicDBObject createNextTriggerQuery(Date noLaterThanDate) {
    BasicDBObject query = new BasicDBObject();
    query.put(Constants.TRIGGER_NEXT_FIRE_TIME, new BasicDBObject("$lte", noLaterThanDate));
    query.put(Constants.TRIGGER_STATE, Constants.STATE_WAITING);
    return query;
  }

  public BasicDBObject matchingKeysConditionFor(GroupMatcher<?> matcher) {
    BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

    final String compareToValue = matcher.getCompareToValue();
    switch (matcher.getCompareWithOperator()) {
      case EQUALS:
        builder.append(KEY_GROUP, compareToValue);
        break;
      case STARTS_WITH:
        builder.append(KEY_GROUP, startsWithRegexDBObject(compareToValue));
        break;
      case ENDS_WITH:
        builder.append(KEY_GROUP, endsWithRegexDBObject(compareToValue));
      case CONTAINS:
        builder.append(KEY_GROUP, containsWithRegexDBObject(compareToValue));
        break;
    }

    return (BasicDBObject) builder.get();
  }

  private BasicDBObject startsWithRegexDBObject(String compareToValue) {
    //TODO rewrite using Filters
    return (BasicDBObject) BasicDBObjectBuilder.start().append("$regex", "^" + compareToValue + ".*").get();
  }

  private BasicDBObject endsWithRegexDBObject(String compareToValue) {
    //TODO rewrite using Filters
    return (BasicDBObject) BasicDBObjectBuilder.start().append("$regex", ".*" + compareToValue + "$").get();
  }

  private BasicDBObject containsWithRegexDBObject(String compareToValue) {
    //TODO rewrite using Filters
    return (BasicDBObject) BasicDBObjectBuilder.start().append("$regex", compareToValue).get();
  }

  public Bson inGroups(Collection<String> groups) {
    return Filters.in(KEY_GROUP, groups);
  }
}
