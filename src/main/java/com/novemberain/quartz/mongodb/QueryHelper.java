package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.Collection;

import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;

public class QueryHelper {
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

  public BasicDBObject startsWithRegexDBObject(String compareToValue) {
    //TODO rewrite using Filters
    return (BasicDBObject) BasicDBObjectBuilder.start().append("$regex", "^" + compareToValue + ".*").get();
  }

  public BasicDBObject endsWithRegexDBObject(String compareToValue) {
    //TODO rewrite using Filters
    return (BasicDBObject) BasicDBObjectBuilder.start().append("$regex", ".*" + compareToValue + "$").get();
  }

  public BasicDBObject containsWithRegexDBObject(String compareToValue) {
    //TODO rewrite using Filters
    return (BasicDBObject) BasicDBObjectBuilder.start().append("$regex", compareToValue).get();
  }

  public Bson inGroups(Collection<String> groups) {
    return Filters.in(KEY_GROUP, groups);
  }
}
