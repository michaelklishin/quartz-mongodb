package com.novemberain.quartz.mongodb;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.Collection;

import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;

public class QueryHelper {
  public DBObject matchingKeysConditionFor(GroupMatcher<?> matcher) {
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

  public DBObject inGroups(Collection<String> groups) {
    return QueryBuilder.start(KEY_GROUP).in(groups).get();
  }
}
